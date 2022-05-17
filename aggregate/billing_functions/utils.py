# pyliny: disable=global-statement
"""
Class of helper functions for billing aggregate functions
"""
import os
import json
import aiohttp
import logging
import pandas as pd
import google.cloud.bigquery as bq

from io import StringIO
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta
from google.cloud import secretmanager
from google.api_core.exceptions import ClientError
from typing import Dict, List, Any, Iterator, Optional, Sequence, Tuple, TypeVar

logging.basicConfig(level=logging.INFO)

# fix this later to be a proper configured logger
logger = logging

GCP_BILLING_BQ_TABLE = (
    'billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343'
)
GCP_AGGREGATE_DEST_TABLE = os.getenv('GCP_AGGREGATE_DEST_TABLE')
logging.info('GCP_AGGREGATE_DEST_TABLE: {}'.format(GCP_AGGREGATE_DEST_TABLE))
assert GCP_AGGREGATE_DEST_TABLE

# 10% overhead
HAIL_SERVICE_FEE = 0.05
ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'
DEFAULT_RANGE_INTERVAL = timedelta(days=2)


HAIL_BASE = 'https://batch.hail.populationgenomics.org.au'
HAIL_UI_URL = HAIL_BASE + '/batches/{batch_id}'
HAIL_BATCHES_API = HAIL_BASE + '/api/v1alpha/batches'
HAIL_JOBS_API = HAIL_BASE + '/api/v1alpha/batches/{batch_id}/jobs/resources'


bigquery_client = bq.Client()
# pyliny: disable=invalid-name
T = TypeVar('T')


def chunk(iterable: Sequence[T], chunk_size=50) -> Iterator[Sequence[T]]:
    """
    Chunk a sequence by yielding lists of `chunk_size`
    """
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i : i + chunk_size]


def get_bq_schema_json():
    """Get the schema for the table"""
    pwd = Path(__file__).parent.parent.resolve()
    schema_path = pwd / 'schema' / 'aggregate_schema.json'
    with open(schema_path, 'r') as f:
        return json.load(f)


def _format_bq_schema_json(schema: Dict[str, Any]):
    """
    Take bq json schema, and convert it to bq.SchemaField objects"""
    formatted_schema = []
    for row in schema:
        kwargs = {
            'name': row['name'],
            'field_type': row['type'],
            'mode': row['mode'],
        }

        if 'fields' in row and row['fields']:
            kwargs['fields'] = _format_bq_schema_json(row['fields'])
        formatted_schema.append(bq.SchemaField(**kwargs))
    return formatted_schema


def get_formatted_bq_schema() -> List[bq.SchemaField]:
    """
    Get schema for bigquery billing table, as a list of bq.SchemaField objects
    """
    return _format_bq_schema_json(get_bq_schema_json())


def parse_hail_time(time_str: str) -> datetime:
    """Parse hail datetime object"""
    return datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%SZ')


def to_bq_time(time: datetime):
    """Convert datetime to transport datetime for bigquery"""
    return time.strftime('%Y-%m-%d %H:%M:%S')


def get_hail_token():
    """
    Get Hail token from local tokens file
    TODO: look at env var for deploy
    """
    with open(os.path.expanduser('~/.hail/tokens.json')) as f:
        config = json.load(f)
        return config['default']


async def get_batches(billing_project: str, last_batch_id: any, token: str):
    """
    Get list of batches for a billing project with no filtering.
    (Optional): from last_batch_id
    """
    q = f'?q=billing_project:{billing_project}'
    if last_batch_id:
        q += f'&last_batch_id={last_batch_id}'

    async with aiohttp.ClientSession() as session:
        async with session.get(
            HAIL_BATCHES_API + q, headers={'Authorization': 'Bearer ' + token}
        ) as resp:
            resp.raise_for_status()

            return await resp.json()


async def get_finished_batches_for_date(
    billing_project: str, start_day: datetime, end_day: datetime, token: str
):
    """
    Get all the batches that started on {date} and are complete.
    We assume that batches are ordered by start time, so we can stop
    when we find a batch that started before the date.
    """
    batches = []
    last_batch_id = None
    n_requests = 0
    skipped = 0

    while True:

        n_requests += 1
        jresponse = await get_batches(billing_project, last_batch_id, token)

        if 'last_batch_id' in jresponse and jresponse['last_batch_id'] == last_batch_id:
            raise ValueError(
                f'Something weird is happening with last_batch_job: {last_batch_id}'
            )
        last_batch_id = jresponse.get('last_batch_id')
        if not jresponse.get('batches'):
            return batches
        for b in jresponse['batches']:
            if not b['time_completed'] or not b['complete']:
                skipped += 1
                continue

            time_completed = parse_hail_time(b['time_completed'])
            in_date_range = start_day <= time_completed < end_day

            if time_completed < start_day:
                logging.info(
                    f'{billing_project} :: Got {len(batches)} batches '
                    f'in {n_requests} requests, skipping {skipped}'
                )
                return batches
            if in_date_range:
                batches.append(b)
            else:
                skipped += 1


async def get_jobs_for_batch(batch_id, token: str) -> List[str]:
    """
    For a single batch, fill in the 'jobs' field.
    """
    jobs = []
    last_job_id = None
    end = False
    iterations = 0
    async with aiohttp.ClientSession() as client:
        while not end:
            iterations += 1

            if iterations > 1 and iterations % 5 == 0:
                logging.info(f'On {iterations} iteration to load jobs for {batch_id}')

            q = '?limit=9999'
            if last_job_id:
                q += f'&last_job_id={last_job_id}'
            url = HAIL_JOBS_API.format(batch_id=batch_id) + q
            async with client.get(
                url, headers={'Authorization': 'Bearer ' + token}
            ) as response:
                response.raise_for_status()

                jresponse = await response.json()
                new_last_job_id = jresponse.get('last_job_id')
                if new_last_job_id is None:
                    end = True
                elif last_job_id and new_last_job_id <= last_job_id:
                    raise ValueError('Something fishy with last job id')
                last_job_id = new_last_job_id
                jobs.extend(jresponse['jobs'])

    return jobs


def insert_new_rows_in_table(table: str, obj: List[Dict[str, Any]]) -> int:
    """Insert JSON rows into a BQ table"""

    _query = f"""
        SELECT id FROM `{table}`
        WHERE id IN UNNEST(@ids);
    """

    ids = set(o['id'] for o in obj)
    if len(ids) != len(obj):
        counter = defaultdict(int)
        for o in obj:
            counter[o['id']] += 1
        duplicates = [k for k, v in counter.items() if v > 1]
        raise ValueError(
            'There are multiple rows with the same id: ' + ', '.join(duplicates)
        )

    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ArrayQueryParameter('ids', 'STRING', list(ids)),
        ]
    )

    result = bigquery_client.query(_query, job_config=job_config).result()
    existing_ids = set(result.to_dataframe()['id'])

    # Filter out any rows that are already in the table
    filtered_obj = [o for o in obj if o['id'] not in existing_ids]

    nrows = len(filtered_obj)

    if nrows == 0:
        logging.info('Not inserting any rows')
        return 0

    # Count number of rows adding
    logging.info(f'Inserting {nrows}/{len(obj)} rows')

    # Insert the new rows
    job_config = bq.LoadJobConfig()
    job_config.source_format = bq.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = get_formatted_bq_schema()

    j = '\n'.join(json.dumps(o) for o in filtered_obj)

    resp = bigquery_client.load_table_from_file(
        StringIO(j), table, job_config=job_config
    )
    try:
        result = resp.result()
        logging.info(f'Inserted {result.output_rows}/{nrows} rows')
    except ClientError as e:
        logging.error(resp.errors)
        raise e

    return nrows


def insert_dataframe_rows_in_table(table: str, df: pd.DataFrame):
    """Insert new rows from dataframe into a table"""

    _query = f"""
        SELECT id FROM `{table}`
        WHERE id IN UNNEST(@ids);
    """
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ArrayQueryParameter('ids', 'STRING', list(set(df['id']))),
        ]
    )
    result = bigquery_client.query(_query, job_config=job_config).result()
    existing_ids = set(result.to_dataframe()['id'])

    # Filter out any rows that are already in the table
    df = df[~df['id'].isin(existing_ids)]

    # Count number of rows adding
    adding_rows = len(df)

    # Insert the new rows
    project_id = table.split('.')[0]
    table_schema = get_bq_schema_json()

    df.to_gbq(
        table,
        project_id=project_id,
        table_schema=table_schema,
        if_exists='append',
    )

    logging.info(f'{adding_rows} new rows inserted')
    return adding_rows


CACHED_CURRENCY_CONVERSION: Dict[str, float] = {}


def get_currency_conversion_rate_for_time(time: datetime):
    """
    Get the currency conversion rate for a given time.
    Noting that GCP conversion rates are decided at the start of the month,
    and apply to each job that starts within the month, regardless of when
    the job finishes.
    """
    global CACHED_CURRENCY_CONVERSION

    key = f'{time.year}-{time.month}'
    if key not in CACHED_CURRENCY_CONVERSION:
        logging.info(f'Looking up currency conversion rate for {key}')
        query = f"""
            SELECT currency_conversion_rate
            FROM {GCP_BILLING_BQ_TABLE}
            WHERE DATE(_PARTITIONTIME) = DATE('{time.date()}')
            LIMIT 1
        """
        for r in bigquery_client.query(query).result():
            CACHED_CURRENCY_CONVERSION[key] = r['currency_conversion_rate']

    return CACHED_CURRENCY_CONVERSION[key]


def _generate_hail_resource_cost_lookup():
    """
    Generate the cost table for the hail resources.
    This is currently set to the australia-southeast-1 region.
    """
    # pylint: disable=import-outside-toplevel
    from hailtop.utils import (
        rate_gib_hour_to_mib_msec,
        rate_gib_month_to_mib_msec,
        rate_cpu_hour_to_mcpu_msec,
        rate_instance_hour_to_fraction_msec,
    )

    rates = [
        # https://cloud.google.com/compute/vm-instance-pricing#:~:text=N1%20custom%20vCPUs,that%20machine%20type.
        ('compute/n1-preemptible/1', rate_cpu_hour_to_mcpu_msec(0.00898)),
        ('compute/n1-nonpreemptible/1', rate_cpu_hour_to_mcpu_msec(0.04488)),
        ('memory/n1-preemptible/1', rate_gib_hour_to_mib_msec(0.00120)),
        ('memory/n1-nonpreemptible/1', rate_gib_hour_to_mib_msec(0.00601)),
        # https://cloud.google.com/compute/disks-image-pricing#persistentdisk
        ('boot-disk/pd-ssd/1', rate_gib_month_to_mib_msec(0.23)),
        ('disk/pd-ssd/1', rate_gib_month_to_mib_msec(0.23)),
        # https://cloud.google.com/compute/disks-image-pricing#localssdpricing
        ('disk/local-ssd/preemptible/1', rate_gib_month_to_mib_msec(0.065)),
        ('disk/local-ssd/nonpreemptible/1', rate_gib_month_to_mib_msec(0.108)),
        # legacy local-ssd
        ('disk/local-ssd/1', rate_gib_month_to_mib_msec(0.108)),
        # https://cloud.google.com/vpc/network-pricing#:~:text=internal%20IP%20addresses.-,External%20IP%20address%20pricing,to%0Athe%20following%20table.,-If%20you%20reserve
        ('ip-fee/1024/1', rate_instance_hour_to_fraction_msec(0.004, 1024)),
        # custom Hail Batch service fee?
        ('service-fee/1', rate_cpu_hour_to_mcpu_msec(0.01)),
    ]
    s = json.dumps(dict(rates))
    print(s)
    return s


AUSTRALIA_SOUTHEAST_1_COST = {
    'compute/n1-preemptible/1': 2.4944444444444447e-12,
    'compute/n1-nonpreemptible/1': 1.2466666666666668e-11,
    'memory/n1-preemptible/1': 3.255208333333333e-13,
    'memory/n1-nonpreemptible/1': 1.6303168402777778e-12,
    'boot-disk/pd-ssd/1': 8.540929918624991e-14,
    'disk/pd-ssd/1': 8.540929918624991e-14,
    'disk/local-ssd/preemptible/1': 2.4137410639592365e-14,
    'disk/local-ssd/nonpreemptible/1': 4.010523613963039e-14,
    'disk/local-ssd/1': 4.010523613963039e-14,
    'ip-fee/1024/1': 1.0850694444444444e-12,
    'service-fee/1': 2.777777777777778e-12,
}


def get_usd_cost_for_resource(batch_resource, usage, region='australia-southeast-1'):
    """
    Get the cost of a resource in USD.
    """
    regions = {
        'australia-southeast-1': AUSTRALIA_SOUTHEAST_1_COST,
    }

    return regions[region][batch_resource] * usage


def get_unit_for_batch_resource_type(batch_resource_type: str) -> str:
    """
    Get the relevant unit for some hail batch resource type
    """
    return {
        'boot-disk/pd-ssd/1': 'mib * msec',
        'disk/local-ssd/preemptible/1': 'mib * msec',
        'disk/local-ssd/nonpreemptible/1': 'mib * msec',
        'disk/local-ssd/1': 'mib * msec',
        'disk/pd-ssd/1': 'mb * msec',
        'compute/n1-nonpreemptible/1': 'mcpu * msec',
        'compute/n1-preemptible/1': 'mcpu * msec',
        'ip-fee/1024/1': 'IP * msec',
        'memory/n1-nonpreemptible/1': 'mib * msec',
        'memory/n1-preemptible/1': 'mib * msec',
        'service-fee/1': '$/msec',
    }.get(batch_resource_type, batch_resource_type)


def get_start_and_end_from_request(
    request,
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Get the start and end times from the cloud function request.
    """
    if request:
        return request.params['start'], request.params['end']
    return (None, None)


def date_range_iterator(
    start,
    end,
    intv=DEFAULT_RANGE_INTERVAL,
) -> Iterator[Tuple[datetime, datetime]]:
    """
    Iterate over a range of dates.
    """
    dt_from = start
    dt_to = start + intv
    while dt_to < end:
        dt_to = min(dt_to, end)
        yield (dt_from, dt_to)
        dt_from += intv
        dt_to += intv


def get_start_and_end_from_data(data) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Get the start and end times from the cloud function data.
    """
    if data:
        dates = dict()
        if data.get('attributes'):
            dates = data.get('attributes', {})
        elif data.get('message'):
            dates = dict(json.loads(data['message'])) or {}

        logging.info(f'data: {data}, dates: {dates}')

        start = dates.get('start', '')
        end = dates.get('end', '')

        if start and end:
            return datetime.fromisoformat(start), datetime.fromisoformat(end)

        return start, end
    return (None, None)


def process_default_start_and_end(start, end) -> Iterator[Tuple[datetime, datetime]]:
    """Process start and end times (and get defaults)"""
    if not start:
        start = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=2)
    if not end:
        end = datetime.now()

    assert isinstance(start, datetime) and isinstance(end, datetime)

    return date_range_iterator(start, end)


def read_secret(project_id: str, secret_name: str) -> Optional[str]:
    """Reads the latest version of a GCP Secret Manager secret.

    Returns None if the secret doesn't exist."""

    secret_manager = secretmanager.SecretManagerServiceClient()
    secret_path = secret_manager.secret_path(project_id, secret_name)

    response = secret_manager.access_secret_version(
        request={'name': f'{secret_path}/versions/latest'}
    )
    return response.payload.data.decode('UTF-8')
