# pylint: disable=global-statement,too-many-arguments,line-too-long
"""
Class of helper functions for billing aggregate functions
"""
import os
import re
import math
import json
import logging
from pathlib import Path
from io import StringIO
from collections import defaultdict
from datetime import datetime, timedelta
import sys
from typing import Any, Iterator, Sequence, TypeVar

import asyncio
import aiohttp
import pandas as pd
import google.cloud.bigquery as bq

from google.cloud import secretmanager
from google.api_core.exceptions import ClientError


logger = logging.getLogger('Cost Aggregate')

handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))

logger.addHandler(handler)
if os.getenv('DEV') in ('1', 'true', 'yes'):
    logger.setLevel(logging.DEBUG)

# pylint: disable=invalid-name
T = TypeVar('T')

DEFAULT_TOPIC = 'admin'

GCP_PROJECT = 'billing-admin-290403'

GCP_BILLING_BQ_TABLE = (
    f'{GCP_PROJECT}.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343'
)
GCP_AGGREGATE_DEST_TABLE = os.getenv(
    'GCP_AGGREGATE_DEST_TABLE', 'billing-admin-290403.billing_aggregate.aggregate'
)

assert GCP_AGGREGATE_DEST_TABLE
logger.info(f'GCP_AGGREGATE_DEST_TABLE: {GCP_AGGREGATE_DEST_TABLE}')

IS_PRODUCTION = os.getenv('PRODUCTION') in ('1', 'true', 'yes')

# 10% overhead
HAIL_SERVICE_FEE = 0.05
DEFAULT_BQ_CHUNK_SIZE = 9000
ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'
DEFAULT_RANGE_INTERVAL = timedelta(days=2)


HAIL_BASE = 'https://batch.hail.populationgenomics.org.au'
HAIL_UI_URL = HAIL_BASE + '/batches/{batch_id}'
HAIL_BATCHES_API = HAIL_BASE + '/api/v1alpha/batches'
HAIL_JOBS_API = HAIL_BASE + '/api/v1alpha/batches/{batch_id}/jobs/resources'

HAIL_PROJECT_FIELD = {
    'id': 'hail-295901',
    'number': '805950571114',
    'name': 'hail-295901',
    'labels': [],
    'ancestry_numbers': '/648561325637/',
    'ancestors': [
        {
            'resource_name': 'projects/805950571114',
            'display_name': 'hail-295901',
        },
        {
            'resource_name': 'organizations/648561325637',
            'display_name': 'populationgenomics.org.au',
        },
    ],
}

SEQR_PROJECT_FIELD = {
    'id': 'seqr-308602',
    'number': '1021400127367',
    'name': 'seqr-308602',
    'labels': [],
    'ancestry_numbers': '/648561325637/',
    'ancestors': [
        {
            'resource_name': 'organizations/648561325637',
            'display_name': 'populationgenomics.org.au',
        }
    ],
}


_BQ_CLIENT: bq.Client = None


def get_bigquery_client():
    """Get instantiated cached bq client"""
    global _BQ_CLIENT
    if not _BQ_CLIENT:
        _BQ_CLIENT = bq.Client()
    return _BQ_CLIENT


async def async_retry_transient_get_json_request(
    url,
    errors: Exception | tuple[Exception, ...],
    *args,
    attempts=5,
    session=None,
    **kwargs,
):
    """
    Retry a function with exponential backoff.
    """

    async def inner_block(session):
        for attempt in range(1, attempts + 1):
            try:
                async with session.get(url, *args, **kwargs) as resp:
                    resp.raise_for_status()
                    j = await resp.json()
                    return j
            # pylint: disable=broad-except
            except Exception as e:
                if not isinstance(e, errors):
                    raise
                if attempt == attempts:
                    raise

            t = 2**attempt
            logger.warning(f'Backing off {t} seconds for {url}')
            asyncio.sleep(t)

    if session:
        return await inner_block(session)

    async with aiohttp.ClientSession() as session2:
        return await inner_block(session2)


def chunk(iterable: Sequence[T], chunk_size) -> Iterator[Sequence[T]]:
    """
    Chunk a sequence by yielding lists of `chunk_size`
    """
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i : i + chunk_size]


def get_bq_schema_json() -> dict[str, any]:
    """Get the schema for the table"""
    pwd = Path(__file__).parent.parent.resolve()
    schema_path = pwd / 'schema' / 'aggregate_schema.json'
    with open(schema_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _format_bq_schema_json(schema: dict[str, Any]):
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


def get_formatted_bq_schema() -> list[bq.SchemaField]:
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


def get_hail_token() -> str:
    """
    Get Hail token from local tokens file
    TODO: look at env var for deploy
    """
    if os.getenv('DEV') in ('1', 'true', 'yes'):
        with open(os.path.expanduser('~/.hail/tokens.json'), encoding='utf-8') as f:
            config = json.load(f)
            return config['default']

    assert GCP_PROJECT
    return read_secret(GCP_PROJECT, 'aggregate-billing-hail-token')


def get_credits(
    entries: list[dict[str, Any]],
    topic: str,
    project: dict,
    description: str,
) -> list[dict[str, any]]:
    """
    Get a hail/seqr credit for each entry
    """

    if not description:
        description = f'{topic} credit to correctly account for costs'

    hail_credits = [{**e} for e in entries]
    for entry in hail_credits:

        entry['topic'] = topic
        entry['id'] += '-credit'
        entry['cost'] = -entry['cost']
        entry['service'] = {
            'id': 'aggregated-credit',
            'description': description,
        }
        sku = {**entry['sku']}
        sku['id'] += '-credit'
        sku['description'] += '-credit'
        entry['project'] = project

    return hail_credits


async def get_batches(
    token: str,
    billing_project: str | None = None,
    last_batch_id: Any | None = None,
) -> dict[str, any]:
    """
    Get list of batches for a billing project with no filtering.
    (Optional): from last_batch_id
    """

    qparams = {}
    params = []
    if billing_project:
        qparams['billing_project'] = billing_project
    else:
        params = ['q=']

    params.extend(f'q={k}:{v}' for k, v in qparams.items())
    if last_batch_id:
        params.append(f'last_batch_id={last_batch_id}')

    q = '?' + '&'.join(params)
    url = HAIL_BATCHES_API + q

    logger.debug(f'Getting batches: {url}')

    return await async_retry_transient_get_json_request(
        url,
        aiohttp.ClientError,
        headers={'Authorization': 'Bearer ' + token},
    )


async def get_finished_batches_for_date(
    start: datetime,
    end: datetime,
    token: str,
    billing_project: str | None = None,
) -> list[dict[str, any]]:
    """
    Get all the batches that started on {date} and are complete.
    We assume that batches are ordered by start time, so we can stop
    when we find a batch that started before the date.
    """
    batches = []
    last_batch_id = None
    n_requests = 0
    skipped = 0

    logger.info(f'Getting batches for range: [{start}, {end}]')

    while True:

        n_requests += 1
        jresponse = await get_batches(
            billing_project=billing_project, last_batch_id=last_batch_id, token=token
        )

        if 'last_batch_id' in jresponse and jresponse['last_batch_id'] == last_batch_id:
            raise ValueError(
                f'Something weird is happening with last_batch_job: {last_batch_id}'
            )
        last_batch_id = jresponse.get('last_batch_id')
        if not jresponse.get('batches'):
            logger.error(f'No batches found for range: [{start}, {end}]')
            return batches
        for b in jresponse['batches']:
            # batch not finished or not finished within the (start, end) range
            if not b['time_completed'] or not b['complete']:
                skipped += 1
                continue

            time_completed = parse_hail_time(b['time_completed'])
            in_date_range = start <= time_completed < end

            if time_completed < start:
                logger.info(
                    f'{billing_project} :: Got {len(batches)} batches '
                    f'in {n_requests} requests, skipping {skipped}'
                )
                return batches
            if in_date_range:
                batches.append(b)
            else:
                skipped += 1


async def get_jobs_for_batch(batch_id, token: str) -> list[dict[str, any]]:
    """
    For a single batch, fill in the 'jobs' field.
    """
    jobs = []
    last_job_id = None
    end = False
    iterations = 0

    async with aiohttp.ClientSession() as session:
        while not end:
            iterations += 1

            if iterations > 1 and iterations % 5 == 0:
                logger.info(f'On {iterations} iteration to load jobs for {batch_id}')

            q = '?limit=9999'
            if last_job_id:
                q += f'&last_job_id={last_job_id}'
            url = HAIL_JOBS_API.format(batch_id=batch_id) + q

            jresponse = await async_retry_transient_get_json_request(
                url,
                aiohttp.ClientError,
                session=session,
                headers={'Authorization': 'Bearer ' + token},
            )
            new_last_job_id = jresponse.get('last_job_id')
            if new_last_job_id is None:
                end = True
            elif last_job_id and new_last_job_id <= last_job_id:
                raise ValueError('Something fishy with last job id')
            last_job_id = new_last_job_id
            jobs.extend(jresponse['jobs'])

    return jobs


async def migrate_entries_from_hail_in_chunks(
    start: datetime,
    end: datetime,
    func_get_finalised_entries_for_batch,
    billing_project: str = None,
    entry_chunk_size=500,
    batch_group_chunk_size=30,
    log_prefix: str = '',
    dry_run=False,
) -> int:
    """
    Migrate all the seqr entries from hail batch,
    and insert them into the aggregate table.

    Break them down by dataset, and then proportion the rest of the costs.
    """

    # pylint: disable=too-many-locals
    token = get_hail_token()
    result = 0
    lp = f'{log_prefix} ::' if log_prefix else ''

    batches = await get_finished_batches_for_date(
        start=start, end=end, token=token, billing_project=billing_project
    )
    if len(batches) == 0:
        return 0

    chnk_counter = 0
    nchnks = math.ceil(len(batches) / entry_chunk_size) * batch_group_chunk_size
    jobs_in_batch = []

    # Process chunks of batches to avoid loading too many entries into memory
    for btch_grp in chunk(batches, entry_chunk_size):
        jobs_in_batch = []
        entries = []

        # Get jobs for a fraction of each chunked batches
        # to avoid hitting hail batch too much
        for batch_group_group in chunk(btch_grp, batch_group_chunk_size):
            chnk_counter += 1
            times = [b['time_created'] for b in batch_group_group]
            min_batch = min(times)
            max_batch = max(times)

            if len(batches) > 100:
                logger.debug(
                    f'{lp}Getting jobs for batch chunk {chnk_counter}/{nchnks} '
                    f'[{min_batch}, {max_batch}]'
                )

            promises = [get_jobs_for_batch(b['id'], token) for b in batch_group_group]
            jobs_in_batch.extend(await asyncio.gather(*promises))

        # insert all entries for this batch
        for batch, jobs in zip(btch_grp, jobs_in_batch):
            batch['jobs'] = jobs
            if len(jobs) > 10000 and len(entries) > 1000:
                logger.info(
                    f'Expecting large number of jobs ({len(jobs)}) from '
                    f'batch {batch["id"]}, inserting contents early'
                )
                result += insert_new_rows_in_table(
                    table=GCP_AGGREGATE_DEST_TABLE, objs=entries, dry_run=dry_run
                )
                entries = []

            entries_for_batch = func_get_finalised_entries_for_batch(batch)
            logger.info(f'Got {len(entries_for_batch)} entries for batch {batch["id"]}')
            entries.extend(entries_for_batch)

            s = sum(sys.getsizeof(e) for e in entries) / 1024 / 1024
            if s > 10:
                logger.info(f'Size of entries: {s} MB, inserting early')
                result += insert_new_rows_in_table(
                    table=GCP_AGGREGATE_DEST_TABLE, objs=entries, dry_run=dry_run
                )
                entries = []

        result += insert_new_rows_in_table(
            table=GCP_AGGREGATE_DEST_TABLE, objs=entries, dry_run=dry_run
        )

    return result


RE_matcher = re.compile(r'-\d+$')


def billing_row_to_topic(row, dataset_to_gcp_map) -> str | None:
    """Convert a billing row to a dataset"""
    project_id = row['project']['id']
    topic = dataset_to_gcp_map.get(project_id, project_id)

    # Default topic, any cost not clearly associated with a project will be considered
    # overhead administrative costs. This category should be minimal
    if not topic:
        return DEFAULT_TOPIC

    topic = RE_matcher.sub('', topic)
    return topic


def insert_new_rows_in_table(
    table: str,
    objs: list[dict[str, Any]],
    dry_run: bool,
    chunk_size=DEFAULT_BQ_CHUNK_SIZE,
) -> int:
    """Insert JSON rows into a BQ table"""

    if not objs:
        logger.info('Not inserting any rows')
        return 0

    n_chunks = math.ceil(len(objs) / chunk_size)
    total_size_mb = sys.getsizeof(objs) / (1024 * 1024)
    if total_size_mb > 10:
        # bigger than 10MB
        if (total_size_mb / n_chunks) > 6:
            chunk_size = math.floor(total_size_mb / 6)
            logger.info(
                'The size of the objects to insert into BQ is very large, '
                f'adjusting the chunk size to {chunk_size}'
            )

    if n_chunks > 1:
        logger.info(f'Will insert {len(objs)} rows in {n_chunks} chunks')

    inserts = 0

    for chunked_objs in chunk(objs, chunk_size):

        _query = f"""
            SELECT id FROM `{table}`
            WHERE id IN UNNEST(@ids);
        """

        ids = set(o['id'] for o in chunked_objs)
        if len(ids) != len(chunked_objs):
            counter = defaultdict(int)
            for o in chunked_objs:
                counter[o['id']] += 1
            duplicates = [f'{k} ({v})' for k, v in counter.items() if v > 1]
            raise ValueError(
                'There are multiple rows with the same id: ' + ', '.join(duplicates)
            )

        job_config = bq.QueryJobConfig(
            query_parameters=[
                bq.ArrayQueryParameter('ids', 'STRING', list(ids)),
            ]
        )

        result = get_bigquery_client().query(_query, job_config=job_config).result()
        existing_ids = set(result.to_dataframe()['id'])

        # Filter out any rows that are already in the table
        filtered_obj = [o for o in chunked_objs if o['id'] not in existing_ids]

        nrows = len(filtered_obj)

        if nrows == 0:
            logger.info(f'Not inserting any rows (0/{len(chunked_objs)})')
            continue

        if dry_run:
            logger.info(f'DRY_RUN: Inserting {nrows}/{len(chunked_objs)} rows')
            inserts += nrows
            continue

        # Count number of rows adding
        logger.info(f'Inserting {nrows}/{len(chunked_objs)} rows')

        # Insert the new rows
        job_config = bq.LoadJobConfig()
        job_config.source_format = bq.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.schema = get_formatted_bq_schema()

        j = '\n'.join(json.dumps(o) for o in filtered_obj)

        resp = get_bigquery_client().load_table_from_file(
            StringIO(j), table, job_config=job_config
        )
        try:
            result = resp.result()
            logger.info(f'Inserted {result.output_rows}/{nrows} rows')
        except ClientError as e:
            logger.error(resp.errors)
            raise e

        inserts += nrows

    return inserts


def insert_dataframe_rows_in_table(table: str, df: pd.DataFrame):
    """Insert new rows from dataframe into a table"""

    _query = f"""
        SELECT id FROM @table
        WHERE id IN UNNEST(@ids);
    """
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ArrayQueryParameter('table', 'STRING', table),
            bq.ArrayQueryParameter('ids', 'STRING', list(set(df['id']))),
        ]
    )
    result = get_bigquery_client().query(_query, job_config=job_config).result()
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
        chunksize=DEFAULT_BQ_CHUNK_SIZE,
    )

    logger.info(f'{adding_rows} new rows inserted')
    return adding_rows


CACHED_CURRENCY_CONVERSION: dict[str, float] = {}


def get_currency_conversion_rate_for_time(time: datetime):
    """
    Get the currency conversion rate for a given time.
    Noting that GCP conversion rates are decided at the start of the month,
    and apply to each job that starts within the month, regardless of when
    the job finishes.
    """

    key = f'{time.year}-{time.month}'
    if key not in CACHED_CURRENCY_CONVERSION:
        logger.info(f'Looking up currency conversion rate for {key}')
        query = f"""
            SELECT currency_conversion_rate
            FROM {GCP_BILLING_BQ_TABLE}
            WHERE DATE(_PARTITIONTIME) = DATE('{time.date()}')
            LIMIT 1
        """
        for r in get_bigquery_client().query(query).result():
            CACHED_CURRENCY_CONVERSION[key] = r['currency_conversion_rate']

    return CACHED_CURRENCY_CONVERSION[key]


def _generate_hail_resource_cost_lookup():
    """
    Generate the cost table for the hail resources.
    This is currently set to the australia-southeast-1 region.
    """
    # pylint: disable=import-outside-toplevel,import-error
    from hailtop.utils import (
        rate_gib_hour_to_mib_msec,
        rate_gib_month_to_mib_msec,
        rate_cpu_hour_to_mcpu_msec,
        rate_instance_hour_to_fraction_msec,
    )

    # Noting that this does not support different prices over time
    # consider implementing something like:
    #   https://github.com/hail-is/hail/pull/11840/files

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
    # TODO: consider extending this to support azure,
    # maybe it's just a different region tag

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
) -> tuple[datetime | None, datetime | None]:
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
) -> Iterator[tuple[datetime, datetime]]:
    """
    Iterate over a range of dates.

    >>> list(date_range_iterator(datetime(2019, 1, 1), datetime(2019, 1, 2)))
    [(datetime.datetime(2019, 1, 1, 0, 0), datetime.datetime(2019, 1, 2, 0, 0))]

    >>> list(date_range_iterator(datetime(2019, 1, 1), datetime(2019, 1, 3)))
    [(datetime.datetime(2019, 1, 1, 0, 0), datetime.datetime(2019, 1, 3, 0, 0))]

    >>> list(date_range_iterator(datetime(2019, 1, 1), datetime(2019, 1, 4)))
    [(datetime.datetime(2019, 1, 1, 0, 0), datetime.datetime(2019, 1, 3, 0, 0)), (datetime.datetime(2019, 1, 3, 0, 0), datetime.datetime(2019, 1, 4, 0, 0))]

    """  # noqa: E501
    dt_from = start
    dt_to = start + intv
    while dt_to < end:
        yield (dt_from, dt_to)
        dt_from += intv
        dt_to += intv

    dt_to = min(dt_to, end)
    if dt_from < end:
        yield (dt_from, end)


def get_start_and_end_from_data(data) -> tuple[datetime | None, datetime | None]:
    """
    Get the start and end times from the cloud function data.
    """
    if data:
        dates = {}
        if data.get('attributes'):
            dates = data.get('attributes', {})
        elif data.get('start') and data.get('end'):
            dates = data
        elif data.get('message'):
            dates = dict(json.loads(data['message'])) or {}

        logger.info(f'data: {data}, dates: {dates}')

        s_raw = dates.get('start')
        e_raw = dates.get('end')

        # this should except if the start/end is in an invalid format
        start = datetime.fromisoformat(s_raw) if s_raw else None
        end = datetime.fromisoformat(e_raw) if e_raw else None

        return (start, end)

    return (None, None)


def process_default_start_and_end(start, end) -> tuple[datetime, datetime]:
    """
    Process the start and end times.
    """
    if not end:
        # start of today
        end = datetime.now()

    if not start:
        start = end - DEFAULT_RANGE_INTERVAL

    assert isinstance(start, datetime) and isinstance(end, datetime)
    return start, end


def get_date_intervals_for(
    start, end, interval=DEFAULT_RANGE_INTERVAL
) -> Iterator[tuple[datetime, datetime]]:
    """Process start and end times (and get defaults)"""
    s, e = process_default_start_and_end(start, end)
    return date_range_iterator(s, e, intv=interval)


def read_secret(project_id: str, secret_name: str) -> str | None:
    """Reads the latest version of a GCP Secret Manager secret.

    Returns None if the secret doesn't exist."""

    secret_manager = secretmanager.SecretManagerServiceClient()
    secret_path = secret_manager.secret_path(project_id, secret_name)

    response = secret_manager.access_secret_version(
        request={'name': f'{secret_path}/versions/latest'}
    )
    return response.payload.data.decode('UTF-8')


def get_hail_entry(
    key: str,
    topic: str,
    service_id: str,
    description: str,
    cost: float,
    currency_conversion_rate: float,
    usage: float,
    batch_resource: str,
    start_time: datetime,
    end_time: datetime,
    labels: dict[str, str] = None,
) -> dict[str, Any]:
    """
    Get well formed entry dictionary from keys
    """

    assert labels is None or isinstance(labels, dict)

    _labels = []
    if labels:
        _labels = [
            {'key': k, 'value': str(v).encode('ascii', 'ignore').decode()}
            for k, v in labels.items()
        ]
    return {
        'id': key,
        'topic': topic,
        'service': {'id': service_id, 'description': description},
        'sku': {
            'id': f'hail-{batch_resource}',
            'description': batch_resource,
        },
        'usage_start_time': to_bq_time(start_time),
        'usage_end_time': to_bq_time(end_time),
        'project': None,
        'labels': _labels,
        'system_labels': [],
        'location': {
            'location': 'australia-southeast1',
            'country': 'Australia',
            'region': 'australia',
            'zone': None,
        },
        'export_time': to_bq_time(datetime.now()),
        'cost': cost,
        'currency': 'AUD',
        'currency_conversion_rate': currency_conversion_rate,
        'usage': {
            'amount': usage,
            'unit': get_unit_for_batch_resource_type(batch_resource),
            'amount_in_pricing_units': cost,
            'pricing_unit': 'AUD',
        },
        'credits': [],
        'invoice': {'month': f'{start_time.year}{str(start_time.month).zfill(2)}'},
        'cost_type': 'regular',
        'adjustment_info': None,
    }
