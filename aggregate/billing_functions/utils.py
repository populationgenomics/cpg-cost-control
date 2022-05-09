import os
from collections import defaultdict
import json
import logging
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Dict, List, Any, Iterator, Sequence, TypeVar

import aiohttp
import pandas as pd

import google.cloud.bigquery as bq
from google.api_core.exceptions import ClientError

logging.basicConfig(level=logging.INFO)

GCP_BILLING_BQ_TABLE = (
    'billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343'
)

# 10% overhead
SERVICE_FEE = 0.1
ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'


HAIL_BASE = 'https://batch.hail.populationgenomics.org.au'
HAIL_UI_URL = HAIL_BASE + '/batches/{batch_id}'
HAIL_BATCHES_API = HAIL_BASE + '/api/v1alpha/batches'
HAIL_JOBS_API = HAIL_BASE + '/api/v1alpha/batches/{batch_id}/jobs/resources'


bigquery_client = bq.Client()
T = TypeVar('T')


def chunk(iterable: Sequence[T], chunk_size=50) -> Iterator[Sequence[T]]:
    """
    Chunk a sequence by yielding lists of `chunk_size`
    """
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i : i + chunk_size]


def get_schema_json():
    """Get the schema for the table"""
    pwd = Path(__file__).parent.parent.resolve()
    schema_path = pwd / 'schema' / 'aggregate_schema.json'
    with open(schema_path, 'r') as f:
        return json.load(f)


def format_schema(schema):
    formatted_schema = []
    for row in schema:
        kwargs = {
            'name': row['name'],
            'field_type': row['type'],
            'mode': row['mode'],
        }

        if 'fields' in row and row['fields']:
            kwargs['fields'] = format_schema(row['fields'])
        formatted_schema.append(bq.SchemaField(**kwargs))
    return formatted_schema


def get_formatted_schema():
    return format_schema(get_schema_json())


def parse_hail_time(time_str: str) -> datetime:
    return datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%SZ')


def to_bq_time(time: datetime):
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
            in_date_range = time_completed >= start_day and time_completed < end_day

            if time_completed < start_day:
                logging.info(
                    f'{billing_project} :: Got batches in {n_requests} requests, skipping {skipped}'
                )
                return batches
            if in_date_range:
                batches.append(b)
            else:
                skipped += 1


async def get_jobs_for_batch(batch_id, token: str) -> List[str]:
    """
    For a single batch, fill in the "jobs" field.

    TODO: use new endpoint with billing info
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


def insert_new_rows_in_table(table: str, obj: List[Dict[str, Any]]):
    """Insert new rows into a table"""

    _query = f"""
        SELECT id FROM `{table}`
        WHERE id IN UNNEST(@ids);
    """
    # ids = df['id']
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

    if len(filtered_obj) == 0:
        logging.info('Not inserting any rows')
        return []

    # Count number of rows adding
    logging.info(f"Inserting {len(filtered_obj)}/{len(obj)} rows")

    # Insert the new rows
    job_config = bq.LoadJobConfig()
    job_config.source_format = bq.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = get_formatted_schema()

    j = '\n'.join(json.dumps(o) for o in filtered_obj)

    resp = bigquery_client.load_table_from_file(
        StringIO(j), table, job_config=job_config
    )
    try:
        result = resp.result()
        logging.info(str(result))
    except ClientError as e:
        logging.error(resp.errors)
        raise e

    return filtered_obj


def insert_dataframe_rows_in_table(table: str, df: pd.DataFrame):
    """Insert new rows into a table"""

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
    table_schema = get_formatted_schema()

    df.to_gbq(
        table,
        project_id=project_id,
        table_schema=table_schema,
        if_exists='append',
    )

    logging.info(f"{adding_rows} new rows inserted")
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
        logging.warn(f'Looking up currency conversion rate for {key}')
        query = f"""
            SELECT currency_conversion_rate
            FROM {GCP_BILLING_BQ_TABLE}
            WHERE DATE(_PARTITIONTIME) = DATE('{time.date()}')
            LIMIT 1
        """
        for r in bigquery_client.query(query).result():
            CACHED_CURRENCY_CONVERSION[key] = r['currency_conversion_rate']

    return CACHED_CURRENCY_CONVERSION[key]


def get_usd_cost_for_resource(batch_resource, usage):
    """
    Get the cost of a resource in USD.
    """
    # TODO: fix these costs, they're the ones from hail directory
    return {
        "boot-disk/pd-ssd/1": 0.0000000000000631286124420108,
        "compute/n1-nonpreemptible/1": 0.00000000000878083333333334,
        "compute/n1-preemptible/1": 0.00000000000184861111111111,
        "disk/local-ssd/1": 0.0000000000000178245493953913,
        "disk/pd-ssd/1": 0.0000000000000631286124420108,
        "ip-fee/1024/1": 0.00000000000108506944444444,
        "memory/n1-nonpreemptible/1": 0.00000000000114935980902778,
        "memory/n1-preemptible/1": 0.000000000000241970486111111,
        "service-fee/1": 0.00000000000277777777777778,
    }[batch_resource] * usage


def get_unit_for_batch_resource_type(batch_resource_type: str) -> str:
    return {
        "boot-disk/pd-ssd/1": "GB/ms",
        "disk/local-ssd/1": "GB/ms",
        "disk/pd-ssd/1": "GB/ms",
        "compute/n1-nonpreemptible/1": "cpu/ms",
        "compute/n1-preemptible/1": 'cpu/ms',
        "ip-fee/1024/1": 'IPs/ms',
        "memory/n1-nonpreemptible/1": 'GB/ms',
        "memory/n1-preemptible/1": 'GB/ms',
        "service-fee/1": '$/ms',
    }.get(batch_resource_type, batch_resource_type)
