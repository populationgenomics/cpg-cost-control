"""
A cloud function that synchronises HAIL billing data to BigQuery

TO:     billing-admin-290403.billing_aggregate.aggregate


Notes:

- The service.id should be "hail"


Tasks:

- Only want to transfer data from the projects in the server-config
- Need to build an endpoint in Hail to service this metadata
    - Take in a project and a date range in the endpoint
    - Result should be of a form like this
        [{
            batch_id: 0,
            jobs: [{"job_id": 0, "mseccpu": 100, "msecgb": 100}]
        }]

    - Should be able to filter on the following properties:
        - search by attribute
        - search by project
- Transform into new generic format
    - One batch per row

- Can't duplicate rows, so determine some ID:
    - And maybe only sync "settled" jobs within datetimes
        (ie: finished between START + END of previous time period)
"""

from collections import defaultdict
from datetime import datetime, timedelta
import os
import json
import logging
from typing import Dict, List
import requests
from google.cloud import bigquery

import pandas as pd

from utils import insert_new_rows_in_table


GCP_BILLING_BQ_TABLE = (
    'billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343'
)
GCP_DEST_TABLE = 'billing-admin-290403.billing_aggregate.michael-dev'
bigquery_client = bigquery.Client()


logging.basicConfig(level=logging.INFO)

SERVICE_ID = 'hail'

BASE = 'https://batch.hail.populationgenomics.org.au'
UI_URL = BASE + '/batches/{batch_id}'
BATCHES_API = BASE + '/api/v1alpha/batches'
JOBS_API = BASE + '/api/v1alpha/batches/{batch_id}/jobs/resources'


def parse_hail_time(time_str: str) -> datetime:
    return datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%SZ')


def get_billing_projects():
    """
    Get Hail billing projects, same names as dataset
    TOOD: Implement
    """
    return ['seqr']


def get_hail_token():
    """
    Get Hail token from local tokens file
    TODO: look at env var for deploy
    """
    with open(os.path.expanduser('~/.hail/tokens.json')) as f:
        config = json.load(f)
        return config['default']


def get_batches(billing_project: str, last_batch_id: any, token: str):
    """
    Get list of batches for a billing project with no filtering.
    (Optional): from last_batch_id
    """
    q = f'?q=billing_project:{billing_project}'
    if last_batch_id:
        q += f'&last_batch_id={last_batch_id}'

    response = requests.get(
        BATCHES_API + q, headers={'Authorization': 'Bearer ' + token}
    )
    response.raise_for_status()

    return response.json()


def get_finished_batches_for_date(
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
        jresponse = get_batches(billing_project, last_batch_id, token)

        if 'last_batch_id' in jresponse and jresponse['last_batch_id'] == last_batch_id:
            raise ValueError(
                f'Something weird is happening with last_batch_job: {last_batch_id}'
            )
        last_batch_id = jresponse['last_batch_id']
        for b in jresponse['batches']:
            if not b['time_completed'] or not b['complete']:
                skipped += 1
                continue

            time_completed = parse_hail_time(b['time_completed'])
            in_date_range = time_completed >= start_day and time_completed < end_day

            if time_completed < start_day:
                logging.info(
                    f'Got batches in {n_requests} requests, skipping {skipped}'
                )
                return batches
            if in_date_range:
                batches.append(b)
            else:
                skipped += 1


def fill_batch_jobs(batch: dict, token: str):
    """
    For a single batch, fill in the "jobs" field.

    TODO: use new endpoint with billing info
    """
    batch['jobs'] = []
    last_job_id = None
    end = False
    iterations = 0
    while not end:
        iterations += 1

        if iterations > 1 and iterations % 5 == 0:
            logging.info(f'On {iterations} iteration to load jobs for {batch["id"]}')

        q = ''
        if last_job_id:
            q = f'?last_job_id={last_job_id}'
        url = JOBS_API.format(batch_id=batch['id']) + q
        response = requests.get(url, headers={'Authorization': 'Bearer ' + token})
        response.raise_for_status()

        jresponse = response.json()
        new_last_job_id = jresponse.get('last_job_id')
        if new_last_job_id is None:
            end = True
        elif last_job_id and new_last_job_id <= last_job_id:
            raise ValueError('Something fishy with last job id')
        last_job_id = new_last_job_id
        batch['jobs'].extend(jresponse['jobs'])

    return batch


CACHED_CURRENCY_CONVERSION: Dict[str, float] = {}


def get_currency_conversion_rate_for_time(time: datetime):
    """
    Get the currency conversion rate for a given time.
    Noting that GCP conversion rates are decided at the start of the month,
    and apply to each job that starts within the month, regardless of when
    the job finishes.
    """
    global CACHED_CURRENCY_CONVERSION

    key = str(time.date())
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


def get_finalised_entries_for_batch(dataset, batch: dict) -> List[Dict]:
    entries = []

    start_time = parse_hail_time(batch['time_created'])
    end_time = parse_hail_time(batch['time_completed'])

    resource_usage = defaultdict(float)
    currency_conversion_rate = get_currency_conversion_rate_for_time(start_time)
    for job in batch['jobs']:
        if not job['resources']:
            continue
        for batch_resource, usage in job['resources'].items():
            # batch_resource is one of ['cpu', 'memory', 'disk']

            resource_usage[batch_resource] += usage

    for batch_resource, usage in resource_usage.items():
        key = f'hail-{dataset}-batch-{batch["id"]}'

        labels = [
            {'name': 'dataset', 'value': dataset},
            {'name': 'url', 'value': UI_URL.replace('{batch_id}', str(batch['id']))},
        ]
        name = batch.get('attributes', {}).get('name')
        if name:
            labels.append({'name': 'name', 'value': name})
        # batch_resource is one of ['cpu', 'memory', 'disk']

        cost = currency_conversion_rate * get_usd_cost_for_resource(
            batch_resource, usage
        )

        entries.append(
            {
                "id": key,
                "dataset": dataset,
                "service": {
                    "id": key,
                    "description": 'Hail compute',
                },
                "sku": {
                    "id": f'hail-{batch_resource}',
                    "description": f'{batch_resource} usage',
                },
                "usage_start_time": start_time,
                "usage_end_time": end_time,
                'project': None,
                "labels": labels,
                "system_labels": [],
                "location": {
                    "location": 'australia-southeast1',
                    "country": 'Australia',
                    "region": 'australia',
                    "zone": None,
                },
                "export_time": datetime.now(),
                "cost": cost,
                "currency": "AUD",
                "currency_conversion_rate": currency_conversion_rate,
                "usage": {
                    "amount": usage,
                    "unit": get_unit_for_batch_resource_type(batch_resource),
                    "amount_in_pricing_units": cost,
                    "pricing_unit": "AUD",
                },
                # 'credits': [],
                "invoice": {"month": f'{start_time.month}{start_time.year}'},
                "cost_type": "regular",
                'adjustment_info': None,
            }
        )

    return entries


def from_request(request):
    main()


def main(start: datetime = None, end: datetime = None):
    """Main body function"""

    if not start:
        start = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=2)
    if not end:
        end = datetime.now()

    assert isinstance(start, datetime) and isinstance(end, datetime)

    token = get_hail_token()
    entries = []
    for bp in get_billing_projects():
        batches = get_finished_batches_for_date(bp, start, end, token)
        batches = [b for b in batches if '...' not in b['attributes']['name']]
        logging.info(f'{bp} :: Filling in information for {len(batches)} batches')

        for batch in batches:
            fill_batch_jobs(batch, token)
            entries.extend(get_finalised_entries_for_batch(bp, batch))

    df = pd.DataFrame.from_records(entries)

    # Insert new rows into aggregation table
    insert_new_rows_in_table(client=bigquery_client, table=GCP_DEST_TABLE, df=df)


if __name__ == '__main__':
    main(start=datetime(2022, 5, 2), end=datetime(2022, 5, 5))
