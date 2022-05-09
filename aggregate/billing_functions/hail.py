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
import math
import asyncio
import logging
import json
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List

from cpg_utils.cloud import read_secret

from utils import (
    chunk,
    insert_new_rows_in_table,
    get_currency_conversion_rate_for_time,
    get_unit_for_batch_resource_type,
    get_usd_cost_for_resource,
    get_finished_batches_for_date,
    get_hail_token,
    get_jobs_for_batch,
    parse_hail_time,
    to_bq_time,
    HAIL_UI_URL,
    SERVICE_FEE,
    ANALYSIS_RUNNER_PROJECT_ID,
)

GCP_DEST_TABLE = 'billing-admin-290403.billing_aggregate.michael-dev'


logging.basicConfig(level=logging.INFO)

SERVICE_ID = 'hail'


def get_billing_projects():
    """
    Get Hail billing projects, same names as dataset
    TOOD: Implement
    """

    server_config = json.loads(read_secret(ANALYSIS_RUNNER_PROJECT_ID, 'server-config'))
    datasets_to_ignore = {'hail', 'seqr'}
    ds = list(set(server_config.keys()) - datasets_to_ignore)
    return ds


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
        key = f'hail-{dataset}-batch-{batch["id"]}-{batch_resource}'

        labels = [
            {'key': 'dataset', 'value': dataset},
            {
                'key': 'url',
                'value': HAIL_UI_URL.replace('{batch_id}', str(batch['id'])),
            },
        ]
        name = batch.get('attributes', {}).get('name')
        if name:
            labels.append(
                {'key': 'name', 'value': name.encode("ascii", "ignore").decode()}
            )

        cost = (
            (1 + SERVICE_FEE)
            * currency_conversion_rate
            * get_usd_cost_for_resource(batch_resource, usage)
        )

        entries.append(
            {
                "id": key,
                "dataset": dataset,
                "service": {
                    "id": key,
                    "description": 'Hail compute: ' + batch_resource,
                },
                "sku": {
                    "id": f'hail-{batch_resource}',
                    "description": f'{batch_resource} usage',
                },
                "usage_start_time": to_bq_time(start_time),
                "usage_end_time": to_bq_time(end_time),
                'project': None,
                "labels": labels,
                "system_labels": [],
                "location": {
                    "location": 'australia-southeast1',
                    "country": 'Australia',
                    "region": 'australia',
                    "zone": None,
                },
                "export_time": to_bq_time(datetime.now()),
                "cost": cost,
                "currency": "AUD",
                "currency_conversion_rate": currency_conversion_rate,
                "usage": {
                    "amount": usage,
                    "unit": get_unit_for_batch_resource_type(batch_resource),
                    "amount_in_pricing_units": cost,
                    "pricing_unit": "AUD",
                },
                'credits': [],
                "invoice": {"month": f'{start_time.month}{start_time.year}'},
                "cost_type": "regular",
                'adjustment_info': None,
            }
        )

    return entries


def from_request(request):
    asyncio.get_event_loop().run_until_complete(main())


async def main(start: datetime = None, end: datetime = None):
    """Main body function"""

    if not start:
        start = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=2)
    if not end:
        end = datetime.now()

    assert isinstance(start, datetime) and isinstance(end, datetime)

    token = get_hail_token()
    promises = [
        get_entries_for_billing_project(bp, start, end, token)
        for bp in get_billing_projects()
    ]
    entries_nested = await asyncio.gather(*promises)
    entries = [entry for entry_list in entries_nested for entry in entry_list]
    # Insert new rows into aggregation table
    insert_new_rows_in_table(table=GCP_DEST_TABLE, obj=entries)


async def get_entries_for_billing_project(bp, start, end, token):
    entries = []
    logging.info(f'{bp} :: loading batches')
    batches = await get_finished_batches_for_date(bp, start, end, token)
    if len(batches) == 0:
        return []
    # batches = [b for b in batches if '...' not in b['attributes']['name']]
    logging.info(f'{bp} :: Filling in information for {len(batches)} batches')
    jobs_in_batch = []
    for chnk, btch_grp in enumerate(chunk(batches, 100)):
        if len(batches) > 100:
            logging.info(
                f'{bp} :: Getting jobs for chunk {chnk+1}/{math.ceil(len(batches)/100)}'
            )
        promises = [get_jobs_for_batch(b['id'], token) for b in btch_grp]
        jobs_in_batch.extend(await asyncio.gather(*promises))
    for batch, jobs in zip(batches, jobs_in_batch):
        batch['jobs'] = jobs
        entries.extend(get_finalised_entries_for_batch(bp, batch))

    return entries


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(
        main(start=datetime(2022, 5, 2), end=datetime(2022, 5, 5))
    )
