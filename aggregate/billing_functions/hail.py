"""
A cloud function that synchronises HAIL billing data to BigQuery

TO:     billing-admin-290403.billing_aggregate.aggregate


Notes:

- The service.id should be 'hail'


Tasks:

- Only want to transfer data from the projects in the server-config
- Need to build an endpoint in Hail to service this metadata
    - Take in a project and a date range in the endpoint
    - Result should be of a form like this
        [{
            batch_id: 0,
            jobs: [{'job_id': 0, 'mseccpu': 100, 'msecgb': 100}]
        }]

    - Should be able to filter on the following properties:
        - search by attribute
        - search by project
- Transform into new generic format
    - One batch per row

- Can't duplicate rows, so determine some ID:
    - And maybe only sync 'settled' jobs within datetimes
        (ie: finished between START + END of previous time period)
"""
import json
from copy import deepcopy
import math
import logging
import asyncio
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List

from cpg_utils.cloud import read_secret

try:
    from .utils import *
except ImportError:
    from utils import *


SERVICE_ID = 'hail'
logging.basicConfig(level=logging.INFO)

HAIL_PROJECT_FIELD = {
    "id": "hail-295901",
    "number": "805950571114",
    "name": "hail-295901",
    "labels": [],
    "ancestry_numbers": "/648561325637/",
    "ancestors": [
        {
            "resource_name": "projects/805950571114",
            "display_name": "hail-295901",
        },
        {
            "resource_name": "organizations/648561325637",
            "display_name": "populationgenomics.org.au",
        },
    ],
}


def get_billing_projects():
    """
    Get Hail billing projects, same names as dataset names
    """

    server_config = json.loads(read_secret(ANALYSIS_RUNNER_PROJECT_ID, 'server-config'))
    datasets_to_ignore = {'hail', 'seqr'}
    ds = list(set(server_config.keys()) - datasets_to_ignore)
    return ds


def get_finalised_entries_for_batch(dataset, batch: dict) -> List[Dict]:
    """
    Take a batch, and generate the actual cost of all the jobs,
    and return a list of BigQuery rows - one per resource type.
    """
    entries = []

    start_time = parse_hail_time(batch['time_created'])
    end_time = parse_hail_time(batch['time_completed'])
    batch_id = batch['id']
    resource_usage = defaultdict(float)
    currency_conversion_rate = get_currency_conversion_rate_for_time(start_time)
    for job in batch['jobs']:
        if not job['resources']:
            continue
        for batch_resource, usage in job['resources'].items():
            # batch_resource is one of ['cpu', 'memory', 'disk']

            resource_usage[batch_resource] += usage

    for batch_resource, usage in resource_usage.items():
        if batch_resource.startswith('service-fee'):
            continue

        key = f'{SERVICE_ID}-{dataset}-batch-{batch_id}-{batch_resource}'.replace(
            "/", "-"
        )

        ui_url = HAIL_UI_URL.replace('{batch_id}', str(batch_id))
        labels = [
            {'key': 'dataset', 'value': dataset},
            {'key': 'batch_id', 'value': str(batch_id)},
        ]
        name = batch.get('attributes', {}).get('name')
        if name:
            labels.append(
                {'key': 'name', 'value': name.encode('ascii', 'ignore').decode()}
            )

        labels.append({'key': 'url', 'value': ui_url})

        cost = (
            (1 + HAIL_SERVICE_FEE)
            * currency_conversion_rate
            * get_usd_cost_for_resource(batch_resource, usage)
        )

        entries.append(
            {
                'id': key,
                'topic': dataset,
                'service': {
                    'id': SERVICE_ID,
                    'description': 'Hail compute',
                },
                'sku': {
                    'id': f'hail-{batch_resource}',
                    'description': batch_resource,
                },
                'usage_start_time': to_bq_time(start_time),
                'usage_end_time': to_bq_time(end_time),
                'project': None,
                'labels': labels,
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
                'invoice': {
                    'month': f'{start_time.year}{str(start_time.month).zfill(2)}'
                },
                'cost_type': 'regular',
                'adjustment_info': None,
            }
        )

    return entries


def get_hail_credits(entries) -> List[Dict[str, Any]]:
    """
    Get a hail credit for each entry
    """

    hail_credits = [deepcopy(e) for e in entries]
    for entry in hail_credits:

        entry['topic'] = 'hail'
        entry['id'] += '-credit'
        entry['cost'] = -entry['cost']
        entry['service'] = {
            'id': 'aggregated-credit',
            'description': 'Hail credit to correctly account for costs',
        }
        entry['project'] = HAIL_PROJECT_FIELD

    return hail_credits


async def process_and_finalise_entries_for(bp, start, end, token) -> int:
    """
    For a given billing project, get all the batches completed in (start, end]
    and convert these batches into entries (1 per batch) for the aggregation table.
    """
    logger.info(f'{bp} :: loading batches')
    batches = await get_finished_batches_for_date(bp, start, end, token)
    if len(batches) == 0:
        return 0
    # batches = [b for b in batches if '...' not in b['attributes']['name']]
    logger.info(f'{bp} :: Filling in information for {len(batches)} batches')

    # we'll process 500 batches, and insert all entries for that
    chnk_counter = 0
    final_chnk_size = 30
    nchnks = math.ceil(len(batches) / 500) * final_chnk_size
    result = 0
    for btch_grp in chunk(batches, 500):
        entries = []
        jobs_in_batch = []

        for batch_group_group in chunk(btch_grp, final_chnk_size):
            chnk_counter += 1
            min_batch = min(batch_group_group, key=lambda b: b['time_created'])
            max_batch = max(batch_group_group, key=lambda b: b['time_created'])

            for batch in batch_group_group:
                entries.extend(get_finalised_entries_for_batch(bp, batch))
                jobs_in_batch.extend(batch['jobs'])
            if len(batches) > 100:
                logger.info(
                    f'{bp} :: Getting jobs for batch chunk {chnk_counter}/{nchnks} [{min_batch}, {max_batch}]'
                )

            promises = [get_jobs_for_batch(b['id'], token) for b in batch_group_group]
            jobs_in_batch.extend(await asyncio.gather(*promises))

        for batch, jobs in zip(btch_grp, jobs_in_batch):
            batch['jobs'] = jobs
            entries.extend(get_finalised_entries_for_batch(bp, batch))

        # Give hail batch a break :sweat:
        await asyncio.sleep(1)

        # Insert new rows into aggregation table
        entries.extend(get_hail_credits(entries))
        logger.info(f'{bp} :: Inserting {len(entries)} entries')
        result += insert_new_rows_in_table(table=GCP_AGGREGATE_DEST_TABLE, obj=entries)

    return entries


def from_request(request):
    """
    From request object, get start and end time if present
    """
    start, end = get_start_and_end_from_request(request)
    asyncio.get_event_loop().run_until_complete(main(start, end))


def from_pubsub(data=None, context=None):
    """
    From pubsub message, get start and end time if present
    """
    start, end = get_start_and_end_from_data(data)
    asyncio.get_event_loop().run_until_complete(main(start, end))


async def migrate_hail_data(start, end, token):
    promises = [
        process_and_finalise_entries_for(bp, start, end, token)
        for bp in get_billing_projects()
    ]
    result_promises = await asyncio.gather(*promises)
    result = sum(result_promises)

    return result


async def main(start: datetime = None, end: datetime = None) -> int:
    """Main body function"""
    start, end = process_default_start_and_end(start, end)

    hail_token = get_hail_token()

    # Migrate the data in batches
    result = await migrate_hail_data(start, end, hail_token)

    logging.info(f"Migrated a total of {result} rows")

    return result


if __name__ == '__main__':
    # test_start, test_end = None, None
    test_start, test_end = datetime(2022, 4, 1), datetime(2022, 5, 3)

    asyncio.get_event_loop().run_until_complete(main(start=test_start, end=test_end))
