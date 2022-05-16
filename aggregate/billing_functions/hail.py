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
import math
import asyncio
import json
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

from cpg_utils.cloud import read_secret

from .utils import (
    get_start_and_end_from_data,
    logger,
    HAIL_UI_URL,
    SERVICE_FEE,
    ANALYSIS_RUNNER_PROJECT_ID,
    GCP_AGGREGATE_DEST_TABLE,
    get_currency_conversion_rate_for_time,
    get_unit_for_batch_resource_type,
    get_usd_cost_for_resource,
    get_finished_batches_for_date,
    get_hail_token,
    get_jobs_for_batch,
    get_start_and_end_from_request,
    process_default_start_and_end,
    chunk,
    to_bq_time,
    parse_hail_time,
    insert_new_rows_in_table,
)


SERVICE_ID = 'hail'


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

        key = f'{SERVICE_ID}-{dataset}-batch-{batch_id}-{batch_resource}'

        hail_ui_url = HAIL_UI_URL.replace('{batch_id}', str(batch_id))
        labels = [
            {'key': 'dataset', 'value': dataset},
            {'key': 'batch_id', 'value': str(batch_id)},
        ]
        name = batch.get('attributes', {}).get('name')
        if name:
            labels.append(
                {'key': 'name', 'value': name.encode('ascii', 'ignore').decode()}
            )

        labels.append({'key': 'url', 'value': hail_ui_url})

        cost = (
            (1 + SERVICE_FEE)
            * currency_conversion_rate
            * get_usd_cost_for_resource(batch_resource, usage)
        )

        entries.append(
            {
                'id': key,
                'dataset': dataset,
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
                'invoice': {'month': f'{start_time.month}{start_time.year}'},
                'cost_type': 'regular',
                'adjustment_info': None,
            }
        )

    return entries


async def get_entries_for_billing_project(bp, start, end, token):
    """
    For a given billing project, get all the batches completed in (start, end]
    and convert these batches into entries (1 per batch) for the aggregation table.
    """
    entries = []
    logger.info(f'{bp} :: loading batches')
    batches = await get_finished_batches_for_date(bp, start, end, token)
    if len(batches) == 0:
        return []
    # batches = [b for b in batches if '...' not in b['attributes']['name']]
    logger.info(f'{bp} :: Filling in information for {len(batches)} batches')
    jobs_in_batch = []
    for chnk, btch_grp in enumerate(chunk(batches, 100)):
        if len(batches) > 100:
            logger.info(
                f'{bp} :: Getting jobs for chunk {chnk+1}/{math.ceil(len(batches)/100)}'
            )
        promises = [get_jobs_for_batch(b['id'], token) for b in btch_grp]
        jobs_in_batch.extend(await asyncio.gather(*promises))
    for batch, jobs in zip(batches, jobs_in_batch):
        batch['jobs'] = jobs
        entries.extend(get_finalised_entries_for_batch(bp, batch))

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
    main(start, end)


async def main(start: datetime = None, end: datetime = None):
    """Main body function"""
    start, end = process_default_start_and_end(start, end)

    token = get_hail_token()
    promises = [
        get_entries_for_billing_project(bp, start, end, token)
        for bp in get_billing_projects()
    ]
    entries_nested = await asyncio.gather(*promises)
    entries = [entry for entry_list in entries_nested for entry in entry_list]
    # Insert new rows into aggregation table
    insert_new_rows_in_table(table=GCP_AGGREGATE_DEST_TABLE, obj=entries)


if __name__ == '__main__':
    test_start, test_end = None, None
    # test_start, test_end = datetime(2022, 5, 2), datetime(2022, 5, 5)

    asyncio.get_event_loop().run_until_complete(main(start=test_start, end=test_end))
