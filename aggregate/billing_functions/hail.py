# pylint: disable=logging-format-interpolation
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
import math
import logging
import asyncio
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

from cpg_utils.cloud import read_secret

try:
    from . import utils
except ImportError:
    import utils


SERVICE_ID = 'hail'
EXCLUDED_BATCH_IDS = {'seqr'}


logger = utils.logger


def get_billing_projects():
    """
    Get Hail billing projects, same names as dataset names
    """

    server_config = json.loads(
        read_secret(utils.ANALYSIS_RUNNER_PROJECT_ID, 'server-config')
    )
    datasets_to_ignore = {'hail', 'seqr'}
    ds = list(set(server_config.keys()) - datasets_to_ignore)
    return ds


def get_finalised_entries_for_batch(batch: dict) -> List[Dict]:
    """
    Take a batch, and generate the actual cost of all the jobs,
    and return a list of BigQuery rows - one per resource type.
    """
    entries = []

    start_time = utils.parse_hail_time(batch['time_created'])
    end_time = utils.parse_hail_time(batch['time_completed'])
    batch_id = batch['id']
    resource_usage = defaultdict(float)
    dataset = batch['billing_project']

    currency_conversion_rate = utils.get_currency_conversion_rate_for_time(start_time)
    for job in batch['jobs']:
        if not job['resources']:
            continue
        for batch_resource, usage in job['resources'].items():
            # batch_resource is one of ['cpu', 'memory', 'disk']

            resource_usage[batch_resource] += usage

    for batch_resource, usage in resource_usage.items():
        if batch_resource.startswith('service-fee'):
            continue

        labels = {
            'dataset': dataset,
            'batch_id': str(batch_id),
            'batch_resource': batch_resource,
        }

        name = batch.get('attributes', {}).get('name')
        if name:
            labels['batch_name'] = name

        labels['url'] = utils.HAIL_UI_URL.replace('{batch_id}', str(batch_id))

        cost = (
            (1 + utils.HAIL_SERVICE_FEE)
            * currency_conversion_rate
            * utils.get_usd_cost_for_resource(batch_resource, usage)
        )
        entries.append(
            utils.get_entry(
                key=f'{SERVICE_ID}-{dataset}-batch-{batch_id}-{batch_resource}'.replace(
                    '/', '-'
                ),
                topic=dataset,
                service_id=SERVICE_ID,
                description='Hail compute',
                cost=cost,
                currency_conversion_rate=currency_conversion_rate,
                usage=usage,
                batch_resource=batch_resource,
                start_time=start_time,
                end_time=end_time,
                labels=labels,
            )
        )

    return entries


async def migrate_hail_data(start, end, token) -> int:
    """
    For a given billing project, get all the batches completed in (start, end]
    and convert these batches into entries (1 per batch) for the aggregation table.
    """
    batches = await utils.get_finished_batches_for_date(
        start=start, end=end, token=token
    )
    if len(batches) == 0:
        return 0
    logger.info(f'Filling in information for {len(batches)} batches')

    batches = [b for b in batches if b['billing_project'] not in EXCLUDED_BATCH_IDS]
    counter = defaultdict(int)
    for o in batches:
        counter[o['billing_project']] += 1
    logger.info(f'Billing project breakdown: {json.dumps(counter)}')

    # we'll process 500 batches, and insert all entries for that
    chnk_counter = 0
    final_chnk_size = 30
    nchnks = math.ceil(len(batches) / 500) * final_chnk_size
    result = 0
    for btch_grp in utils.chunk(batches, 500):
        entries = []
        jobs_in_batch = []

        for batch_group_group in utils.chunk(btch_grp, final_chnk_size):
            chnk_counter += 1
            times = [b['time_created'] for b in batch_group_group]
            min_batch = min(times)
            max_batch = max(times)

            if len(batches) > 100:
                logger.info(
                    f' Getting jobs for batch chunk {chnk_counter}/{nchnks} '
                    f'[{min_batch}, {max_batch}]'
                )

            promises = [
                utils.get_jobs_for_batch(b['id'], token) for b in batch_group_group
            ]
            jobs_in_batch.extend(await asyncio.gather(*promises))

        for batch, jobs in zip(btch_grp, jobs_in_batch):
            batch['jobs'] = jobs
            entries.extend(get_finalised_entries_for_batch(batch))

        # Give hail batch a break :sweat:
        await asyncio.sleep(1)

        # Insert new rows into aggregation table
        entries.extend(utils.get_hail_credits(entries))
        logger.info(f'Inserting {len(entries)} entries')
        result += utils.insert_new_rows_in_table(
            table=utils.GCP_AGGREGATE_DEST_TABLE, obj=entries
        )

    return result


def from_request(request):
    """
    From request object, get start and end time if present
    """
    start, end = utils.get_start_and_end_from_request(request)
    asyncio.new_event_loop().run_until_complete(main(start, end))


def from_pubsub(data=None, _=None):
    """
    From pubsub message, get start and end time if present
    """
    start, end = utils.get_start_and_end_from_data(data)
    asyncio.new_event_loop().run_until_complete(main(start, end))


async def main(start: datetime = None, end: datetime = None) -> int:
    """Main body function"""

    start, end = utils.process_default_start_and_end(start, end)

    hail_token = utils.get_hail_token()

    # Migrate the data in batches
    result = await migrate_hail_data(start, end, hail_token)

    logging.info(f'Migrated a total of {result} rows')

    return result


if __name__ == '__main__':
    test_start, test_end = None, None
    # test_start, test_end = datetime(2022, 4, 1), datetime(2022, 5, 3)

    asyncio.new_event_loop().run_until_complete(main(start=test_start, end=test_end))
