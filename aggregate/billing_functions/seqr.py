"""
This cloud function runs DAILY, and distributes the cost of
SEQR on the sample size within SEQR.

- It first pulls the cost of the seqr project (relevant components within it):
    - Elasticsearch, instance cost
    - Cost of loading data into seqr might be difficult:
        - At the moment this in dataproc, so covered by previous result
        - Soon to move to Hail Query, which means we have to query hail
            to get the 'cost of loading'
- It determines the relative heuristic (of all projects loaded into seqr):
    - EG: 'relative size of GVCFs by project', eg:
        - $DATASET has 12GB of GVCFs of a total 86GB of all seqr GVCFs,
        - therefore it's relative heuristic is 12/86 = 0.1395,
            and share of seqr cost is 13.95%

- Insert rows in aggregate cost table for each of these costs:
    - The service.id should be 'seqr' (or something similar)
    - Maybe the label could include the relative heuristic


TO DO :

- Add cram size to SM
- Ensure getting latest joint call is split by sequence type,
    or some other metric (exome vs genome)
- Getting latest cram for sample by sequence type (eg: exome / genome)
"""

from collections import defaultdict
import os
import math
import json
from datetime import datetime, timedelta, timezone

import asyncio
import requests
import pandas as pd
from google.cloud import bigquery
import sample_metadata as sm


from .utils import (
    logger,
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
    GCP_AGGREGATE_DEST_TABLE,
)

SERVICE_ID = 'seqr'
SEQR_HAIL_BILLING_PROJECT = 'seqr'

GCP_BILLING_BQ_TABLE = (
    'billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343'
)
DESTINATION_TABLE = 'sabrina-dev-337923.billing.aggregate'

BASE = 'https://batch.hail.populationgenomics.org.au'
BATCHES_API = BASE + '/api/v1alpha/batches'
JOBS_API = BASE + '/api/v1alpha/batches/{batch_id}/jobs/resources'

bigquery_client = bigquery.Client()


def get_datasets():
    """
    Get Hail billing projects, same names as dataset
    TOOD: Implement
    """
    return ['acute-care', 'perth-neuro']


def get_sample_fractional_breakdown_by_dataset(time: datetime):
    """
    Get the fractional breakdown of samples by dataset.
    """
    latest_joint_call = sm.AnalysisApi().get_latest_complete_analysis_for_type(
        project='seqr',
        analysis_type='joint_call',
        # TODO: time=time ??
    )

    samples_list = sm.SampleApi().get_samples_by_criteria(
        {'sample_ids': latest_joint_call['sample_ids']}
    )

    project_ids = {p['name']: p['id'] for p in sm.ProjectApi().get_my_projects()}
    counter = defaultdict(int)
    for sample in samples_list:
        counter[project_ids[sample['project_id']]] += 1

    return {p: c / len(samples_list) for p, c in counter.items()}


def from_request(request):
    """
    From request object, get start and end time if present
    """
    start, end = get_start_and_end_from_request(request)
    asyncio.get_event_loop().run_until_complete(main(start, end))


async def main(start: datetime = None, end: datetime = None):
    """Main body function"""
    start, end = process_default_start_and_end(start, end)

    token = get_hail_token()

    batches = await get_finished_batches_for_date(
        SEQR_HAIL_BILLING_PROJECT, start, end, token
    )
    if len(batches) == 0:
        return []

    jobs_in_batch = []
    for chnk, btch_grp in enumerate(chunk(batches, 100)):
        if len(batches) > 100:
            logger.info(
                f'seqr :: Getting jobs for chunk {chnk+1}/{math.ceil(len(batches)/100)}'
            )
        promises = [get_jobs_for_batch(b['id'], token) for b in btch_grp]
        jobs_in_batch.extend(await asyncio.gather(*promises))
    for batch, jobs in zip(batches, jobs_in_batch):
        batch['jobs'] = jobs

    entry_items = []
    jobs_with_no_dataset = []
    for batch in batches:
        for job in batch['jobs']:
            dataset = job['attributes'].get('dataset')
            if not dataset:
                jobs_with_no_dataset.append(job)
                continue

            sample = job['attributes'].get('sample')
            name = job['attributes'].get('name')

            labels = [
                {'name': 'dataset', 'value': dataset},
                {'name': 'name', 'value': name},
            ]
            if sample:
                labels.append({'name': 'sample', 'value': sample})

            for batch_resource, usage in job['resources']:
                # batch_resource is one of ['cpu', 'memory', 'disk']

                key = get_key_from_batch_job(batch, job, batch_resource)

                currency_conversion_rate = get_currency_conversion_rate_for_time(
                    job['start_time']
                )
                cost = currency_conversion_rate * get_usd_cost_for_resource(
                    batch_resource, usage
                )

                entry_items.append(
                    {
                        'id': key,
                        'dataset': dataset,
                        'service': {
                            'id': key,
                            'description': 'SEQR processing (no-aggregated)',
                        },
                        'sku': {
                            'id': f'hail-{batch_resource}',
                            'description': f'{batch_resource} usage',
                        },
                        'usage_start_time': job['start_time'],
                        'usage_end_time': job['finish_time'],
                        'labels': labels,
                        'system_labels': [],
                        'location': {
                            'location': 'australia-southeast1',
                            'country': 'Australia',
                            'region': 'australia',
                            'zone': None,
                        },
                        'export_time': datetime.now().isoformat(),
                        'cost': cost,
                        'currency': 'AUD',
                        'currency_conversion_rate': currency_conversion_rate,
                        'usage': {
                            'amount': usage,
                            'unit': get_unit_for_batch_resource_type(batch_resource),
                            'amount_in_pricing_units': cost,
                            'pricing_unit': 'AUD',
                        },
                        'invoice': {
                            'month': job['start_time'].month + job['start_time'].year
                        },
                        'cost_type': 'regular',
                    }
                )

    # Insert new rows into aggregation table
    insert_new_rows_in_table(table=GCP_AGGREGATE_DEST_TABLE, obj=entry_items)


def get_key_from_batch_job(batch, job, batch_resource):
    """Get unique key for entry from params"""
    dataset = job.get('attributes', {}).get('dataset')
    sample = job.get('attributes', {}).get('sample')

    key_components = [SERVICE_ID]
    if dataset:
        key_components.append(dataset)
        if sample:
            key_components.append(sample)

    key_components.extend(['batch', str(batch['id']), 'job', str(job['id'])])
    key_components.append(batch_resource)

    return '-'.join(key_components)


if __name__ == '__main__':
    test_start, test_end = None, None
    # test_start, test_end = datetime(2022, 5, 2), datetime(2022, 5, 5)

    asyncio.get_event_loop().run_until_complete(main(start=test_start, end=test_end))
