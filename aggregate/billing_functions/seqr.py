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

from google.cloud import bigquery

from sample_metadata.apis import SampleApi, ProjectApi, AnalysisApi
from sample_metadata.model.sequence_type import SequenceType
from sample_metadata.model.analysis_type import AnalysisType
from sample_metadata.model.analysis_status import AnalysisStatus
from sample_metadata.model.analysis_query_model import AnalysisQueryModel

try:
    from .utils import *
except ImportError:
    from utils import *


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
papi = ProjectApi()
sapi = SampleApi()
aapi = AnalysisApi()


def get_datasets():
    """
    Get Hail billing projects, same names as dataset
    TOOD: Implement
    """
    return [
        "acute-care",
        "perth-neuro",
        # "rdnow",
        "heartkids",
        "ravenscroft-rdstudy",
        # "ohmr3-mendelian",
        # "ohmr4-epilepsy",
        # "flinders-ophthal",
        "circa",
        # "schr-neuro",
        # "brain-malf",
        # "leukodystrophies",
        # "mito-disease",
        "ravenscroft-arch",
        "hereditary-neuro",
    ]


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


async def get_entries_from_hail(start: datetime, end: datetime):
    token = get_hail_token()

    batches = await get_finished_batches_for_date(
        SEQR_HAIL_BILLING_PROJECT, start, end, token
    )
    if len(batches) == 0:
        return []

    # we'll process 500 batches, and insert all entries for that
    chnk_counter = 0
    final_chnk_size = 30
    nchnks = math.ceil(len(batches) / 500) * final_chnk_size
    jobs_in_batch = []
    for btch_grp in chunk(batches, 500):
        entries = []
        jobs_in_batch = []

        for batch_group_group in chunk(btch_grp, final_chnk_size):
            chnk_counter += 1
            min_batch = min(batch_group_group, key=lambda b: b['time_created'])
            max_batch = max(batch_group_group, key=lambda b: b['time_created'])

            for batch in batch_group_group:
                # entries.extend(get_finalised_entries_for_batch('seqr', batch))
                jobs_in_batch.extend(batch['jobs'])
            if len(batches) > 100:
                logger.info(
                    f'seqr :: Getting jobs for batch chunk {chnk_counter}/{nchnks} [{min_batch}, {max_batch}]'
                )

            promises = [get_jobs_for_batch(b['id'], token) for b in batch_group_group]
            jobs_in_batch.extend(await asyncio.gather(*promises))

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
                        'topic': dataset,
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


async def generate_proportionate_map_of_dataset(start, finish, projects: List[str]):

    # from 2022-06-01, we use it based on es-index, otherwise joint-calling
    relevant_analysis = []
    sm_projects = await papi.get_all_projects_async()
    project_id_to_name = {p['id']: p['name'] for p in sm_projects}

    if start < datetime(2022, 6, 1):
        joint_calls = await aapi.query_analyses_async(
            AnalysisQueryModel(
                type=AnalysisType('joint-calling'),
                status=AnalysisStatus('completed'),
                projects=['seqr'],
            )
        )

        relevant_analysis.extend(joint_calls)

    elif finish > datetime(2022, 6, 1):
        es_indices = await aapi.query_analyses_async(
            AnalysisQueryModel(
                type=AnalysisType('es-index'),
                status=AnalysisStatus('completed'),
                projects=['seqr'],
            )
        )
        relevant_analysis.extend(es_indices)
    assert relevant_analysis
    sorted_calls = sorted(relevant_analysis, key=lambda x: x['timestamp_completed'])

    all_samples = set(s for a in relevant_analysis for s in a['sample_ids'])
    crams = await aapi.query_analyses_async(
        AnalysisQueryModel(
            sample_ids=list(all_samples),
            type=AnalysisType('cram'),
            projects=projects,
            status=AnalysisStatus('completed'),
        )
    )

    cram_map = {c['sample_ids'][0]: c for c in crams}

    missing_samples = set()
    missing_sizes = {}

    proportioned_datasets: Dict[datetime, Dict[str, float]] = {}
    for analysis in sorted_calls:
        dt = datetime.fromisoformat(analysis['timestamp_completed'])
        samples = analysis['sample_ids']

        size_per_project = defaultdict(int)
        for s in samples:
            cram = cram_map.get(s)
            if not cram:
                missing_samples.add(s)
                continue
            cram_size = cram['meta'].get('size')
            if not cram_size:
                missing_sizes[s] = cram['output']
            project_id = cram['project']
            size_per_project[project_id] += cram_size

        total_size = sum(size_per_project.values())
        proportioned_datasets[dt] = {
            project_id_to_name[project_id]: size / total_size
            for project_id, size in size_per_project.items()
        }

    print('Missing crams: ' + ', '.join(missing_samples))
    print('Missing sizes: ' + ', '.join(f'{k} ({v})' for k, v in missing_sizes.items()))

    return proportioned_datasets


async def main(start: datetime = None, end: datetime = None):
    """Main body function"""
    start, end = process_default_start_and_end(start, end)

    projects = get_datasets()

    prop_map = await generate_proportionate_map_of_dataset(start, end, projects)
    print(prop_map)

    entry_items = get_entries_from_hail(start, end)

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
