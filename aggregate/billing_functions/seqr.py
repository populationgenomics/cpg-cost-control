# pylint: disable=logging-format-interpolation
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


# write some code here

import json
import math
import hashlib
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any

import asyncio

import google.cloud.bigquery as bq

from sample_metadata.apis import SampleApi, ProjectApi, AnalysisApi
from sample_metadata.model.analysis_type import AnalysisType
from sample_metadata.model.analysis_status import AnalysisStatus
from sample_metadata.model.analysis_query_model import AnalysisQueryModel

try:
    from . import utils
except ImportError:
    import utils


ProportionateMapType = List[Tuple[datetime, Dict[str, float]]]

SERVICE_ID = 'seqr'
SEQR_HAIL_BILLING_PROJECT = 'seqr'


GCP_BILLING_BQ_TABLE = (
    'billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343'
)
DESTINATION_TABLE = 'sabrina-dev-337923.billing.aggregate'

BASE = 'https://batch.hail.populationgenomics.org.au'
BATCHES_API = BASE + '/api/v1alpha/batches'
JOBS_API = BASE + '/api/v1alpha/batches/{batch_id}/jobs/resources'

logger = utils.logger

papi = ProjectApi()
sapi = SampleApi()
aapi = AnalysisApi()


def get_seqr_datasets():
    """
    Get Hail billing projects, same names as dataset
    TOOD: Implement
    """

    # response = requests.get(
    #     'https://raw.githubusercontent.com/populationgenomics/
    #          analysis-runner/main/stack/Pulumi.seqr.yaml'
    # )
    # response.raise_for_status()

    # return response.text()['datasets']

    return [
        'acute-care',
        'perth-neuro',
        # 'rdnow',
        'heartkids',
        'ravenscroft-rdstudy',
        # 'ohmr3-mendelian',
        # 'ohmr4-epilepsy',
        # 'flinders-ophthal',
        'circa',
        # 'schr-neuro',
        # 'brain-malf',
        # 'leukodystrophies',
        # 'mito-disease',
        'ravenscroft-arch',
        'hereditary-neuro',
    ]


def from_request(request):
    """
    From request object, get start and end time if present
    """
    start, end = utils.get_start_and_end_from_request(request)
    asyncio.new_event_loop().run_until_complete(main(start, end))


def get_ratios_from_date(
    date: datetime, prop_map: ProportionateMapType
) -> Dict[str, float]:
    """
    Get ratios for a date
    """
    assert isinstance(date, datetime)
    for dt, m in prop_map[::-1]:
        # first entry BEFORE the date
        if dt <= date:
            return m

    raise AssertionError(f'No ratio found for date {date}')


def from_pubsub(data=None, _=None):
    """
    From pubsub message, get start and end time if present
    """
    start, end = utils.get_start_and_end_from_data(data)
    asyncio.new_event_loop().run_until_complete(main(start, end))


async def migrate_entries_from_hail_batch(
    start: datetime, end: datetime, proportion_map: ProportionateMapType, dry_run=False
) -> int:
    """
    Migrate all the seqr entries from hail batch,
    and insert them into the aggregate table.

    Break them down by dataset, and then proportion the rest of the costs.
    """
    token = utils.get_hail_token()
    result = 0

    batches = await utils.get_finished_batches_for_date(
        start=start, end=end, token=token, billing_project=SEQR_HAIL_BILLING_PROJECT
    )
    if len(batches) == 0:
        return 0

    # we'll process 500 batches, and insert all entries for that
    chnk_counter = 0
    final_chnk_size = 30
    nchnks = math.ceil(len(batches) / 500) * final_chnk_size
    jobs_in_batch = []
    for btch_grp in utils.chunk(batches, 500):
        jobs_in_batch = []
        entries = []

        for batch_group_group in utils.chunk(btch_grp, final_chnk_size):
            chnk_counter += 1
            times = [b['time_created'] for b in batch_group_group]
            min_batch = min(times)
            max_batch = max(times)

            if len(batches) > 100:
                logger.info(
                    f'seqr :: Getting jobs for batch chunk {chnk_counter}/{nchnks} '
                    f'[{min_batch}, {max_batch}]'
                )

            promises = [
                utils.get_jobs_for_batch(b['id'], token) for b in batch_group_group
            ]
            jobs_in_batch.extend(await asyncio.gather(*promises))

        # insert all entries for this batch
        for batch, jobs in zip(btch_grp, jobs_in_batch):
            batch['jobs'] = jobs

            entries.extend(get_finalised_entries_for_batch(batch, proportion_map))

        if dry_run:
            result = len(entries)
        else:
            result += utils.insert_new_rows_in_table(
                table=utils.GCP_AGGREGATE_DEST_TABLE, obj=entries
            )

    return result


def get_finalised_entries_for_batch(
    batch, proportion_map: ProportionateMapType
) -> List[Dict[str, Any]]:
    """
    Get finalised entries for a batch
    """

    batch_id = batch['id']
    batch_name = batch.get('attributes', {}).get('name')

    start_time = utils.parse_hail_time(batch['time_created'])
    end_time = utils.parse_hail_time(batch['time_completed'])
    prop_map = get_ratios_from_date(start_time, proportion_map)

    currency_conversion_rate = utils.get_currency_conversion_rate_for_time(start_time)

    jobs_with_no_dataset: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    entries: List[Dict[str, Any]] = []

    for job in batch['jobs']:
        dataset = job['attributes'].get('dataset', '').replace('-test', '')
        if not dataset:
            jobs_with_no_dataset.append(job)
            continue

        sample = job['attributes'].get('sample')
        job_name = job['attributes'].get('name')

        hail_ui_url = utils.HAIL_UI_URL.replace('{batch_id}', str(batch_id))

        labels = {
            'dataset': dataset,
            'job_name': job_name,
            'batch_name': batch_name,
            'batch_id': str(batch_id),
            'job_id': job['job_id'],
        }
        if sample:
            labels['sample'] = sample

        resources = job['resources']
        if not resources:
            resources = {}

        for batch_resource, usage in resources.items():
            if batch_resource.startswith('service-fee'):
                continue

            labels['batch_resource'] = batch_resource
            labels['url'] = hail_ui_url

            cost = currency_conversion_rate * utils.get_usd_cost_for_resource(
                batch_resource, usage
            )

            entries.append(
                utils.get_entry(
                    key=get_key_from_batch_job(dataset, batch, job, batch_resource),
                    topic=dataset,
                    service_id=SERVICE_ID,
                    description='Seqr compute',
                    cost=cost,
                    currency_conversion_rate=currency_conversion_rate,
                    usage=usage,
                    batch_resource=batch_resource,
                    start_time=start_time,
                    end_time=end_time,
                    labels=labels,
                )
            )

    extra_job_resources = defaultdict(int)
    for job in jobs_with_no_dataset:

        for batch_resource, usage in job['resources'].items():
            if batch_resource.startswith('service-fee'):
                continue

            extra_job_resources[batch_resource] += usage

        for batch_resource, usage in extra_job_resources.items():

            batch_id = batch['id']
            hail_ui_url = utils.HAIL_UI_URL.replace('{batch_id}', str(batch_id))

            labels = {
                'batch_name': batch_name,
                'batch_id': str(batch_id),
            }
            labels['batch_resource'] = batch_resource
            labels['url'] = hail_ui_url

            gross_cost = currency_conversion_rate * utils.get_usd_cost_for_resource(
                batch_resource, usage
            )

            for dataset, fraction in prop_map.items():
                # Distribute the remaining cost across all datasets proportionately
                cost = gross_cost * fraction

                entries.append(
                    utils.get_entry(
                        key='-'.join(
                            [
                                SERVICE_ID,
                                'distributed',
                                dataset,
                                batch_resource,
                                batch_id,
                            ]
                        ),
                        topic=dataset,
                        service_id=SERVICE_ID,
                        description='Seqr compute (distributed)',
                        cost=cost,
                        currency_conversion_rate=currency_conversion_rate,
                        usage=round(usage * fraction),
                        batch_resource=batch_resource,
                        start_time=start_time,
                        end_time=end_time,
                        labels={
                            **labels,
                            'dataset': dataset,
                            'fraction': round(100 * fraction) / 100,
                        },
                    )
                )

    entries.extend(utils.get_hail_credits(entries))

    return entries


def migrate_entries_from_bq(
    start, end, prop_map: ProportionateMapType, dry_run=False
) -> int:
    """
    Migrate entries from BQ to GCP, using the given proportionate maps
    """
    # pylint: disable=too-many-branches
    _query = f"""
        SELECT * FROM `{GCP_BILLING_BQ_TABLE}`
        WHERE export_time >= @start
            AND export_time <= @end
            AND project.id = @project
        ORDER BY usage_start_time
    """

    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ScalarQueryParameter('start', 'STRING', str(start)),
            bq.ScalarQueryParameter('end', 'STRING', str(end)),
            bq.ScalarQueryParameter('project', 'STRING', 'seqr-308602'),
        ]
    )

    df = (
        utils.bigquery_client.query(_query, job_config=job_config)
        .result()
        .to_dataframe()
    )
    json_obj = json.loads(df.to_json(orient='records'))

    entries_by_id = {}
    result = 0
    param_map, current_date = None, None
    for obj in json_obj:

        if obj['cost'] == 0:
            # come on now
            continue
        usage_start_time = datetime.fromtimestamp(int(obj['usage_start_time'] / 1000))
        del obj['billing_account_id']
        del obj['project']
        if current_date is None or usage_start_time > current_date:
            # use and abuse the
            param_map = get_ratios_from_date(usage_start_time, prop_map)

        labels = obj['labels']

        for dataset, ratio in param_map.items():

            new_entry = deepcopy(obj)

            new_entry['topic'] = dataset
            new_entry['service']['id'] = SERVICE_ID
            new_entry['labels'] = [*labels, {'key': 'proportion', 'value': ratio}]
            new_entry['cost'] *= ratio

            dates = ['usage_start_time', 'usage_end_time', 'export_time']
            for k in dates:
                new_entry[k] = utils.to_bq_time(
                    datetime.fromtimestamp(int(obj[k] / 1000))
                )

            new_entry['id'] = '-'.join(
                [SERVICE_ID, dataset, billing_row_to_key(new_entry)]
            )

            if new_entry['id'] in entries_by_id:
                if new_entry != entries_by_id[new_entry['id']]:
                    logger.warning(
                        f'WARNING: duplicate entry {new_entry["id"]} with diff values'
                    )
                continue

            entries_by_id[new_entry['id']] = new_entry

        if len(entries_by_id) > 10000:

            # insert all entries here
            credit_entries = {
                e['id']: e for e in utils.get_seqr_credits(entries_by_id.values())
            }
            entries_by_id.update(credit_entries)

            if len(entries_by_id) != 2 * len(credit_entries):
                logger.warning('Mismatched length of credits')
            if dry_run:
                result += len(entries_by_id)
            else:
                result += utils.insert_new_rows_in_table(
                    table=utils.GCP_AGGREGATE_DEST_TABLE, obj=entries_by_id.values()
                )
            entries_by_id = {}

    if entries_by_id:
        entries_by_id.update(
            {e['id']: e for e in utils.get_seqr_credits(entries_by_id.values())}
        )
        if dry_run:
            result += len(entries_by_id)
        else:
            result += utils.insert_new_rows_in_table(
                table=utils.GCP_AGGREGATE_DEST_TABLE, obj=entries_by_id.values()
            )

    return result


def billing_row_to_key(row) -> str:
    """Convert a billing row to a hash which will be the row key"""
    identifier = hashlib.md5()
    for k, v in row.items():
        identifier.update((k + str(v)).encode('utf-8'))

    return identifier.hexdigest()


async def generate_proportionate_map_of_dataset(
    start, finish, projects: List[str]
) -> ProportionateMapType:
    """
    Generate a proportionate map of datasets from list of samples
    in the relevant joint-calls (< 2022-06-01) or es-index (>= 2022-06-01)
    """

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

    proportioned_datasets: ProportionateMapType = []

    s = datetime.now()
    for analysis in relevant_analysis:

        # Using timestamp_completed as the start time for the propmap
        # is a small problem because then this script won't charge the new samples
        # for the current joint-call as:
        #   joint_call.completed_timestamp > hail_joint_call.started_timestamp
        # We might be able to roughly accept this by subtracting a day from the
        # joint-call, and sort of hope that no joint-call runs over 24 hours.
        dt = datetime.fromisoformat(analysis['timestamp_completed']) - timedelta(days=1)
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
        proportioned_datasets.append(
            (
                dt,
                {
                    project_id_to_name[project_id]: size / total_size
                    for project_id, size in size_per_project.items()
                },
            )
        )

    if missing_samples:
        print('Missing crams: ' + ', '.join(missing_samples))
    if missing_sizes:
        print(
            'Missing sizes: '
            + ', '.join(f'{k} ({v})' for k, v in missing_sizes.items())
        )

    return sorted(proportioned_datasets, key=lambda x: x[0])


async def main(start: datetime = None, end: datetime = None, dry_run=False):
    """Main body function"""
    start, end = utils.process_default_start_and_end(start, end)

    projects = get_seqr_datasets()

    prop_map = await generate_proportionate_map_of_dataset(start, end, projects)
    result = 0
    # result += migrate_entries_from_bq(start, end, prop_map, dry_run=dry_run)
    result += await migrate_entries_from_hail_batch(
        start, end, prop_map, dry_run=dry_run
    )

    if dry_run:
        print(f'Finished dry run, would have inserted {result} entries')
    else:
        print(f'Inserted {result} entries')


def get_key_from_batch_job(dataset, batch, job, batch_resource):
    """Get unique key for entry from params"""
    assert dataset is not None
    sample = job.get('attributes', {}).get('sample')

    key_components = [SERVICE_ID]
    if dataset:
        key_components.append(dataset)
        if sample:
            key_components.append(sample)

    key_components.extend(['batch', str(batch['id']), 'job', str(job['job_id'])])
    key_components.append(batch_resource)

    return '-'.join(key_components)


if __name__ == '__main__':
    test_start, test_end = None, datetime.now()
    # test_start, test_end = datetime(2022, 5, 2), datetime(2022, 5, 5)

    asyncio.new_event_loop().run_until_complete(
        main(start=test_start, end=test_end, dry_run=False)
    )
