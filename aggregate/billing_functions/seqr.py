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
import asyncio
import hashlib
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta

import yaml
import requests

import google.cloud.bigquery as bq

from sample_metadata.apis import SampleApi, ProjectApi, AnalysisApi
from sample_metadata.model.analysis_type import AnalysisType
from sample_metadata.model.analysis_status import AnalysisStatus
from sample_metadata.model.analysis_query_model import AnalysisQueryModel

try:
    from . import utils
except ImportError:
    import utils


# ie: [(datetime, {[dataset]: fractional_breakdown})}
# eg: [('2022-01-06', {'d1': 0.3, 'd2': 0.5, 'd3': 0.2})]
ProportionateMapType = list[tuple[datetime, dict[str, float]]]

SERVICE_ID = 'seqr'
SEQR_HAIL_BILLING_PROJECT = 'seqr'
ES_ANALYSIS_OBJ_INTRO_DATE = datetime(2022, 6, 30)

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


def get_finalised_entries_for_batch(
    batch, proportion_map: ProportionateMapType
) -> list[dict[str, any]]:
    """
    Take a batch dictionary, and the full proportion map
    and return a list of entries for the Hail batch.
    """

    batch_id = batch['id']
    batch_name = batch.get('attributes', {}).get('name')

    start_time = utils.parse_hail_time(batch['time_created'])
    end_time = utils.parse_hail_time(batch['time_completed'])
    _, prop_map = get_ratios_from_date(start_time, proportion_map)

    currency_conversion_rate = utils.get_currency_conversion_rate_for_time(start_time)

    jobs_with_no_dataset: list[tuple[dict[str, any], dict[str, any]]] = []
    entries: list[dict[str, any]] = []

    for job in batch['jobs']:
        dataset = job['attributes'].get('dataset', '').replace('-test', '')
        if not dataset:
            jobs_with_no_dataset.append(job)
            continue

        entries.extend(
            get_finalised_entries_for_dataset_batch_and_job(
                dataset=dataset,
                batch_id=batch_id,
                batch_name=batch_name,
                batch_start_time=start_time,
                batch_end_time=end_time,
                job=job,
                currency_conversion_rate=currency_conversion_rate,
            )
        )

    # Now go through each job within the batch withOUT a dataset
    # and proportion a fraction of them to each relevant dataset.
    for job in jobs_with_no_dataset:

        job_id = job['job_id']
        if not job['resources']:
            continue

        for batch_resource, usage in job['resources'].items():
            if batch_resource.startswith('service-fee'):
                continue

            hail_ui_url = utils.HAIL_UI_URL.replace('{batch_id}', str(batch_id))

            labels = {
                'batch_name': batch_name,
                'batch_id': str(batch_id),
                'batch_resource': batch_resource,
                'url': hail_ui_url,
            }

            gross_cost = currency_conversion_rate * utils.get_usd_cost_for_resource(
                batch_resource, usage
            )

            for dataset, fraction in prop_map.items():
                # Distribute the remaining cost across all datasets proportionately
                key = '-'.join(
                    (
                        SERVICE_ID,
                        'distributed',
                        dataset,
                        'batch',
                        str(batch_id),
                        'job',
                        str(job_id),
                        batch_resource,
                    )
                )
                entries.append(
                    utils.get_hail_entry(
                        key=key,
                        topic=dataset,
                        service_id=SERVICE_ID,
                        description='Seqr compute (distributed)',
                        cost=gross_cost * fraction,
                        currency_conversion_rate=currency_conversion_rate,
                        usage=round(usage * fraction),
                        batch_resource=batch_resource,
                        start_time=start_time,
                        end_time=end_time,
                        labels={
                            **labels,
                            'dataset': dataset,
                            # awkward way to round to 2 decimal places in Python
                            'fraction': round(100 * fraction) / 100,
                        },
                    )
                )

    entries.extend(
        utils.get_credits(entries, topic='hail', project=utils.HAIL_PROJECT_FIELD)
    )

    return entries


def get_finalised_entries_for_dataset_batch_and_job(
    dataset: str,
    batch_id: int,
    batch_name: str,
    batch_start_time: datetime,
    batch_end_time: datetime,
    job: dict[str, any],
    currency_conversion_rate: float,
):
    """
    Get the list of entries for a dataset attributed job

    """
    entries = []

    job_id = job['job_id']
    job_name = job['attributes'].get('name')
    sample = job['attributes'].get('sample')

    hail_ui_url = utils.HAIL_UI_URL.replace('{batch_id}', str(batch_id))

    labels = {
        'dataset': dataset,
        'job_name': job_name,
        'batch_name': batch_name,
        'batch_id': str(batch_id),
        'job_id': str(job_id),
    }
    if sample:
        labels['sample'] = sample

    resources = job['resources']
    if not resources:
        return []

    for batch_resource, usage in resources.items():
        if batch_resource.startswith('service-fee'):
            continue

        labels['batch_resource'] = batch_resource
        labels['url'] = hail_ui_url

        cost = currency_conversion_rate * utils.get_usd_cost_for_resource(
            batch_resource, usage
        )

        key = '-'.join(
            (
                SERVICE_ID,
                dataset,
                'batch',
                str(batch_id),
                'job',
                str(job_id),
                batch_resource,
            )
        )
        entries.append(
            utils.get_hail_entry(
                key=key,
                topic=dataset,
                service_id=SERVICE_ID,
                description='Seqr compute',
                cost=cost,
                currency_conversion_rate=currency_conversion_rate,
                usage=usage,
                batch_resource=batch_resource,
                start_time=batch_start_time,
                end_time=batch_end_time,
                labels=labels,
            )
        )

    return entries


def migrate_entries_from_bq(
    start: datetime,
    end: datetime,
    prop_map: ProportionateMapType,
    dry_run: bool = False,
) -> int:
    """
    Migrate entries from BQ to GCP, using the given proportionate maps
    """

    logger.debug('Migrating seqr data to BQ')
    for istart, iend in utils.get_date_intervals_for(start, end):
        logger.info(
            f'Migrating seqr BQ data [{istart.isoformat()}, {iend.isoformat()}]'
        )
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
                bq.ScalarQueryParameter('start', 'STRING', str(istart)),
                bq.ScalarQueryParameter('end', 'STRING', str(iend)),
                bq.ScalarQueryParameter('project', 'STRING', 'seqr-308602'),
            ]
        )

        df = (
            utils.get_bigquery_client()
            .query(_query, job_config=job_config)
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

            del obj['billing_account_id']
            del obj['project']
            labels = obj['labels']

            usage_start_time = datetime.fromtimestamp(
                int(obj['usage_start_time'] / 1000)
            )

            if current_date is None or usage_start_time > current_date:
                # use and abuse the
                current_date, param_map = get_ratios_from_date(
                    date=usage_start_time, prop_map=prop_map
                )

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

                nid = '-'.join([SERVICE_ID, dataset, billing_obj_to_key(new_entry)])

                new_entry['id'] = nid
                if nid in entries_by_id:
                    if new_entry != entries_by_id[nid]:

                        logger.warning(
                            f'WARNING: duplicate entry {nid} with diff values'
                        )
                    continue

                entries_by_id[nid] = new_entry

        # insert all entries here
        entries = list(entries_by_id.values())
        entries.extend(
            utils.get_credits(
                entries_by_id.values(),
                topic='seqr',
                project=utils.SEQR_PROJECT_FIELD,
            )
        )

        result += utils.insert_new_rows_in_table(
            table=utils.GCP_AGGREGATE_DEST_TABLE,
            objs=entries,
            dry_run=dry_run,
        )

    return result


def billing_obj_to_key(obj: dict[str, any]) -> str:
    """Convert a billing row to a hash which will be the row key"""
    identifier = hashlib.md5()
    for k, v in obj.items():
        identifier.update((k + str(v)).encode('utf-8'))

    return identifier.hexdigest()


# Proportionate map functions


async def generate_proportionate_map_of_dataset(
    start: datetime, end: datetime, projects: list[str]
) -> ProportionateMapType:
    """
    Generate a proportionate map of datasets from list of samples
    in the relevant joint-calls (< 2022-06-01) or es-index (>= 2022-06-01)
    """

    # pylint: disable=too-many-locals

    # from 2022-06-01, we use it based on es-index, otherwise joint-calling
    relevant_analysis = []
    sm_projects = await papi.get_all_projects_async()
    sm_pid_to_dataset = {p['id']: p['dataset'] for p in sm_projects}

    if end > ES_ANALYSIS_OBJ_INTRO_DATE:
        es_indices = await aapi.query_analyses_async(
            AnalysisQueryModel(
                type=AnalysisType('es-index'),
                status=AnalysisStatus('completed'),
                projects=['seqr'],
            )
        )
        relevant_analysis.extend(a for a in es_indices)

    start_es_date = None
    if relevant_analysis:
        start_es_date = min(
            datetime.fromtimestamp(a['timestamp_completed']) for a in relevant_analysis
        )

    # if there are no ES-indices, or es-indices don't cover the range,
    # then full in the remainder of the range
    if not start_es_date or start < start_es_date:

        joint_calls = await aapi.query_analyses_async(
            AnalysisQueryModel(
                type=AnalysisType('joint-calling'),
                status=AnalysisStatus('completed'),
                projects=['seqr'],
            )
        )

        jc_end = start_es_date or end

        relevant_analysis.extend(
            a
            for a in joint_calls
            if datetime.fromisoformat(a['timestamp_completed']) <= jc_end
        )

    relevant_analysis = sorted(
        relevant_analysis, key=lambda a: a['timestamp_completed']
    )

    for idx in range(len(relevant_analysis) - 1, -1, -1):
        if (
            datetime.fromisoformat(relevant_analysis[idx]['timestamp_completed'])
            < start
        ):
            relevant_analysis = relevant_analysis[idx:]
            break

    all_samples = set(s for a in relevant_analysis for s in a['sample_ids'])

    filtered_projects = list(set(sm_pid_to_dataset.values()).intersection(projects))
    missing_projects = set(projects) - set(sm_pid_to_dataset.values())
    if missing_projects:
        logger.warning(
            f'The datasets {", ".join(missing_projects)} were not found in SM'
        )

    crams = await aapi.query_analyses_async(
        AnalysisQueryModel(
            sample_ids=list(all_samples),
            type=AnalysisType('cram'),
            projects=filtered_projects,
            status=AnalysisStatus('completed'),
        )
    )

    # Crams in SM only have one sample_id, so easy to link
    # the cram back to the specific internal sample ID
    cram_map = {c['sample_ids'][0]: c for c in crams}

    missing_samples = set()
    missing_sizes = {}

    proportioned_datasets: ProportionateMapType = []

    for analysis in relevant_analysis:

        # Using timestamp_completed as the start time for the propmap
        # is a small problem because then this script won't charge the new samples
        # for the current joint-call as:
        #   joint_call.completed_timestamp > hail_joint_call.started_timestamp
        # We might be able to roughly accept this by subtracting a day from the
        # joint-call, and sort of hope that no joint-call runs over 24 hours.
        dt = datetime.fromisoformat(analysis['timestamp_completed']) - timedelta(days=2)
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

        # Size is in bytes, and plain Python int type is unbound
        total_size = sum(size_per_project.values())
        proportioned_datasets.append(
            (
                dt,
                {
                    sm_pid_to_dataset[project_id]: size / total_size
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

    # We'll sort ASC, which makes it easy to find the relevant entry later
    return proportioned_datasets


def get_ratios_from_date(
    date: datetime, prop_map: ProportionateMapType
) -> tuple[datetime, dict[str, float]]:
    """
    From the prop_map, get the ratios for the applicable date.

    >>> get_ratios_from_date(
    ...     datetime(2020, 1, 1),
    ...     [(datetime(2019,12,31), {'d1': 1.0})]
    ... )
    (datetime.datetime(2019, 12, 31, 0, 0), {'d1': 1.0})

    >>> get_ratios_from_date(
    ...     datetime(2020, 1, 13),
    ...     [(datetime(2019,12,31), {'d1': 1.0}), (datetime(2020,1,12), {'d1': 1.0})]
    ... )
    (datetime.datetime(2020, 1, 12, 0, 0), {'d1': 1.0})

    >>> get_ratios_from_date(
    ...     datetime(2020, 1, 1), [(datetime(2020,1,2), {'d1': 1.0})]
    ... )
    Traceback (most recent call last):
    ...
    AssertionError: No ratio found for date 2020-01-01 00:00:00
    """
    assert isinstance(date, datetime)

    # prop_map is sorted ASC by date, so we
    # can find the latest element that is <= date
    for dt, m in prop_map[::-1]:
        # first entry BEFORE the date
        if dt <= date:
            return dt, m

    raise AssertionError(f'No ratio found for date {date}')


# UTIL specific to seqr billing


def get_seqr_datasets() -> list[str]:
    """
    Get Hail billing projects, same names as dataset
    """

    response = requests.get(
        'https://raw.githubusercontent.com/populationgenomics/'
        'analysis-runner/main/stack/Pulumi.seqr.yaml'
    )
    response.raise_for_status()

    d = yaml.safe_load(response.text)
    datasets = json.loads(d['config']['datasets:depends_on'])
    return datasets


# DRIVER functions


async def main(start: datetime = None, end: datetime = None, dry_run=False):
    """Main body function"""
    start, end = utils.process_default_start_and_end(start, end)

    projects = get_seqr_datasets()

    prop_map = await generate_proportionate_map_of_dataset(start, end, projects)
    result = 0

    result += migrate_entries_from_bq(start, end, prop_map, dry_run=dry_run)

    def func_get_finalised_entries(batch):
        return get_finalised_entries_for_batch(batch, prop_map)

    result += await utils.migrate_entries_from_hail_in_chunks(
        start=start,
        end=end,
        billing_project=SEQR_HAIL_BILLING_PROJECT,
        func_get_finalised_entries_for_batch=func_get_finalised_entries,
        dry_run=dry_run,
    )

    if dry_run:
        print(f'Finished dry run, would have inserted {result} entries')
    else:
        print(f'Inserted {result} entries')


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


if __name__ == '__main__':
    test_start, test_end = datetime(2022, 5, 25), None
    # test_start, test_end = datetime(2022, 5, 2), datetime(2022, 5, 5)

    asyncio.new_event_loop().run_until_complete(
        main(start=test_start, end=test_end, dry_run=True)
    )
