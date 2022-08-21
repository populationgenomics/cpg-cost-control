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
from datetime import datetime, timedelta, date

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
ES_ANALYSIS_OBJ_INTRO_DATE = datetime(2022, 12, 31)

SEQR_FIRST_LOAD = datetime(2021, 9, 1)

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
    attributes = batch.get('attributes', {})
    batch_name = attributes.get('name')

    start_time = utils.parse_hail_time(batch['time_created'])
    end_time = utils.parse_hail_time(batch['time_completed'])

    # Assign all seqr cost to seqr topic before first ever load
    # Otherwise, determine proportion cost across topics
    if start_time < SEQR_FIRST_LOAD:
        prop_map = {'seqr': 1.0}
    else:
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

            # Add all batch attributes, removing any duped labels
            labels.update(attributes)
            if labels.get('name'):
                labels.pop('name')

            # Remove any labels with falsey values e.g. None, '', 0
            labels = dict(filter(lambda l: l[1], labels.items()))

            gross_cost = utils.get_total_hail_cost(
                currency_conversion_rate, batch_resource, usage
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
        utils.get_credits(
            entries=entries, topic='hail', project=utils.HAIL_PROJECT_FIELD
        )
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

        cost = utils.get_total_hail_cost(
            currency_conversion_rate, batch_resource, usage
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

            # Assign all seqr cost to seqr topic before first ever load
            # Otherwise, determine proportion cost across topics
            if usage_start_time < SEQR_FIRST_LOAD:
                param_map = {'seqr': 1.0}
            elif current_date is None or usage_start_time > current_date:
                current_date, param_map = get_ratios_from_date(
                    dt=usage_start_time, prop_map=prop_map
                )

            for dataset, ratio in param_map.items():

                new_entry = {**obj}

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
                entries=entries_by_id.values(),
                topic='seqr',
                project=utils.SEQR_PROJECT_FIELD,
            )
        )

        result += utils.upsert_rows_into_bigquery(
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


async def generate_proportionate_maps_of_datasets(
    start: datetime, end: datetime, projects: list[str]
) -> tuple[ProportionateMapType, ProportionateMapType]:
    """
    Generate a proportionate map of datasets from list of samples
    in the relevant joint-calls (< 2022-06-01) or es-index (>= 2022-06-01)
    """

    # pylint: disable=too-many-locals
    sm_projects = await papi.get_all_projects_async()
    sm_pid_to_dataset = {p['id']: p['dataset'] for p in sm_projects}

    filtered_projects = list(set(sm_pid_to_dataset.values()).intersection(projects))
    missing_projects = set(projects) - set(sm_pid_to_dataset.values())
    if missing_projects:
        raise ValueError(
            f'The datasets {", ".join(missing_projects)} were not found in SM'
        )

    logger.info(f'Getting proportionate map for projects: {filtered_projects}')

    analyses, crams = await get_analysis_objects_and_crams_for_seqr_prop_map(
        start, end, filtered_projects
    )
    seqr_map = get_seqr_hosting_prop_map_from(
        analyses, crams, project_id_map=sm_pid_to_dataset
    )
    hail_map = get_shared_computation_prop_map(
        crams, project_id_map=sm_pid_to_dataset, min_datetime=start, max_datetime=end
    )

    return seqr_map, hail_map


async def get_analysis_objects_and_crams_for_seqr_prop_map(
    start: datetime, end: datetime, projects: list[str]
):
    """
    Fetch the relevant analysis objects + crams from sample-metadata
    to put together the proportionate_map.
    """
    # from 2022-06-01, we use it based on es-index, otherwise joint-calling
    timeit_start = datetime.now()
    relevant_analysis = []

    if end > ES_ANALYSIS_OBJ_INTRO_DATE:
        es_indices = await aapi.query_analyses_async(
            AnalysisQueryModel(
                type=AnalysisType('es-index'),
                status=AnalysisStatus('completed'),
                projects=['seqr'],
            )
        )
        relevant_analysis.extend(a for a in es_indices if a['timestamp_completed'])

    start_es_date = None
    if relevant_analysis:
        start_es_date = min(
            datetime.fromisoformat(a['timestamp_completed']) for a in relevant_analysis
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
            if a['timestamp_completed']
            and datetime.fromisoformat(a['timestamp_completed']) <= jc_end
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

    crams = await aapi.query_analyses_async(
        AnalysisQueryModel(
            type=AnalysisType('cram'),
            projects=projects,
            status=AnalysisStatus('completed'),
        )
    )

    # hackyish remove duplicates
    cram_output_paths_dedup: set[str] = set()
    deduped_crams = []
    for cram in crams:
        if cram['output'] in cram_output_paths_dedup:
            continue
        deduped_crams.append(cram)
        cram_output_paths_dedup.add(cram['output'])

    logger.debug(
        f'Took {(datetime.now() - timeit_start).total_seconds()} to get analyses'
    )

    return relevant_analysis, deduped_crams


def get_seqr_hosting_prop_map_from(
    relevant_analyses: list[dict], crams: list[dict], project_id_map: dict[int, str]
) -> ProportionateMapType:
    """From prefetched analysis, compute the proprtionate_map"""

    # Crams in SM only have one sample_id, so easy to link
    # the cram back to the specific internal sample ID
    cram_map = {c['sample_ids'][0]: c for c in crams}

    timeit_start = datetime.now()

    missing_samples = set()
    missing_sizes = {}

    proportioned_datasets: ProportionateMapType = []

    for analysis in relevant_analyses:

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
                    project_id_map[project_id]: size / total_size
                    for project_id, size in size_per_project.items()
                },
            )
        )

    logger.debug(
        f'Took {(datetime.now() - timeit_start).total_seconds()} to prepare prop map'
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


def get_shared_computation_prop_map(
    crams: list[dict],
    project_id_map: dict[str | int, str],
    min_datetime: datetime,
    max_datetime: datetime,
) -> ProportionateMapType:
    """
    This is a bit more complex, but for hail we want to proportion any downstream
    costs as soon as there CRAMs available. This kind of means we build up a
    continuous map of costs (but fragmented by days).

    We'll do this in three steps:

    1. First assign the cram a {dataset: total_size} map on the day cram was aligned.
        This generates a diff of each dataset by day.

    2. Iterate over the days, and progressively sum up the sizes in the map.

    3. Iterate over the days, and proportion each day by total size in the day.
    """

    # 1.
    by_date_diff: dict[date, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for cram in crams:
        tc = utils.parse_hail_time(cram.get('timestamp_completed'))
        tc = tc if tc else utils.parse_hail_time(cram.get('time_completed'))
        project_name = project_id_map[cram['project']]
        if tc < min_datetime:
            # avoid computation by capping dates to minimum
            tc = min_datetime
        elif tc > max_datetime:
            # skip any crams AFTER the max date
            continue
        by_date_diff[tc.date()][project_name] += cram['meta'].get('size')

    # 2: progressively sum up the sizes, prepping for step 3

    by_date_totals: list[tuple[date, dict[str, int]]] = []
    sorted_days = list(sorted(by_date_diff.items(), key=lambda el: el[0]))
    for idx, (dt, project_map) in enumerate(sorted_days):
        if idx == 0:
            by_date_totals.append((dt, project_map))
            continue

        new_project_map = {**by_date_totals[idx - 1][1]}

        for pn, cram_size in project_map.items():
            if pn not in new_project_map:
                new_project_map[pn] = cram_size
            else:
                new_project_map[pn] += cram_size

        by_date_totals.append((dt, new_project_map))

    # 3: proportion each day
    prop_map = []
    for dt, project_map in by_date_totals:
        total_size = sum(project_map.values())
        prop_project_map = {
            project_id: size / total_size for project_id, size in project_map.items()
        }
        prop_map.append((datetime.combine(dt, datetime.min.time()), prop_project_map))

    return prop_map


def get_ratios_from_date(
    dt: datetime, prop_map: ProportionateMapType
) -> tuple[datetime, dict[str, float]]:
    """
    From the prop_map, get the ratios for the applicable date.

    >>> get_ratios_from_date(
    ...     datetime(2020, 1, 1),
    ...     [(datetime(2020,12,31), {'d1': 1.0})]
    ... )
    (datetime.datetime(2020, 1, 1, 0, 0), {})

    >>> get_ratios_from_date(
    ...     datetime(2023, 1, 1),
    ...     [(datetime(2022,12,31), {'d1': 1.0})]
    ... )
    (datetime.datetime(2022, 12, 31, 0, 0), {'d1': 1.0})

    >>> get_ratios_from_date(
    ...     datetime(2023, 1, 13),
    ...     [(datetime(2022,12,31), {'d1': 1.0}), (datetime(2023,1,12), {'d1': 1.0})]
    ... )
    (datetime.datetime(2023, 1, 12, 0, 0), {'d1': 1.0})

    >>> get_ratios_from_date(
    ...     datetime(2023, 1, 1), [(datetime(2023,1,2), {'d1': 1.0})]
    ... )
    Traceback (most recent call last):
    ...
    AssertionError: No ratio found for date 2023-01-01 00:00:00
    """
    assert isinstance(dt, datetime)

    # prop_map is sorted ASC by date, so we
    # can find the latest element that is <= date
    for idt, m in prop_map[::-1]:
        # first entry BEFORE the date
        if idt <= dt:
            return idt, m

    logger.error(dt)
    logger.error(prop_map)
    raise AssertionError(f'No ratio found for date {dt}')


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

    seqr_map, hail_map = await generate_proportionate_maps_of_datasets(
        start, end, projects
    )
    result = 0

    result += migrate_entries_from_bq(start, end, seqr_map, dry_run=dry_run)

    def func_get_finalised_entries(batch):
        return get_finalised_entries_for_batch(batch, hail_map)

    result += await utils.process_entries_from_hail_in_chunks(
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
    test_start, test_end = None, None

    # 2022-05-17 03:17:40
    test_start, test_end = datetime(2022, 3, 1), datetime(2022, 6, 1)
    prjcts = get_seqr_datasets()

    asyncio.new_event_loop().run_until_complete(
        generate_proportionate_maps_of_datasets(test_start, test_end, prjcts)
    )

    # asyncio.new_event_loop().run_until_complete(
    #     main(start=test_start, end=test_end, dry_run=False)
    # )
