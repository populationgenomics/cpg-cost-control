"""
This cloud function runs WEEKLY, and distributes the cost of SEQR on the sample size within SEQR.

- It first pulls the cost of the seqr project (relevant components within it):
    - Elasticsearch, instance cost
    - Cost of loading data into seqr might be difficult:
        - At the moment this in dataproc, so covered by previous result
        - Soon to move to Hail Query, which means we have to query hail to get the "cost of loading"
- It determines the relative heuristic (of all projects loaded into seqr):
    - EG: "relative size of GVCFs by project", eg:
        - $DATASET has 12GB of GVCFs of a total 86GB of all seqr GVCFs, 
        - therefore it's relative heuristic is 12/86 = 0.1395, and share of seqr cost is 13.95%

- Insert rows in aggregate cost table for each of these costs:
    - The service.id should be "seqr" (or something similar)
    - Maybe the label could include the relative heuristic
"""

import os
import json
import logging
import requests
import pandas as pd

from datetime import datetime
from google.cloud import bigquery
from collections import defaultdict

from ..utils import insert_new_rows_in_table


logging.basicConfig(level=logging.INFO)

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


def get_hail_token():
    """
    Get Hail token from local tokens file
    TODO: look at env var for deploy
    """
    with open(os.path.expanduser('~/.hail/tokens.json')) as f:
        config = json.load(f)
        return config['default']


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
        "boot-disk/pd-ssd/1": "GB/ms",  # 0.0000000000000631286124420108,
        "compute/n1-nonpreemptible/1": "cpu/ms",  # 0.00000000000878083333333334,
        "compute/n1-preemptible/1": 0.00000000000184861111111111,
        "disk/local-ssd/1": 0.0000000000000178245493953913,
        "disk/pd-ssd/1": 0.0000000000000631286124420108,
        "ip-fee/1024/1": 0.00000000000108506944444444,
        "memory/n1-nonpreemptible/1": 0.00000000000114935980902778,
        "memory/n1-preemptible/1": 0.000000000000241970486111111,
        "service-fee/1": 0.00000000000277777777777778,
    }.get(batch_resource_type, batch_resource_type)


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

        if jresponse['last_batch_id'] == last_batch_id:
            raise ValueError(
                f'Something weird is happening with last_batch_job: {last_batch_id}'
            )
        last_batch_id = jresponse['last_batch_id']
        for b in jresponse['batches']:
            time_created = datetime.strptime(b['time_created'], '%Y-%m-%dT%H:%M:%SZ')
            in_date_range = True or (
                time_created >= start_day and time_created < end_day
            )
            is_completed = b['complete']
            if time_created < start_day:
                logging.info(
                    f'Got batches in {n_requests} requests, skipping {skipped}'
                )
                return batches
            if in_date_range and is_completed:
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
    while not end:
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


def finalise_batch_cost(batch: dict) -> float:
    """
    Calculate the cost of a batch from the job aggregated resources
    No filtering is done on attributes here.
    """
    raise NotImplementedError


def get_currency_conversion_rate_for_time(time: datetime):
    """
    Get the currency conversion rate for a given time.
    Noting that GCP conversion rates are decided at the start of the month,
    and apply to each job that starts within the month, regardless of when
    the job finishes.
    """
    query = f"""
        SELECT currency_conversion_rate
        FROM {GCP_BILLING_BQ_TABLE}
        WHER DATE(_PARTITIONTIME) = DATE('{time.date()}')
        LIMIT 1
    """
    result = bigquery_client.query(query).result().next()
    return result['currency_conversion_rate']


def main(request=None):
    """Main body function"""
    end_day = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_day = end_day - datetime.timedelta(days=1)
    if request:
        start_day = request.start_day
        end_day = request.end_day

    token = get_hail_token()
    batches = get_finished_batches_for_date(
        SEQR_HAIL_BILLING_PROJECT, start_day, end_day, token
    )

    for batch in batches:
        fill_batch_jobs(batch, token)
        print(json.dumps(batches, indent=4))

    # Now we want to do a bunch of aggregation on the seqr jobs to find:
    #   - Jobs that belong to a specific billing project

    entry_items = []
    jobs_with_no_dataset = []
    for batch in batches:
        for job in batch['jobs']:
            dataset = job['attributes'].get('dataset')
            if not dataset:
                jobs_with_no_dataset.append(job)
                continue

            key = get_key_from_batch_job(batch, job)
            sample = job['attributes'].get('sample')
            name = job['attributes'].get('name')

            labels = [{'name': 'dataset', 'value': dataset}]
            if sample:
                labels.append({'name': 'sample', 'value': sample})

            for batch_resource, usage in job['resources']:
                # batch_resource is one of ['cpu', 'memory', 'disk']

                currency_conversion_rate = get_currency_conversion_rate_for_time(
                    job['start_time']
                )
                cost = currency_conversion_rate * get_usd_cost_for_resource(
                    batch_resource, usage
                )

                entry_items.append(
                    {
                        "id": key,
                        "dataset": dataset,
                        "service": {
                            "id": key,
                            "description": 'SEQR processing (no-aggregated)',
                        },
                        "sku": {
                            "id": name,
                            "description": 'seqr processing (non-aggregated)',
                        },
                        "usage_start_time": job['start_time'],
                        "usage_end_time": job['finish_time'],
                        # "project": {
                        #      "id",
                        #      "number",
                        #      "labels",
                        #              "key",
                        #              "value",
                        #      "ancestry_numbers",
                        #      "ancestors",
                        #              "resource_name",
                        #              "display_name",
                        # }
                        "labels": labels,
                        "system_labels": [],
                        "location": {
                            "location": 'australia-southeast1',
                            "country": 'Australia',
                            "region": 'australia',
                            "zone": None,
                        },
                        "export_time": datetime.now().isoformat(),
                        "cost": 0.0,
                        "currency": "AUD",
                        "currency_conversion_rate": currency_conversion_rate,
                        "usage": {
                            "amount": usage,
                            "unit": get_unit_for_batch_resource_type(batch_resource),
                            "amount_in_pricing_units": cost,
                            "pricing_unit": "AUD",
                        },
                        # "credits",
                        #     "amount",
                        #     "full_name",
                        #     "id",
                        #     "type",
                        "invoice": {
                            "month": job['start_time'].month + job['start_time'].year
                        },
                        # "cost_type",
                        # "adjustment_info",
                        #     "id",
                        #     "description",
                        #     "mode",
                        #     "type",
                    }
                )

    # Convert dict to dataframe
    df = pd.DataFrame.from_records(entry_items)

    # Insert new rows into aggregation table
    insert_new_rows_in_table(GCP_BILLING_BQ_TABLE, df, (start_day, end_day))


def get_key_from_batch_job(batch, job):
    dataset = job.get('attributes', {}).get('dataset')
    sample = job.get('attributes', {}).get('sample')

    key_components = [SERVICE_ID]
    if dataset:
        key_components.append(dataset)
        if sample:
            key_components.append(sample)

    key_components.extend(['batch', str(batch["id"]), 'job', str(job["id"])])
    return '-'.join(key_components)


if __name__ == '__main__':
    main()
