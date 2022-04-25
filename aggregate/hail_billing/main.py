"""
A cloud function that synchronises HAIL billing data to BigQuery

TO:     billing-admin-290403.billing_aggregate.aggregate


Notes:

- The service.id should be "hail"


Tasks:

- Only want to transfer data from the projects in the server-config
- Need to build an endpoint in Hail to service this metadata
- Transform into new generic format
- Can't duplicate rows, so determine some ID:
    - And maybe only sync "settled" jobs within datetimes
        (ie: finished between START + END of previous time period)
"""

from datetime import datetime
import os
import json
import logging
import requests

logging.basicConfig(level=logging.INFO)

SERVICE_ID = 'hail'

BASE = 'https://batch.hail.populationgenomics.org.au'
BATCHES_API = BASE + '/api/v1alpha/batches'
JOBS_API = BASE + '/api/v1alpha/batches/{batch_id}/jobs'


def get_billing_projects():
    """
    Get Hail billing projects, same names as dataset
    TOOD: Implement
    """
    return ['seqr']


def get_hail_token():
    """
    Get Hail token from local tokens file
    TODO: look at env var for deploy
    """
    with open(os.path.expanduser('~/.hail/tokens.json')) as f:
        config = json.load(f)
        return config['default']


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


def get_finished_batches_for_date(billing_project: str, day: datetime, token: str):
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
            same_day = True or (
                day.year == time_created.year
                and day.month == time_created.month
                and day.day == time_created.day
            )
            is_completed = b['complete']
            if time_created < day:
                logging.info(
                    f'Got batches in {n_requests} requests, skipping {skipped}'
                )
                return batches
            if same_day and is_completed:
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


def main(request=None):
    """Main body function"""
    day = datetime(year=2022, month=3, day=20)
    if request:
        day = request.day

    token = get_hail_token()
    for bp in get_billing_projects():
        batches = get_finished_batches_for_date(bp, day, token)

        for batch in batches:
            fill_batch_jobs(batch, token)
        print(json.dumps(batches, indent=4))


if __name__ == '__main__':
    main()
