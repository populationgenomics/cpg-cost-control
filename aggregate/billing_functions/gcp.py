"""
Cloud function that runs once a month that synchronises a portion of data from:

FROM:   billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343
TO:     billing-admin-290403.billing_aggregate.aggregate

Tasks:

- Needs to convert {billing_project_id} into DATSET
- Only want to transfer data from the projects in the server-config
- Can't duplicate rows, so:
    - just grab only settled data within START + END of previous time period
- Service ID should be faithfully handed over
- Should search and update for [START_PERIOD, END_PERIOD)
"""

import json
import hashlib

from datetime import datetime
from typing import Dict
from pandas import DataFrame

from cpg_utils.cloud import read_secret
import google.cloud.bigquery as bq

try:
    from . import utils
except ImportError:
    import utils


logger = utils.logger


##########################
#    INPUT PROCESSORS    #
##########################


def from_request(request):
    """
    From request object, get start and end time if present
    """
    start, end = utils.get_start_and_end_from_request(request)
    main(start, end)


def from_pubsub(data, _):
    """
    From pubsub message, get start and end time if present
    """
    start, end = utils.get_start_and_end_from_data(data)
    main(start, end)


#################
#    MIGRATE    #
#################


def migrate_billing_data(start, end, dataset_to_topic) -> int:
    """
    Gets the billing date in the time period
    Filter out any rows that aren't in the allowed project ids
    :return: The number of migrated rows
    """

    def get_topic(row):
        return utils.billing_row_to_topic(row, dataset_to_topic)

    migrate_rows = get_billing_data(start, end)

    if not migrate_rows:
        logger.info('No rows to migrate')
        return 0

    # Add id and topic to the row
    migrate_rows = migrate_rows.drop(columns=['billing_account_id'])
    migrate_rows.insert(0, 'topic', migrate_rows.apply(get_topic, axis=1))
    migrate_rows.insert(0, 'id', migrate_rows.apply(billing_row_to_key, axis=1))

    result = utils.upsert_aggregated_dataframe_into_bigquery(df=migrate_rows)

    return result


#################
#    HELPERS    #
#################


def get_billing_data(start: datetime, end: datetime) -> DataFrame:
    """
    Retrieve the billing data from start to end date inclusive
    Return results as a dataframe
    """

    _query = f"""
        SELECT * FROM `{utils.GCP_BILLING_BQ_TABLE}`
        WHERE export_time >= @start
            AND export_time <= @end
    """
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ScalarQueryParameter('start', 'STRING', str(start)),
            bq.ScalarQueryParameter('end', 'STRING', str(end)),
        ]
    )

    migrate_rows = (
        utils.get_bigquery_client()
        .query(_query, job_config=job_config)
        .result()
        .to_dataframe()
    )

    return migrate_rows


def billing_row_to_key(row) -> str:
    """Convert a billing row to a hash which will be the row key"""
    data = tuple(row)
    identifier = hashlib.md5()

    for item in data:
        identifier.update(str(item).encode('utf-8'))

    return identifier.hexdigest()


def get_dataset_to_topic_map() -> Dict[str, str]:
    """Get the server-config from the secret manager"""
    server_config = json.loads(
        read_secret(utils.ANALYSIS_RUNNER_PROJECT_ID, 'server-config')
    )
    return {v['projectId']: k for k, v in server_config.items()}


##############
#    MAIN    #
##############


def main(start: datetime = None, end: datetime = None) -> int:
    """Main body function"""
    interval_iterator = utils.get_date_intervals_for(start, end)

    # Storing topic map means we don't repeatedly call to access the topic
    # data mapping for each batch
    dataset_to_topic_map = get_dataset_to_topic_map()

    # Migrate the data in batches
    # This is because depending on the start-end interval all of the billing
    # data may not be able to be held in memory during the migration
    # Memory is particularly limited for cloud functions
    result = 0
    for begin, finish in interval_iterator:
        logger.info(f'Migrating data from {begin} to {finish}')
        result += migrate_billing_data(begin, finish, dataset_to_topic_map)

    logger.info(f'Migrated a total of {result} rows')

    return result


if __name__ == '__main__':
    main()
