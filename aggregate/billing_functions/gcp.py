"""
Cloud function that runs {TBA} that synchronises a portion of data from:

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

import re
import json
import hashlib
import logging
from datetime import datetime
from typing import Dict, Optional

try:
    from . import utils
except ImportError:
    import utils


logger = utils.logger


def from_request(request):
    """
    From request object, get start and end time if present
    """
    start, end = utils.get_start_and_end_from_request(request)
    main(start, end)


def from_pubsub(data=None, _=None):
    """
    From pubsub message, get start and end time if present
    """
    start, end = utils.get_start_and_end_from_data(data)
    main(start, end)


def migrate_billing_data(start, end, dataset_to_gcp_map) -> int:
    """
    Get the billing date in the time period
    Filter out any rows that aren't in the allowed project ids
    """

    def get_topic(row):
        return billing_row_to_topic(row, dataset_to_gcp_map)

    _query = f"""
        SELECT * FROM `{utils.GCP_BILLING_BQ_TABLE}`
        WHERE export_time >= '{start.isoformat()}'
            AND export_time <= '{end.isoformat()}'
    """

    migrate_rows = utils.bigquery_client.query(_query).result().to_dataframe()

    if len(migrate_rows) == 0:
        logging.info(f'No rows to migrate')
        return 0

    # Add id and dataset to the row
    migrate_rows = migrate_rows.drop(columns=['billing_account_id'])
    migrate_rows.insert(0, 'topic', migrate_rows.apply(get_topic, axis=1))
    migrate_rows.insert(0, 'id', migrate_rows.apply(billing_row_to_key, axis=1))

    result = utils.insert_dataframe_rows_in_table(
        utils.GCP_AGGREGATE_DEST_TABLE, migrate_rows
    )

    return result


def main(start: datetime = None, end: datetime = None) -> int:
    """Main body function"""
    interval_iterator = utils.get_date_intervals_for(start, end)

    # Get the dataset to GCP project map
    dataset_to_gcp_map = get_dataset_to_gcp_map()

    # specific hail topic :)
    dataset_to_gcp_map['hail-295901'] = 'hail'

    # Migrate the data in batches
    result = 0
    for s, f in interval_iterator:
        logging.info(f'Migrating data from {s} to {f}')
        result += migrate_billing_data(s, f, dataset_to_gcp_map)

    logging.info(f'Migrated a total of {result} rows')

    return result


def billing_row_to_key(row) -> str:
    """Convert a billing row to a hash which will be the row key"""
    data = tuple(row)
    identifier = hashlib.md5()

    for item in data:
        identifier.update(str(item).encode('utf-8'))

    return identifier.hexdigest()


RE_matcher = re.compile(r'-\d+$')


def billing_row_to_topic(row, dataset_to_gcp_map) -> Optional[str]:
    """Convert a billing row to a dataset"""
    project_id = row['project']['id']
    topic = dataset_to_gcp_map.get(project_id, project_id)
    if not topic:
        return 'admin'

    topic = RE_matcher.sub('', topic)
    return topic


def get_dataset_to_gcp_map() -> Dict[str, str]:
    """Get the server-config from the secret manager"""
    server_config = json.loads(
        utils.read_secret(utils.ANALYSIS_RUNNER_PROJECT_ID, 'server-config')
    )
    return {v['projectId']: k for k, v in server_config.items()}


if __name__ == '__main__':
    main()
