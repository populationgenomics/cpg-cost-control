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

import json
import hashlib
import logging
from datetime import datetime
from typing import Dict, Optional
from cpg_utils.cloud import read_secret

from .utils import (
    bigquery_client,
    insert_dataframe_rows_in_table,
    get_start_and_end_from_request,
    process_default_start_and_end,
)

logging.basicConfig(level=logging.INFO)

ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'
SOURCE_TABLE = 'billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343'
DESTINATION_TABLE = 'sabrina-dev-337923.billing.aggregate'


def from_request(request):
    """
    From request object, get start and end time if present
    """
    start, end = get_start_and_end_from_request(request)
    main(start, end)


def main(start: datetime = None, end: datetime = None) -> int:
    """Main body function"""
    start, end = process_default_start_and_end(start, end)

    # Get the dataset to GCP project map
    dataset_to_gcp_map = get_dataset_to_gcp_map()

    def get_dataset(row):
        return billing_row_to_dataset(row, dataset_to_gcp_map)

    allowed_project_ids = "'" + "','".join(dataset_to_gcp_map.keys()) + "'"

    # Get the billing date in the time period
    # Filter out any rows that aren't in the allowed project ids
    _query = f"""
        SELECT * FROM `{SOURCE_TABLE}`
        WHERE usage_end_time >= '{start.isoformat()}'
            AND usage_end_time < '{end.isoformat()}'
            AND project.id IN ({allowed_project_ids})
    """

    migrate_rows = bigquery_client.query(_query).result().to_dataframe()

    if len(migrate_rows) == 0:
        logging.info(f"No rows to migrate")
        return 0

    # Add id and dataset to the row
    migrate_rows.insert(0, 'id', migrate_rows.apply(billing_row_to_key, axis=1))
    migrate_rows.insert(1, 'dataset', migrate_rows.apply(get_dataset, axis=1))

    # Remove billing account id
    migrate_rows = migrate_rows.drop(columns=['billing_account_id'])

    result = insert_dataframe_rows_in_table(DESTINATION_TABLE, migrate_rows)

    return result


def billing_row_to_key(row) -> str:
    """Convert a billing row to a hash which will be the row key"""
    data = tuple(row)
    identifier = hashlib.md5()

    for item in data:
        identifier.update(str(item).encode('utf-8'))

    return identifier.hexdigest()


def billing_row_to_dataset(row, dataset_to_gcp_map) -> Optional[str]:
    """Convert a billing row to a dataset"""
    return dataset_to_gcp_map.get(row['project']['id'], None)


def get_dataset_to_gcp_map() -> Dict[str, str]:
    """Get the server-config from the secret manager"""
    server_config = json.loads(read_secret(ANALYSIS_RUNNER_PROJECT_ID, 'server-config'))
    return {v['projectId']: k for k, v in server_config.items()}


if __name__ == '__main__':
    main()
