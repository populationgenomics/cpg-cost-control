"""
Cloud function that runs {TBA} that synchronises a portion of data from:

FROM:   billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343 
TO:     billing-admin-290403.billing_aggregate.aggregate

Tasks:

- Needs to convert {billing_project_id} into DATSET
- Only want to transfer data from the projects in the server-config
- Can't duplicate rows (so maybe just grab only settled data within START + END of previous time period)
- Service ID should be faithfully handed over
- Should search and update for [START_PERIOD, END_PERIOD)
"""

import json
import hashlib
import logging

from datetime import datetime, timedelta
from google.cloud import bigquery
from cpg_utils.cloud import read_secret
from .utils import insert_dataframe_rows_in_table

logging.basicConfig(level=logging.INFO)

ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'
SOURCE_TABLE = 'billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343'
DESTINATION_TABLE = 'sabrina-dev-337923.billing.aggregate'


def main(request=None):
    """Main entry point for the Cloud Function"""
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = today - timedelta(days=1)
    start_day = end_day - timedelta(days=1)
    if request:
        start_day = request.start_day
        end_day = request.end_day

    bigquery_client = bigquery.Client()

    # Get the dataset to GCP project map
    dataset_to_gcp_map = get_dataset_to_gcp_map()

    def get_dataset(row):
        return billing_row_to_dataset(row, dataset_to_gcp_map)

    allowed_project_ids = "'" + "','".join(dataset_to_gcp_map.keys()) + "'"

    # Get the billing date in the time period
    # Filter out any rows that aren't in the allowed project ids
    _query = f"""
        SELECT * FROM `{SOURCE_TABLE}`
        WHERE usage_end_time >= '{start_day.isoformat()}'
            AND usage_end_time < '{end_day.isoformat()}'
            AND project.id IN ({allowed_project_ids})
    """

    migrate_rows = bigquery_client.query(_query).result().to_dataframe()

    if len(migrate_rows) == 0:
        logging.info(f"No rows to migrate")
        return

    # Add id and dataset to the row
    migrate_rows.insert(0, 'id', migrate_rows.apply(billing_row_to_key, axis=1))
    migrate_rows.insert(1, 'dataset', migrate_rows.apply(get_dataset, axis=1))

    # Remove billing account id
    migrate_rows = migrate_rows.drop(columns=['billing_account_id'])

    result = insert_dataframe_rows_in_table(DESTINATION_TABLE, migrate_rows)

    return result


def billing_row_to_key(row):
    """Convert a billing row to a hash which will be the row key"""
    data = tuple(row)
    id = hashlib.md5()

    for item in data:
        id.update(str(item).encode('utf-8'))

    return id.hexdigest()


def billing_row_to_dataset(row, dataset_to_gcp_map):
    """Convert a billing row to a dataset"""
    return dataset_to_gcp_map.get(row['project']['id'], None)


def get_dataset_to_gcp_map() -> dict:
    """Get the server-config from the secret manager"""
    server_config = json.loads(read_secret(ANALYSIS_RUNNER_PROJECT_ID, 'server-config'))
    return {v['projectId']: k for k, v in server_config.items()}


if __name__ == '__main__':
    main()
