"""
Cloud function that runs {TBA} that synchronises a portion of data from:

FROM:   billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343 
TO:     billing-admin-290403.billing_aggregate.aggregate

Tasks:

- Needs to convert {billing_project_id} into DATSET
- Only want to transfer data from the projects in the server-config
- Can't duplicate rows (so maybe just grab only settled data within START + END of previous time period)
- Service ID should be faithfully handed over
"""

import json

from google.cloud import bigquery

from cpg_utils.cloud import read_secret


ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'
SOURCE_TABLE = 'billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343'
DESTINATION_TABLE = 'sabrina-dev-337923.billing.aggregate'

bigquery_client = bigquery.Client()


def main():
    """Main entry point for the Cloud Function"""
    start_period = "2022-04-05"
    finish_period = "2022-04-05"

    # Get the dataset to GCP project map
    dataset_to_gcp_map = get_dataset_to_gcp_map()
    allowed_project_ids = "'" + "','".join(dataset_to_gcp_map.values()) + "'"

    # Get the billing date in the time period
    # Filter out any rows that aren't in the allowed project ids
    _query = f"""
        SELECT * FROM `{SOURCE_TABLE}`
        WHERE DATE(usage_end_time) >= '{start_period}'
            AND DATE(usage_end_time) <= '{finish_period}'
            AND project.id IN ({allowed_project_ids})
    """

    migrate_rows = bigquery_client.query(_query).result().to_dataframe()
    migrate_rows['id'] = migrate_rows.apply(billing_row_to_key, axis=1)
    migrate_rows['dataset'] = migrate_rows.apply(
        billing_row_to_dataset, axis=1, args=(dataset_to_gcp_map,)
    )

    # Merge the rows with the destination table

    migrate_rows.to_gbq(DESTINATION_TABLE, if_exists='merge')

    return


def billing_row_to_key(row):
    """Convert a billing row to a key"""
    """
        service.id, usage_start_time, usage_end_time, labels.bucket, usage[0].amount,
    """
    return (
        row['service']['id'],
        row['usage_start_time'],
        row['usage_end_time'],
        row['labels'][0].get('bucket', None) if row['labels'].any() else None,
        row['usage']['amount'],
    )


def billing_row_to_dataset(row, dataset_to_gcp_map):
    """Convert a billing row to a dataset"""
    return dataset_to_gcp_map.get(row['project']['id'], None)


def get_dataset_to_gcp_map() -> dict:
    """Get the server-config from the secret manager"""
    server_config = json.loads(read_secret(ANALYSIS_RUNNER_PROJECT_ID, 'server-config'))
    return {k: v['projectId'] for k, v in server_config.items()}


if __name__ == '__main__':
    main()
