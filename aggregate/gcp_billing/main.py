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
import hashlib
from pathlib import Path
import pandas as pd
from typing import Tuple
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
    allowed_project_ids = "'" + "','".join(dataset_to_gcp_map.keys()) + "'"

    # Get the billing date in the time period
    # Filter out any rows that aren't in the allowed project ids
    _query = f"""
        SELECT * FROM `{SOURCE_TABLE}`
        WHERE DATE(usage_end_time) >= '{start_period}'
            AND DATE(usage_end_time) <= '{finish_period}'
            AND project.id IN ({allowed_project_ids})
    """

    migrate_rows = bigquery_client.query(_query).result().to_dataframe()
    migrate_rows.insert(0, 'id', migrate_rows.apply(billing_row_to_key, axis=1))
    migrate_rows.insert(
        1,
        'dataset',
        migrate_rows.apply(billing_row_to_dataset, axis=1, args=(dataset_to_gcp_map,)),
    )

    result = insert_new_rows(
        DESTINATION_TABLE, migrate_rows, (start_period, finish_period)
    )
    print(f"{result} new rows inserted")

    return result


def insert_new_rows(table: str, df: pd.DataFrame, date_range: Tuple[str, str] = None):
    """Insert new rows into a table"""
    new_ids = "'" + "','".join(df['id'].tolist()) + "'"
    _query = f"""
        SELECT id FROM `{table}`
        WHERE DATE(usage_end_time) >= '{date_range[0]}'
            AND DATE(usage_end_time) <= '{date_range[1]}'
            AND id IN ({new_ids})
    """
    existing_ids = set(bigquery_client.query(_query).result().to_dataframe()['id'])

    # Filter out any rows that are already in the table
    df = df[~df['id'].isin(existing_ids)]

    # Remove billing account id
    df = df.drop(columns=['billing_account_id'])

    # Count number of rows adding
    adding_rows = len(df)

    # Insert the new rows
    project_id = table.split('.')[0]
    table_schema = get_schema()
    try:
        df.to_gbq(
            table, project_id=project_id, table_schema=table_schema, if_exists='append'
        )
    except Exception as e:
        raise Exception(f'Failed to insert rows: {e}')
    finally:
        return adding_rows


def get_schema():
    pwd = Path(__file__).parent.parent.resolve()
    schema_path = pwd / 'schema' / 'schema.json'
    with open(schema_path, 'r') as f:
        return json.load(f)


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
