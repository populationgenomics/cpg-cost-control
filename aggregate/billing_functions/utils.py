import json
import logging
import pandas as pd

from typing import Tuple
from pathlib import Path
from datetime import datetime
from google.cloud.bigquery import (
    Client,
    QueryJobConfig,
    ArrayQueryParameter,
    ScalarQueryParameter,
)

logging.basicConfig(level=logging.INFO)


def get_schema():
    """Get the schema for the table"""
    pwd = Path(__file__).parent.parent.resolve()
    schema_path = pwd / 'schema' / 'aggregate_schema.json'
    with open(schema_path, 'r') as f:
        return json.load(f)


def insert_new_rows_in_table(client: Client, table: str, df: pd.DataFrame):
    """Insert new rows into a table"""

    _query = f"""
        SELECT id FROM `{table}`
        WHERE id IN UNNEST(@ids);
    """
    job_config = QueryJobConfig(
        query_parameters=[
            ArrayQueryParameter('ids', 'STRING', list(set(df['id']))),
        ]
    )
    result = client.query(_query, job_config=job_config).result()
    existing_ids = set(result.to_dataframe()['id'])

    # Filter out any rows that are already in the table
    df = df[~df['id'].isin(existing_ids)]

    # Count number of rows adding
    adding_rows = len(df)

    # Insert the new rows
    project_id = table.split('.')[0]

    # also doesn't work with failure:
    #   BadRequest: 400 Error while reading data, error message:
    #       Schema mismatch: referenced variable 'labels.$is_not_null' has array levels of 1,
    #       while the corresponding field path to Parquet column has 0 repeated fields.

    # result = client.load_table_from_dataframe(
    #     df,
    #     table,
    # ).result()
    # print(result)

    table_schema = get_schema()

    df.to_gbq(
        table,
        project_id=project_id,
        table_schema=table_schema,
        if_exists='append',
    )

    logging.info(f"{adding_rows} new rows inserted")
    return adding_rows
