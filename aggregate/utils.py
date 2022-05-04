import json
import logging
import pandas as pd

from typing import Tuple
from pathlib import Path
from datetime import datetime
from google.cloud.bigquery import Client

logging.basicConfig(level=logging.INFO)


def get_schema():
    """Get the schema for the table"""
    pwd = Path(__file__).parent.resolve()
    schema_path = pwd / 'schema' / 'aggregate_schema.json'
    with open(schema_path, 'r') as f:
        return json.load(f)


def insert_new_rows_in_table(
    client: Client,
    table: str,
    df: pd.DataFrame,
    date_range: Tuple[datetime, datetime] = None,
):
    """Insert new rows into a table"""
    new_ids = "'" + "','".join(df['id'].tolist()) + "'"
    _query = f"""
        SELECT id FROM `{table}`
        WHERE usage_end_time >= {date_range[0].isoformat()}
            AND usage_end_time < {date_range[1].isoformat()}
            AND id IN ({new_ids})
    """
    existing_ids = set(client.query(_query).result().to_dataframe()['id'])

    # Filter out any rows that are already in the table
    df = df[~df['id'].isin(existing_ids)]

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
        logging.info(f"{adding_rows} new rows inserted")
        return adding_rows
