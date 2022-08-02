"""A Cloud Function to update the status of genomic samples."""

import json
import asyncio
import logging

from datetime import datetime

import google.cloud.bigquery as bq

from flask import abort, Response
from pandas import DataFrame

from airtable import Airtable
from google.cloud import secretmanager
from requests.exceptions import HTTPError

BQ_CLIENT = bq.Client()
GCP_PROJECT = 'billing-admin-290403'
GCP_MONTHLY_BILLING_BQ_TABLE = f'{GCP_PROJECT}.billing_aggregate.aggregate_monthly_cost'

secret_manager = secretmanager.SecretManagerServiceClient()


def main(data, _):
    """Main function"""
    loop = asyncio.get_event_loop() or asyncio.new_event_loop()
    loop.run_until_complete(update_sample_status(data))


def abort_message(status: int, message: str):
    """Custom abort wrapper that allows for error messages to be passed through"""
    return abort(Response(json.dumps({'message': message}), status))


async def update_sample_status(data):
    """Main entry point for the Cloud Function."""

    if not data.get('attributes'):
        return abort_message(400, 'No attributes found in data')

    request_json = data.get('attributes')

    # Verify input parameters.
    year = request_json.get('year')
    month = request_json.get('month')
    if not year:
        return abort(400)

    if not month:
        month = datetime.now().strftime('%m')

    logging.info(f'Processing request: {request_json}')

    # Fetch the per-project configuration from the Secret Manager.
    secret_name = (
        f'projects/{GCP_PROJECT}/secrets'
        '/billing-airtable-monthly-upload-apikeys/versions/latest'
    )
    config_str = secret_manager.access_secret_version(
        request={'name': secret_name}
    ).payload.data.decode('UTF-8')
    config = json.loads(config_str)

    airtable_config = config.get(year)
    if not airtable_config:
        return abort(406, f'Airtable config could not be found for year {year}')

    # Get the Airtable credentials.
    base_key = airtable_config.get('baseKey')
    table_name = airtable_config.get('tableName')
    api_key = airtable_config.get('apiKey')
    if not base_key or not table_name or not api_key:
        return abort(500)

    await airtable_overwrite_yearly_billing_month(
        year, month, base_key, table_name, api_key
    )

    # upload_monthly_report_pdf()


def get_billing_data(year: str, month: str) -> DataFrame:
    """
    Retrieve the billing data for a particular billing month from the aggreagtion table
    Return results as a dataframe
    """

    _query = f"""
        SELECT * FROM `{GCP_MONTHLY_BILLING_BQ_TABLE}`
        WHERE month = @yearmonth
        ORDER BY topic
    """

    yearmonth = year + month

    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ScalarQueryParameter('yearmonth', 'STRING', str(yearmonth)),
        ]
    )

    migrate_rows = (
        BQ_CLIENT.query(_query, job_config=job_config).result().to_dataframe()
    )

    return migrate_rows


async def airtable_overwrite_yearly_billing_month(
    year, month, base_key, table_name, api_key
):
    """
    Retrieve and upload the montly billing info from GCP and upload to airtable
    Return success or failure status
    """

    airtable = Airtable(base_key, table_name, api_key)
    airtable.API_LIMIT = 0.0001
    data = get_billing_data(year, month)
    data['cost'].fillna(0)

    # Insert any missing topics
    topics = data['topic'].unique()
    at_topics = {r['fields']['Topic'] for r in airtable.get_all()}
    missing = [{'Topic': t} for t in topics if t not in at_topics]
    airtable.batch_insert(missing)

    # Update the field values
    topic_calls = [airtable_upsert_topic_row(airtable, data, t) for t in topics]
    await asyncio.gather(*topic_calls)

    return ('', 204)


async def airtable_upsert_topic_row(airtable: Airtable, df: DataFrame, topic: str):
    """
    Mangle the data into the correct format from df to suite the Airtable API
    """
    df = df.loc[df['topic'] == topic, :].copy()

    def field_name(row):
        month = convert_date(row['month'], '%Y%m', '%B')
        return f'{month} ({row["cost_category"]})'

    # Create airtable field names, then index on them
    df['field'] = df.apply(field_name, axis=1)
    df = df.set_index('field')
    fields = json.loads(df['cost'].to_json())

    response = airtable.update_by_field('Topic', topic, fields)

    if not response:
        raise HTTPError(f'Could not update topic {topic}')

    return response


def convert_date(date: datetime, frmt: str, frmt_to: str):
    """Convert date string format"""
    return datetime.strptime(date, frmt).strftime(frmt_to)


if __name__ == '__main__':
    YEAR = '2022'
    MONTH = '05'
    main({'attributes': {'year': YEAR, 'month': MONTH}}, None)
