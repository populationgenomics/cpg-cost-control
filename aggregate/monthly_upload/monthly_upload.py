"""A Cloud Function to update the status of genomic samples."""

import os
import json
import logging

from datetime import datetime

import google.cloud.bigquery as bq

from flask import abort
from pandas import DataFrame

# from airtable import Airtable
from google.cloud import secretmanager

# from requests.exceptions import HTTPError


BQ_CLIENT = bq.Client()
GCP_MONTHLY_BILLING_BQ_TABLE = (
    'billing-admin-290403.billing_aggregate.aggregate_monthly_cost'
)

secret_manager = secretmanager.SecretManagerServiceClient()


def update_sample_status(data, _):  # pylint: disable=R0911
    """Main entry point for the Cloud Function."""

    if not data.get('attributes'):
        return abort(405)

    request_json = json.loads(data.get('attributes'))

    # Verify input parameters.
    year = request_json.get('year')
    month = request_json.get('month')
    if not year:
        return abort(400)

    if not month:
        month = datetime.now().strftime('%m')

    logging.info(f'Processing request: {request_json}')

    # Fetch the per-project configuration from the Secret Manager.
    gcp_project = os.getenv('GCP_PROJECT')
    secret_name = (
        f'projects/{gcp_project}/secrets'
        '/billing-airtable-monthly-upload-apikeys/versions/latest'
    )
    config_str = secret_manager.access_secret_version(
        request={'name': secret_name}
    ).payload.data.decode('UTF-8')
    config = json.loads(config_str)

    airtable_config = config.get(year)
    if not airtable_config:
        return abort(404)

    # Get the Airtable credentials.
    base_key = airtable_config.get('baseKey')
    table_name = airtable_config.get('tableName')
    api_key = airtable_config.get('apiKey')
    if not base_key or not table_name or not api_key:
        return abort(500)

    return airtable_overwrite_yearly_billing_month(
        year, month, base_key, table_name, api_key
    )


def get_billing_data(year: str, month: str) -> DataFrame:
    """
    Retrieve the billing data for a particular billing month from the aggreagtion table
    Return results as a dataframe
    """

    _query = f"""
        SELECT * FROM `{GCP_MONTHLY_BILLING_BQ_TABLE}`
        WHERE month = @yearmonth
        ORDER BY cost DESC
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


def airtable_overwrite_yearly_billing_month(year, month, base_key, table_name, api_key):
    """
    Retrieve and upload the montly billing info from GCP and upload to airtable
    Return success or failure status
    """

    print(year, month, base_key, table_name, api_key)

    # Update the entry.
    # airtable = Airtable(base_key, table_name, api_key)
    # try:
    #     response = airtable.update_by_field()
    # except HTTPError as err:  # Invalid status enum.
    #     logging.error(err)
    #     return abort(400)

    # if not response:  # Sample not found.
    #     return abort(404)

    return ('', 204)
