"""A Cloud Function to update the status of genomic samples."""

import json
import os
import logging
from airtable import Airtable
from flask import abort
from google.cloud import secretmanager
from requests.exceptions import HTTPError


secret_manager = secretmanager.SecretManagerServiceClient()


def update_sample_status(request):  # pylint: disable=R0911
    """Main entry point for the Cloud Function."""

    if request.method != 'PUT':
        return abort(405)

    # Verify input parameters.
    request_json = request.get_json()
    project = request_json.get('project')
    sample = request_json.get('sample')
    status = request_json.get('status')
    if not project or not sample or not status:
        return abort(400)

    logging.info(f'Processing request: {request_json}')

    # Fetch the per-project configuration from the Secret Manager.
    gcp_project = os.getenv('GCP_PROJECT')
    secret_name = (
        f'projects/{gcp_project}/secrets' '/update-sample-status-config/versions/latest'
    )
    config_str = secret_manager.access_secret_version(
        request={'name': secret_name}
    ).payload.data.decode('UTF-8')
    config = json.loads(config_str)

    project_config = config.get(project)
    if not project_config:
        return abort(404)

    # Get the Airtable credentials.
    base_key = project_config.get('baseKey')
    table_name = project_config.get('tableName')
    api_key = project_config.get('apiKey')
    if not base_key or not table_name or not api_key:
        return abort(500)

    # Update the entry.
    airtable = Airtable(base_key, table_name, api_key)
    try:
        response = airtable.update_by_field('Sample ID', sample, {'Status': status})
    except HTTPError as err:  # Invalid status enum.
        logging.error(err)
        return abort(400)

    if not response:  # Sample not found.
        return abort(404)

    return ('', 204)
