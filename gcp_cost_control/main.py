"""A Cloud Function to process GCP billing budget notifications."""

import base64
import json
import logging
import os

from google.cloud import secretmanager
from googleapiclient import discovery

import slack
from slack.errors import SlackApiError

PROJECT_ID = os.getenv('GCP_PROJECT')
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL')
SLACK_TOKEN_SECRET_NAME = (
    f'projects/{PROJECT_ID}/secrets/slack-gcp-cost-control/versions/latest'
)

# Cache the Slack client.
secret_manager = secretmanager.SecretManagerServiceClient()
slack_token_response = secret_manager.access_secret_version(
    request={'name': SLACK_TOKEN_SECRET_NAME}
)
slack_token = slack_token_response.payload.data.decode('UTF-8')
slack_client = slack.WebClient(token=slack_token)


def gcp_cost_control(data, unused_context):
    """Main entry point for the Cloud Function."""

    pubsub_budget_notification_data = json.loads(
        base64.b64decode(data['data']).decode('utf-8')
    )
    logging.info(f'Received notification: {pubsub_budget_notification_data}')

    budget = pubsub_budget_notification_data['budgetAmount']
    cost = pubsub_budget_notification_data['costAmount']

    if cost <= budget:
        logging.info('Still under budget')
        return

    # The budget alert name must correspond to the corresponding project ID.
    budget_project_id = pubsub_budget_notification_data['budgetDisplayName']

    billing = discovery.build('cloudbilling', 'v1', cache_discovery=False)
    projects = billing.projects()  # pylint: disable=no-member

    # If the billing is already disabled, there's nothing to do.
    if not is_billing_enabled(budget_project_id, projects):
        logging.info('Billing is already disabled')
        return

    logging.info('Over budget (%f > %f), disabling billing', cost, budget)
    disable_billing_for_project(budget_project_id, projects)

    currency = pubsub_budget_notification_data['currencyCode']
    post_slack_message(
        f'*Warning:* disabled billing for GCP project "{budget_project_id}", '
        f'which is over budget ({cost} {currency} > {budget} {currency}).'
    )


def is_billing_enabled(project_id, projects):
    """Determine whether billing is enabled for a project.

    @param {string} project_id ID of project to check if billing is enabled
    @return {bool} Whether project has billing enabled or not
    """
    try:
        res = projects.getBillingInfo(name=f'projects/{project_id}').execute()
        return res['billingEnabled']
    except KeyError:
        # If billingEnabled isn't part of the return, billing is not enabled
        return False
    except Exception:  # pylint: disable=broad-except
        logging.error(
            'Unable to determine if billing is enabled on specified '
            'project, assuming billing is enabled'
        )
        return True


def disable_billing_for_project(project_id, projects):
    """Disable billing for a project by removing its billing account.

    @param {string} project_id ID of project disable billing on
    """
    body = {'billingAccountName': ''}  # Disable billing
    try:
        res = projects.updateBillingInfo(
            name=f'projects/{project_id}', body=body
        ).execute()
        logging.error(f'Billing disabled: {json.dumps(res)}')
    except Exception as e:  # pylint: disable=broad-except
        logging.error(f'Failed to disable billing, possibly check permissions: {e}')


def post_slack_message(text):
    """Posts the given text as message to Slack."""

    try:
        slack_client.api_call(
            'chat.postMessage',
            json={
                'channel': SLACK_CHANNEL,
                'text': text,
            },
        )
    except SlackApiError as err:
        logging.error(f'Error posting to Slack: {err}')
