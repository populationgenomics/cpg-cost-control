"""A Cloud Function to send a daily GCP cost report to Slack."""

import logging
import os
from collections import defaultdict

from google.cloud import bigquery
from google.cloud import secretmanager

import slack
from slack.errors import SlackApiError

PROJECT_ID = os.getenv('GCP_PROJECT')
BIGQUERY_BILLING_TABLE = os.getenv('BIGQUERY_BILLING_TABLE')
QUERY_TIME_ZONE = os.getenv('QUERY_TIME_ZONE') or 'UTC'

# Query monthly cost per project and join that with cost over the last day.
BIGQUERY_QUERY = f"""
SELECT
  month.id as project_id,
  month.cost as month,
  day.cost as day,
  month.currency as currency
FROM
  (
    SELECT
      *
    FROM
      (
        SELECT
          project.id,
          ROUND(SUM(cost), 2) as cost,
          currency
        FROM
          `{BIGQUERY_BILLING_TABLE}`
        WHERE
          _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 32 DAY)
          AND invoice.month = FORMAT_TIMESTAMP("%Y%m", CURRENT_TIMESTAMP(),
                                               "{QUERY_TIME_ZONE}")
        GROUP BY
          project.id,
          currency
      )
    WHERE
      cost > 0.1
  ) month
  LEFT JOIN (
    SELECT
      project.id,
      ROUND(SUM(cost), 2) as cost,
      currency
    FROM
      `{BIGQUERY_BILLING_TABLE}`
    WHERE
      _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
      AND export_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
      AND invoice.month = FORMAT_TIMESTAMP("%Y%m", CURRENT_TIMESTAMP(),
                                           "{QUERY_TIME_ZONE}")
    GROUP BY
      project.id,
      currency
  ) day ON month.id = day.id AND month.currency = day.currency
ORDER BY
  day DESC;
"""

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

bigquery_client = bigquery.Client()


def gcp_cost_report(unused_data, unused_context):
    """Main entry point for the Cloud Function."""

    totals = defaultdict(lambda: defaultdict(float))
    lines = []
    for row in bigquery_client.query(BIGQUERY_QUERY):
        line = (
            f'*{row["project_id"]}:* ' f'this month: {row["month"]} {row["currency"]}'
        )
        totals[row['currency']]['month'] += row['month']
        if row['day']:
            line += f', last 24h: {row["day"]} {row["currency"]}'
            totals[row['currency']]['day'] += row['day']
        lines.append(line)

    total_lines = []
    for currency, vals in totals.items():
        total_lines.append(
            f'_*All projects:*_ this month: '
            f'{round(vals["month"], 2)} {currency}, '
            f'last 24h: {round(vals["day"], 2)} {currency}'
        )

    if lines:
        post_slack_message('\n'.join(total_lines + lines))


def post_slack_message(text):
    """Posts the given text as message to Slack."""

    try:
        slack_client.api_call(  # pylint: disable=duplicate-code
            'chat.postMessage',
            json={
                'channel': SLACK_CHANNEL,
                'text': text,
            },
        )
    except SlackApiError as err:
        logging.error(f'Error posting to Slack: {err}')
