"""A Cloud Function to send a daily GCP cost report to Slack."""

# import json
import json
import logging
import os
from collections import defaultdict
from typing import Tuple, List

from google.cloud import bigquery
from google.cloud import secretmanager
import google.cloud.billing.budgets_v1.services.budget_service as budget

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
BILLING_ACCOUNT_ID = os.getenv('BILLING_ACCOUNT_ID')

# Cache the Slack client.
secret_manager = secretmanager.SecretManagerServiceClient()
slack_token_response = secret_manager.access_secret_version(
    request={'name': SLACK_TOKEN_SECRET_NAME}
)
slack_token = slack_token_response.payload.data.decode('UTF-8')
slack_client = slack.WebClient(token=slack_token)

bigquery_client = bigquery.Client()
budget_client = budget.BudgetServiceClient()


def try_cast_int(i):
    """Cast i to int, else return None if ValueError"""
    try:
        return int(i)
    except ValueError:
        return None


def try_cast_float(f):
    """Cast i to float, else return None if ValueError"""
    try:
        return float(f)
    except ValueError:
        return None


def gcp_cost_report(unused_data, unused_context):
    """Main entry point for the Cloud Function."""

    totals = defaultdict(lambda: defaultdict(float))
    budgets = budget_client.list_budgets(parent=f'billingAccounts/{BILLING_ACCOUNT_ID}')
    budgets_map = {b.display_name: b for b in budgets}

    def add_currency_to_non_null_els(fields, currency, separator=' | '):
        return separator.join(f'{el} {currency}' for el in fields if el is not None)

    summary_header = ('Project', '24h | month (% used)')
    project_summary: List[Tuple[str, str]] = []
    totals_summary: List[Tuple[str, str]] = []

    for row in bigquery_client.query(BIGQUERY_QUERY):
        project_id = row['project_id']
        currency = row['currency']
        last_month = row['month']
        last_month_str = f'{last_month:.2f}'
        last_day_str = None
        percent_used = None

        if row['day']:
            last_day_str = f'{row["day"]:.2f}'
            totals[currency]['day'] += row['day']

        row_str = add_currency_to_non_null_els([last_day_str, last_month_str], currency)

        if project_id in budgets_map:
            percent_used, percent_used_str = get_percent_used_from_budget(
                budgets_map[project_id],
                last_month,
                currency,
            )
            if percent_used_str:
                row_str += f' ({percent_used_str})'

        else:
            logging.warning(
                f"Couldn't find project_id {project_id} in "
                f"budgets: {', '.join(budgets_map.keys())}"
            )

        # potential formating
        if percent_used is not None:
            if percent_used >= 0.8:
                # make fields bold
                project_id = f'*{project_id}*'
                row_str = f'*{row_str}*'

        project_summary.append((project_id, row_str))

    for currency, vals in totals.items():
        last_day_str = f'{vals["day"]:.2f}'
        last_month_str = f'{vals["month"]:.2f}'

        # totals don't have percent used
        totals_summary.append(
            (
                '_All projects:_',
                add_currency_to_non_null_els([last_day_str, last_month], currency),
            )
        )

        all_rows = [summary_header, *totals_summary, *project_summary]
        if len(all_rows) > 1:

            def wrap_in_mrkdwn(a):
                return {'type': 'mrkdwn', 'text': a}

            body = [
                wrap_in_mrkdwn('\n'.join(a[0] for a in all_rows)),
                wrap_in_mrkdwn('\n'.join(a[1] for a in all_rows)),
            ]
            blocks = [{'type': 'section', 'fields': body}]
            post_slack_message(blocks=blocks)


def get_percent_used_from_budget(b, last_month_total, currency) -> Tuple[float, str]:
    """Get percent_used as a string from GCP billing budget"""
    percent_used = None
    percent_used_str = ''
    inner_amount = b.amount.specified_amount
    if not inner_amount:
        return None, ''
    budget_currency = inner_amount.currency_code

    # 'units' is an int64, which is represented as a string in JSON,
    # this can be safely stored in Python3: https://stackoverflow.com/a/46699498
    budget_total = try_cast_int(inner_amount.units)
    monthly_used_float = try_cast_float(last_month_total)

    if budget_total and monthly_used_float:
        percent_used = monthly_used_float / budget_total
        percent_used_str = f'{round(percent_used * 100)}%'
        if budget_currency != currency:
            # there's a currency mismatch
            percent_used_str += (
                f' (mismatch currency, budget: {budget_currency} | total: {currency})'
            )

    else:
        logging.warning(
            "Couldn't determine the budget amount from the budget, "
            f'inner_amount.units: {inner_amount.units}, '
            f'monthly_used_float: {monthly_used_float}'
        )

    return percent_used, percent_used_str


def post_slack_message(blocks):
    """Posts the given text as message to Slack."""
    try:
        slack_client.api_call(  # pylint: disable=duplicate-code
            'chat.postMessage',
            json={
                'channel': SLACK_CHANNEL,
                'blocks': json.dumps(blocks),
            },
        )
    except SlackApiError as err:
        logging.error(f'Error posting to Slack: {err}')
