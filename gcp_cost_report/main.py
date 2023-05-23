# pylint: disable=too-many-locals
"""A Cloud Function to send a daily GCP cost report to Slack."""

# import json
import json
import logging
import os
from collections import defaultdict
from math import ceil, floor

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
  month.currency as currency,
  month.cost_category as cost_category
FROM
  (
    SELECT
      *
    FROM
      (
        SELECT
          project.id,
          ROUND(SUM(cost), 2) as cost,
          currency,
          (CASE
            WHEN service.description='Cloud Storage' THEN 'Storage Cost'
            ELSE 'Compute Cost'
            END) as cost_category
        FROM
          `{BIGQUERY_BILLING_TABLE}`
        WHERE
          _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 32 DAY)
          AND invoice.month = FORMAT_TIMESTAMP("%Y%m", CURRENT_TIMESTAMP(),
                                               "{QUERY_TIME_ZONE}")
        GROUP BY
          project.id,
          currency,
          cost_category
      )
    WHERE
      cost > 0.1
  ) month
  LEFT JOIN (
    SELECT
      project.id,
      ROUND(SUM(cost), 2) as cost,
      currency,
      (CASE
        WHEN service.description='Cloud Storage' THEN 'Storage Cost'
        ELSE 'Compute Cost'
        END) as cost_category
    FROM
      `{BIGQUERY_BILLING_TABLE}`
    WHERE
      _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
      AND export_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
      AND invoice.month = FORMAT_TIMESTAMP("%Y%m", CURRENT_TIMESTAMP(),
                                           "{QUERY_TIME_ZONE}")
    GROUP BY
      project.id,
      currency,
      cost_category
  ) day ON month.id = day.id AND month.currency = day.currency
  AND month.cost_category = day.cost_category
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

    totals = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    budgets = budget_client.list_budgets(parent=f'billingAccounts/{BILLING_ACCOUNT_ID}')
    budgets_map = {b.display_name: b for b in budgets}

    def format_billing_row(project_id, fields, currency):
        def format_cost_categories(data):
            values = [
                f'{k.capitalize()[0]}: {data[k]:.2f}' for k in sorted(data.keys())
            ]
            return ' '.join(values) + ' ' + currency

        row_str_1 = format_cost_categories(fields['day'])
        row_str_2 = format_cost_categories(fields['month'])

        if project_id in budgets_map:
            _, percent_used_str = get_percent_used_from_budget(
                budgets_map[project_id],
                fields['month']['total'],
                currency,
            )
            if percent_used_str:
                row_str_2 += f' ({percent_used_str})'
        else:
            logging.warning(
                f"Couldn't find project_id {project_id} in "
                f"budgets: {', '.join(budgets_map.keys())}"
            )

        return row_str_1, row_str_2

    summary_header = (
        '*Project*',
        '*24h cost*',
        '*Project*',
        '*Month cost (% total used of budget)*',
    )
    project_summary: list[tuple[str, str]] = []
    totals_summary: list[tuple[str, str]] = []
    grouped_rows = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    )

    for row in bigquery_client.query(BIGQUERY_QUERY):
        project_id = row['project_id'] or '<none>'
        currency = row['currency']
        cost_category = row['cost_category']
        last_month = row['month']
        percent_used = None

        totals[currency][cost_category]['month'] += row['month']

        if row['day']:
            totals[currency][cost_category]['day'] += row['day']

        grouped_rows[project_id][currency]['day'][cost_category] = row['day']
        grouped_rows[project_id][currency]['month'][cost_category] = row['month']
        grouped_rows[project_id][currency]['day']['total'] += row['day']
        grouped_rows[project_id][currency]['month']['total'] += row['month']

    for project_id, by_currency in grouped_rows.items():
        for currency, row in by_currency.items():
            row_str_1, row_str_2 = format_billing_row(project_id, row, currency)

            # potential formatting
            if percent_used is not None:
                if percent_used >= 0.8:
                    # make fields bold
                    project_id = f'*{project_id}*'
                    row_str_1 = f'*{row_str_1}*'
                    row_str_2 = f'*{row_str_2}*'

            project_summary.append((project_id, row_str_1, project_id, row_str_2))

    if len(totals) == 0:
        logging.info(
            "No information to log, this function won't log anything to slack."
        )
        return

    for currency, by_category in totals.items():
        fields = defaultdict(lambda: defaultdict(float))
        day_total = 0
        month_total = 0
        for cost_category, vals in by_category.items():
            last_day = vals['day']
            last_month = vals['month']
            day_total += last_day
            month_total += last_month
            fields['day'][cost_category] = last_day
            fields['month'][cost_category] = last_month
            fields['day']['total'] += last_day
            fields['month']['total'] += last_month

        # totals don't have percent used
        a, b = format_billing_row(None, fields, currency)
        totals_summary.append(
            (
                '_All projects:_',
                a,
                '_All projects:_',
                b,
            )
        )

        header_message = (
            'Costs are Compute (C), Storage (S) and Total (T) by the past 24h '
            'and then by month. '
            'For further details, visit our new cost dashboard '
            '<https://lookerstudio.google.com/s/jRJO_N3R9a4 | cost dashboard '
            '(by topic)> '
            'or our old cost dashboard for costs by gcp project directly '
            '<https://lookerstudio.google.com/s/o0SqK6vPhkc | cost dashboard '
            '(gcp direct)>.'
        )
        dashboard_message = {
            'type': 'mrkdwn',
            'text': header_message,
        }
        all_rows = [*totals_summary, *project_summary]

        def chunks(lst, n):
            step = floor(len(lst) / n)
            for i in range(0, len(lst), step):
                yield lst[i : i + step]

        def num_chars(header_message, lst):
            return len(header_message + ''.join(lst))

        if len(all_rows) > 1:

            def wrap_in_mrkdwn(a):
                return {'type': 'mrkdwn', 'text': a}

            n_chunks = ceil(num_chars(header_message, list(sum(all_rows, ()))) / 2200)
            logging.info(f'Breaking body into {n_chunks}')
            logging.info(f'Total num rows: {len(all_rows)}')

            for chunk in list(chunks(all_rows, n_chunks)):
                n_chars = num_chars(header_message, [''.join(list(a)) for a in chunk])
                logging.info(f'Chunk rows: {len(chunk)}')
                logging.info(f'Chunk size: {n_chars}')

                # Add header at the start
                chunk = [summary_header] + chunk

                # Add blank row at the end
                chunk.append(['*--------------------*'] * 4)

                body = [
                    wrap_in_mrkdwn('\n'.join(a[0] for a in chunk)),
                    wrap_in_mrkdwn('\n'.join(a[1] for a in chunk)),
                    wrap_in_mrkdwn('\n'.join(a[2] for a in chunk)),
                    wrap_in_mrkdwn('\n'.join(a[3] for a in chunk)),
                ]

                blocks = [
                    {'type': 'section', 'text': dashboard_message, 'fields': body}
                ]
                post_slack_message(blocks=blocks)


def get_percent_used_from_budget(b, last_month_total, currency):
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
