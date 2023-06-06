# pylint: disable=too-many-locals
"""A Cloud Function to send a daily GCP cost report to Slack."""

# import json
import os
import json
import logging
import calendar
from datetime import date
from collections import defaultdict
from math import ceil

from google.cloud import bigquery
from google.cloud import secretmanager
import google.cloud.billing.budgets_v1.services.budget_service as budget

import slack
from slack.errors import SlackApiError


PROJECT_ID = os.getenv('GCP_PROJECT')
BIGQUERY_BILLING_TABLE = os.getenv('BIGQUERY_BILLING_TABLE')
QUERY_TIME_ZONE = os.getenv('QUERY_TIME_ZONE') or 'UTC'
SLACK_MESSAGE_MAX_CHARS = 2000

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


def month_progress() -> float:
    """Return the percentage we are through the month"""
    monthrange = calendar.monthrange(date.today().year, date.today().month)[1]
    return date.today().day / monthrange


def gcp_cost_report(unused_data, unused_context):
    """Main entry point for the Cloud Function."""

    totals = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    budgets = budget_client.list_budgets(parent=f'billingAccounts/{BILLING_ACCOUNT_ID}')
    budgets_map = {b.display_name: b for b in budgets}

    # Work out what percentage of the way we are through the month
    percent_threshold = month_progress()

    def format_billing_row(project_id, fields, currency, percent_threshold=0):
        def money_format(money):
            if money > 100:
                return f'{money:.0f}'
            return f'{money:.2f}'

        def format_cost_categories(data, currency):
            values = [
                f'{k.capitalize()[0]}: {money_format(data[k])}'
                for k in sorted(data.keys())
            ]
            currency = ' ' + currency if 'AUD' not in currency else ''
            return ' '.join(values) + currency

        row_str_1 = format_cost_categories(fields['day'], currency)
        row_str_2 = format_cost_categories(fields['month'], currency)

        percent_used = 0
        if project_id in budgets_map:
            percent_used, percent_used_str = get_percent_used_from_budget(
                budgets_map[project_id],
                fields['month']['total'],
                currency,
            )
            if percent_used_str:
                row_str_2 += f' ({percent_used_str})'

            # potential formatting
            if percent_used is not None:
                if percent_used >= 0.8:
                    # make fields bold
                    project_id = f'*{project_id}*'
                    row_str_1 = f'*{row_str_1}*'
                    row_str_2 = f'*{row_str_2}*'

        else:
            logging.warning(
                f"Couldn't find project_id {project_id} in "
                f"budgets: {', '.join(budgets_map.keys())}"
            )

        sort_key = (
            percent_used if percent_used >= percent_threshold else 0,
            sum(fields['day'].values()),
        )

        return sort_key, project_id, row_str_1, row_str_2

    summary_header = [
        '*Flagged Projects*',
        '*24h cost/Month cost (% used)*',
    ]
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

        totals[currency][cost_category]['month'] += row['month']

        if row['day']:
            totals[currency][cost_category]['day'] += row['day']

        grouped_rows[project_id][currency]['day'][cost_category] = row['day']
        grouped_rows[project_id][currency]['month']['total'] += row['month']

    for project_id, by_currency in grouped_rows.items():
        for currency, row in by_currency.items():
            sort_key, project_id, row_str_1, row_str_2 = format_billing_row(
                project_id, row, currency, percent_threshold
            )
            project_summary.append(
                {'sort': sort_key, 'value': (project_id, row_str_1 + ' / ' + row_str_2)}
            )

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
            # fields['month'][cost_category] = last_month
            # fields['day']['total'] += last_day
            fields['month']['total'] += last_month

        # totals don't have percent used
        _, _, a, b = format_billing_row(None, fields, currency)
        totals_summary.append(
            (
                '_All projects:_',
                a + ' / ' + b,
            )
        )

        header_message = (
            'Costs are Compute (C), Storage (S) and Total (T) by the past 24h and then '
            f'by month. Sorted by percent used (if > {percent_threshold*100:.0f}%) '
            'followed by sum of daily cost descending. This first message contains all '
            'flagged projects that are exceeding the budget so far this month. '
            'For visual breakdowns see the '
            '<https://lookerstudio.google.com/s/jRJO_N3R9a4 | new cost dashboard '
            '(by topic)> or our '
            '<https://lookerstudio.google.com/s/o0SqK6vPhkc | old cost dashboard'
            ' (gcp direct)> for costs by gcp project directly.'
        )
        dashboard_message = {
            'type': 'mrkdwn',
            'text': header_message,
        }
        flagged_projects = [
            x['value']
            for x in sorted(project_summary, key=lambda x: x['sort'], reverse=True)
            if x['sort'][0]
        ]
        sorted_projects = [
            x['value']
            for x in sorted(project_summary, key=lambda x: x['sort'], reverse=True)
            if not x['sort'][0]
        ]

        all_rows = [*totals_summary, *sorted_projects]

        def chunk_list(lst):
            n = max(n, 1)
            step = ceil(len(lst) / n)
            for i in range(0, len(lst), step):
                yield lst[i : i + step]

        def num_chars(lst):
            return len(''.join(lst))

        if len(all_rows) > 1 or len(flagged_projects) > 1:

            def wrap_in_mrkdwn(a):
                return {'type': 'mrkdwn', 'text': a}

            n_chunks = ceil(
                num_chars(list(sum(all_rows, ()))) / SLACK_MESSAGE_MAX_CHARS
            )
            logging.info(f'Breaking body into {n_chunks}')
            logging.info(f'Total num rows: {len(all_rows)}')

            # Make first chunk the flagged projects, then chunk by size after
            chunks = [flagged_projects] + list(chunk_list(all_rows, n_chunks))

            for i, chunk in enumerate(chunks):
                n_chars = num_chars([''.join(list(a)) for a in chunk])
                logging.info(f'Chunk rows: {len(chunk)}')
                logging.info(f'Chunk size: {n_chars}')

                # Add header at the start
                logging.info(f'Chunk: {chunk}')

                # Only post dashboard message on first chunk
                # and switch to 'Projects' not 'Flagged Projects' after first chunk
                if i != 0:
                    summary_header[0] = '*Projects*'
                    dashboard_message['text'] = ' '

                # Add header to the top and a blank row at the end
                chunk = [summary_header] + chunk
                chunk.append(['*--------------------*'] * 2)

                body = [
                    wrap_in_mrkdwn('\n'.join(a[0] for a in chunk)),
                    wrap_in_mrkdwn('\n'.join(a[1] for a in chunk)),
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


if __name__ == '__main__':
    gcp_cost_report(None, None)
