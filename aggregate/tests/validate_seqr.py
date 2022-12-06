# pylint: disable=W0603
"""
    Script to validate the seqr credits
"""

import os
import logging

import numpy as np
from pandas import DataFrame
import google.cloud.bigquery as bq

# Logging
logger = logging.getLogger('validating-seqr')

# Constants
GCP_AGGREGATE_DEST_TABLE = os.getenv(
    'GCP_AGGREGATE_DEST_TABLE', 'billing-admin-290403.billing_aggregate.aggregate'
)
_BQ_CLIENT: bq.Client = None


def get_bigquery_client():
    """Get instantiated cached bq client"""
    global _BQ_CLIENT
    if not _BQ_CLIENT:
        _BQ_CLIENT = bq.Client()
    return _BQ_CLIENT


def get_bq_data(query: str, params: list | None) -> DataFrame:
    """
    Retrieve the billing data using the provided query
    Return results as a dataframe
    """

    job_config = bq.QueryJobConfig(query_parameters=params)
    data = (
        get_bigquery_client()
        .query(query, job_config=job_config)
        .result()
        .to_dataframe()
    )

    cost_sign = np.vectorize(lambda x: 1 if x > 0 else -1)

    data = data.assign(abscost=abs(data.cost))
    data = data.assign(signcost=cost_sign(data.cost))
    data = data[data.cost != 0]
    data = data.sort_values(['abscost', 'signcost'], ascending=False)
    grouped = data.groupby(['abscost', 'signcost'], sort=False)

    # Get group counts
    counts = grouped.size().reset_index().rename(columns={0: 'count'})
    counts['signcount'] = counts['signcost'] * counts['count']

    # Sum group counts and find any that aren't zero
    cost_group = counts.groupby('abscost')['signcount'].sum().reset_index()
    unequal_credits = cost_group[cost_group.signcount != 0].reset_index()
    unequal_credits['difference'] = unequal_credits.abscost * unequal_credits.signcount
    credit_diff = sum(unequal_credits.difference)

    # Report incorrect groups
    if credit_diff != 0:
        logger.error(f'Credits are unbalanced by {credit_diff}')
        logger.error(f'Check these costs: {unequal_credits.abscost}')

    # Problem data
    issues = data[data.abscost.isin(unequal_credits.abscost)]
    return data, issues


def main():
    """Main function"""

    query = f"""
        SELECT *
        FROM `{GCP_AGGREGATE_DEST_TABLE}`
        WHERE service.id = @service_id
        AND invoice.month = '202211'
    """
    params = [
        bq.ScalarQueryParameter('invoice_month', 'STRING', '202211'),
        bq.ScalarQueryParameter('service_id', 'STRING', 'seqr'),
    ]
    get_bq_data(query, params)


if __name__ == '__main__':
    main()
