"""
We want to make sure the different aggregators are accounting costs correctly
"""
import unittest.mock
from datetime import datetime

from aggregate.billing_functions.hail import (
    get_finalised_entries_for_batch as hail_finalise_batch,
)
from aggregate.billing_functions.seqr import (
    get_finalised_entries_for_batch as seqr_finalise_batch,
)

resources = {
    'compute/n1-preemptible/1': 27176048000,
    'disk/local-ssd/1': 652225152000,
    'disk/pd-ssd/1': 17392670720,
    'ip-fee/1024/1': 1739267072,
    'memory/n1-preemptible/1': 104356024320,
    'service-fee/1': 27176048000,
}

cost = {
    'compute/n1-preemptible/1': 0.00015,
    'disk/local-ssd/1': 0.00176,
    'disk/pd-ssd/1': 0.00063,
    'ip-fee/1024/1': 0.00000079,
    'memory/n1-preemptible/1': 0.00001,
    'service-fee/1': 100,
}
batch = {
    'id': 42,
    'time_created': '2023-03-03T11:22:33Z',
    'time_completed': '2023-03-03T12:22:33Z',
    'billing_project': 'DS1',
    'jobs': [
        {
            'job_id': 1,
            'resources': resources,
            'cost': cost,
            'attributes': {'dataset': 'DS1', 'name': 'ALL COST for DS1'},
        },
        {
            'job_id': 2,
            'resources': resources,
            'cost': cost,
            'attributes': {'name': 'DISTRUBUTED cost for DS1/DS2'},
        },
    ],
    'attributes': {'name': 'TESTBATCH'},
}


class TestCombinationCosts(unittest.TestCase):
    """
    Test that hail / seqr aggregators return equivalent costs
    """

    @unittest.mock.patch(
        'aggregate.billing_functions.utils.get_currency_conversion_rate_for_time'
    )
    def test_complex_job(self, mock_currency_conversion_rate):
        """
        Test a batch that:
            - hail aggregator will create one entry per resource per batch
            - seqr aggregator may create many entries per resource per batch:
                - Divided based on the proportional map and dataset attribute
        """
        mock_currency_conversion_rate.return_value = 2.0

        prop_map = [(datetime(2023, 1, 1), {'DS1': (0.3, 300), 'DS2': (0.7, 700)})]

        seqr_entries = seqr_finalise_batch(batch, prop_map)
        hail_entries = hail_finalise_batch(batch)

        seqr_debit_entries = [
            e for e in seqr_entries if not e['id'].endswith('-credit')
        ]
        hail_debit_entries = [
            e for e in hail_entries if not e['id'].endswith('-credit')
        ]

        seqr_cost = sum(e['cost'] for e in seqr_debit_entries)
        hail_cost = sum(e['cost'] for e in hail_debit_entries)

        # at some point there is some precision here
        self.assertAlmostEqual(seqr_cost, hail_cost, places=17)
