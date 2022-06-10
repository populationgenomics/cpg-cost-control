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
    "compute/n1-preemptible/1": 27176048000,
    "disk/local-ssd/1": 652225152000,
    "disk/pd-ssd/1": 17392670720,
    "ip-fee/1024/1": 1739267072,
    "memory/n1-preemptible/1": 104356024320,
    "service-fee/1": 27176048000,
}
batch = {
    'id': 42,
    'time_created': '2020-03-03T11:22:33Z',
    'time_completed': '2020-03-03T12:22:33Z',
    'billing_project': 'DS1',
    'jobs': [
        {
            'job_id': 1,
            'resources': resources,
            'attributes': {'dataset': 'DS1', 'name': 'ALL COST for DS1'},
        },
        {
            'job_id': 2,
            'resources': resources,
            'attributes': {'name': 'DISTRUBUTED cost for DS1/DS2'},
        },
    ],
    'attributes': {'name': 'TESTBATCH'},
}


class TestCombinationCosts(unittest.TestCase):
    @unittest.mock.patch(
        'aggregate.billing_functions.utils.get_currency_conversion_rate_for_time'
    )
    def test_simple_single_job(self, mock_currency_conversion_rate):

        mock_currency_conversion_rate.return_value = 2.0

        prop_map = [(datetime(2020, 1, 1), {'DS1': 0.3, 'DS2': 0.7})]

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

        self.assertEqual(seqr_cost, hail_cost)
