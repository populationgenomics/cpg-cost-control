"""Test hail aggregate functionality"""
import unittest
from collections import defaultdict
from unittest.mock import patch

from aggregate.billing_functions.hail import get_finalised_entries_for_batch


class TestHailGetFinalisedEntriesForBatch(unittest.TestCase):
    """Test the batch -> list[entries] functionality"""

    @patch('aggregate.billing_functions.utils.get_currency_conversion_rate_for_time')
    def test_simple(self, mock_currency_conversion_rate):
        """
        Pretty comprehensive function that tests
            get_finalised_entries_for_batch

        Also check there are the correct number of corresponding credits
        """
        # mock currency_conversion request to avoid hitting BQ
        mock_currency_conversion_rate.return_value = 2.0

        resources = {
            'compute/n1-preemptible/1': 1e6,
            'memory/n1-preemptible/1': 1e6,
            'service-fee/1': 1,  # this should get filtered out
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
                    'attributes': {'name': 'PROPORTIONATE_COST across DS1 / DS2'},
                },
            ],
            'attributes': {'name': 'TESTBATCH'},
        }

        # 2, one for the batch, one for the corresponding credit
        entries = get_finalised_entries_for_batch(batch)

        expected_debits = entries[:2]
        expected_credits = entries[2:]

        # some basic checking
        self.assertEqual(len(entries), 4)
        self.assertEqual(len(expected_credits), len(expected_debits))
        self.assertFalse(all(e['id'].endswith('-credit') for e in expected_debits))
        self.assertTrue(all(e['id'].endswith('-credit') for e in expected_credits))
        self.assertTrue(all(e['cost'] >= 0 for e in expected_debits))
        self.assertTrue(all(e['cost'] <= 0 for e in expected_credits))

        # we might have residual, due to rounding
        self.assertAlmostEqual(0, sum(e['cost'] for e in entries), places=10)

        count_per_topic = defaultdict(int)
        for e in entries:
            count_per_topic[e['topic']] += 1

        self.assertSetEqual({'DS1'}, set(e['topic'] for e in expected_debits))
        self.assertSetEqual({'hail'}, set(e['topic'] for e in expected_credits))
        self.assertSetEqual({'hail'}, set(e['service']['id'] for e in entries))
        self.assertEqual(
            {'Hail compute Credit', 'Hail compute'},
            set(e['service']['description'] for e in entries),
        )
        self.assertEqual(4, len(set(e['id'] for e in entries)))

    def test_should_ignore_seqr(self):
        """
        The hail aggregator should ignore any batches in
        seqr / hail billing projects.
        """
        resources = {
            'compute/n1-preemptible/1': 1e6,
            'memory/n1-preemptible/1': 1e6,
            'service-fee/1': 1,  # this should get filtered out
        }
        batch = {
            'id': 42,
            'time_created': '2020-03-03T11:22:33Z',
            'time_completed': '2020-03-03T12:22:33Z',
            'billing_project': 'seqr',
            'jobs': [{'job_id': 1, 'resources': resources}],
            'attributes': {},
        }

        seqr_entries = get_finalised_entries_for_batch(batch)
        self.assertEqual(0, len(seqr_entries))

        batch['billing_project'] = 'hail'
        hail_entries = get_finalised_entries_for_batch(batch)
        self.assertEqual(0, len(hail_entries))
