import unittest
from collections import defaultdict
from datetime import datetime, timedelta
from unittest.mock import patch

from aggregate.billing_functions.seqr import (
    get_prop_map_from,
    get_finalised_entries_for_batch,
)


class TestSeqrAggregateBillingFunctionality(unittest.TestCase):
    def test_seqr_prop_map_simple(self):
        project_id_map = {1: 'DS1', 2: 'DS2'}
        project_ids = list(project_id_map.keys())
        sid_to_size = [('CPG1', 100), ('CPG2', 300)]
        crams = [
            {
                'sample_ids': [sid],
                'meta': {'size': size},
                'project': project_ids[i % len(project_ids)],
            }
            for i, (sid, size) in enumerate(sid_to_size)
        ]
        dt = datetime(2020, 3, 1)
        analyses = [
            {
                'timestamp_completed': dt.isoformat(),
                'sample_ids': [s for s, _ in sid_to_size],
            }
        ]
        prop_map = get_prop_map_from(analyses, crams, project_id_map=project_id_map)
        print(prop_map)

        prop_map_expected = {'DS1': 0.25, 'DS2': 0.75}

        self.assertEqual(1, len(prop_map))
        self.assertEqual(dt - timedelta(days=2), prop_map[0][0])
        self.assertDictEqual(prop_map_expected, prop_map[0][1])

    def test_seqr_prop_map_complex(self):

        project_id_map = {1: 'DS1', 2: 'DS2', 3: 'DS3'}
        project_ids = list(project_id_map.keys())

        sid_to_size = [(f'CPG{i}', i * 100) for i in range(1, 11)]
        crams = [
            {
                'sample_ids': [sid],
                'meta': {'size': size},
                'project': project_ids[i % len(project_ids)],
            }
            for i, (sid, size) in enumerate(sid_to_size)
        ]

        analyses = [
            {
                'timestamp_completed': datetime(2020, 3, 1).isoformat(),
                'sample_ids': [s for s, _ in sid_to_size[:5]],
            },
            {
                'timestamp_completed': datetime(2020, 4, 1).isoformat(),
                'sample_ids': [s for s, _ in sid_to_size],
            },
        ]
        prop_map = get_prop_map_from(analyses, crams, project_id_map=project_id_map)
        print(prop_map)

        # 100 + 400 + 700 + 1000:
        # 200 + 500 + 800: 700 0.4666
        # 300 + 600 + 900      : 300 0.2

        prop_map1 = prop_map[0][1]
        self.assertAlmostEqual(0.33333, prop_map1['DS1'], places=3)
        self.assertAlmostEqual(0.46666, prop_map1['DS2'], places=3)
        self.assertAlmostEqual(0.2, prop_map1['DS3'])
        self.assertEqual(1, sum(prop_map1.values()))

        prop_map2 = prop_map[1][1]
        self.assertEqual(1, sum(prop_map1.values()))
        self.assertAlmostEqual(0.4, prop_map2['DS1'])
        self.assertAlmostEqual(0.272727, prop_map2['DS2'], places=3)
        self.assertAlmostEqual(0.327272, prop_map2['DS3'], places=3)


class TestSeqrGetFinalisedEntriesForBatch(unittest.TestCase):
    @patch('aggregate.billing_functions.utils.get_currency_conversion_rate_for_time')
    def test_simple(self, mock_currency_conversion_rate):
        """
        Pretty comprehensive function that tests
            get_finalised_entries_for_batch
        by setting up a batch with two jobs:
            1: dataset listed -> all assigned to DS1
            2: no dataset listed -> proportionally split

        Also check there are the correct number of corresponding credits
        """
        # mock currency_conversion request to avoid hitting BQ
        mock_currency_conversion_rate.return_value = 2.0

        prop_map = [(datetime(2020, 1, 1), {'DS1': 0.25, 'DS2': 0.75})]
        resources = {
            'compute/n1-preemptible/1': 1e6,
            'memory/n1-preemptible/1': 1e6,
            'service-fee/1': 1, # this should get filtered out
        }
        batch = {
            'id': 42,
            'time_created': '2020-03-03T11:22:33Z',
            'time_completed': '2020-03-03T12:22:33Z',
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

        # (2 + 4) * 2 = 12
        # n entries => (2 (for job 1) + 4 (for job 2: one for each dataset)) * 2 (for credits)
        entries = get_finalised_entries_for_batch(batch, prop_map)

        expected_debits = entries[:6]
        expected_credits = entries[6:]

        # some basic checking
        self.assertEqual(len(entries), 12)
        self.assertFalse(all(e['id'].endswith('-credit') for e in expected_debits))
        self.assertTrue(all(e['id'].endswith('-credit') for e in expected_credits))
        self.assertTrue(all(e['cost'] >= 0 for e in expected_debits))
        self.assertTrue(all(e['cost'] <= 0 for e in expected_credits))

        # we might have residual, due to rounding
        self.assertAlmostEqual(0, sum(e['cost'] for e in entries), places=10)

        # check the prop map number of entries is working as expected
        count_per_topic = defaultdict(int)
        for e in expected_debits:
            count_per_topic[e['topic']] += 1

        self.assertDictEqual({'DS1': 4, 'DS2': 2}, count_per_topic)
        self.assertSetEqual({'hail'}, set(e['topic'] for e in expected_credits))
        self.assertEqual(12, len(set(e['id'] for e in entries)))

        # check the proportionate cost is working correctly
        debits_for_job_2 = entries[2:6]
        total_debits_for_job_2 = sum(e['cost'] for e in debits_for_job_2)
        debits_for_job_2_ds1 = sum(e['cost'] for e in debits_for_job_2 if e['topic'] == 'DS1')
        debits_for_job_2_ds2 = sum(e['cost'] for e in debits_for_job_2 if e['topic'] == 'DS2')
        self.assertAlmostEqual(0.25, debits_for_job_2_ds1 / total_debits_for_job_2)
        self.assertAlmostEqual(0.75, debits_for_job_2_ds2 / total_debits_for_job_2)
