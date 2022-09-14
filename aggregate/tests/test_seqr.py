"""seqr aggregator testing"""

import unittest
from collections import defaultdict
from datetime import datetime, timedelta
from unittest.mock import patch

from aggregate.billing_functions.seqr import (
    get_seqr_hosting_prop_map_from,
    get_shared_computation_prop_map,
    get_finalised_entries_for_batch,
)


def _get_default_sample_size_map(sample_id, size_offset: list[tuple[int, int]]):
    if any(day_offset > 15 for _, day_offset in size_offset):
        raise ValueError(f'Date offset too high (>15) for {size_offset}')
    return {
        'sample': sample_id,
        'dates': [
            {
                'start': datetime(2023, 1, day_offset or 1).date().isoformat(),
                'finish': None
                # finish := None if last in list
                if idx == (len(size_offset) - 1)
                # the next interval's start - 1 day
                else datetime(2023, 1, size_offset[idx + 1][1] - 1).date().isoformat(),
                'size': {'genome': size},
            }
            for idx, (size, day_offset) in enumerate(size_offset)
        ],
    }


class TestSeqrHostingPropMapFunctionality(unittest.TestCase):
    """
    Test the propmap functionality of the seqr billing aggregator
    """

    def test_seqr_hosting_prop_map_simple(self):
        """
        Simple test map with two datasets summing to 4:
            DS1: 1 sample with size=1 -> 25%
            DS2: 1 sample with size=3 -> 75%
        """
        sid_to_size = [('DS1', 'CPG1', 1), ('DS2', 'CPG2', 3)]

        sample_sizes_by_project = defaultdict(list)
        for i, (dataset, s, sz) in enumerate(sid_to_size):
            sample_sizes_by_project[dataset].append(
                _get_default_sample_size_map(s, [(sz, i + 1)])
            )

        dt = datetime(2023, 3, 1)
        analyses = [
            {
                'timestamp_completed': dt.isoformat(),
                'sample_ids': [s for _, s, _ in sid_to_size],
            }
        ]
        prop_map = get_seqr_hosting_prop_map_from(
            relevant_analyses=analyses, sample_sizes_by_project=sample_sizes_by_project
        )

        prop_map_expected = {'DS1': (0.25, 1), 'DS2': (0.75, 3)}
        self.assertEqual(1, len(prop_map))
        # noting that the propmap wipes two days off the analysis date
        # to cover analysis covered to generate the propmap
        self.assertEqual(dt - timedelta(days=2), prop_map[0][0])
        self.assertDictEqual(prop_map_expected, prop_map[0][1])

    def test_seqr_hosting_prop_map_complex(self):
        """
        More complex analysis with
            3 datasets across 2 dates
            w/ varying levels of samples in each
        calculations are listed below
        """

        project_list = ['DS1', 'DS2', 'DS3']

        sid_to_size = [(f'CPG{i}', i) for i in range(1, 11)]

        sample_sizes_by_project = defaultdict(list)
        for i, (s, sz) in enumerate(sid_to_size):
            p = project_list[i % len(project_list)]
            sample_sizes_by_project[p].append(
                _get_default_sample_size_map(s, [(sz, i + 1)])
            )

        analyses = [
            {
                'timestamp_completed': datetime(2023, 3, 1).isoformat(),
                'sample_ids': [s for s, _ in sid_to_size[:5]],
            },
            {
                'timestamp_completed': datetime(2023, 4, 1).isoformat(),
                'sample_ids': [s for s, _ in sid_to_size],
            },
        ]
        prop_map = get_seqr_hosting_prop_map_from(
            relevant_analyses=analyses, sample_sizes_by_project=sample_sizes_by_project
        )

        # 1 + 4:    5/15 => 0.33...
        # 2 + 5:    7/15 => 0.466...
        # 3:        3/15 => 0.2
        prop_map1 = prop_map[0][1]
        self.assertAlmostEqual(0.33333, prop_map1['DS1'][0], places=3)
        self.assertEqual(5, prop_map1['DS1'][1])
        self.assertAlmostEqual(0.46666, prop_map1['DS2'][0], places=3)
        self.assertEqual(7, prop_map1['DS2'][1])
        self.assertAlmostEqual(0.2, prop_map1['DS3'][0])
        self.assertEqual(3, prop_map1['DS3'][1])
        self.assertEqual(1, sum(v[0] for v in prop_map1.values()))

        # 1 + 4 + 7 + 10:   22/55 => 0.4
        # 2 + 5 + 8:        15/55 => 0.27...
        # 3 + 6 + 9:        18/55 => 0.3272...
        prop_map2 = prop_map[1][1]
        self.assertEqual(1, sum(v[0] for v in prop_map1.values()))
        self.assertAlmostEqual(0.4, prop_map2['DS1'][0])
        self.assertEqual(22, prop_map2['DS1'][1])
        self.assertAlmostEqual(0.272727, prop_map2['DS2'][0], places=3)
        self.assertEqual(15, prop_map2['DS2'][1])
        self.assertAlmostEqual(0.327272, prop_map2['DS3'][0], places=3)
        self.assertEqual(18, prop_map2['DS3'][1])


class TestSeqrGetFinalisedEntriesForBatch(unittest.TestCase):
    """Test the batch -> list[entries] functionality"""

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

        prop_map = [(datetime(2023, 1, 1), {'DS1': 0.25, 'DS2': 0.75})]
        resources = {
            'compute/n1-preemptible/1': 1e6,
            'memory/n1-preemptible/1': 1e6,
            'service-fee/1': 1,  # this should get filtered out
        }
        batch = {
            'id': 42,
            'time_created': '2023-03-03T11:22:33Z',
            'time_completed': '2023-03-03T12:22:33Z',
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

        #
        # n entries => (2 + 4) * 2 = 12 => (
        #   2 (for job 1) + 4 (for job 2: one for each dataset)
        #   * 2 (for credits)
        entries = get_finalised_entries_for_batch(batch, prop_map)

        expected_debits = entries[:6]
        expected_credits = entries[6:]

        # some basic checking
        self.assertEqual(12, len(entries))
        self.assertEqual(len(expected_credits), len(expected_debits))
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
        self.assertSetEqual({'seqr'}, set(e['service']['id'] for e in entries))
        self.assertEqual(
            {
                'Seqr compute Credit',
                'Seqr compute (distributed) Credit',
                'Seqr compute (distributed)',
                'Seqr compute',
            },
            set(e['service']['description'] for e in entries),
        )
        self.assertEqual(12, len(set(e['id'] for e in entries)))

        # check the proportionate cost is working correctly
        debits_for_job_2 = entries[2:6]
        total_debits_for_job_2 = sum(e['cost'] for e in debits_for_job_2)
        debits_for_job_2_ds1 = sum(
            e['cost'] for e in debits_for_job_2 if e['topic'] == 'DS1'
        )
        debits_for_job_2_ds2 = sum(
            e['cost'] for e in debits_for_job_2 if e['topic'] == 'DS2'
        )
        self.assertAlmostEqual(0.25, debits_for_job_2_ds1 / total_debits_for_job_2)
        self.assertAlmostEqual(0.75, debits_for_job_2_ds2 / total_debits_for_job_2)


class TestSeqrComputationPropMap(unittest.TestCase):
    """
    Test seqr hail prop map (by cram time_completed)
    """

    def test_seqr_computation_prop_map_basic(self):
        """Test super basic prop_map, 2 entries"""

        sid_to_size = [('DS1', 'CPG1', 1), ('DS2', 'CPG2', 3)]
        sample_sizes_by_project = defaultdict(list)
        for i, (d, s, sz) in enumerate(sid_to_size):
            sample_sizes_by_project[d].append(
                _get_default_sample_size_map(s, [(sz, i * 2 + 1)])
            )

        prop_map = get_shared_computation_prop_map(
            sample_sizes_by_project=sample_sizes_by_project,
            min_datetime=datetime.min,
            max_datetime=datetime.max,
        )

        self.assertEqual(2, len(prop_map))

        self.assertDictEqual({'DS1': (1.0, 1)}, prop_map[0][1])
        self.assertDictEqual({'DS1': (0.25, 1), 'DS2': (0.75, 3)}, prop_map[1][1])

    def test_seqr_computation_prop_map_more_complex(self):
        """
        Test seqr_computation prop_map generation on 11 crams
        across varous days, sizes and projects.
        """

        project_list = ['DS1', 'DS2', 'DS3']
        sid_to_size = [(f'CPG{i}', i) for i in range(1, 11)]
        sample_sizes_by_project = defaultdict(list)
        for i, (s, sz) in enumerate(sid_to_size):
            sample_sizes_by_project[project_list[i % len(project_list)]].append(
                _get_default_sample_size_map(s, [(sz, i + 1)])
            )

        prop_map = get_shared_computation_prop_map(
            sample_sizes_by_project=sample_sizes_by_project,
            min_datetime=datetime.min,
            max_datetime=datetime.max,
        )
        # we can sort of cheat and just check the last one, because they all
        # build off each other, so if the last is correct, it's likely they're
        # all correct, which means which we can pull the math from the seqr equiv:

        #   1 + 4 + 7 + 10:   22/55 => 0.4
        #   2 + 5 + 8:        15/55 => 0.27...
        #   3 + 6 + 9:        18/55 => 0.3272...
        prop_map_last = prop_map[-1][1]
        self.assertEqual(0.4, prop_map_last['DS1'][0])
        self.assertEqual(22, prop_map_last['DS1'][1])
        self.assertAlmostEqual(0.272727, prop_map_last['DS2'][0], places=3)
        self.assertEqual(15, prop_map_last['DS2'][1])
        self.assertAlmostEqual(0.327272, prop_map_last['DS3'][0], places=3)
        self.assertEqual(18, prop_map_last['DS3'][1])

    def test_seqr_computation_prop_map_condensed(self):
        """
        Compare a trunctated by min / max datetime to an non-condensed version
        """
        project_list = ['DS1', 'DS2', 'DS3']
        sid_to_size = [(f'CPG{i}', i) for i in range(1, 11)]
        cram_days = [1, 2, 3, 4]

        sample_sizes_by_project = defaultdict(list)
        for i, (s, sz) in enumerate(sid_to_size):
            sample_sizes_by_project[project_list[i % len(project_list)]].append(
                _get_default_sample_size_map(s, [(sz, cram_days[i % len(cram_days)])])
            )

        uncondensed_prop_map = get_shared_computation_prop_map(
            sample_sizes_by_project=sample_sizes_by_project,
            min_datetime=datetime.min,
            max_datetime=datetime.max,
        )
        # condensed map
        condensed_prop_map = get_shared_computation_prop_map(
            sample_sizes_by_project=sample_sizes_by_project,
            min_datetime=datetime(2023, 1, 2),
            max_datetime=datetime(2023, 1, 3, 23, 59),
        )

        self.assertEqual(4, len(uncondensed_prop_map))
        self.assertEqual(2, len(condensed_prop_map))

        self.assertDictEqual(uncondensed_prop_map[1][1], condensed_prop_map[0][1])
        self.assertDictEqual(uncondensed_prop_map[2][1], condensed_prop_map[1][1])
