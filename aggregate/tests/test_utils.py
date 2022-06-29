"""
    Test gcp aggregate functionality
"""

import unittest
from datetime import datetime, timedelta

import pandas as pd

from aggregate.billing_functions.utils import (
    parse_hail_time,
    billing_row_to_topic,
    date_range_iterator,
    get_start_and_end_from_data,
    process_default_start_and_end,
)


class TestUtilsFunctions(unittest.TestCase):
    """Test the helper functions functionality"""

    def test_parse_hail_time(self):
        """Check the parsing of hail time"""
        time = '2022-06-09T04:59:58Z'
        expected = datetime.fromisoformat('2022-06-09T04:59:58')
        result = parse_hail_time(time)
        self.assertEqual(result, expected)

    def test_date_range_iterator(self):
        """Check the date range iterator"""

        start, end = datetime(2019, 1, 1), datetime(2019, 1, 2)
        expected = [(datetime(2019, 1, 1, 0, 0), datetime(2019, 1, 2, 0, 0))]
        self.assertEqual(expected, list(date_range_iterator(start, end)))

        start, end = datetime(2019, 1, 1), datetime(2019, 1, 3)
        expected = [(datetime(2019, 1, 1, 0, 0), datetime(2019, 1, 3, 0, 0))]
        self.assertEqual(expected, list(date_range_iterator(start, end)))

        start, end = datetime(2019, 1, 1), datetime(2019, 1, 4)
        expected = [
            (datetime(2019, 1, 1, 0, 0), datetime(2019, 1, 3, 0, 0)),
            (datetime(2019, 1, 3, 0, 0), datetime.datetime(2019, 1, 4, 0, 0)),
        ]
        self.assertEqual(expected, list(date_range_iterator(start, end)))

    def test_billing_row_topic(self):
        """
        Check the conversion of rows to row with added topic
        """

        dataset_to_gcp_map = {
            'acute-care-321904': 'acute-care',
            'circa-716939': 'circa-name-is-different',
            'fewgenomes': 'fewgenomes',
            '123456-numbers-before': 'numbers-before',
        }

        rows = [
            {'project': {'id': 'abc-27361817262'}},
            {'project': None},
            {'project': {'id': None}},
            {'project': {'id': 'acute-care-321904'}},
            {'project': {'id': 'circa-716939'}},
            {'project': {'id': 'fewgenomes'}},
            {'project': {'id': '123456-numbers-before'}},
        ]

        expected_topics = [
            {'datamap': 'abc', 'no-datamap': 'abc'},
            {'datamap': 'admin', 'no-datamap': 'admin'},
            {'datamap': 'admin', 'no-datamap': 'admin'},
            {'datamap': 'acute-care', 'no-datamap': 'acute-care'},
            {'datamap': 'circa-name-is-different', 'no-datamap': 'circa'},
            {'datamap': 'fewgenomes', 'no-datamap': 'fewgenomes'},
            {'datamap': 'numbers-before', 'no-datamap': '123456-numbers-before'},
        ]

        rows = pd.DataFrame.from_dict(rows)

        # Call topic function
        for idx, expected in enumerate(expected_topics):
            row = rows.iloc[idx]
            result_datamap = billing_row_to_topic(row, dataset_to_gcp_map)
            result_no_datamap = billing_row_to_topic(row, {})

            self.assertEqual(result_datamap, expected['datamap'])
            self.assertEqual(result_no_datamap, expected['no-datamap'])

    def test_get_start_and_end_from_data(self):
        """Test the parsing of incoming data json"""

        json_str = '{"start": "2022-01-01", "end": "2022-01-02"}'
        strt, end = '2019-01-01', '2019-01-02'
        strt_dt, end_dt = datetime.fromisoformat(strt), datetime.fromisoformat(end)

        # Invalid or empty data #

        # No data
        data = None
        self.assertEqual(get_start_and_end_from_data(data), (None, None))

        # Has empty attributes
        data = {'attributes': None}
        self.assertEqual(get_start_and_end_from_data(data), (None, None))
        data = {'attributes': {}}
        self.assertEqual(get_start_and_end_from_data(data), (None, None))

        # Has start and/or end being empty
        data = {'start': None, 'end': None}
        self.assertEqual(get_start_and_end_from_data(data), (None, None))
        data = {'start': None, 'end': end}
        self.assertEqual(get_start_and_end_from_data(data), (None, end_dt))
        data = {'start': strt, 'end': None}
        self.assertEqual(get_start_and_end_from_data(data), (strt_dt, None))

        # Empty or meaningless message
        data = {'message': 'Hi I am a message'}
        self.assertEqual(get_start_and_end_from_data(data), (None, None))
        data = {'message': None}
        self.assertEqual(get_start_and_end_from_data(data), (None, None))

        # Valid data #

        # Has valid attributes and attributes takes priority
        data = {'attributes': {'start': strt, 'end': '2019-01-02'}}
        self.assertEqual(get_start_and_end_from_data(data), (strt_dt, end_dt))
        data = {
            'attributes': {'start': strt, 'end': '2019-01-02'},
            'message': 'Hi I am a message',
        }
        self.assertEqual(get_start_and_end_from_data(data), (strt_dt, end_dt))
        data = {'attributes': {'start': strt, 'end': '2019-01-02'}, 'message': json_str}
        self.assertEqual(get_start_and_end_from_data(data), (strt_dt, end_dt))
        data = {
            'attributes': {'start': strt, 'end': '2019-01-02'},
            'start': '2020-01-01',
            'end': '2020-01-02',
        }
        self.assertEqual(get_start_and_end_from_data(data), (strt_dt, end_dt))

        # Has valid start and end
        data = {'start': strt, 'end': '2019-01-02'}
        self.assertEqual(get_start_and_end_from_data(data), (strt_dt, end_dt))

        # Has a valid json message
        data = {'message': json_str}
        self.assertEqual(get_start_and_end_from_data(data), (strt_dt, end_dt))

    def test_process_default_start_and_end(self):
        """Test the default start and end"""

        # No data
        interval = timedelta(days=2)
        start, end = (None, None)
        strt_expected, end_expected = (datetime.now(), datetime.now() - interval)
        self.assertAlmostEqual(
            process_default_start_and_end(start, end, interval=interval),
            (strt_expected, end_expected),
        )

        # End only
        start, end = (None, datetime.fromisoformat('2019-01-02'))
        strt_expected, end_expected = (end - interval, end)
        self.assertEqual(
            process_default_start_and_end(start, end, interval=interval),
            (strt_expected, end_expected),
        )

        # Start only
        start, end = (datetime.fromisoformat('2019-01-01'), None)
        strt_expected, end_expected = (start, start + interval)
        self.assertEqual(
            process_default_start_and_end(start, end, interval=interval),
            (strt_expected, end_expected),
        )

        # Start and end
        start, end = (
            datetime.fromisoformat('2019-01-01'),
            datetime.fromisoformat('2019-01-02'),
        )
        self.assertEqual(
            process_default_start_and_end(start, end, interval=interval),
            (start, end),
        )