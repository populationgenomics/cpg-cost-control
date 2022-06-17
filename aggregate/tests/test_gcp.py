"""
    Test gcp aggregate functionality
"""

import unittest
import pandas as pd

# from collections import defaultdict
# from unittest.mock import patch

from aggregate.billing_functions.gcp import billing_row_to_topic  # , billing_row_to_key


class TestProcessingFunctions(unittest.TestCase):
    """Test the helper functions functionality"""

    def test_billing_row_topic(self):
        """
        Check the conversion of rows to row with added topic
        """

        dataset_to_gcp_map = {
            'acute-care-321904': 'acute-care',
            'circa-716939': 'circa-name-is-different',
            'fewgenomes': 'fewgenomes',
        }

        rows = [
            {'project': {'id': 'abc-27361817262'}},
            {'project': None},
            {'project': {'id': None}},
            {'project': {'id': 'acute-care-321904'}},
            {'project': {'id': 'circa-716939'}},
            {'project': {'id': 'fewgenomies'}},
        ]
        rows = pd.read_json(rows)

        topics = ['abc', 'admin', 'admin'] + rows.values().map(
            lambda x: x.get('project', {}).get('id')
        )

        # Call topic function
        rows['topic'] = rows.apply(
            lambda x: billing_row_to_topic(x, dataset_to_gcp_map), axis=1
        )
        topic_results = rows['topic'].values.tolist()

        # some basic checking
        self.assertEqual(topics, topic_results)
