"""A handful of integration tests to check for idiomatic use cases
that we want make sure are as convinient as possible for users.
"""

import logging
import unittest
from io import StringIO

try:
    import pandas as pd
except ImportError:
    pd = None

from toron.node import Node
from toron.graph import load_mapping


class TestIdiomaticUsage(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

        self.node1 = Node()
        self.node1.add_index_columns('idx1', 'idx2', 'idx3')
        self.node1.add_discrete_categories({'idx1'}, {'idx1', 'idx2'})
        self.node1.insert_index([
            ['idx1', 'idx2', 'idx3'],
            ['A', 'z', 'a'],
            ['B', 'x', 'b'],
            ['B', 'y', 'c'],
            ['C', 'x', 'd'],
            ['C', 'y', 'e'],
            ['D', 'x', 'f'],
            ['D', 'x', 'g'],
            ['D', 'y', 'h'],
            ['D', 'y', 'i'],
        ])
        self.node1.add_weight_group('wght', make_default=True)
        self.node1.insert_weights(
            weight_group_name='wght',
            data=[
                ['idx1', 'idx2', 'idx3', 'wght'],
                ['A', 'z', 'a', 72],
                ['B', 'x', 'b', 37.5],
                ['B', 'y', 'c', 62.5],
                ['C', 'x', 'd', 75],
                ['C', 'y', 'e', 25],
                ['D', 'x', 'f', 25],
                ['D', 'x', 'g', 0],
                ['D', 'y', 'h', 50],
                ['D', 'y', 'i', 25],
            ],
        )
        self.node1.insert_quantities(
            data=[
                ['idx1', 'idx2', 'idx3', 'variable', 'value'],
                ['A', 'z', 'a', 'foo', 100],
                ['B', 'x', 'b', 'foo', 100],
                ['B', 'y', 'c', 'bar', 100],
                ['C', 'x', 'd', 'bar', 100],
                ['C', 'y', 'e', 'bar', 100],
                ['D', 'x', 'f', 'bar', 100],
                ['D', 'x', 'g', 'baz', 100],
                ['D', 'y', 'h', 'baz', 100],
                ['D', 'y', 'i', 'baz', 100],
            ],
            value='value',
            attributes='variable',
        )

        self.node2 = Node()
        self.node2.add_index_columns('idx1', 'idx2')
        self.node2.add_discrete_categories({'idx1'})
        self.node2.insert_index([
            ['idx1', 'idx2'],
            ['A', 'Athens'],
            ['A', 'Boston'],
            ['B', 'Charleston'],
            ['C', 'Dover'],
            ['C', 'Erie'],
            ['D', 'Fayetteville'],
            ['D', 'Greensboro'],
            ['D', 'Hartford'],
            ['D', 'Irvine'],
        ])
        self.node2.add_weight_group('wght', make_default=True)
        self.node2.insert_weights(
            weight_group_name='wght',
            data=[
                ['idx1', 'idx2', 'wght'],
                ['A', 'Athens', 25],
                ['A', 'Boston', 75],
                ['B', 'Charleston', 80],
                ['C', 'Dover', 25],
                ['C', 'Erie', 75],
                ['D', 'Fayetteville', 37.5],
                ['D', 'Greensboro', 43.75],
                ['D', 'Hartford', 31.25],
                ['D', 'Irvine', 31.25],
            ],
        )

        # Set up stream object to capture log messages.
        self.log_stream = StringIO()
        self.addCleanup(self.log_stream.close)

        # Add handler to 'app-toron' logger.
        applogger = logging.getLogger('app-toron')
        handler = logging.StreamHandler(self.log_stream)
        handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        applogger.addHandler(handler)
        self.addCleanup(lambda: applogger.removeHandler(handler))

        # Add an exact mapping between node1 and node2.
        load_mapping(
            left_node=self.node1,
            direction='->',
            right_node=self.node2,
            crosswalk_name='population',
            data=[
                ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2'],
                ['A', 'z', 'a',  25, 'A', 'Athens'],
                ['A', 'z', 'a',  25, 'A', 'Boston'],
                ['B', 'x', 'b',  50, 'B', 'Charleston'],
                ['B', 'y', 'c',  50, 'B', 'Charleston'],
                ['C', 'x', 'd',  55, 'C', 'Dover'],
                ['C', 'y', 'e',  50, 'C', 'Erie'],
                ['D', 'x', 'f', 100, 'D', 'Fayetteville'],
                ['D', 'x', 'g', 100, 'D', 'Greensboro'],
                ['D', 'y', 'h', 100, 'D', 'Hartford'],
                ['D', 'y', 'i', 100, 'D', 'Irvine'],
            ],
        )

    def test_disagg_translate(self):
        result_iter = self.node1('[variable="foo"]') >> self.node2

        self.assertEqual(
            result_iter.columns,
            ('idx1', 'idx2', 'variable', 'value'),
        )
        self.assertEqual(
            list(result_iter),
            [('A', 'Athens', 'foo', 50.0),
             ('A', 'Boston', 'foo', 50.0),
             ('B', 'Charleston', 'foo', 100.0)],
        )

    @unittest.skipUnless(pd, 'requires pandas')
    def test_disagg_translate_to_pandas(self):
        df = (self.node1('[variable="foo"]') >> self.node2).to_pandas()

        expected_df = pd.DataFrame({
            'idx1': pd.Series(['A', 'A', 'B'], dtype='string'),
            'idx2': pd.Series(['Athens', 'Boston', 'Charleston'], dtype='string'),
            'variable': pd.Series(['foo', 'foo', 'foo'], dtype='string'),
            'value': pd.Series([50.0, 50.0, 100.0], dtype='float64'),
        })
        pd.testing.assert_frame_equal(df, expected_df)
