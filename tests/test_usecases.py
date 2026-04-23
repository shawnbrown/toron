"""A handful of integration tests to check for idiomatic use cases
that we want make sure are as convinient as possible for users.
"""

import argparse
import logging
import unittest
from io import StringIO

try:
    import pandas as pd
except ImportError:
    pd = None

from .common import DummyRedirection
from toron.node import TopoNode
from toron.graph import load_mapping
from toron import cli


class TestBuildUsingAPI(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

        self.node1 = TopoNode()
        self.node1.add_index_columns('idx1', 'idx2', 'idx3')
        self.node1.add_discrete_categories({'idx1'}, {'idx1', 'idx2'})
        self.node1.add_weight_group('wght', make_default=True)
        self.node1.insert_index([
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
        ])
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

        self.node2 = TopoNode()
        self.node2.add_index_columns('idx1', 'idx2')
        self.node2.add_discrete_categories({'idx1'})
        self.node2.add_weight_group('wght', make_default=True)
        self.node2.insert_index([
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

        ])

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

    def test_multiple_disagg(self):
        """Should be able to iterate over multiple disaggregations concurrently."""
        result_iter1 = self.node1('[variable="foo"]')
        result_iter2 = self.node1('[variable="bar"]')

        # Iterate over multiple disaggregations at the same time.
        self.assertEqual(next(result_iter1), ('A', 'z', 'a', 'foo', 100.0))
        self.assertEqual(next(result_iter2), ('B', 'y', 'c', 'bar', 100.0))
        self.assertEqual(next(result_iter1), ('B', 'x', 'b', 'foo', 100.0))
        self.assertEqual(next(result_iter2), ('C', 'x', 'd', 'bar', 100.0))
        self.assertEqual(next(result_iter2), ('C', 'y', 'e', 'bar', 100.0))
        self.assertEqual(next(result_iter2), ('D', 'x', 'f', 'bar', 100.0))
        with self.assertRaises(StopIteration):
            next(result_iter1)
        with self.assertRaises(StopIteration):
            next(result_iter2)

    def test_disagg_trans(self):
        result_iter = self.node1('[variable="foo"]') >> self.node2

        self.assertEqual(
            result_iter.columns,
            ['idx1', 'idx2', 'variable', 'value'],
        )
        self.assertEqual(
            set(result_iter),
            {('A', 'Athens', 'foo', 50.0),
             ('A', 'Boston', 'foo', 50.0),
             ('B', 'Charleston', 'foo', 100.0)},
        )

    def test_disagg_trans_negative_quantities(self):
        """Test disaggregation including negative quantities."""
        self.node1.insert_quantities(
            data=[
                ['idx1', 'idx2', 'idx3', 'variable', 'value'],
                ['A', 'z', 'a', 'qux', -8.0],
                ['B', 'x', 'b', 'qux', +3.0],
                ['B', 'y', 'c', 'qux', -8.0],
            ],
            value='value',
            attributes='variable',
        )

        result_iter = self.node1('[variable="foo"]', '[variable="qux"]') >> self.node2

        self.assertEqual(
            result_iter.columns,
            ['idx1', 'idx2', 'variable', 'value'],
        )
        self.assertEqual(
            set(result_iter),
            {('A', 'Athens',     'foo',  50.0),
             ('A', 'Boston',     'foo',  50.0),
             ('B', 'Charleston', 'foo', 100.0),
             ('A', 'Athens',     'qux',  -4.0),   # <- Half of source (-8.0).
             ('A', 'Boston',     'qux',  -4.0),   # <- Half of source (-8.0).
             ('B', 'Charleston', 'qux',  -5.0)},  # <- Combined sources (-8.0 and +3.0).
        )

    @unittest.skipUnless(pd, 'requires pandas')
    def test_disagg_trans_pandas(self):
        # Disaggregate, translate, and convert to DataFrame.
        df = (self.node1('[variable="foo"]') >> self.node2).to_pandas()

        expected_df = pd.DataFrame({
            'idx1': pd.Series(['A', 'A', 'B'], dtype='string'),
            'idx2': pd.Series(['Athens', 'Boston', 'Charleston'], dtype='string'),
            'variable': pd.Series(['foo', 'foo', 'foo'], dtype='string'),
            'value': pd.Series([50.0, 50.0, 100.0], dtype='float64'),
        })
        pd.testing.assert_frame_equal(df, expected_df)

        # Disaggregate, translate, convert to DataFrame, and set the index.
        df = (self.node1('[variable="foo"]') >> self.node2).to_pandas(index=True)

        expected_df = pd.DataFrame({
            'idx1': pd.Series(['A', 'A', 'B'], dtype='string'),
            'idx2': pd.Series(['Athens', 'Boston', 'Charleston'], dtype='string'),
            'variable': pd.Series(['foo', 'foo', 'foo'], dtype='string'),
            'value': pd.Series([50.0, 50.0, 100.0], dtype='float64'),
        })
        expected_df.set_index(['idx1', 'idx2'], inplace=True)
        pd.testing.assert_frame_equal(df, expected_df)

    @unittest.skipUnless(pd, 'requires pandas')
    def test_disagg_trans_pandas_pivot(self):
        # Disaggregate, translate, convert to DataFrame, and pivot.
        data = self.node1() >> self.node2
        df_pivoted = data.to_pandas().pivot_table(
            index=data.index_columns,
            columns=('variable',),
            values='value',
        )

        # Create the expected DataFrame.
        df_expected = pd.DataFrame(
            data=[
                [float('nan'), float('nan'), 50.0],
                [float('nan'), float('nan'), 50.0],
                [100.0,        float('nan'), 100.0],
                [100.0,        float('nan'), float('nan')],
                [100.0,        float('nan'), float('nan')],
                [100.0,        float('nan'), float('nan')],
                [float('nan'), 100.0,        float('nan')],
                [float('nan'), 100.0,        float('nan')],
                [float('nan'), 100.0,        float('nan')],
            ],
            index=pd.MultiIndex.from_arrays([
                pd.Series(
                    data=['A', 'A', 'B', 'C', 'C', 'D', 'D', 'D', 'D'],
                    dtype='string',
                    name='idx1',
                ),
                pd.Series(
                    data=['Athens', 'Boston', 'Charleston', 'Dover', 'Erie',
                          'Fayetteville', 'Greensboro', 'Hartford', 'Irvine'],
                    dtype='string',
                    name='idx2',
                ),
            ]),
            columns=pd.Index(pd.Series(
                data=['bar', 'baz', 'foo'], dtype='string', name='variable'
            )),
            dtype='float64',
        )

        pd.testing.assert_frame_equal(df_pivoted, df_expected)


class TestBuildUsingCLI(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

        self.node1 = TopoNode()
        self.node1._connector._unique_id = '11111111-1111-1111-1111-111111111111'
        cli.command_add.add_label(argparse.Namespace(
            command='add',
            element='label',
            node=self.node1,
            labels=['idx1', 'idx2', 'idx3']
        ))
        cli.command_add.add_weight(argparse.Namespace(
            command='add',
            element='weight',
            node=self.node1,
            weight='wght',
            description=None,
            selectors=None,
            make_default=True,
        ))
        cli.command_index.read_from_stdin(argparse.Namespace(
            command='index',
            node=self.node1,
            on_label_conflict='abort',
            on_weight_conflict='abort',
            stdin=DummyRedirection(
                'idx1,idx2,idx3,wght\n'
                'A,z,a,72\n'
                'B,x,b,37.5\n'
                'B,y,c,62.5\n'
                'C,x,d,75\n'
                'C,y,e,25\n'
                'D,x,f,25\n'
                'D,x,g,0\n'
                'D,y,h,50\n'
                'D,y,i,25\n'
            ),
        ))
        self.node1.insert_quantities(  # <- Using API, CLI not ready yet.
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

        self.node2 = TopoNode()
        self.node2._connector._unique_id = '22222222-2222-2222-2222-222222222222'
        cli.command_add.add_label(argparse.Namespace(
            command='add',
            element='label',
            node=self.node2,
            labels=['idx1', 'idx2']
        ))
        cli.command_add.add_weight(argparse.Namespace(
            command='add',
            element='weight',
            node=self.node2,
            weight='wght',
            description=None,
            selectors=None,
            make_default=True,
        ))
        cli.command_index.read_from_stdin(argparse.Namespace(
            command='index',
            node=self.node2,
            on_label_conflict='abort',
            on_weight_conflict='abort',
            stdin=DummyRedirection(
                'idx1,idx2,wght\n'
                'A,Athens,25\n'
                'A,Boston,75\n'
                'B,Charleston,80\n'
                'C,Dover,25\n'
                'C,Erie,75\n'
                'D,Fayetteville,37.5\n'
                'D,Greensboro,43.75\n'
                'D,Hartford,31.25\n'
                'D,Irvine,31.25\n'
            ),
        ))

        cli.command_add.add_crosswalk(argparse.Namespace(
            command='add',
            element='crosswalk',
            node1=self.node1,
            node2=self.node2,
            crosswalk='population',
            direction='both',
            description=None,
            selectors=None,
            make_default=True,
        ))
        cli.command_crosswalk.read_from_stdin(argparse.Namespace(
            command='crosswalk',
            node1=self.node1,
            node2=self.node2,
            crosswalk='population',
            direction='both',
            match_limit=1,
            allow_overlapping=False,
            stdin=DummyRedirection(
                'index_code,population,index_code\n'
                '0X27B3B62D,0.0,0X7054347B\n'
                '1XA0157D6E,25.0,1XF7F2FF38\n'
                '1XA0157D6E,25.0,2XA468A4BC\n'
                '2XF38F26EA,50.0,3X23CE6FFF\n'
                '3X7429EDA9,50.0,3X23CE6FFF\n'
                '4X54BB91E2,55.0,4X035C13B4\n'
                '5XD31D5AA1,45.0,5X84FAD8F7\n'
                '0X27B3B62D,5.0,5X84FAD8F7\n'
                '6X80870125,100.0,6XD7608373\n'
                '7X0721CA66,90.0,7X50C64830\n'
                '7X0721CA66,10.0,0X7054347B\n'
                '8XC1A3F9B3,100.0,8X96447BE5\n'
                '9X460532F0,100.0,9X11E2B0A6\n'
            ),
        ))

    def test_disaggregate_translate(self):
        result_iter = self.node1() >> self.node2

        self.assertEqual(
            result_iter.columns,
            ['idx1', 'idx2', 'variable', 'value'],
        )
        self.assertEqual(
            set(result_iter),
            {('-', '-',            'baz',  10.0),
             ('A', 'Athens',       'foo',  50.0),
             ('A', 'Boston',       'foo',  50.0),
             ('B', 'Charleston',   'foo', 100.0),
             ('B', 'Charleston',   'bar', 100.0),
             ('C', 'Dover',        'bar', 100.0),
             ('C', 'Erie',         'bar', 100.0),
             ('D', 'Fayetteville', 'bar', 100.0),
             ('D', 'Greensboro',   'baz',  90.0),
             ('D', 'Hartford',     'baz', 100.0),
             ('D', 'Irvine',       'baz', 100.0)},
        )
