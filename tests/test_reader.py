"""Tests for toron/reader.py module."""

import os
import sqlite3
import weakref
import unittest
from contextlib import closing

try:
    import pandas as pd
except ImportError:
    pd = None

from toron.node import TopoNode
from toron.reader import (
    NodeReader,
    pivot_reader,
    pivot_reader_to_pandas,
)


class TestNodeReader(unittest.TestCase):
    def test_minimal_instantiation(self):
        reader = NodeReader([], TopoNode())
        self.assertEqual(list(reader), [])

    def test_close_finalizer(self):
        reader = NodeReader([], TopoNode(), cache_to_drive=True)

        filepath = reader._current_working_path  # Get database file path.
        self.assertTrue(os.path.isfile(filepath))

        self.assertIsInstance(reader.close, weakref.finalize)

        reader.close()  # Call finalizer immediately.
        self.assertFalse(os.path.isfile(filepath))

    def test_loading_data(self):
        reader = NodeReader(
            data=[
                (10, {'a': 'foo'}, 25.0),
                (11, {'a': 'foo'}, 75.0),
                (12, {'a': 'bar'}, 50.0),
            ],
            node=TopoNode(),
        )

        # Check column names.
        self.assertEqual(reader.index_columns, [])
        self.assertEqual(reader.columns, ['a', 'value'])

        with reader._managed_connection() as con:
            with closing(con.cursor()) as cur:
                cur.execute('SELECT * FROM attr_data')
                attr_data = [
                    (1, '{"a": "foo"}', None),
                    (2, '{"a": "bar"}', None),
                ]
                self.assertEqual(cur.fetchall(), attr_data)

                cur.execute('SELECT * FROM quant_data')
                quant_data = [
                    (10, 1, 25.0),
                    (11, 1, 75.0),
                    (12, 2, 50.0),
                ]
                self.assertEqual(cur.fetchall(), quant_data)

    def test_iteration_and_aggregation(self):
        node = TopoNode()
        node.add_index_columns('county', 'town')
        node.insert_index([
            ('county',  'town'),
            ('ALAMEDA', 'HAYWARD'),
            ('BUTTE',   'PALERMO'),
            ('COLUSA',  'GRIMES'),
        ])
        reader = NodeReader(
            data=[
                (1, {'attr1': 'foo'},                 25.0),
                (2, {'attr1': 'foo'},                 75.0),
                (3, {'attr1': 'bar', 'attr2': 'baz'}, 25.0),
                (3, {'attr1': 'bar', 'attr2': 'baz'}, 25.0),
            ],
            node=node,
        )

        self.assertEqual(reader.index_columns, ['county', 'town'])
        self.assertEqual(reader.columns, ['county', 'town', 'attr1', 'attr2', 'value'])

        result = list(reader)
        expected = [
            ('ALAMEDA', 'HAYWARD', 'foo', None,  25.0),
            ('BUTTE',   'PALERMO', 'foo', None,  75.0),
            ('COLUSA',  'GRIMES',  'bar', 'baz', 50.0),
        ]
        self.assertEqual(result, expected)

    def test_iteration_and_cleanup(self):
        node = TopoNode()
        node.add_index_columns('county', 'town')
        node.insert_index([
            ('county',  'town'),
            ('ALAMEDA', 'HAYWARD'),
            ('BUTTE',   'PALERMO'),
            ('COLUSA',  'GRIMES'),
        ])

        reader = NodeReader(
            data=[
                (1, {'someattr': 'foo'}, 25.0),
                (2, {'someattr': 'foo'}, 75.0),
                (3, {'someattr': 'bar'}, 50.0),
            ],
            node=node,
            cache_to_drive=True,
        )
        next(reader)  # Start iteration.
        reader.close()  # Call finalizer before iteration is finished.

        self.assertFalse(os.path.isfile(reader._current_working_path))  # File should be removed.
        self.assertEqual(list(reader), [])  # No more records after closing.

    @unittest.skipUnless(pd, 'requires pandas')
    def test_to_pandas(self):
        """Check convertion to Pandas DataFrame."""
        node = TopoNode()
        node.add_index_columns('county', 'town')
        node.insert_index([
            ('county',  'town'),
            ('ALAMEDA', 'HAYWARD'),
            ('BUTTE',   'PALERMO'),
            ('COLUSA',  'GRIMES'),
        ])
        reader = NodeReader(
            data=[
                (1, {'attr1': 'foo'},                 25.0),
                (2, {'attr1': 'foo'},                 75.0),
                (3, {'attr1': 'bar', 'attr2': 'baz'}, 25.0),
                (3, {'attr1': 'bar', 'attr2': 'baz'}, 25.0),
            ],
            node=node,
        )

        df = reader.to_pandas()  # <- Method under test.

        expected_df = pd.DataFrame({
            'county': pd.Series(['ALAMEDA', 'BUTTE', 'COLUSA'], dtype='string'),
            'town': pd.Series(['HAYWARD', 'PALERMO', 'GRIMES'], dtype='string'),
            'attr1': pd.Series(['foo', 'foo', 'bar'], dtype='string'),
            'attr2': pd.Series([None, None, 'baz'], dtype='string'),
            'value': pd.Series([25.0, 75.0, 50.0], dtype='float64'),
        })
        pd.testing.assert_frame_equal(df, expected_df)

    @unittest.skipUnless(pd, 'requires pandas')
    def test_to_pandas_with_index(self):
        """Check convertion to Pandas DataFrame."""
        node = TopoNode()
        node.add_index_columns('county', 'town')
        node.insert_index([
            ('county',  'town'),
            ('ALAMEDA', 'HAYWARD'),
            ('BUTTE',   'PALERMO'),
            ('COLUSA',  'GRIMES'),
        ])
        reader = NodeReader(
            data=[
                (1, {'attr1': 'foo'},                 25.0),
                (2, {'attr1': 'foo'},                 75.0),
                (3, {'attr1': 'bar', 'attr2': 'baz'}, 25.0),
                (3, {'attr1': 'bar', 'attr2': 'baz'}, 25.0),
            ],
            node=node,
        )

        df = reader.to_pandas(index=True)  # <- Method under test.

        expected_df = pd.DataFrame({
            'county': pd.Series(['ALAMEDA', 'BUTTE', 'COLUSA'], dtype='string'),
            'town': pd.Series(['HAYWARD', 'PALERMO', 'GRIMES'], dtype='string'),
            'attr1': pd.Series(['foo', 'foo', 'bar'], dtype='string'),
            'attr2': pd.Series([None, None, 'baz'], dtype='string'),
            'value': pd.Series([25.0, 75.0, 50.0], dtype='float64'),
        })
        expected_df.set_index(['county', 'town'], inplace=True)
        pd.testing.assert_frame_equal(df, expected_df)


class TestNodeReaderTranslate(unittest.TestCase):
    def setUp(self):
        mock_node = unittest.mock.Mock()
        mock_node.unique_id = '00000000-0000-0000-0000-000000000000'
        mock_node.path_hint = 'other-file.toron'

        self.node = TopoNode()
        self.node.add_index_columns('A', 'B', 'C')
        self.node.add_discrete_categories({'A', 'B', 'C'})
        self.node.insert_index([
            ['A', 'B', 'C'],
            ['a1', 'b1', 'c1'],  # <- index_id=1
            ['a1', 'b1', 'c2'],  # <- index_id=2
            ['a1', 'b2', 'c3'],  # <- index_id=3
            ['a1', 'b2', 'c4'],  # <- index_id=4
        ])
        self.node.add_crosswalk(
            node=mock_node,
            crosswalk_name='edge 1',
            description='Edge one description.',
            selectors=['[foo="bar"]'],
            is_default=True,
        )
        self.node.insert_relations(
            node_or_ref='other-file',
            crosswalk_name='edge 1',
            data=[
                ('other_index_id', 'edge 1', 'index_id', 'A', 'B', 'C'),
                (1,  39.0, 1, 'a1', 'b1', 'c1'),  # proportion: 0.6
                (1,  26.0, 2, 'a1', 'b1', 'c2'),  # proportion: 0.4
                (2,  16.0, 2, 'a1', 'b1', 'c2'),  # proportion: 1.0
                (3,  50.0, 2, 'a1', 'b1', 'c2'),  # proportion: 0.250
                (3,  25.0, 3, 'a1', 'b2', 'c3'),  # proportion: 0.125
                (3, 125.0, 4, 'a1', 'b2', 'c4'),  # proportion: 0.625
                (4,  64.0, 3, 'a1', 'b2', 'c3'),  # proportion: 1.0
                (5,  19.0, 3, 'a1', 'b2', 'c3'),  # proportion: 0.38
                (5,  31.0, 4, 'a1', 'b2', 'c4'),  # proportion: 0.62
                (0,   0.0, 0, '-',  '-',  '-' ),  # proportion: 1.0
            ],
        )
        self.node.add_crosswalk(
            node=mock_node,
            crosswalk_name='edge 2',
            description='Edge two description.',
            selectors=['[foo]'],
        )
        self.node.insert_relations(
            node_or_ref='other-file',
            crosswalk_name='edge 2',
            data=[
                ('other_index_id', 'edge 2', 'index_id', 'A', 'B', 'C'),
                (1, 32.0,  1, 'a1', 'b1', 'c1'),  # proportion: 0.5
                (1, 32.0,  2, 'a1', 'b1', 'c2'),  # proportion: 0.5
                (2, 15.0,  2, 'a1', 'b1', 'c2'),  # proportion: 1.0
                (3, 85.5,  2, 'a1', 'b1', 'c2'),  # proportion: 0.333984375
                (3, 85.25, 3, 'a1', 'b2', 'c3'),  # proportion: 0.3330078125
                (3, 85.25, 4, 'a1', 'b2', 'c4'),  # proportion: 0.3330078125
                (4, 64.0,  3, 'a1', 'b2', 'c3'),  # proportion: 1.0
                (5, 50.0,  3, 'a1', 'b2', 'c3'),  # proportion: 0.5
                (5, 50.0,  4, 'a1', 'b2', 'c4'),  # proportion: 0.5
                (0,  0.0,  0, '-',  '-',  '-' ),  # proportion: 1.0
            ],
        )

    def test_simple_case(self):
        source_node = TopoNode()
        source_node._connector._unique_id = '00000000-0000-0000-0000-000000000000'
        source_node.add_index_columns('X')
        source_node.insert_index(
            data=[['aaa'], ['bbb'], ['ccc'], ['ddd'], ['eee']],
            columns=['X']
        )
        data = [
            (1, {'foo': 'bar'}, 100),
            (2, {'foo': 'bar'}, 100),
            (3, {'foo': 'bar'}, 100),
            (4, {'foo': 'bar'}, 100),
            (5, {'foo': 'bar'}, 100),
        ]
        reader = NodeReader(data, source_node)

        reader.translate(self.node)

        expected = {
            ('a1', 'b1', 'c1', 'bar', 60.0),
            ('a1', 'b1', 'c2', 'bar', 165.0),
            ('a1', 'b2', 'c3', 'bar', 150.5),
            ('a1', 'b2', 'c4', 'bar', 124.5)
        }
        self.assertEqual(set(reader), expected)

    def test_handling_multiple_edges(self):
        """Check that quantities are translated using appropriate edges.

        Quantities should be matched by their attributes to the edge
        with the greatest unique specificity or the default edge if
        there is no unique match.
        """
        source_node = TopoNode()
        source_node._connector._unique_id = '00000000-0000-0000-0000-000000000000'
        source_node.add_index_columns('X')
        source_node.insert_index(
            data=[['aaa'], ['bbb'], ['ccc'], ['ddd'], ['eee']],
            columns=['X']
        )
        data = [
            # Attributes {'foo': 'bar'} match 'edge 1' ([foo="bar"])
            # and 'edge 2' ([foo]), but 'edge 1' is used because it
            # has a greater specificity.
            (1, {'foo': 'bar'}, 100),
            (2, {'foo': 'bar'}, 100),
            (3, {'foo': 'bar'}, 100),
            (4, {'foo': 'bar'}, 100),
            (5, {'foo': 'bar'}, 100),

            # Attributes {'foo': 'baz'} match 'edge 2' ([foo]).
            (1, {'foo': 'baz'}, 100),
            (2, {'foo': 'baz'}, 100),
            (3, {'foo': 'baz'}, 100),
            (4, {'foo': 'baz'}, 100),

            # Attributes {'qux': 'corge'} has no match, uses default ('edge 1').
            (5, {'qux': 'corge'}, 100),
        ]
        reader = NodeReader(data, source_node)

        reader.translate(self.node)

        # If `new_quantities` were accumulated, it would be:
        expected = {
            ('a1', 'b1', 'c1', 'bar', None,    60),            # <- Edge 1
            ('a1', 'b1', 'c2', 'bar', None,    165),           # <- Edge 1
            ('a1', 'b2', 'c3', None,  'corge', 38.0),          # <- Default (Edge 1)
            ('a1', 'b2', 'c3', 'bar', None,    150.5),         # <- Edge 1
            ('a1', 'b2', 'c4', None,  'corge', 62.0),          # <- Default (Edge 1)
            ('a1', 'b2', 'c4', 'bar', None,    124.5),         # <- Edge 1
            ('a1', 'b1', 'c1', 'baz', None,    50),            # <- Edge 2
            ('a1', 'b1', 'c2', 'baz', None,    183.3984375),   # <- Edge 2
            ('a1', 'b2', 'c3', 'baz', None,    133.30078125),  # <- Edge 2
            ('a1', 'b2', 'c4', 'baz', None,    33.30078125),   # <- Edge 2
        }
        self.assertEqual(set(reader), expected)

    def test_quantize(self):
        """Check that values are quantized properly."""
        source_node = TopoNode()
        source_node._connector._unique_id = '00000000-0000-0000-0000-000000000000'
        source_node.add_index_columns('X')
        source_node.insert_index(
            data=[['aaa'], ['bbb'], ['ccc'], ['ddd'], ['eee']],
            columns=['X']
        )
        data = [
            (1, {'foo': 'bar'}, 100),
            (2, {'foo': 'bar'}, 100),
            (3, {'foo': 'bar'}, 100),
            (4, {'foo': 'bar'}, 100),
            (5, {'foo': 'bar'}, 100),
            (1, {'foo': 'baz'}, 100),
            (2, {'foo': 'baz'}, 100),
            (3, {'foo': 'baz'}, 100),
            (4, {'foo': 'baz'}, 100),
            (5, {'qux': 'corge'}, 100),
        ]
        reader = NodeReader(data, source_node)

        reader.translate(self.node, quantize=True)  # <- Quantized translation.

        expected = {
            ('a1', 'b1', 'c1', 'bar', None,     60.0),  # <- Unchanged (Edge 1)
            ('a1', 'b1', 'c2', 'bar', None,    165.0),  # <- Unchanged (Edge 1)
            ('a1', 'b2', 'c3', None,  'corge',  38.0),  # <- Unchanged (Default, Edge 1)
            ('a1', 'b2', 'c3', 'bar', None,    151.0),  # <- Gets whole remainder (Edge 1)
            ('a1', 'b2', 'c4', None,  'corge',  62.0),  # <- Unchanged (Default, Edge 1)
            ('a1', 'b2', 'c4', 'bar', None,    124.0),  # <- Remainder dropped (Edge 1)
            ('a1', 'b1', 'c1', 'baz', None,     50.0),  # <- Unchanged (Edge 2)
            ('a1', 'b1', 'c2', 'baz', None,    184.0),  # <- Gets whole remainder (Edge 2)
            ('a1', 'b2', 'c3', 'baz', None,    133.0),  # <- Remainder dropped (Edge 2)
            ('a1', 'b2', 'c4', 'baz', None,     33.0),  # <- Remainder dropped (Edge 2)
        }
        self.assertEqual(set(reader), expected)

        # If values were not quantized, the result would be:
        #
        #    ('a1', 'b1', 'c1', 'bar', None,     60.0),
        #    ('a1', 'b1', 'c2', 'bar', None,    165.0),
        #    ('a1', 'b2', 'c3', None,  'corge',  38.0),
        #    ('a1', 'b2', 'c3', 'bar', None,    150.5),
        #    ('a1', 'b2', 'c4', None,  'corge',  62.0),
        #    ('a1', 'b2', 'c4', 'bar', None,    124.5),
        #    ('a1', 'b1', 'c1', 'baz', None,     50.0),
        #    ('a1', 'b1', 'c2', 'baz', None,    183.3984375),
        #    ('a1', 'b2', 'c3', 'baz', None,    133.30078125),
        #    ('a1', 'b2', 'c4', 'baz', None,     33.30078125),

    def test_rshift_operator(self):
        source_node = TopoNode()
        source_node._connector._unique_id = '00000000-0000-0000-0000-000000000000'
        source_node.add_index_columns('X')
        source_node.insert_index(
            data=[['aaa'], ['bbb'], ['ccc'], ['ddd'], ['eee']],
            columns=['X']
        )
        data = [
            (1, {'foo': 'bar'}, 100),
            (2, {'foo': 'bar'}, 100),
            (3, {'foo': 'bar'}, 100),
            (4, {'foo': 'bar'}, 100),
            (5, {'foo': 'bar'}, 100),
        ]

        # Translate reader using right-shift operator.
        reader = NodeReader(data, source_node)
        reader = reader >> self.node  # Right-shift!
        expected = {
            ('a1', 'b1', 'c1', 'bar',  60.0),
            ('a1', 'b1', 'c2', 'bar', 165.0),
            ('a1', 'b2', 'c3', 'bar', 150.5),
            ('a1', 'b2', 'c4', 'bar', 124.5),
        }
        self.assertEqual(set(reader), expected)

        # Translate reader whose `quantize_default` is True using right-shift.
        reader = NodeReader(data, source_node, quantize_default=True)
        reader = reader >> self.node  # Right-shift!
        expected = {
            ('a1', 'b1', 'c1', 'bar',  60.0),
            ('a1', 'b1', 'c2', 'bar', 165.0),
            ('a1', 'b2', 'c3', 'bar', 151.0),  # <- Gets whole remainder.
            ('a1', 'b2', 'c4', 'bar', 124.0),  # <- Loses fractional part.
        }
        self.assertEqual(set(reader), expected)


class TestPivotReader(unittest.TestCase):
    def setUp(self):
        node = TopoNode()
        node.add_index_columns('county', 'town')
        node.insert_index([
            ('county',  'town'),
            ('ALAMEDA', 'HAYWARD'),
            ('BUTTE',   'PALERMO'),
            ('COLUSA',  'GRIMES'),
        ])
        reader = NodeReader(
            data=[
                (1, {'attr1': 'foo', 'attr2': 'bar'}, 15.0),
                (1, {'attr2': 'bar'},                 17.0),
                (1, {'attr1': 'foo'},                 30.0),
                (2, {'attr1': 'foo', 'attr2': 'bar'}, 25.0),
                (2, {'attr2': 'bar'},                 27.0),
                (3, {'attr1': 'foo', 'attr2': 'bar'}, 35.0),
                (3, {'attr1': 'foo'},                 22.0),
                (3, {'attr1': 'foo'},                 22.0),
                (3, {'attr3': 'qux'},                 60.0),
            ],
            node=node,
        )
        self.reader = reader

    def test_pivot(self):
        """Check convertion to pivoted format."""
        expected = [
            ['county',  'town',    'bar', 'foo', ('foo', 'bar')],
            ['ALAMEDA', 'HAYWARD', 17.0,  30.0,  15.0],
            ['BUTTE',   'PALERMO', 27.0,  None,  25.0],
            ['COLUSA',  'GRIMES',  None,  44.0,  35.0],
        ]

        result = pivot_reader(self.reader, ['attr1', 'attr2'])
        self.assertEqual(list(result), expected)

    @unittest.skipUnless(pd, 'requires pandas')
    def test_pivot_to_pandas(self):
        """Check conversion to pivoted format pandas DataFrame."""
        expected_df = pd.DataFrame({
            'county': pd.Series(['ALAMEDA', 'BUTTE', 'COLUSA'], dtype='string'),
            'town': pd.Series(['HAYWARD', 'PALERMO', 'GRIMES'], dtype='string'),
            'bar': pd.Series([17.0, 27.0, None], dtype='float64'),
            'foo': pd.Series([30.0, None, 44.0], dtype='float64'),
            ('foo', 'bar'): pd.Series([15.0, 25.0, 35.0], dtype='float64'),
        })

        # Test without explicit index.
        df = pivot_reader_to_pandas(self.reader, ['attr1', 'attr2'])
        pd.testing.assert_frame_equal(df, expected_df)

        # Test with `index=True`.
        df2 = pivot_reader_to_pandas(self.reader, ['attr1', 'attr2'], index=True)
        expected_df2 = expected_df.set_index(['county', 'town'])
        pd.testing.assert_frame_equal(df2, expected_df2)
