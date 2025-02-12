"""Tests for toron/reader.py module."""

import os
import sqlite3
import weakref
import unittest
from contextlib import closing

from toron.node import TopoNode
from toron.reader import NodeReader, translate2, _managed_reader_connection


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

        with _managed_reader_connection(reader) as con:
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
            ('ALAMEDA', 'HAYWARD', 'foo', '',    25.0),
            ('BUTTE',   'PALERMO', 'foo', '',    75.0),
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


class TestTranslate2(unittest.TestCase):
    def setUp(self):
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
            other_unique_id='00000000-0000-0000-0000-000000000000',
            other_filename_hint='other-file.toron',
            name='edge 1',
            description='Edge one description.',
            selectors=['[foo="bar"]'],
            is_default=True,
        )
        self.node.insert_relations(
            node='other-file',
            name='edge 1',
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
            other_unique_id='00000000-0000-0000-0000-000000000000',
            other_filename_hint='other-file.toron',
            name='edge 2',
            description='Edge two description.',
            selectors=['[foo]'],
        )
        self.node.insert_relations(
            node='other-file',
            name='edge 2',
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
        reader = NodeReader(data, source_node, cache_to_drive=True)

        new_reader = translate2(reader, self.node)

        expected = [
            ('a1', 'b1', 'c1', 'bar', 60.0),
            ('a1', 'b1', 'c2', 'bar', 165.0),
            ('a1', 'b2', 'c3', 'bar', 150.5),
            ('a1', 'b2', 'c4', 'bar', 124.5)
        ]
        self.assertEqual(sorted(new_reader), expected)

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
        reader = NodeReader(data, source_node, cache_to_drive=True)

        new_reader = translate2(reader, self.node)

        # If `new_quantities` were accumulated, it would be:
        expected = [
            ('a1', 'b1', 'c1', 'bar', '',      60),            # <- Edge 1
            ('a1', 'b1', 'c1', 'baz', '',      50),            # <- Edge 2
            ('a1', 'b1', 'c2', 'bar', '',      165),           # <- Edge 1
            ('a1', 'b1', 'c2', 'baz', '',      183.3984375),   # <- Edge 2
            ('a1', 'b2', 'c3', '',    'corge', 38.0),          # <- Default (Edge 1)
            ('a1', 'b2', 'c3', 'bar', '',      150.5),         # <- Edge 1
            ('a1', 'b2', 'c3', 'baz', '',      133.30078125),  # <- Edge 2
            ('a1', 'b2', 'c4', '',    'corge', 62.0),          # <- Default (Edge 1)
            ('a1', 'b2', 'c4', 'bar', '',      124.5),         # <- Edge 1
            ('a1', 'b2', 'c4', 'baz', '',      33.30078125),   # <- Edge 2
        ]
        self.assertEqual(sorted(new_reader), expected)
