"""Tests for toron/graph.py module."""

import logging
import unittest
import warnings
from io import StringIO

from toron.node import Node
from toron.xnode import xNode
from toron._utils import (
    ToronWarning,
    BitFlags,
)
from toron.data_models import (
    Index,
    AttributeGroup,
    QuantityIterator,
)
from toron.graph import (
    load_mapping,
    _translate,
    translate,
    xadd_edge,
)


class TestLoadMapping(unittest.TestCase):
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

        self.node2 = Node()
        self.node2.add_index_columns('idx1', 'idx2', 'idx3')
        self.node2.add_discrete_categories({'idx1'}, {'idx1', 'idx2'})
        self.node2.insert_index([
            ['idx1', 'idx2', 'idx3'],
            ['A', 'z', 'a'],
            ['A', 'z', 'b'],
            ['B', 'x', 'c'],
            ['C', 'x', 'd'],
            ['C', 'y', 'e'],
            ['D', 'x', 'f'],
            ['D', 'x', 'g'],
            ['D', 'y', 'h'],
            ['D', 'y', 'i'],
        ])
        self.node2.add_weight_group('wght', make_default=True)
        self.node2.insert_weights(
            weight_group_name='wght',
            data=[
                ['idx1', 'idx2', 'idx3', 'wght'],
                ['A', 'z', 'a', 25],
                ['A', 'z', 'b', 75],
                ['B', 'x', 'c', 80],
                ['C', 'x', 'd', 25],
                ['C', 'y', 'e', 75],
                ['D', 'x', 'f', 37.5],
                ['D', 'x', 'g', 43.75],
                ['D', 'y', 'h', 31.25],
                ['D', 'y', 'i', 31.25],
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

    def test_all_exact(self):
        mapping_data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['A', 'z', 'a',  25, 'A', 'z', 'a'],
            ['A', 'z', 'a',  25, 'A', 'z', 'b'],
            ['B', 'x', 'b',  50, 'B', 'x', 'c'],
            ['B', 'y', 'c',  50, 'B', 'x', 'c'],
            ['C', 'x', 'd',  55, 'C', 'x', 'd'],
            ['C', 'y', 'e',  50, 'C', 'y', 'e'],
            ['D', 'x', 'f', 100, 'D', 'x', 'f'],
            ['D', 'x', 'g', 100, 'D', 'x', 'g'],
            ['D', 'y', 'h', 100, 'D', 'y', 'h'],
            ['D', 'y', 'i', 100, 'D', 'y', 'i'],
        ]
        load_mapping(  # <- The method under test.
            left_node=self.node1,
            direction='->',
            right_node=self.node2,
            crosswalk_name='population',
            data=mapping_data,
        )

        self.assertEqual(
            self.log_stream.getvalue(),
            ("WARNING: setting default crosswalk: 'population'\n"
             "INFO: loaded 10 relations\n")
        )

        with self.node2._managed_cursor() as cur:
            results = cur.execute('SELECT * FROM relation').fetchall()
            expected = [
                (1,  1, 1, 1, b'\xe0',  25.0, 0.5),
                (2,  1, 1, 2, b'\xe0',  25.0, 0.5),
                (3,  1, 2, 3, b'\xe0',  50.0, 1.0),
                (4,  1, 3, 3, b'\xe0',  50.0, 1.0),
                (5,  1, 4, 4, b'\xe0',  55.0, 1.0),
                (6,  1, 5, 5, b'\xe0',  50.0, 1.0),
                (7,  1, 6, 6, b'\xe0', 100.0, 1.0),
                (8,  1, 7, 7, b'\xe0', 100.0, 1.0),
                (9,  1, 8, 8, b'\xe0', 100.0, 1.0),
                (10, 1, 9, 9, b'\xe0', 100.0, 1.0),
                (11, 1, 0, 0, None,      0.0, 1.0),
            ]
            self.assertEqual(results, expected)

    def test_some_ambiguous(self):
        mapping_data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['A', 'z', '',   50, 'A', 'z', ''],   # <- Matched to 2 right-side records.
            ['B', '',  '',  100, 'B', '',  ''],   # <- Exact right-side match because there's only one "B".
            ['C', '',  '',  105, 'C', '',  ''],   # <- Matched to 2 right-side records.
            ['D', 'x', 'g', 100, 'D', 'x', 'g'],  # <- Exact match (overlaps the records matched on "D" alone).
            ['D', '',  '',  300, 'D', '',  ''],   # <- Matched to 3 right-side records (4-ambiguous, minus 1-exact overlap).
        ]

        load_mapping(  # <- The method under test.
            left_node=self.node1,
            direction='->',
            right_node=self.node2,
            crosswalk_name='population',
            data=mapping_data,
            is_default=True,
            match_limit=4,
        )

        self.assertEqual(
            self.log_stream.getvalue(),
            ('WARNING: omitted 1 ambiguous matches that overlap with records that were already matched at a finer level of granularity\n'
             'WARNING: omitted 1 ambiguous matches that overlap with records that were already matched at a finer level of granularity\n'
             'INFO: loaded 18 relations\n'),
        )

        with self.node2._managed_cursor() as cur:
            results = cur.execute('SELECT * FROM relation').fetchall()
            expected = [
                (1,  1, 1, 1, b'\xc0', 12.5,    0.25),
                (2,  1, 1, 2, b'\xc0', 37.5,    0.75),
                (3,  1, 2, 3, b'\x80', 37.5,    1.0),
                (4,  1, 3, 3, b'\x80', 62.5,    1.0),
                (5,  1, 4, 4, b'\x80', 19.6875, 0.25),
                (6,  1, 4, 5, b'\x80', 59.0625, 0.75),
                (7,  1, 5, 4, b'\x80', 6.5625,  0.25),
                (8,  1, 5, 5, b'\x80', 19.6875, 0.75),
                (9,  1, 6, 6, b'\x80', 28.125,  0.375),
                (10, 1, 6, 8, b'\x80', 23.4375, 0.3125),
                (11, 1, 6, 9, b'\x80', 23.4375, 0.3125),
                (12, 1, 7, 7, b'\xe0', 100.0,   1.0),
                (13, 1, 8, 6, b'\x80', 56.25,   0.375),
                (14, 1, 8, 8, b'\x80', 46.875,  0.3125),
                (15, 1, 8, 9, b'\x80', 46.875,  0.3125),
                (16, 1, 9, 6, b'\x80', 28.125,  0.375),
                (17, 1, 9, 8, b'\x80', 23.4375, 0.3125),
                (18, 1, 9, 9, b'\x80', 23.4375, 0.3125),
                (19, 1, 0, 0, None,    0.0,     1.0),
            ]
            self.assertEqual(results, expected)

    def test_bidirectional_mapping(self):
        mapping_data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['A', 'z', 'a',  50, 'A', 'z', ''],   # <- Matched to 2 right-side records.
            ['B', '',  '',  100, 'B', '',  ''],   # <- Exact right-side match because there's only one "B".
            ['C', 'x', 'd',  55, 'C', 'x', 'd'],
            ['C', 'y', 'e',  50, 'C', 'y', 'e'],
        ]

        load_mapping(  # <- The method under test.
            left_node=self.node1,
            direction='<-->',  # <- Makes crosswalks in both directions!
            right_node=self.node2,
            crosswalk_name='population',
            data=mapping_data,
            match_limit=2,
            is_default=True,
        )

        self.assertEqual(
            self.log_stream.getvalue(),
            ('INFO: loaded 6 relations\n'
             'INFO: loaded 6 relations\n'),
        )

        # Check left-to-right relations (node2 -> node1).
        with self.node1._managed_cursor() as cur:
            results = cur.execute('SELECT * FROM relation').fetchall()
            expected = [
                (1, 1, 1, 1, b'\xe0', 12.5, 1.0),
                (2, 1, 2, 1, b'\xe0', 37.5, 1.0),
                (3, 1, 3, 2, b'\x80', 37.5, 0.375),
                (4, 1, 3, 3, b'\x80', 62.5, 0.625),
                (5, 1, 4, 4, b'\xe0', 55.0, 1.0),
                (6, 1, 5, 5, b'\xe0', 50.0, 1.0),
                (7, 1, 0, 0, None,     0.0, 1.0),
            ]
            self.assertEqual(results, expected)

        # Check right-to-left relations (node1 -> node2).
        with self.node2._managed_cursor() as cur:
            results = cur.execute('SELECT * FROM relation').fetchall()
            expected = [
                (1, 1, 1, 1, b'\xc0', 12.5, 0.25),
                (2, 1, 1, 2, b'\xc0', 37.5, 0.75),
                (3, 1, 2, 3, b'\x80', 37.5, 1.0),
                (4, 1, 3, 3, b'\x80', 62.5, 1.0),
                (5, 1, 4, 4, b'\xe0', 55.0, 1.0),
                (6, 1, 5, 5, b'\xe0', 50.0, 1.0),
                (7, 1, 0, 0, None,     0.0, 1.0),
            ]
            self.assertEqual(results, expected)


class TestTranslate(unittest.TestCase):
    def setUp(self):
        self.node = Node()
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

    def test_translate_generator(self):
        quantities = QuantityIterator(
            unique_id='00000000-0000-0000-0000-000000000000',
            index_hash='55e56a09c8793714d050eb888d945ca3b66d10ce5c5b489946df6804dd60324e',
            domain={},
            data=[(Index(1, 'aaa'), AttributeGroup(1, {'foo': 'bar'}), 100),
                  (Index(2, 'bbb'), AttributeGroup(1, {'foo': 'bar'}), 100),
                  (Index(3, 'ccc'), AttributeGroup(1, {'foo': 'bar'}), 100),
                  (Index(4, 'ddd'), AttributeGroup(1, {'foo': 'bar'}), 100),
                  (Index(5, 'eee'), AttributeGroup(1, {'foo': 'bar'}), 100)],
            label_names=['X'],
            attribute_keys=['foo'],
        )

        results_generator = _translate(quantities, self.node)

        self.assertEqual(
            list(results_generator),
            [(Index(id=1, labels=('a1', 'b1', 'c1')), AttributeGroup(id=1, value={'foo': 'bar'}), 60.0),
             (Index(id=2, labels=('a1', 'b1', 'c2')), AttributeGroup(id=1, value={'foo': 'bar'}), 40.0),
             (Index(id=2, labels=('a1', 'b1', 'c2')), AttributeGroup(id=1, value={'foo': 'bar'}), 100.0),
             (Index(id=2, labels=('a1', 'b1', 'c2')), AttributeGroup(id=1, value={'foo': 'bar'}), 25.0),
             (Index(id=3, labels=('a1', 'b2', 'c3')), AttributeGroup(id=1, value={'foo': 'bar'}), 12.5),
             (Index(id=4, labels=('a1', 'b2', 'c4')), AttributeGroup(id=1, value={'foo': 'bar'}), 62.5),
             (Index(id=3, labels=('a1', 'b2', 'c3')), AttributeGroup(id=1, value={'foo': 'bar'}), 100.0),
             (Index(id=3, labels=('a1', 'b2', 'c3')), AttributeGroup(id=1, value={'foo': 'bar'}), 38.0),
             (Index(id=4, labels=('a1', 'b2', 'c4')), AttributeGroup(id=1, value={'foo': 'bar'}), 62.0)],
        )

    def test_simple_case(self):
        quantities = QuantityIterator(
            unique_id='00000000-0000-0000-0000-000000000000',
            index_hash='55e56a09c8793714d050eb888d945ca3b66d10ce5c5b489946df6804dd60324e',
            domain={},
            data=[(Index(1, 'aaa'), AttributeGroup(1, {'foo': 'bar'}), 100),
                  (Index(2, 'bbb'), AttributeGroup(1, {'foo': 'bar'}), 100),
                  (Index(3, 'ccc'), AttributeGroup(1, {'foo': 'bar'}), 100),
                  (Index(4, 'ddd'), AttributeGroup(1, {'foo': 'bar'}), 100),
                  (Index(5, 'eee'), AttributeGroup(1, {'foo': 'bar'}), 100)],
            label_names=['X'],
            attribute_keys=['foo'],
        )

        new_quantities = translate(quantities, self.node)

        self.assertIsInstance(new_quantities, QuantityIterator)
        self.assertEqual(
            new_quantities.columns,
            ('A', 'B', 'C', 'foo', 'value'),
        )
        self.assertNotEqual(
            new_quantities.unique_id,
            quantities.unique_id,
            msg='new result should NOT match previous unique_id',
        )
        self.assertEqual(
            new_quantities.unique_id,
            self.node.unique_id,
            msg='new result should match unique_id of target node',
        )
        self.assertEqual(
            list(new_quantities),
            [('a1', 'b1', 'c1', 'bar', 60.0),
             ('a1', 'b1', 'c2', 'bar', 40.0),
             ('a1', 'b1', 'c2', 'bar', 100.0),
             ('a1', 'b1', 'c2', 'bar', 25.0),
             ('a1', 'b2', 'c3', 'bar', 12.5),
             ('a1', 'b2', 'c4', 'bar', 62.5),
             ('a1', 'b2', 'c3', 'bar', 100.0),
             ('a1', 'b2', 'c3', 'bar', 38.0),
             ('a1', 'b2', 'c4', 'bar', 62.0)]
        )

        # If `new_quantities` were accumulated, it would be:
        #[('a1', 'b1', 'c1', 'bar', 60),
        # ('a1', 'b1', 'c2', 'bar', 165),
        # ('a1', 'b2', 'c3', 'bar', 150.5),
        # ('a1', 'b2', 'c4', 'bar', 124.5)]

    def test_handling_multiple_edges(self):
        """Check that quantities are translated using appropriate edges.

        Quantities should be matched by their attributes to the edge
        with the greatest unique specificity or the default edge if
        there is no unique match.
        """
        quantities = QuantityIterator(
            unique_id='00000000-0000-0000-0000-000000000000',
            index_hash='55e56a09c8793714d050eb888d945ca3b66d10ce5c5b489946df6804dd60324e',
            domain={},
            data=[
                # Attributes {'foo': 'bar'} match 'edge 1' ([foo="bar"])
                # and 'edge 2' ([foo]), but 'edge 1' is used because it
                # has a greater specificity.
                (Index(1, 'aaa'), AttributeGroup(1, {'foo': 'bar'}), 100),
                (Index(2, 'bbb'), AttributeGroup(1, {'foo': 'bar'}), 100),
                (Index(3, 'ccc'), AttributeGroup(1, {'foo': 'bar'}), 100),
                (Index(4, 'ddd'), AttributeGroup(1, {'foo': 'bar'}), 100),
                (Index(5, 'eee'), AttributeGroup(1, {'foo': 'bar'}), 100),

                # Attributes {'foo': 'baz'} match 'edge 2' ([foo]).
                (Index(1, 'aaa'), AttributeGroup(1, {'foo': 'baz'}), 100),
                (Index(2, 'bbb'), AttributeGroup(1, {'foo': 'baz'}), 100),
                (Index(3, 'ccc'), AttributeGroup(1, {'foo': 'baz'}), 100),
                (Index(4, 'ddd'), AttributeGroup(1, {'foo': 'baz'}), 100),

                # Attributes {'qux': 'corge'} has no match, uses default ('edge 1').
                (Index(5, 'eee'), AttributeGroup(1, {'qux': 'corge'}), 100),
            ],
            label_names=['X'],
            attribute_keys=['foo', 'qux'],
        )

        new_quantities = translate(quantities, self.node)

        expected = [
            ('a1', 'b1', 'c1', 'bar', None, 60.0),         # <- Edge 1
            ('a1', 'b1', 'c2', 'bar', None, 40.0),         # <- Edge 1
            ('a1', 'b1', 'c2', 'bar', None, 100.0),        # <- Edge 1
            ('a1', 'b1', 'c2', 'bar', None, 25.0),         # <- Edge 1
            ('a1', 'b2', 'c3', 'bar', None, 12.5),         # <- Edge 1
            ('a1', 'b2', 'c4', 'bar', None, 62.5),         # <- Edge 1
            ('a1', 'b2', 'c3', 'bar', None, 100.0),        # <- Edge 1
            ('a1', 'b2', 'c3', 'bar', None, 38.0),         # <- Edge 1
            ('a1', 'b2', 'c4', 'bar', None, 62.0),         # <- Edge 1
            ('a1', 'b1', 'c1', 'baz', None, 50.0),         # <- Edge 2
            ('a1', 'b1', 'c2', 'baz', None, 50.0),         # <- Edge 2
            ('a1', 'b1', 'c2', 'baz', None, 100.0),        # <- Edge 2
            ('a1', 'b1', 'c2', 'baz', None, 33.3984375),   # <- Edge 2
            ('a1', 'b2', 'c3', 'baz', None, 33.30078125),  # <- Edge 2
            ('a1', 'b2', 'c4', 'baz', None, 33.30078125),  # <- Edge 2
            ('a1', 'b2', 'c3', 'baz', None, 100.0),        # <- Edge 2
            ('a1', 'b2', 'c3', None, 'corge', 38.0),       # <- Default (Edge 1)
            ('a1', 'b2', 'c4', None, 'corge', 62.0),       # <- Default (Edge 1)
        ]
        self.assertEqual(list(new_quantities), expected)

        # If `new_quantities` were accumulated, it would be:
        #[('a1', 'b1', 'c1', 'bar', None, 60),            # <- Edge 1
        # ('a1', 'b1', 'c2', 'bar', None, 165),           # <- Edge 1
        # ('a1', 'b2', 'c3', 'bar', None, 150.5),         # <- Edge 1
        # ('a1', 'b2', 'c4', 'bar', None, 124.5),         # <- Edge 1
        # ('a1', 'b1', 'c1', 'baz', None, 50),            # <- Edge 2
        # ('a1', 'b1', 'c2', 'baz', None, 183.3984375),   # <- Edge 2
        # ('a1', 'b2', 'c3', 'baz', None, 133.30078125),  # <- Edge 2
        # ('a1', 'b2', 'c4', 'baz', None, 33.30078125),   # <- Edge 2
        # ('a1', 'b2', 'c3', None, 'corge', 38.0),        # <- Default (Edge 1)
        # ('a1', 'b2', 'c4', None, 'corge', 62.0)]        # <- Default (Edge 1)


class TestXAddEdge(unittest.TestCase):
    def setUp(self):
        node1_data = [
            ['idx1', 'idx2', 'idx3', 'wght'],
            ['A', 'z', 'a', 72],
            ['B', 'x', 'b', 37.5],
            ['B', 'y', 'c', 62.5],
            ['C', 'x', 'd', 75],
            ['C', 'y', 'e', 25],
            ['D', 'x', 'f', 25],
            ['D', 'x', 'g', None],
            ['D', 'y', 'h', 50],
            ['D', 'y', 'i', 25],
        ]
        node1 = xNode()
        node1.add_index_columns(['idx1', 'idx2', 'idx3'])
        node1.add_index_records(node1_data)
        node1.add_weights(node1_data, 'wght', selectors=['[attr1]'])
        self.node1 = node1

        node2_data = [
            ['idx1', 'idx2', 'idx3', 'wght'],
            ['A', 'z', 'a', 25],
            ['A', 'z', 'b', 75],
            ['B', 'x', 'c', 80],
            ['C', 'x', 'd', 25],
            ['C', 'y', 'e', 75],
            ['D', 'x', 'f', 37.5],
            ['D', 'x', 'g', 43.75],
            ['D', 'y', 'h', 31.25],
            ['D', 'y', 'i', 31.25],
        ]
        node2 = xNode()
        node2.add_index_columns(['idx1', 'idx2', 'idx3'])
        node2.add_index_records(node2_data)
        node2.add_weights(node2_data, 'wght', selectors=['[attr1]'])
        self.node2 = node2

    def test_all_exact(self):
        mapping_data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['A', 'z', 'a',  25, 'A', 'z', 'a'],
            ['A', 'z', 'a',  25, 'A', 'z', 'b'],
            ['B', 'x', 'b',  50, 'B', 'x', 'c'],
            ['B', 'y', 'c',  50, 'B', 'x', 'c'],
            ['C', 'x', 'd',  55, 'C', 'x', 'd'],
            ['C', 'y', 'e',  50, 'C', 'y', 'e'],
            ['D', 'x', 'f', 100, 'D', 'x', 'f'],
            ['D', 'x', 'g', 100, 'D', 'x', 'g'],
            ['D', 'y', 'h', 100, 'D', 'y', 'h'],
            ['D', 'y', 'i', 100, 'D', 'y', 'i'],
        ]
        xadd_edge(  # <- The method under test.
            data=mapping_data,
            name='population',
            left_node=self.node1,
            direction='-->',
            right_node=self.node2,
        )

        con = self.node2._dal._get_connection()
        results = con.execute('SELECT * FROM relation').fetchall()
        expected = [
            (1,  1, 1, 1,  25.0, 0.5, None),
            (2,  1, 1, 2,  25.0, 0.5, None),
            (3,  1, 2, 3,  50.0, 1.0, None),
            (4,  1, 3, 3,  50.0, 1.0, None),
            (5,  1, 4, 4,  55.0, 1.0, None),
            (6,  1, 5, 5,  50.0, 1.0, None),
            (7,  1, 6, 6, 100.0, 1.0, None),
            (8,  1, 7, 7, 100.0, 1.0, None),
            (9,  1, 8, 8, 100.0, 1.0, None),
            (10, 1, 9, 9, 100.0, 1.0, None),
            (11, 1, 0, 0,   0.0, 1.0, None),
        ]
        self.assertEqual(results, expected)

    def test_some_ambiguous(self):
        self.maxDiff = None

        self.node1.add_discrete_categories([{'idx1'}])
        self.node2.add_discrete_categories([{'idx1'}, {'idx1', 'idx2'}])

        mapping_data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['A', 'z', '',   50, 'A', 'z', ''],   # <- Matched to 2 right-side records.
            ['B', '',  '',  100, 'B', '',  ''],   # <- Exact right-side match because there's only one "B".
            ['C', '',  '',  105, 'C', '',  ''],   # <- Matched to 2 right-side records.
            ['D', 'x', 'g', 100, 'D', 'x', 'g'],  # <- Exact match (overlaps the records matched on "D" alone).
            ['D', '',  '',  300, 'D', '',  ''],   # <- Matched to 3 right-side records (4-ambiguous, minus 1-exact overlap).
        ]
        xadd_edge(  # <- The method under test.
            data=mapping_data,
            name='population',
            left_node=self.node1,
            direction='-->',
            right_node=self.node2,
            match_limit=4,
        )

        con = self.node2._dal._get_connection()
        results = con.execute('SELECT * FROM main.relation').fetchall()
        expected = [
            (1,  1, 1, 1, 12.5,    0.25,   BitFlags(1, 1, 0)),
            (2,  1, 1, 2, 37.5,    0.75,   BitFlags(1, 1, 0)),
            (3,  1, 2, 3, 37.5,    1.0,    None),
            (4,  1, 3, 3, 62.5,    1.0,    None),
            (5,  1, 4, 4, 19.6875, 0.25,   BitFlags(1, 0, 0)),
            (6,  1, 4, 5, 59.0625, 0.75,   BitFlags(1, 0, 0)),
            (7,  1, 5, 4, 6.5625,  0.25,   BitFlags(1, 0, 0)),
            (8,  1, 5, 5, 19.6875, 0.75,   BitFlags(1, 0, 0)),
            (9,  1, 6, 6, 28.125,  0.375,  BitFlags(1, 0, 0)),
            (10, 1, 6, 8, 23.4375, 0.3125, BitFlags(1, 0, 0)),
            (11, 1, 6, 9, 23.4375, 0.3125, BitFlags(1, 0, 0)),
            (12, 1, 7, 7, 100.0,   1.0,    None),
            (13, 1, 8, 6, 56.25,   0.375,  BitFlags(1, 0, 0)),
            (14, 1, 8, 8, 46.875,  0.3125, BitFlags(1, 0, 0)),
            (15, 1, 8, 9, 46.875,  0.3125, BitFlags(1, 0, 0)),
            (16, 1, 9, 6, 28.125,  0.375,  BitFlags(1, 0, 0)),
            (17, 1, 9, 8, 23.4375, 0.3125, BitFlags(1, 0, 0)),
            (18, 1, 9, 9, 23.4375, 0.3125, BitFlags(1, 0, 0)),
            (19, 1, 0, 0, 0.0,     1.0,    None)
        ]
        self.assertEqual(results, expected)
