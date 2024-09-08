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
from toron.graph import (
    load_mapping,
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
        self.node1.add_weight_group('wght')
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
        self.node2.add_weight_group('wght')
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

        # Add handler to 'toron' logger.
        logger = logging.getLogger('toron')
        handler = logging.StreamHandler(self.log_stream)
        handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        logger.addHandler(handler)
        self.addCleanup(lambda: logger.removeHandler(handler))

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
            'INFO: loaded 10 relations\n',
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
