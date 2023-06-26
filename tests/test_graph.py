"""Tests for toron/graph.py module."""

import unittest
import warnings

from toron.node import Node
from toron._schema import BitFlags
from toron._utils import (
    ToronWarning,
)
from toron.graph import (
    add_edge,
)


class TestAddEdge(unittest.TestCase):
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
        node1 = Node()
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
        node2 = Node()
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
        add_edge(  # <- The method under test.
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
            ['D', 'x', 'g', 100, 'D', 'x', 'g'],  # <- Exact match (overlapps the records matched on "D" alone).
            ['D', '',  '',  300, 'D', '',  ''],   # <- Matched to 3 right-side records (4-ambiguous, minus 1-exact overlap).
        ]
        add_edge(  # <- The method under test.
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
