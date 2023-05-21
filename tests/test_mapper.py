"""Tests for toron/_mapper.py module."""

import unittest

from toron.node import Node
from toron._mapper import (
    Mapper,
)


class TestMapper(unittest.TestCase):
    def test_name_exact(self):
        """Matches "population" column exactly."""
        data = [
            ['idx1', 'population', 'idx1', 'idx2'],
            ['A', 70, 'A', 'x'],
            ['B', 80, 'B', 'y'],
        ]
        mapper = Mapper(data, 'population')  # <- Matches name of column exactly.

        self.assertEqual(mapper.left_keys, ['idx1'])
        self.assertEqual(mapper.right_keys, ['idx1', 'idx2'])

        mapper.cur.execute('SELECT * FROM temp.source_mapping')
        expected = {
            (1, '["A"]', '["A", "x"]', 70.0),
            (2, '["B"]', '["B", "y"]', 80.0),
        }
        self.assertEqual(set(mapper.cur.fetchall()), expected)

    def test_name_shorthand_syntax(self):
        """Matches name in column "population: node1 --> node2"."""
        data = [
            ['idx1', 'population: node1 --> node2', 'idx1', 'idx2'],
            ['A', 70, 'A', 'x'],
            ['B', 80, 'B', 'y'],
        ]
        mapper = Mapper(data, 'population')  # <- Matches name in shorthand syntax.

        self.assertEqual(mapper.left_keys, ['idx1'])
        self.assertEqual(mapper.right_keys, ['idx1', 'idx2'])

        mapper.cur.execute('SELECT * FROM temp.source_mapping')
        expected = {
            (1, '["A"]', '["A", "x"]', 70.0),
            (2, '["B"]', '["B", "y"]', 80.0),
        }
        self.assertEqual(set(mapper.cur.fetchall()), expected)


class TestMapperFindMatches(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.data = [
            ['idx', 'population', 'idx1', 'idx2'],
            ['A', 10, 'A', 'x'],
            ['A', 70, 'A', 'y'],
            ['B', 20, 'B', 'x'],
            ['B', 60, 'B', 'y'],
            ['C', 30, 'C', 'x'],
            ['C', 50, 'C', 'y'],
        ]

    def test_find_matches_format_data_exact(self):
        mapper = Mapper(self.data, 'population')

        node = Node()
        node_data = [
            ['idx1', 'idx2', 'wght'],
            ['A', 'x', 3],
            ['A', 'y', 15],
            ['B', 'x', 3],
            ['B', 'y', 7],
            ['C', 'x', 13],
            ['C', 'y', 22],
        ]
        node.add_index_columns(['idx1', 'idx2'])
        node.add_index_records(node_data)
        node.add_weights(node_data, 'wght', selectors=['[attr1]'])

        keys = ['idx1', 'idx2']
        iterable = [
            # Records contain `(label_values, run_id)`.
            ('["A", "x"]', 101),
            ('["A", "y"]', 102),
            ('["B", "x"]', 103),
            ('["B", "y"]', 104),
            ('["C", "x"]', 105),
            ('["C", "y"]', 106),
        ]
        formatted = mapper._find_matches_format_data(node, keys, iterable)
        result = [(a, b, list(c)) for a, b, c in formatted]

        expected = [
            ([101], {'idx1': 'A', 'idx2': 'x'}, [(1, 'A', 'x')]),
            ([102], {'idx1': 'A', 'idx2': 'y'}, [(2, 'A', 'y')]),
            ([103], {'idx1': 'B', 'idx2': 'x'}, [(3, 'B', 'x')]),
            ([104], {'idx1': 'B', 'idx2': 'y'}, [(4, 'B', 'y')]),
            ([105], {'idx1': 'C', 'idx2': 'x'}, [(5, 'C', 'x')]),
            ([106], {'idx1': 'C', 'idx2': 'y'}, [(6, 'C', 'y')]),
        ]
        self.assertEqual(result, expected)