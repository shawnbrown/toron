"""Tests for toron/mapper.py module."""

import unittest

from toron.node import Node
from toron.mapper import Mapper


class TestMapperInit(unittest.TestCase):
    @staticmethod
    def get_mapping_data(mapper):
        """Helper method to get contents of 'mapping_data' table."""
        mapper.cur.execute('SELECT * FROM mapping_data')
        return set(mapper.cur.fetchall())

    def test_exact_crosswalk_name(self):
        """Test crosswalk name matches value column exactly."""
        data = [
            ['idx1', 'population', 'idx1', 'idx2'],
            ['A', 70, 'A', 'x'],
            ['B', 80, 'B', 'y'],
            ['A', 7, 'A', ''],
            ['B', 8, '', 'y'],
        ]
        mapper = Mapper(
            crosswalk_name='population',  # <- Matches name of column exactly.
            data=data,
        )

        self.assertEqual(mapper.left_keys, ['idx1'])
        self.assertEqual(mapper.right_keys, ['idx1', 'idx2'])
        self.assertEqual(
            self.get_mapping_data(mapper),
            {(1, '["A"]', b'\x80', '["A", "x"]', b'\xc0', 70.0),
             (2, '["B"]', b'\x80', '["B", "y"]', b'\xc0', 80.0),
             (3, '["A"]', b'\x80', '["A", ""]',  b'\x80', 7.0),
             (4, '["B"]', b'\x80', '["", "y"]',  b'\x40', 8.0)},
        )

    def test_parsed_crosswalk_name(self):
        """Test crosswalk name parsed from shorthand-syntax."""
        data = [
            ['idx1', 'population: node1 --> node2', 'idx1', 'idx2'],
            ['A', 70, 'A', 'x'],
            ['B', 80, 'B', 'y'],
        ]
        mapper = Mapper(
            crosswalk_name='population',  # <- Matches name in shorthand syntax.
            data=data,
        )

        self.assertEqual(mapper.left_keys, ['idx1'])
        self.assertEqual(mapper.right_keys, ['idx1', 'idx2'])
        self.assertEqual(
            self.get_mapping_data(mapper),
            {(1, '["A"]', b'\x80', '["A", "x"]', b'\xc0', 70.0),
             (2, '["B"]', b'\x80', '["B", "y"]', b'\xc0', 80.0)},
        )

    def test_empty_rows_in_data(self):
        """Empty rows should be skipped."""
        data = [
            ['idx1', 'population', 'idx1', 'idx2'],
            ['A', 70, 'A', 'x'],
            ['B', 80, 'B', 'y'],
            [],  # <- Empty row to simulate trailing newline from text file input.
        ]
        mapper = Mapper('population', data)

        self.assertEqual(
            self.get_mapping_data(mapper),
            {(1, '["A"]', b'\x80', '["A", "x"]', b'\xc0', 70.0),
             (2, '["B"]', b'\x80', '["B", "y"]', b'\xc0', 80.0)},
        )


class TestMapperMethods(unittest.TestCase):
    def setUp(self):
        self.node2 = Node()
        self.node2.add_index_columns('idx1', 'idx2')
        self.node2.insert_index([
            ['idx1', 'idx2'],
            ['A', 'x'],
            ['A', 'y'],
            ['B', 'x'],
            ['B', 'y'],
            ['C', 'x'],
            ['C', 'y'],
        ])
        self.node2.add_weight_group('wght')
        self.node2.insert_weights(
            weight_group_name='wght',
            data=[
                ['idx1', 'idx2', 'wght'],
                ['A', 'x', 3],
                ['A', 'y', 15],
                ['B', 'x', 3],
                ['B', 'y', 7],
                ['C', 'x', 13],
                ['C', 'y', 22],
            ],
        )
        self.node2.add_discrete_categories({'idx1'})
