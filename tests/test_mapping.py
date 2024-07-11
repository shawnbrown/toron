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
        ]
        mapper = Mapper(
            crosswalk_name='population',  # <- Matches name of column exactly.
            data=data,
        )

        self.assertEqual(mapper.left_keys, ['idx1'])
        self.assertEqual(mapper.right_keys, ['idx1', 'idx2'])
        self.assertEqual(
            self.get_mapping_data(mapper),
            {(1, '["A"]', '["A", "x"]', 70.0),
             (2, '["B"]', '["B", "y"]', 80.0)},
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
            {(1, '["A"]', '["A", "x"]', 70.0),
             (2, '["B"]', '["B", "y"]', 80.0)},
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
            {(1, '["A"]', '["A", "x"]', 70.0),
             (2, '["B"]', '["B", "y"]', 80.0)},
        )
