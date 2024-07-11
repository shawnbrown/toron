"""Tests for toron/mapper.py module."""

import unittest

from toron.node import Node
from toron.mapper import Mapper


class TestMapperInit(unittest.TestCase):
    def test_exact_crosswalk_name(self):
        """Test crosswalk name matches value column exactly."""
        data = [
            ['idx1', 'population', 'idx1', 'idx2'],
            ['A', 70, 'A', 'x'],
            ['B', 80, 'B', 'y'],
            [],  # <- Empty row to simulate trailing newline from text file input.
        ]
        mapper = Mapper(
            crosswalk_name='population',  # <- Matches name of column exactly.
            data=data,
        )

        self.assertEqual(mapper.left_keys, ['idx1'])
        self.assertEqual(mapper.right_keys, ['idx1', 'idx2'])

        mapper.cur.execute('SELECT * FROM mapping_data')
        expected = {
            (1, '["A"]', '["A", "x"]', 70.0),
            (2, '["B"]', '["B", "y"]', 80.0),
        }
        self.assertEqual(set(mapper.cur.fetchall()), expected)
