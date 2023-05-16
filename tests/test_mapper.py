"""Tests for toron/_mapper.py module."""

import unittest

from toron._mapper import (
    Mapper,
)


class TestMapper(unittest.TestCase):
    def test_name_exact(self):
        """Matches name in column "population."""
        data = [
            ['idx', 'population', 'idx1', 'idx2'],
            ['A', 70, 'A', 'x'],
            ['B', 80, 'B', 'y'],
        ]
        mapper = Mapper(data, 'population')  # <- Matches name of column exactly.

        mapper.cur.execute('SELECT * FROM temp.source_mapping')
        expected = {
            (1, '["A"]', '["A", "x"]', 70.0),
            (2, '["B"]', '["B", "y"]', 80.0),
        }
        self.assertEqual(set(mapper.cur.fetchall()), expected)

    def test_name_shorthand_syntax(self):
        """Matches name in column "population: node1 --> node2"."""
        data = [
            ['idx', 'population: node1 --> node2', 'idx1', 'idx2'],
            ['A', 70, 'A', 'x'],
            ['B', 80, 'B', 'y'],
        ]
        mapper = Mapper(data, 'population')  # <- Matches name in shorthand syntax.

        mapper.cur.execute('SELECT * FROM temp.source_mapping')
        expected = {
            (1, '["A"]', '["A", "x"]', 70.0),
            (2, '["B"]', '["B", "y"]', 80.0),
        }
        self.assertEqual(set(mapper.cur.fetchall()), expected)
