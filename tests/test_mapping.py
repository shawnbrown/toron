"""Tests for toron/mapper.py module."""

import unittest

from toron.node import Node
from toron.mapper import Mapper
from toron.data_models import Structure
from toron._utils import BitFlags


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

    def test_parse_mapping_flags(self):
        mapping_flags = [
            b'\xe0',  # 1, 1, 1
            b'\xc0',  # 1, 1, 0
            b'\x80',  # 1, 0, 0
            b'\x60',  # 0, 1, 1
            b'\x20',  # 0, 0, 1
        ]
        mapping_keys = ['A', 'B', 'C']
        node_structures = [
            Structure(id=4, granularity=3.0,  bits=(1, 1, 1)),
            Structure(id=3, granularity=2.0,  bits=(1, 1, 0)),
            Structure(id=2, granularity=1.0,  bits=(1, 0, 0)),
            Structure(id=1, granularity=None, bits=(0, 0, 0)),
        ]
        node_columns = ['A', 'B', 'C']

        results = Mapper._parse_mapping_flags(  # <- Method under test.
            mapping_flags,
            mapping_keys,
            node_structures,
            node_columns,
        )
        valid_levels, invalid_levels = results

        self.assertEqual(
            valid_levels,
            [(b'\xe0', ('A', 'B', 'C'), BitFlags(1, 1, 1)),
             (b'\xc0', ('A', 'B'), BitFlags(1, 1, 0)),
             (b'\x80', ('A',), BitFlags(1, 0, 0))],
        )

        self.assertEqual(
            invalid_levels,
            [(b'\x60', ('B', 'C'), BitFlags(0, 1, 1)),
             (b'\x20', ('C',), BitFlags(0, 0, 1))],
        )

    def test_parse_mapping_flags_different_mapping_order(self):
        """Mapping keys may be in different order than node columns."""
        mapping_flags = [
            b'\xe0',  # 1, 1, 1
            b'\x60',  # 0, 1, 1
            b'\x20',  # 0, 0, 1
            b'\xc0',  # 1, 1, 0
            b'\x80',  # 1, 0, 0
        ]
        mapping_keys = ['C', 'B', 'A']  # <- Different order than node_columns, below.

        node_structures = [
            Structure(id=4, granularity=3.0,  bits=(1, 1, 1)),
            Structure(id=3, granularity=2.0,  bits=(1, 1, 0)),
            Structure(id=2, granularity=1.0,  bits=(1, 0, 0)),
            Structure(id=1, granularity=None, bits=(0, 0, 0)),
        ]
        node_columns = ['A', 'B', 'C']  # <- Different order than mapping_keys, above.

        results = Mapper._parse_mapping_flags(  # <- Method under test.
            mapping_flags,
            mapping_keys,
            node_structures,
            node_columns,
        )
        valid_levels, invalid_levels = results

        self.assertEqual(
            valid_levels,
            [(b'\xe0', ('C', 'B', 'A'), BitFlags(1, 1, 1)),
             (b'\x60', ('B', 'A'), BitFlags(1, 1, 0)),
             (b'\x20', ('A',), BitFlags(1, 0, 0))],
        )

        self.assertEqual(
            invalid_levels,
            [(b'\xc0', ('C', 'B'), BitFlags(0, 1, 1)),
             (b'\x80', ('C',), BitFlags(0, 0, 1))],
        )
