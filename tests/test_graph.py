"""Tests for toron/graph.py module."""

import unittest
import warnings

from toron.node import Node
from toron._utils import (
    ToronWarning,
)
from toron.graph import (
    add_edge,
    _EdgeMapper,
)


class TwoNodesTestCase(unittest.TestCase):
    def setUp(self):
        self.node1 = Node()
        data1 = [
            ['idx', 'wght'],
            ['A', 16],
            ['B', 8],
            ['C', 32],
        ]
        self.node1.add_index_columns(['idx'])
        self.node1.add_index_records(data1)
        self.node1.add_weights(data1, 'wght', selectors=['[attr1]'])

        self.node2 = Node()
        data2 = [
            ['idx1', 'idx2', 'wght'],
            ['A', 'x', 3],
            ['A', 'y', 15],
            ['B', 'x', 3],
            ['B', 'y', 7],
            ['C', 'x', 13],
            ['C', 'y', 22],
        ]
        self.node2.add_index_columns(['idx1', 'idx2'])
        self.node2.add_index_records(data2)
        self.node2.add_weights(data2, 'wght', selectors=['[attr1]'])


class TestEdgeMapper(TwoNodesTestCase):
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

    def test_init(self):
        mapper = _EdgeMapper(self.data, 'population', self.node1, self.node2)

        mapper.cur.execute('SELECT * FROM temp.source_mapping')
        expected = [
            (1, '["A"]', '["A", "x"]', 10.0),
            (2, '["A"]', '["A", "y"]', 70.0),
            (3, '["B"]', '["B", "x"]', 20.0),
            (4, '["B"]', '["B", "y"]', 60.0),
            (5, '["C"]', '["C", "x"]', 30.0),
            (6, '["C"]', '["C", "y"]', 50.0),
        ]
        self.assertEqual(mapper.cur.fetchall(), expected)

    def test_init_shared_column_names(self):
        node3 = Node()
        data3 = [
            ['idx', 'wght'],
            ['D', 20],
            ['E', 10],
            ['F', 37],
        ]
        node3.add_index_columns(['idx'])
        node3.add_index_records(data3)
        node3.add_weights(data3, 'wght', selectors=['[attr1]'])

        mapper_data = [
            ('idx', 'population', 'idx'),
            ('A', '20', 'D'),
            ('B', '10', 'E'),
            ('C', '37', 'F'),
        ]
        mapper = _EdgeMapper(mapper_data, 'population', self.node1, node3)

        mapper.cur.execute('SELECT * FROM temp.source_mapping')
        expected = [
            (1, '["A"]', '["D"]', 20.0),
            (2, '["B"]', '["E"]', 10.0),
            (3, '["C"]', '["F"]', 37.0),
        ]
        self.assertEqual(mapper.cur.fetchall(), expected)

    def test_find_matches_format_data_exact(self):
        mapper = _EdgeMapper(self.data, 'population', self.node1, self.node2)

        node = self.node2
        keys = ['idx1', 'idx2']
        iterable = [
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

    def test_find_matches_format_data_ambiguous(self):
        mapper = _EdgeMapper(self.data, 'population', self.node1, self.node2)

        node = self.node2
        keys = ['idx1', 'idx2']
        iterable = [
            ('["A", "x"]',  101),
            ('["A", "y"]',  102),
            ('["B", ""]',   103),  # <- Should match 2 index records.
            ('["C", null]', 104),  # <- Should match 2 index records.
        ]
        formatted = mapper._find_matches_format_data(node, keys, iterable)
        result = [(a, b, list(c)) for a, b, c in formatted]

        expected = [
            ([101], {'idx1': 'A', 'idx2': 'x'}, [(1, 'A', 'x')]),
            ([102], {'idx1': 'A', 'idx2': 'y'}, [(2, 'A', 'y')]),
            ([103], {'idx1': 'B'}, [(3, 'B', 'x'), (4, 'B', 'y')]),
            ([104], {'idx1': 'C'}, [(5, 'C', 'x'), (6, 'C', 'y')]),
        ]
        self.assertEqual(result, expected)

    def test_find_matches_format_data_none_found(self):
        mapper = _EdgeMapper(self.data, 'population', self.node1, self.node2)

        node = self.node2
        keys = ['idx1', 'idx2']
        iterable = [
            ('["X", "xxx"]', 997),
            ('["Y", "yyy"]', 998),
            ('["Z", "zzz"]', 999),
        ]
        formatted = mapper._find_matches_format_data(node, keys, iterable)
        result = [(a, b, list(c)) for a, b, c in formatted]

        expected = [
            ([997], {'idx1': 'X', 'idx2': 'xxx'}, []),
            ([998], {'idx1': 'Y', 'idx2': 'yyy'}, []),
            ([999], {'idx1': 'Z', 'idx2': 'zzz'}, []),
        ]
        self.assertEqual(result, expected)

    def test_find_matches(self):
        mapper = _EdgeMapper(self.data, 'population', self.node1, self.node2)

        mapper.find_matches('left')  # <- Method under test.
        mapper.cur.execute('SELECT * FROM temp.left_matches')
        expected = [
            (1, 1, None, None, None),
            (2, 1, None, None, None),
            (3, 2, None, None, None),
            (4, 2, None, None, None),
            (5, 3, None, None, None),
            (6, 3, None, None, None),
        ]
        self.assertEqual(mapper.cur.fetchall(), expected)

        mapper.find_matches('right')  # <- Method under test.
        mapper.cur.execute('SELECT * FROM temp.right_matches')
        expected = [
            (1, 1, None, None, None),
            (2, 2, None, None, None),
            (3, 3, None, None, None),
            (4, 4, None, None, None),
            (5, 5, None, None, None),
            (6, 6, None, None, None),
        ]
        self.assertEqual(mapper.cur.fetchall(), expected)

        with self.assertRaises(ValueError):
            mapper.find_matches('blerg')  # <- Method under test.

    def test_find_matches_none_found(self):
        data = [
            ['idx', 'population', 'idx1', 'idx2'],
            ['X', 10, 'X', 'X'],
            ['Y', 70, 'Y', 'Y'],
            ['Z', 20, 'Z', 'Z'],
        ]
        mapper = _EdgeMapper(data, 'population', self.node1, self.node2)

        regex = 'skipped 3 values that matched no records'
        with self.assertWarnsRegex(ToronWarning, regex):
            mapper.find_matches('left')  # <- Method under test.

        mapper.cur.execute('SELECT * FROM temp.left_matches')
        no_results = []
        self.assertEqual(mapper.cur.fetchall(), no_results)

    def test_get_relations(self):
        mapper = _EdgeMapper(self.data, 'population', self.node1, self.node2)
        mapper.find_matches('left')
        mapper.find_matches('right')

        relations = mapper.get_relations('right')  # <- Method under test.

        expected = [
            (1, 1, 10.0),
            (1, 2, 70.0),
            (2, 3, 20.0),
            (2, 4, 60.0),
            (3, 5, 30.0),
            (3, 6, 50.0),
        ]
        self.assertEqual(list(relations), expected)


class TestEdgeMapperWithAmbiguousMappings(unittest.TestCase):
    def setUp(self):
        node1_data = [
            ['idx1', 'idx2', 'idx3', 'wght'],
            ['A', 'x', 'a', 72],
            ['B', 'x', 'b', 37.5],
            ['B', 'y', 'c', 62.5],
            ['C', 'x', 'd', 30],
            ['C', 'y', 'e', 70],
            ['D', 'x', 'f', 18.75],
            ['D', 'x', 'g', 31.25],
            ['D', 'y', 'h', 12.5],
            ['D', 'y', 'i', 37.5],
        ]
        node1 = Node()
        node1.add_index_columns(['idx1', 'idx2', 'idx3'])
        node1.add_discrete_categories([{'idx1'}, {'idx1', 'idx2'}])
        node1.add_index_records(node1_data)
        node1.add_weights(node1_data, 'wght', selectors=['[attr1]'])
        self.node1 = node1

        node2_data = [
            ['idx1', 'idx2', 'idx3', 'wght'],
            ['A', 'x', 'a', 25],
            ['A', 'y', 'b', 75],
            ['B', 'x', 'c', 80],
            ['C', 'x', 'd', 40],
            ['C', 'y', 'e', 60],
            ['D', 'x', 'f', 6.25],
            ['D', 'x', 'g', 43.75],
            ['D', 'y', 'h', 18.75],
            ['D', 'y', 'i', 31.25],
        ]
        node2 = Node()
        node2.add_index_columns(['idx1', 'idx2', 'idx3'])
        node2.add_discrete_categories([{'idx1'}, {'idx2', 'idx3'}])
        node2.add_index_records(node2_data)
        node2.add_weights(node2_data, 'wght', selectors=['[attr1]'])
        self.node2 = node2

    def test_find_matches_bad_match_limit(self):
        data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['A', 'x', 'a', 100, 'A', 'x', 'a'],
        ]
        mapper = _EdgeMapper(data, 'population', self.node1, self.node2)

        regex = 'match_limit must be 1 or greater, got 0'
        with self.assertRaisesRegex(ValueError, regex):
            mapper.find_matches('left', match_limit=0)

        regex = "match_limit must be int or float, got 'foo'"
        with self.assertRaisesRegex(TypeError, regex):
            mapper.find_matches('left', match_limit='foo')

    def test_find_matches_warn(self):
        # Check that no warnings are raised when relevant args are 0.
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            _EdgeMapper._find_matches_warn(
                unresolvable_count=0, overlimit_count=0, overlimit_max=0, match_limit=1,
            )

        # Check warning for values with no matches.
        regex = 'skipped 11 values that matched no records'
        with self.assertWarnsRegex(ToronWarning, regex):
            _EdgeMapper._find_matches_warn(
                unresolvable_count=11, overlimit_count=0, overlimit_max=0, match_limit=1,
            )

        # Check warning for values matching too many records.
        regex = (
            'skipped 7 values that matched too many records, '
            'current match_limit is 3 but data includes values that match up to 5 records'
        )
        with self.assertWarnsRegex(ToronWarning, regex):
            _EdgeMapper._find_matches_warn(
                unresolvable_count=0, overlimit_count=7, overlimit_max=5, match_limit=3,
            )

        # Check warnings on all conditions.
        regex = (
            'skipped 11 values that matched no records, '
            'skipped 7 values that matched too many records, '
            'current match_limit is 3 but data includes values that match up to 5 records'
        )
        with self.assertWarnsRegex(ToronWarning, regex):
            _EdgeMapper._find_matches_warn(
                unresolvable_count=11, overlimit_count=7, overlimit_max=5, match_limit=3,
            )


class TestAddEdge(TwoNodesTestCase):
    def test_basics(self):
        mapping_data = [
            ['idx', 'population', 'idx1', 'idx2'],
            ['A', 10, 'A', 'x'],
            ['A', 70, 'A', 'y'],
            ['B', 20, 'B', 'x'],
            ['B', 60, 'B', 'y'],
            ['C', 30, 'C', 'x'],
            ['C', 50, 'C', 'y'],
        ]

        add_edge(                 # <- The method under test.
            data=mapping_data,
            name='population',
            left_node=self.node1,
            direction='-->',
            right_node=self.node2,
        )

        con = self.node2._dal._get_connection()
        results = con.execute('SELECT * FROM relation').fetchall()

        expected = [
            (1, 1, 1, 1, 10.0, 0.125, None),
            (2, 1, 1, 2, 70.0, 0.875, None),
            (3, 1, 2, 3, 20.0, 0.25,  None),
            (4, 1, 2, 4, 60.0, 0.75,  None),
            (5, 1, 3, 5, 30.0, 0.375, None),
            (6, 1, 3, 6, 50.0, 0.625, None),
            (7, 1, 0, 0,  0.0, 1.0,   None),
        ]
        self.assertEqual(results, expected)
