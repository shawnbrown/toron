"""Tests for toron/graph.py module."""

import unittest
import warnings

from toron.node import Node
from toron._schema import BitList
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
            (1, 1, None, 1.0, None),
            (2, 1, None, 1.0, None),
            (3, 2, None, 1.0, None),
            (4, 2, None, 1.0, None),
            (5, 3, None, 1.0, None),
            (6, 3, None, 1.0, None),
        ]
        self.assertEqual(mapper.cur.fetchall(), expected)

        mapper.find_matches('right')  # <- Method under test.
        mapper.cur.execute('SELECT * FROM temp.right_matches')
        expected = [
            (1, 1, None, 1.0, None),
            (2, 2, None, 1.0, None),
            (3, 3, None, 1.0, None),
            (4, 4, None, 1.0, None),
            (5, 5, None, 1.0, None),
            (6, 6, None, 1.0, None),
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
            (1, 1, 10.0, None),
            (1, 2, 70.0, None),
            (2, 3, 20.0, None),
            (2, 4, 60.0, None),
            (3, 5, 30.0, None),
            (3, 6, 50.0, None),
        ]
        self.assertEqual(list(relations), expected)

    def test_get_relations_ambiguous(self):
        data = [
            ['idx', 'population', 'idx1', 'idx2'],
            ['A', 90, 'A', ''],   # <- Matched to 2 right-side records.
            ['B', 20, 'B', 'x'],  # <- Exact match.
            ['B', 60, 'B', 'y'],  # <- Exact match.
            ['C', 28, 'C', ''],   # <- Matched to 1 right-side record (2-ambiguous, minus 1-exact overlap).
            ['C', 7, 'C', 'y'],   # <- Exact match (overlapps the records matched on "C" alone).
        ]
        self.node2.add_discrete_categories([{'idx1'}])
        mapper = _EdgeMapper(data, 'population', self.node1, self.node2)
        mapper.find_matches('left')
        mapper.find_matches('right', match_limit=2)

        relations = mapper.get_relations('right')  # <- Method under test.

        expected = [
            (1, 1, 15.0, b'\x80'),
            (1, 2, 75.0, b'\x80'),
            (2, 3, 20.0, None),
            (2, 4, 60.0, None),
            (3, 5, 28.0, b'\x80'),
            (3, 6,  7.0, None),
        ]
        self.assertEqual(list(relations), expected)

    def test_get_relations_ambiguous_allow_overlapping(self):
        data = [
            ['idx', 'population', 'idx1', 'idx2'],
            ['A', 90, 'A', ''],   # <- Matched to 2 right-side records.
            ['B', 20, 'B', 'x'],  # <- Exact match.
            ['B', 60, 'B', 'y'],  # <- Exact match.
            ['C', 28, 'C', ''],   # <- Matched to 2 right-side record (2-ambiguous, allowing overlap).
            ['C', 7, 'C',  'y'],  # <- Exact match (overlapps the records matched on "C" alone).
        ]
        self.node2.add_discrete_categories([{'idx1'}])
        mapper = _EdgeMapper(data, 'population', self.node1, self.node2)

        mapper.find_matches('left')
        mapper.find_matches('right', match_limit=2, allow_overlapping=True)
        relations = mapper.get_relations('right')  # <- Method under test.

        expected = [
            (1, 1, 15.0, b'\x80'),
            (1, 2, 75.0, b'\x80'),
            (2, 3, 20.0, None),
            (2, 4, 60.0, None),
            (3, 5, 10.4, b'\x80'),
            (3, 6, 7.0,  None),    # <- Exact match overlapped by ambiguous match.
            (3, 6, 17.6, b'\x80')  # <- Ambiguous match that overlaps exact.
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
            ['D', 'x', 'g', None],
            ['D', 'y', 'h', 12.5],
            ['D', 'y', 'i', 37.5],
        ]
        node1 = Node()
        node1.add_index_columns(['idx1', 'idx2', 'idx3'])
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
                unresolvable_count=0,
                invalid_count=0,
                invalid_categories=set(),
                overlimit_count=0,
                overlimit_max=0,
                match_limit=1,
            )

        # Check warning for values with no matches.
        regex = 'skipped 11 values that matched no records'
        with self.assertWarnsRegex(ToronWarning, regex):
            _EdgeMapper._find_matches_warn(
                unresolvable_count=11,
            )

        # Check warning for values matching too many records.
        regex = (
            'skipped 7 values that matched too many records, '
            'current match_limit is 3 but data includes values that match up to 5 records'
        )
        with self.assertWarnsRegex(ToronWarning, regex):
            _EdgeMapper._find_matches_warn(
                overlimit_count=7,
                overlimit_max=5,
                match_limit=3,
            )

        # Check warnings on all conditions.
        regex = (
            'skipped 13 values that matched no records, '
            'skipped 7 values that matched too many records, '
            'current match_limit is 3 but data includes values that match up to 5 records, '
            'skipped 11 values that used invalid categories:\n'
            '  B\n'
            '  B, C'
        )
        with self.assertWarnsRegex(ToronWarning, regex):
            _EdgeMapper._find_matches_warn(
                unresolvable_count=13,
                invalid_count=11,
                invalid_categories={('B', 'C'), ('B',)},
                overlimit_count=7,
                overlimit_max=5,
                match_limit=3,
            )

    def test_refresh_proportions(self):
        data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            # <data rows omitted--not needed for this test>
        ]
        self.node1.add_discrete_categories([{'idx1'}, {'idx1', 'idx2'}])
        mapper = _EdgeMapper(data, 'population', self.node1, self.node2)

        sql = 'INSERT INTO temp.left_matches VALUES (?, ?, ?, ?, ?)'
        parameters = [
            (1, 6, 18.75, None, b'\x80'),
            (2, 7, None,  None, None),
            (3, 8, 12.5,  None, b'\xc0'),
            (3, 9, 37.5,  None, b'\xc0')
        ]
        mapper.cur.executemany(sql, parameters)

        mapper._refresh_proportions('left')  # <- Method under test.

        mapper.cur.execute('SELECT * FROM temp.left_matches')
        expected = [
            (1, 6, 18.75, 1.00, b'\x80'),
            (2, 7, None,  1.00, None),
            (3, 8, 12.5,  0.25, b'\xc0'),
            (3, 9, 37.5,  0.75, b'\xc0')
        ]
        self.assertEqual(mapper.cur.fetchall(), expected)

    def test_find_matches_invalid_structure(self):
        data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['',  'x', '', 100, 'A', 'x', 'a'],
            ['D', '',  '', 100, 'D', 'y', 'h'],
            ['D', 'x', '', 100, 'D', 'x', 'g'],
        ]
        mapper = _EdgeMapper(data, 'population', self.node1, self.node2)

        regex = (
            'skipped 3 values that used invalid categories:\n'
            '  idx1\n'
            '  idx1, idx2\n'
            '  idx2'
        )
        with self.assertWarnsRegex(ToronWarning, regex):
            mapper.find_matches('left')

        # Add one of the missing categories and try again.
        self.node1.add_discrete_categories([{'idx1'}])
        regex = (
            'skipped 2 values that used invalid categories:\n'
            '  idx1, idx2\n'
            '  idx2'
        )
        with self.assertWarnsRegex(ToronWarning, regex):
            mapper.find_matches('left')

        # Add another missing category and try again.
        self.node1.add_discrete_categories([{'idx1', 'idx2'}])
        regex = (
            'skipped 1 values that used invalid categories:\n'
            '  idx2'
        )
        with self.assertWarnsRegex(ToronWarning, regex):
            mapper.find_matches('left')

    def test_find_matches_ambiguous(self):
        data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['B', '',  '', 100, 'B', '', ''],
            ['D', 'y', '', 50,  'D', 'y', 'h'],
            ['D', 'y', '', 50,  'D', 'y', 'i'],
        ]
        self.node1.add_discrete_categories([{'idx1'}, {'idx1', 'idx2'}])
        mapper = _EdgeMapper(data, 'population', self.node1, self.node2)

        mapper.find_matches('left', match_limit=2)  # <- Method under test.

        mapper.cur.execute('SELECT * FROM temp.left_matches')
        expected = [
            (1, 2, 37.5, 0.375, b'\x80'),
            (1, 3, 62.5, 0.625, b'\x80'),
            (2, 8, 12.5, 0.25,  b'\xc0'),
            (2, 9, 37.5, 0.75,  b'\xc0'),
            (3, 8, 12.5, 0.25,  b'\xc0'),
            (3, 9, 37.5, 0.75,  b'\xc0')
        ]
        self.assertEqual(mapper.cur.fetchall(), expected)

    def test_find_matches_ambiguous_no_missing_weight(self):
        data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['D', 'x', '', 100,  'D', 'x', ''],  # <- Matches D/x/f (weight: 18.75) and D/x/g (weight: None).
            ['D', 'y', '', 100,  'D', 'y', ''],
        ]
        self.node1.add_discrete_categories([{'idx1'}, {'idx1', 'idx2'}])
        mapper = _EdgeMapper(data, 'population', self.node1, self.node2)

        regex = (
            'skipped 1 values that ambiguously matched to one or more '
            'records that have no associated weight'
        )
        with self.assertWarnsRegex(ToronWarning, regex):
            mapper.find_matches('left', match_limit=2)  # <- Method under test.

        mapper.cur.execute('SELECT * FROM temp.left_matches')
        expected = [
            (2, 8, 12.5, 0.25, b'\xc0'),
            (2, 9, 37.5, 0.75, b'\xc0'),
        ]
        self.assertEqual(mapper.cur.fetchall(), expected, msg="""
            The left-hand node does not have a weight for index D/x/g.
            This means that the left-side match to D/x does not have a
            full set of weights and cannot be handled as an ambiguous
            match. So the only records in the `expected` list are those
            for D/y/h (weight: 12.5) and D/y/i (weight: 37.5).
        """)

    def test_find_matches_ambiguous_without_overlapping(self):
        """Resolve overlapping matches."""
        data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['D', '',  '',  100, 'D', '',  ''],
            ['D', 'x', 'g', 100, 'D', 'x', 'g'],
            ['D', 'y', '',  100, 'D', 'y', ''],
        ]
        self.node1.add_discrete_categories([{'idx1'}, {'idx1', 'idx2'}])
        mapper = _EdgeMapper(data, 'population', self.node1, self.node2)

        mapper.find_matches('left', match_limit=4, allow_overlapping=False)  # <- Method under test.

        mapper.cur.execute('SELECT * FROM temp.left_matches ORDER BY run_id')
        expected = [
            (1, 6, 18.75, 1.0,  b'\x80'),  # <- Matched by 'D'
            (2, 7, None,  1.0,  None),     # <- Exact match.
            (3, 8, 12.5,  0.25, b'\xc0'),  # <- Matched by 'D/y'
            (3, 9, 37.5,  0.75, b'\xc0')   # <- Matched by 'D/y'
        ]
        self.assertEqual(mapper.cur.fetchall(), expected)

    def test_find_matches_ambiguous_with_overlapping(self):
        data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['B', 'x', '',  100, 'B', 'x', ''],
            ['B', '',  '',  100, 'B', '',  ''],
        ]
        self.node1.add_discrete_categories([{'idx1'}, {'idx1', 'idx2'}])
        mapper = _EdgeMapper(data, 'population', self.node1, self.node2)

        mapper.find_matches('left', match_limit=4, allow_overlapping=True)  # <- Method under test.

        mapper.cur.execute('SELECT * FROM temp.left_matches ORDER BY run_id')
        expected = [
            (1, 2, None, 1.0,   None),     # <- Exact match.
            (2, 2, 37.5, 0.375, b'\x80'),  # <- Matched by 'B' (overlaps the exact match)
            (2, 3, 62.5, 0.625, b'\x80'),  # <- Matched by 'B'
        ]
        self.assertEqual(mapper.cur.fetchall(), expected)


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
            (1,  1, 1, 1, 12.5,    0.25,   BitList([1, 1, 0])),
            (2,  1, 1, 2, 37.5,    0.75,   BitList([1, 1, 0])),
            (3,  1, 2, 3, 37.5,    1.0,    None),
            (4,  1, 3, 3, 62.5,    1.0,    None),
            (5,  1, 4, 4, 19.6875, 0.25,   BitList([1, 0, 0])),
            (6,  1, 4, 5, 59.0625, 0.75,   BitList([1, 0, 0])),
            (7,  1, 5, 4, 6.5625,  0.25,   BitList([1, 0, 0])),
            (8,  1, 5, 5, 19.6875, 0.75,   BitList([1, 0, 0])),
            (9,  1, 6, 6, 28.125,  0.375,  BitList([1, 0, 0])),
            (10, 1, 6, 8, 23.4375, 0.3125, BitList([1, 0, 0])),
            (11, 1, 6, 9, 23.4375, 0.3125, BitList([1, 0, 0])),
            (12, 1, 7, 7, 100.0,   1.0,    None),
            (13, 1, 8, 6, 56.25,   0.375,  BitList([1, 0, 0])),
            (14, 1, 8, 8, 46.875,  0.3125, BitList([1, 0, 0])),
            (15, 1, 8, 9, 46.875,  0.3125, BitList([1, 0, 0])),
            (16, 1, 9, 6, 28.125,  0.375,  BitList([1, 0, 0])),
            (17, 1, 9, 8, 23.4375, 0.3125, BitList([1, 0, 0])),
            (18, 1, 9, 9, 23.4375, 0.3125, BitList([1, 0, 0])),
            (19, 1, 0, 0, 0.0,     1.0,    None)
        ]
        self.assertEqual(results, expected)
