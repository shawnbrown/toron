"""Tests for toron/_mapper.py module."""

import sqlite3
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


class TestFindMatchesFormatData(unittest.TestCase):
    def setUp(self):
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
        self.node = node

    def test_find_matches_format_data_exact(self):
        formatted = Mapper._find_matches_format_data(
            self.node,
            column_names=['idx1', 'idx2'],
            iterable=[
                # Tuples contain `label_values` and `run_id`.
                ('["A", "x"]', 101),
                ('["A", "y"]', 102),
                ('["B", "x"]', 103),
                ('["B", "y"]', 104),
                ('["C", "x"]', 105),
                ('["C", "y"]', 106),
            ],
        )

        # Materialize generators as lists, check against `expected`.
        actual = [(a, b, list(c)) for a, b, c in formatted]
        expected = [
            ([101], {'idx1': 'A', 'idx2': 'x'}, [(1, 'A', 'x')]),
            ([102], {'idx1': 'A', 'idx2': 'y'}, [(2, 'A', 'y')]),
            ([103], {'idx1': 'B', 'idx2': 'x'}, [(3, 'B', 'x')]),
            ([104], {'idx1': 'B', 'idx2': 'y'}, [(4, 'B', 'y')]),
            ([105], {'idx1': 'C', 'idx2': 'x'}, [(5, 'C', 'x')]),
            ([106], {'idx1': 'C', 'idx2': 'y'}, [(6, 'C', 'y')]),
        ]
        self.assertEqual(actual, expected)

    def test_find_matches_format_data_ambiguous(self):
        formatted = Mapper._find_matches_format_data(
            self.node,
            column_names=['idx1', 'idx2'],
            iterable=[
                ('["A", "x"]',  101),
                ('["A", "y"]',  102),
                ('["B", ""]',   103),  # <- Should match 2 index records.
                ('["C", null]', 104),  # <- Should match 2 index records.
            ],
        )

        # Materialize generators as lists, check against `expected`.
        actual = [(a, b, list(c)) for a, b, c in formatted]
        expected = [
            ([101], {'idx1': 'A', 'idx2': 'x'}, [(1, 'A', 'x')]),
            ([102], {'idx1': 'A', 'idx2': 'y'}, [(2, 'A', 'y')]),
            ([103], {'idx1': 'B'}, [(3, 'B', 'x'), (4, 'B', 'y')]),
            ([104], {'idx1': 'C'}, [(5, 'C', 'x'), (6, 'C', 'y')]),
        ]
        self.assertEqual(actual, expected)

    def test_find_matches_format_data_none_found(self):
        formatted = Mapper._find_matches_format_data(
            self.node,
            column_names=['idx1', 'idx2'],
            iterable=[
                ('["X", "xxx"]', 997),
                ('["Y", "yyy"]', 998),
                ('["Z", "zzz"]', 999),
            ],
        )

        # Materialize generators as lists, check against `expected`.
        actual = [(a, b, list(c)) for a, b, c in formatted]
        expected = [
            ([997], {'idx1': 'X', 'idx2': 'xxx'}, []),
            ([998], {'idx1': 'Y', 'idx2': 'yyy'}, []),
            ([999], {'idx1': 'Z', 'idx2': 'zzz'}, []),
        ]
        self.assertEqual(actual, expected)


class TestMatchExactOrGetInfo(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.cursor = self.connection.executescript("""
            CREATE TEMP TABLE right_matches(
                run_id INTEGER NOT NULL REFERENCES source_mapping(run_id),
                index_id INTEGER,
                weight_value REAL CHECK (0.0 <= weight_value),
                proportion REAL CHECK (0.0 <= proportion AND proportion <= 1.0),
                mapping_level BLOB_BITFLAGS
            );
        """)

    def test_exact_match(self):
        info_dict = Mapper._match_exact_or_get_info(
            self.cursor,
            side='right',
            run_ids=[101],
            key={'idx1': 'A', 'idx2': 'x'},
            matches=iter([(1, 'A', 'x')]),
        )

        self.assertEqual(info_dict, {}, msg='expecting empty dictionary')

        self.cursor.execute('SELECT * FROM temp.right_matches')
        expected = [(101, 1, None, None, None)]
        self.assertEqual(self.cursor.fetchall(), expected)

    def test_no_match(self):
        info_dict = Mapper._match_exact_or_get_info(
            self.cursor,
            side='right',
            run_ids=[101],
            key={'idx1': 'A', 'idx2': 'x'},
            matches=iter([]),  # <- Empty matches iterator.
        )

        self.assertEqual(info_dict, {'unresolvable_count': 1})

        self.cursor.execute('SELECT * FROM temp.right_matches')
        self.assertEqual(self.cursor.fetchall(), [], msg='no record inserted')

    def test_ambiguous_match_not_allowed(self):
        """If match is ambiguous and over the limit, it is logged
        using `'overlimit_count'` in the returned info_dict.
        """
        # Single record from Mapper._find_matches_format_data()
        # is a three-tuple like `(run_ids, key, matches)`.
        run_ids = [103]
        key = {'idx1': 'B'}
        matches = [(3, 'B', 'x'), (4, 'B', 'y')]

        # When no `match_limit` is given, it defaults to 1.
        info_dict = Mapper._match_exact_or_get_info(
            self.cursor, 'right', run_ids, key, iter(matches), match_limit=1
        )
        expected_dict = {
            'num_of_matches': 2,
            'overlimit_count': 1,
        }
        self.assertEqual(info_dict, expected_dict)

        # Check that no records have been added.
        self.cursor.execute('SELECT * FROM temp.right_matches')
        self.assertEqual(self.cursor.fetchall(), [], msg='should be no records')

    def test_ambiguous_match_allowed(self):
        """If match is ambiguous but equal to or under the limit,
        log the match using `'ambiguous_matches'` and also log the
        column names used for the match using `'matched_category'`
        in the returned info_dict.
        """
        # Single record from Mapper._find_matches_format_data()
        # is a three-tuple like `(run_ids, key, matches)`.
        run_ids = [103]
        key = {'idx1': 'B'}
        matches = [(3, 'B', 'x'), (4, 'B', 'y')]

        # Check using `match_limit=3`.
        info_dict = Mapper._match_exact_or_get_info(
            self.cursor, 'right', run_ids, key, iter(matches), match_limit=2
        )
        expected_dict = {
            'ambiguous_matches': [([103], {'idx1': 'B'}, 2)],
            'matched_category': ['idx1'],
        }
        self.assertEqual(info_dict, expected_dict)

        # Check that no records have been added.
        self.cursor.execute('SELECT * FROM temp.right_matches')
        self.assertEqual(self.cursor.fetchall(), [], msg='should be no records')


class TestMatchAmbiguousOrGetInfo(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.cursor = self.connection.executescript("""
            CREATE TEMP TABLE right_matches(
                run_id INTEGER NOT NULL REFERENCES source_mapping(run_id),
                index_id INTEGER,
                weight_value REAL CHECK (0.0 <= weight_value),
                proportion REAL CHECK (0.0 <= proportion AND proportion <= 1.0),
                mapping_level BLOB_BITFLAGS
            );
        """)

        node = Node()
        data = [
            ['idx1', 'idx2', 'population'],
            ['A', 'x', 3],
            ['A', 'y', 15],
            ['B', 'x', 3],
            ['B', 'y', 7],
            ['C', 'x', 13],
            ['C', 'y', 22],
        ]
        node.add_index_columns(['idx1', 'idx2'])
        node.add_index_records(data)
        node.add_weights(data, 'population', selectors=['[attr1]'])
        self.node = node

    def test_matches_one_to_many(self):
        raise NotImplementedError

    def test_matches_many_to_many(self):
        raise NotImplementedError


class TestMatchRefreshProportions(unittest.TestCase):
    def setUp(self):
        connection = sqlite3.connect(':memory:')

        # Create simplified dummy table for testing.
        self.cursor = connection.execute("""
            CREATE TEMP TABLE right_matches(
                run_id, index_id, weight_value, proportion, mapping_level
            )
        """)

    def test_one_to_one(self):
        self.cursor.execute("""
            INSERT INTO
                right_matches
            VALUES
                (1, 1, NULL, NULL, NULL),
                (2, 2, NULL, NULL, NULL),
                (3, 3, NULL, NULL, NULL),
                (4, 4, NULL, NULL, NULL)
        """)

        Mapper._refresh_proportions(self.cursor, 'right')  # <- Method under test.

        self.cursor.execute('SELECT * FROM temp.right_matches')
        expected = [
            (1, 1, None, 1.0, None),
            (2, 2, None, 1.0, None),
            (3, 3, None, 1.0, None),
            (4, 4, None, 1.0, None),
        ]
        self.assertEqual(self.cursor.fetchall(), expected)

    def test_many_to_one(self):
        self.cursor.execute("""
            INSERT INTO
                right_matches
            VALUES
                (1, 1, NULL, NULL, NULL),
                (2, 1, NULL, NULL, NULL),
                (3, 2, NULL, NULL, NULL),
                (4, 2, NULL, NULL, NULL)
        """)

        Mapper._refresh_proportions(self.cursor, 'right')  # <- Method under test.

        self.cursor.execute('SELECT * FROM temp.right_matches')
        expected = [
            (1, 1, None, 1.0, None),
            (2, 1, None, 1.0, None),
            (3, 2, None, 1.0, None),
            (4, 2, None, 1.0, None),
        ]
        self.assertEqual(self.cursor.fetchall(), expected)

    def test_one_to_many(self):
        self.cursor.execute("""
            INSERT INTO
                right_matches
            VALUES
                (1, 1, NULL, NULL, NULL),
                (2, 2, NULL, NULL, NULL),
                (3, 3, 12.5, NULL, X'C0'),
                (3, 4, 37.5, NULL, X'C0')
        """)

        Mapper._refresh_proportions(self.cursor, 'right')  # <- Method under test.

        self.cursor.execute('SELECT * FROM temp.right_matches')
        expected = [
            (1, 1, None, 1.0, None),
            (2, 2, None, 1.0, None),
            (3, 3, 12.5, 0.25, b'\xc0'),
            (3, 4, 37.5, 0.75, b'\xc0'),
        ]
        self.assertEqual(self.cursor.fetchall(), expected)

    def test_many_to_many(self):
        self.cursor.execute("""
            INSERT INTO
                right_matches
            VALUES
                (1, 1, 20.0, NULL, X'80'),
                (1, 2, 12.0, NULL, X'80'),
                (2, 1, 12.5, NULL, X'C0'),
                (2, 2, 37.5, NULL, X'C0')
        """)

        Mapper._refresh_proportions(self.cursor, 'right')  # <- Method under test.

        self.cursor.execute('SELECT * FROM temp.right_matches')
        expected = [
            (1, 1, 20.0, 0.625, b'\x80'),
            (1, 2, 12.0, 0.375, b'\x80'),
            (2, 1, 12.5, 0.250, b'\xc0'),
            (2, 2, 37.5, 0.750, b'\xc0'),
        ]
        self.assertEqual(self.cursor.fetchall(), expected)


class TestMapperFindMatches(unittest.TestCase):
    def setUp(self):
        node1 = Node()
        data1 = [
            ['idx', 'wght'],
            ['A', 16],
            ['B', 8],
            ['C', 32],
        ]
        node1.add_index_columns(['idx'])
        node1.add_index_records(data1)
        node1.add_weights(data1, 'wght', selectors=['[attr1]'])
        self.node1 = node1

    def test_find_matches_side(self):
        mapper = Mapper([['idx', 'dummy_weight', 'idx1']], 'dummy_weight')

        # Check valid *side* arguments.
        mapper.find_matches(self.node1, 'left')
        mapper.find_matches(self.node1, 'right')

        # Check invalid *side* argument.
        regex = "side must be 'left' or 'right', got 'bad'"
        with self.assertRaisesRegex(ValueError, regex):
            mapper.find_matches(self.node1, 'bad')
