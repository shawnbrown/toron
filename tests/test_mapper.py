"""Tests for toron/_mapper.py module."""

import sqlite3
import unittest
import warnings

from toron.node import Node
from toron._utils import ToronWarning
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

    def test_matches_one_to_one(self):
        info_dict = Mapper._match_exact_or_get_info(
            self.cursor,
            side='right',
            index_columns=['idx1', 'idx2', 'idx3'],
            structure_set={(0, 0, 0), (1, 1, 1)},
            run_ids=[101],
            key={'idx1': 'A', 'idx2': 'x'},
            matches=iter([(1, 'A', 'x')]),
        )

        self.assertEqual(info_dict, {}, msg='expecting empty dictionary')

        self.cursor.execute('SELECT * FROM temp.right_matches')
        expected = [(101, 1, None, None, None)]
        self.assertEqual(self.cursor.fetchall(), expected)

    def test_matches_many_to_one(self):
        info_dict = Mapper._match_exact_or_get_info(
            self.cursor,
            side='right',
            index_columns=['idx1', 'idx2', 'idx3'],
            structure_set={(0, 0, 0), (1, 1, 1)},
            run_ids=[101, 102, 103],  # <- Many source records.
            key={'idx1': 'A', 'idx2': 'x'},
            matches=iter([(1, 'A', 'x')]),  # <- Exact destination match.
        )

        self.assertEqual(info_dict, {}, msg='expecting empty dictionary')

        self.cursor.execute('SELECT * FROM temp.right_matches')
        expected = [
            (101, 1, None, None, None),
            (102, 1, None, None, None),
            (103, 1, None, None, None),
        ]
        self.assertEqual(self.cursor.fetchall(), expected)

    def test_no_match(self):
        info_dict = Mapper._match_exact_or_get_info(
            self.cursor,
            side='right',
            index_columns=['idx1', 'idx2', 'idx3'],
            structure_set={(0, 0, 0), (1, 1, 1)},
            run_ids=[101],
            key={'idx1': 'A', 'idx2': 'x'},
            matches=iter([]),  # <- Empty matches iterator.
        )

        self.assertEqual(info_dict, {'count_unmatchable': 1})

        self.cursor.execute('SELECT * FROM temp.right_matches')
        self.assertEqual(self.cursor.fetchall(), [], msg='no record inserted')

    def test_ambiguous_match_not_allowed(self):
        """If match is ambiguous and over the limit, it is logged
        using `'count_overlimit'` in the returned info_dict.
        """
        # Single record from Mapper._find_matches_format_data()
        # is a three-tuple like `(run_ids, key, matches)`.
        run_ids = [103]
        key = {'idx1': 'B'}
        matches = [(3, 'B', 'x'), (4, 'B', 'y')]

        # When no `match_limit` is given, it defaults to 1.
        info_dict = Mapper._match_exact_or_get_info(
            self.cursor,
            'right',
            ['idx1', 'idx2', 'idx3'],
            {(0, 0, 0), (1, 1, 1), (1, 0, 0)},
            run_ids, key, iter(matches),
            match_limit=1,
        )
        expected_dict = {
            'num_of_matches': 2,
            'count_overlimit': 1,
        }
        self.assertEqual(info_dict, expected_dict)

        # Check that no records have been added.
        self.cursor.execute('SELECT * FROM temp.right_matches')
        self.assertEqual(self.cursor.fetchall(), [], msg='should be no records')

    def test_ambiguous_match_allowed(self):
        """If match is ambiguous but equal to or under the limit,
        log the match using `'list_ambiguous'` and also log the
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
            self.cursor,
            'right',
            ['idx1', 'idx2', 'idx3'],
            {(0, 0, 0), (1, 1, 1), (1, 0, 0)},
            run_ids, key, iter(matches),
            match_limit=2,
        )
        expected_dict = {
            'list_ambiguous': [([103], {'idx1': 'B'}, 2)],
        }
        self.assertEqual(info_dict, expected_dict)

        # Check that no records have been added.
        self.cursor.execute('SELECT * FROM temp.right_matches')
        self.assertEqual(self.cursor.fetchall(), [], msg='should be no records')

    def test_invalid_categories(self):
        info_dict = Mapper._match_exact_or_get_info(
            self.cursor,
            side='right',
            index_columns=['idx1', 'idx2', 'idx3'],
            structure_set={(0, 0, 0), (1, 1, 1)},
            run_ids=[103],
            key={'idx1': 'B'},  # <- Matches on `idx1` but (1, 0, 0) is not valid.
            matches=iter([(3, 'B', 'x'), (4, 'B', 'y')]),
            match_limit=2,
        )
        expected_dict = {
            'count_invalid': 1,
            'invalid_categories': {('idx1',)},
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
        ambiguous_match = ([3], {'idx1': 'C'}, 2)
        run_ids, where_dict, _ = ambiguous_match  # Unpack (discards count).

        info_dict = Mapper._match_ambiguous_or_get_info(  # <- Method under test.
            node=self.node,
            cursor=self.cursor,
            side='right',
            run_ids=run_ids,
            where_dict=where_dict,
            index_columns=['idx1', 'idx2'],
            weight_name='population',
        )

        self.assertEqual(info_dict, {}, msg='expecting empty dictionary')

        self.cursor.execute('SELECT * FROM temp.right_matches')
        expected = [
            (3, 5, 13.0, None, b'\x80'),
            (3, 6, 22.0, None, b'\x80'),
        ]
        self.assertEqual(self.cursor.fetchall(), expected)

    def test_matches_many_to_many(self):
        ambiguous_match = ([2, 3], {'idx1': 'C'}, 2)
        run_ids, where_dict, _ = ambiguous_match  # Unpack (discards count).

        info_dict = Mapper._match_ambiguous_or_get_info(  # <- Method under test.
            node=self.node,
            cursor=self.cursor,
            side='right',
            run_ids=run_ids,
            where_dict=where_dict,
            index_columns=['idx1', 'idx2'],
            weight_name='population',
        )

        self.assertEqual(info_dict, {}, msg='expecting empty dictionary')

        self.cursor.execute('SELECT * FROM temp.right_matches')
        expected = [
            (2, 5, 13.0, None, b'\x80'),
            (2, 6, 22.0, None, b'\x80'),
            (3, 5, 13.0, None, b'\x80'),
            (3, 6, 22.0, None, b'\x80')
        ]
        self.assertEqual(self.cursor.fetchall(), expected)

    def test_allow_overlapping(self):
        # Add an existing match.
        info_dict = Mapper._match_exact_or_get_info(
            self.cursor,
            'right',
            ['idx1', 'idx2', 'idx3'],
            {(0, 0, 0), (1, 1, 1), (1, 1, 0)},
            [101], {'idx1': 'C', 'idx2': 'x'}, iter([(5, 'C', 'x')]),
        )

        ambiguous_match = ([102], {'idx1': 'C'}, 2)
        run_ids, where_dict, _ = ambiguous_match  # Unpack (discards count).

        info_dict = Mapper._match_ambiguous_or_get_info(  # <- Method under test.
            node=self.node,
            cursor=self.cursor,
            side='right',
            run_ids=run_ids,
            where_dict=where_dict,
            index_columns=['idx1', 'idx2'],
            weight_name='population',
            allow_overlapping=True,  # <- Allows matches to overlap.
        )

        self.assertEqual(info_dict, {}, msg='expecting empty dictionary')

        self.cursor.execute('SELECT * FROM temp.right_matches')
        expected = [
            (101, 5, None, None, None),     # <- Exact match (5).
            (102, 5, 13.0, None, b'\x80'),  # <- Overlaps exact match (5).
            (102, 6, 22.0, None, b'\x80'),
        ]
        self.assertEqual(self.cursor.fetchall(), expected)

    def test_disallow_overlapping(self):
        # Add an existing match.
        info_dict = Mapper._match_exact_or_get_info(
            self.cursor,
            'right',
            ['idx1', 'idx2', 'idx3'],
            {(0, 0, 0), (1, 1, 1), (1, 1, 0)},
            [101], {'idx1': 'C', 'idx2': 'x'}, iter([(5, 'C', 'x')]),
        )

        ambiguous_match = ([102], {'idx1': 'C'}, 2)
        run_ids, where_dict, _ = ambiguous_match  # Unpack (discards count).

        info_dict = Mapper._match_ambiguous_or_get_info(  # <- Method under test.
            node=self.node,
            cursor=self.cursor,
            side='right',
            run_ids=run_ids,
            where_dict=where_dict,
            index_columns=['idx1', 'idx2'],
            weight_name='population',
            allow_overlapping=False,  # <- False is the default.
        )

        self.assertEqual(info_dict, {}, msg='expecting empty dictionary')

        self.cursor.execute('SELECT * FROM temp.right_matches')
        expected = [
            (101, 5, None, None, None),     # <- Exact match (5).
            (102, 6, 22.0, None, b'\x80'),  # <- Only one record (overlap of 5 is omitted).
        ]
        self.assertEqual(self.cursor.fetchall(), expected)


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


class TestMapperWarnMatchStats(unittest.TestCase):
    def test_no_warning(self):
        """Check no warnings are raised when relevant args are 0."""
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            Mapper._warn_match_stats(
                count_unmatchable=0,
                count_invalid=0,
                invalid_categories=set(),
                count_overlimit=0,
                overlimit_max=0,
                match_limit=1,
            )

    def test_warn_unresolvable(self):
        """Check warning for values with no matches."""
        regex = 'skipped 11 values that matched no records'
        with self.assertWarnsRegex(ToronWarning, regex):
            Mapper._warn_match_stats(
                count_unmatchable=11,
            )

    def test_warn_overlimit(self):
        """Check warning for values matching too many records."""
        regex = (
            'skipped 7 values that matched too many records, '
            'current match_limit is 3 but data includes values that match up to 5 records'
        )
        with self.assertWarnsRegex(ToronWarning, regex):
            Mapper._warn_match_stats(
                count_overlimit=7,
                overlimit_max=5,
                match_limit=3,
            )

    def test_warn_multiple(self):
        """Check warnings on all conditions."""
        regex = (
            'skipped 13 values that matched no records, '
            'skipped 7 values that matched too many records, '
            'current match_limit is 3 but data includes values that match up to 5 records, '
            'skipped 11 values that used invalid categories:\n'
            '  B\n'
            '  B, C'
        )
        with self.assertWarnsRegex(ToronWarning, regex):
            Mapper._warn_match_stats(
                count_unmatchable=13,
                count_invalid=11,
                invalid_categories={('B', 'C'), ('B',)},
                count_overlimit=7,
                overlimit_max=5,
                match_limit=3,
            )


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

        node2 = Node()
        data2 = [
            ['idx1', 'idx2', 'wght'],
            ['A', 'x', 3],
            ['A', 'y', 15],
            ['B', 'x', 3],
            ['B', 'y', 7],
            ['C', 'x', 13],
            ['C', 'y', 22],
        ]
        node2.add_index_columns(['idx1', 'idx2'])
        node2.add_index_records(data2)
        node2.add_weights(data2, 'wght', selectors=['[attr1]'])
        self.node2 = node2

    def test_find_matches_side(self):
        mapper = Mapper([['idx', 'dummy_weight', 'idx1']], 'dummy_weight')

        # Check valid *side* arguments.
        mapper.find_matches(self.node1, 'left')
        mapper.find_matches(self.node1, 'right')

        # Check invalid *side* argument.
        regex = "side must be 'left' or 'right', got 'bad'"
        with self.assertRaisesRegex(ValueError, regex):
            mapper.find_matches(self.node1, 'bad')

    def test_exact_matching(self):
        mapper = Mapper(
            data=[
                ['idx', 'population', 'idx1', 'idx2'],
                ['A', 10, 'A', 'x'],
                ['A', 70, 'A', 'y'],
                ['B', 20, 'B', 'x'],
                ['B', 60, 'B', 'y'],
                ['C', 30, 'C', 'x'],
                ['C', 50, 'C', 'y'],
            ],
            name='population',
        )

        mapper.find_matches(self.node1, 'left')  # <- Method under test.
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

        mapper.find_matches(self.node2, 'right')  # <- Method under test.
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

        regex = "side must be 'left' or 'right', got 'blerg'"
        with self.assertRaisesRegex(ValueError, regex):
            mapper.find_matches(self.node1, 'blerg')  # <- Method under test.

    def test_no_matches_found(self):
        data = [
            ['idx', 'population', 'idx1', 'idx2'],
            ['X', 10, 'X', 'X'],
            ['Y', 70, 'Y', 'Y'],
            ['Z', 20, 'Z', 'Z'],
        ]

        mapper = Mapper(data, 'population')

        regex = 'skipped 3 values that matched no records'
        with self.assertWarnsRegex(ToronWarning, regex):
            mapper.find_matches(self.node1, 'left')  # <- Method under test.

        mapper.cur.execute('SELECT * FROM temp.left_matches')
        no_results = []
        self.assertEqual(mapper.cur.fetchall(), no_results)

    def test_bad_match_limit(self):
        data = [
            ['idx', 'population', 'idx1', 'idx2'],
            ['X', 10, 'X', 'X'],
        ]
        mapper = Mapper(data, 'population')

        regex = 'match_limit must be 1 or greater, got 0'
        with self.assertRaisesRegex(ValueError, regex):
            mapper.find_matches(self.node1, 'left', match_limit=0)

        regex = "match_limit must be int or float, got 'foo'"
        with self.assertRaisesRegex(TypeError, regex):
            mapper.find_matches(self.node1, 'left', match_limit='foo')


class TestMapperFindMatches2(unittest.TestCase):
    """Additional find_matches() tests using other node fixtures."""
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

    def test_invalid_structure(self):
        data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['',  'x', '', 100, 'A', 'x', 'a'],
            ['D', '',  '', 100, 'D', 'y', 'h'],
            ['D', 'x', '', 100, 'D', 'x', 'g'],
        ]
        mapper = Mapper(data, 'population')

        regex = (
            'skipped 3 values that used invalid categories:\n'
            '  idx1\n'
            '  idx1, idx2\n'
            '  idx2'
        )
        with self.assertWarnsRegex(ToronWarning, regex):
            mapper.find_matches(self.node1, 'left')

        # Add one of the missing categories and try again.
        self.node1.add_discrete_categories([{'idx1'}])
        regex = (
            'skipped 2 values that used invalid categories:\n'
            '  idx1, idx2\n'
            '  idx2'
        )
        with self.assertWarnsRegex(ToronWarning, regex):
            mapper.find_matches(self.node1, 'left')

        # Add another missing category and try again.
        self.node1.add_discrete_categories([{'idx1', 'idx2'}])
        regex = (
            'skipped 1 values that used invalid categories:\n'
            '  idx2'
        )
        with self.assertWarnsRegex(ToronWarning, regex):
            mapper.find_matches(self.node1, 'left')

    def test_ambiguous_matches(self):
        data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['B', '',  '', 100, 'B', '', ''],
            ['D', 'y', '', 50,  'D', 'y', 'h'],
            ['D', 'y', '', 50,  'D', 'y', 'i'],
        ]
        self.node1.add_discrete_categories([{'idx1'}, {'idx1', 'idx2'}])
        mapper = Mapper(data, 'population')

        mapper.find_matches(self.node1, 'left', match_limit=2)  # <- Method under test.

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

    def test_ambiguous_matches_no_missing_weight(self):
        data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['D', 'x', '', 100,  'D', 'x', ''],  # <- Matches D/x/f (weight: 18.75) and D/x/g (weight: None).
            ['D', 'y', '', 100,  'D', 'y', ''],
        ]
        self.node1.add_discrete_categories([{'idx1'}, {'idx1', 'idx2'}])
        mapper = Mapper(data, 'population')

        regex = (
            'skipped 1 values that ambiguously matched to one or more '
            'records that have no associated weight'
        )
        with self.assertWarnsRegex(ToronWarning, regex):
            mapper.find_matches(self.node1, 'left', match_limit=2)  # <- Method under test.

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

    def test_ambiguous_matches_without_overlapping(self):
        """Resolve overlapping matches."""
        data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['D', '',  '',  100, 'D', '',  ''],
            ['D', 'x', 'g', 100, 'D', 'x', 'g'],
            ['D', 'y', '',  100, 'D', 'y', ''],
        ]
        self.node1.add_discrete_categories([{'idx1'}, {'idx1', 'idx2'}])
        mapper = Mapper(data, 'population')

        mapper.find_matches(self.node1, 'left', match_limit=4, allow_overlapping=False)  # <- Method under test.

        mapper.cur.execute('SELECT * FROM temp.left_matches ORDER BY run_id')
        expected = [
            (1, 6, 18.75, 1.0,  b'\x80'),  # <- Matched by 'D'
            (2, 7, None,  1.0,  None),     # <- Exact match.
            (3, 8, 12.5,  0.25, b'\xc0'),  # <- Matched by 'D/y'
            (3, 9, 37.5,  0.75, b'\xc0')   # <- Matched by 'D/y'
        ]
        self.assertEqual(mapper.cur.fetchall(), expected)

    def test_ambiguous_matches_with_overlapping(self):
        data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['B', 'x', '',  100, 'B', 'x', ''],
            ['B', '',  '',  100, 'B', '',  ''],
        ]
        self.node1.add_discrete_categories([{'idx1'}, {'idx1', 'idx2'}])
        mapper = Mapper(data, 'population')

        mapper.find_matches(self.node1, 'left', match_limit=4, allow_overlapping=True)  # <- Method under test.

        mapper.cur.execute('SELECT * FROM temp.left_matches ORDER BY run_id')
        expected = [
            (1, 2, None, 1.0,   None),     # <- Exact match.
            (2, 2, 37.5, 0.375, b'\x80'),  # <- Matched by 'B' (overlaps the exact match)
            (2, 3, 62.5, 0.625, b'\x80'),  # <- Matched by 'B'
        ]
        self.assertEqual(mapper.cur.fetchall(), expected)
