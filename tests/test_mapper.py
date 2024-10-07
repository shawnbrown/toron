"""Tests for toron/mapper.py module."""

import logging
import sqlite3
import unittest
from contextlib import closing
from io import StringIO

from toron.node import Node
from toron.mapper import Mapper
from toron.data_models import Structure
from toron._utils import BitFlags


class TestMapperInit(unittest.TestCase):
    @staticmethod
    def get_mapping_data(mapper):
        """Helper method to get contents of 'mapping_data' table."""
        with closing(mapper.con.cursor()) as cur:
            cur.execute('SELECT * FROM mapping_data')
            return set(cur.fetchall())

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

        self.assertEqual(mapper.left_columns, ['idx1'])
        self.assertEqual(mapper.right_columns, ['idx1', 'idx2'])
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

        self.assertEqual(mapper.left_columns, ['idx1'])
        self.assertEqual(mapper.right_columns, ['idx1', 'idx2'])
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


class TestMapperGetLevelPairs(unittest.TestCase):
    def test_same_column_order(self):
        right_columns = ['A', 'B', 'C']
        right_levels = [
            b'\xe0',  # 1, 1, 1
            b'\xc0',  # 1, 1, 0
            b'\x80',  # 1, 0, 0
            b'\x60',  # 0, 1, 1
            b'\x20',  # 0, 0, 1
        ]
        node_columns = ['A', 'B', 'C']
        node_structures = [
            Structure(id=4, granularity=3.0,  bits=(1, 1, 1)),
            Structure(id=3, granularity=2.0,  bits=(1, 1, 0)),
            Structure(id=2, granularity=1.0,  bits=(1, 0, 0)),
            Structure(id=1, granularity=None, bits=(0, 0, 0)),
        ]

        level_pairs = Mapper._get_level_pairs(  # <- Method under test.
            right_columns,
            right_levels,
            node_columns,
            node_structures,
        )

        self.assertEqual(
            level_pairs,
            [(b'\xe0', b'\xe0'),  # A, B, C
             (b'\xc0', b'\xc0'),  # A, B
             (b'\x80', b'\x80'),  # A
             (b'\x60', None),     # B, C
             (b'\x20', None)]     # C
        )

    def test_different_column_order(self):
        """Mapping columns may be in different order than node columns."""
        right_columns = ['C', 'B', 'A']  # <- Different order than node_columns, below.
        right_levels = [
            b'\xe0',  # 1, 1, 1
            b'\x60',  # 0, 1, 1
            b'\x20',  # 0, 0, 1
            b'\xc0',  # 1, 1, 0
            b'\x80',  # 1, 0, 0
        ]
        node_columns = ['A', 'B', 'C']  # <- Different order than right_columns, above.
        node_structures = [
            Structure(id=4, granularity=3.0,  bits=(1, 1, 1)),
            Structure(id=3, granularity=2.0,  bits=(1, 1, 0)),
            Structure(id=2, granularity=1.0,  bits=(1, 0, 0)),
            Structure(id=1, granularity=None, bits=(0, 0, 0)),
        ]

        level_pairs = Mapper._get_level_pairs(  # <- Method under test.
            right_columns,
            right_levels,
            node_columns,
            node_structures,
        )

        self.assertEqual(
            level_pairs,
            [(b'\xe0', b'\xe0'),  # A, B, C
             (b'\x60', b'\xc0'),  # A, B
             (b'\x20', b'\x80'),  # A
             (b'\xc0', None),     # B, C
             (b'\x80', None)]     # C
        )


class TestMatchRefreshProportions(unittest.TestCase):
    def setUp(self):
        # Create simplified dummy table for testing.
        connection = sqlite3.connect(':memory:')
        self.addCleanup(connection.close)
        self.cursor = connection.execute("""
            CREATE TEMP TABLE right_matches(
                run_id, index_id, weight_value, mapping_level, proportion
            )
        """)

    def select_all_helper(self):
        """Helper method to get contents of 'right_matches' table."""
        self.cursor.execute('SELECT * FROM right_matches')
        return self.cursor.fetchall()

    def test_one_to_one(self):
        self.cursor.execute("""
            INSERT INTO
                right_matches
            VALUES
                (1, 1,  3.0, X'C0', NULL),
                (2, 2, 15.0, X'C0', NULL),
                (3, 3,  3.0, X'C0', NULL),
                (4, 4,  7.0, X'C0', NULL)
        """)

        Mapper._refresh_proportions(self.cursor, 'right')  # <- Method under test.

        self.assertEqual(self.select_all_helper(), [(1, 1,  3.0, b'\xc0', 1.0),
                                                    (2, 2, 15.0, b'\xc0', 1.0),
                                                    (3, 3,  3.0, b'\xc0', 1.0),
                                                    (4, 4,  7.0, b'\xc0', 1.0)])

    def test_many_to_one(self):
        self.cursor.execute("""
            INSERT INTO
                right_matches
            VALUES
                (1, 1,  3.0, X'C0', NULL),
                (2, 1, 15.0, X'C0', NULL),
                (3, 2,  3.0, X'C0', NULL),
                (4, 2,  7.0, X'C0', NULL)
        """)

        Mapper._refresh_proportions(self.cursor, 'right')  # <- Method under test.

        self.assertEqual(self.select_all_helper(), [(1, 1,  3.0, b'\xc0', 1.0),
                                                    (2, 1, 15.0, b'\xc0', 1.0),
                                                    (3, 2,  3.0, b'\xc0', 1.0),
                                                    (4, 2,  7.0, b'\xc0', 1.0)])

    def test_one_to_many(self):
        self.cursor.execute("""
            INSERT INTO
                right_matches
            VALUES
                (1, 1,  3.0, X'C0', NULL),
                (2, 2,  7.0, X'C0', NULL),
                (3, 3, 12.5, X'C0', NULL),
                (3, 4, 37.5, X'C0', NULL)
        """)

        Mapper._refresh_proportions(self.cursor, 'right')  # <- Method under test.

        self.assertEqual(self.select_all_helper(), [(1, 1,  3.0, b'\xc0', 1.00),
                                                    (2, 2,  7.0, b'\xc0', 1.00),
                                                    (3, 3, 12.5, b'\xc0', 0.25),
                                                    (3, 4, 37.5, b'\xc0', 0.75)])

    def test_many_to_many(self):
        self.cursor.execute("""
            INSERT INTO
                right_matches
            VALUES
                (1, 1, 20.0, X'80', NULL),
                (1, 2, 12.0, X'80', NULL),
                (2, 1, 12.5, X'C0', NULL),
                (2, 2, 37.5, X'C0', NULL)
        """)

        Mapper._refresh_proportions(self.cursor, 'right')  # <- Method under test.

        self.assertEqual(self.select_all_helper(), [(1, 1, 20.0, b'\x80', 0.625),
                                                    (1, 2, 12.0, b'\x80', 0.375),
                                                    (2, 1, 12.5, b'\xc0', 0.250),
                                                    (2, 2, 37.5, b'\xc0', 0.750)])

    def test_many_to_many_zero_weight(self):
        self.cursor.execute("""
            INSERT INTO
                right_matches
            VALUES
                (1, 1, 0.0, X'E0', NULL),
                (2, 2, 0.0, X'C0', NULL),
                (2, 3, 0.0, X'C0', NULL),
                (3, 2, 0.0, X'80', NULL),
                (3, 3, 0.0, X'80', NULL),
                (3, 4, 0.0, X'80', NULL),
                (3, 5, 0.0, X'80', NULL)
        """)

        Mapper._refresh_proportions(self.cursor, 'right')  # <- Method under test.

        self.assertEqual(self.select_all_helper(), [(1, 1, 0.0, b'\xe0', 1.00),
                                                    (2, 2, 0.0, b'\xc0', 0.50),
                                                    (2, 3, 0.0, b'\xc0', 0.50),
                                                    (3, 2, 0.0, b'\x80', 0.25),
                                                    (3, 3, 0.0, b'\x80', 0.25),
                                                    (3, 4, 0.0, b'\x80', 0.25),
                                                    (3, 5, 0.0, b'\x80', 0.25)])


class TwoNodesBaseTest(unittest.TestCase):
    """A base class that sets-up node fixtures and a logging handler."""
    def setUp(self):
        self.node1 = Node()
        self.node1.add_index_columns('idx')
        self.node1.insert_index([['idx'], ['A'], ['B'], ['C']])
        self.node1.add_weight_group('wght', make_default=True)
        self.node1.insert_weights(
            weight_group_name='wght',
            data=[['idx', 'wght'], ['A', 16], ['B', 8], ['C', 32]],
        )
        self.node1.add_discrete_categories({'idx'})

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
        self.node2.add_weight_group('wght', make_default=True)
        self.node2.insert_weights(
            weight_group_name='wght',
            data=[
                ['idx1', 'idx2', 'wght'],
                ['A', 'x',  5],
                ['A', 'y', 15],
                ['B', 'x',  3],
                ['B', 'y',  5],
                ['C', 'x', 13],
                ['C', 'y', 22],
            ],
        )
        self.node2.add_discrete_categories({'idx1'})

        # Set up stream object to capture log messages.
        self.log_stream = StringIO()
        self.addCleanup(self.log_stream.close)

        # Add handler to 'app-toron' logger.
        applogger = logging.getLogger('app-toron')
        handler = logging.StreamHandler(self.log_stream)
        handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        applogger.addHandler(handler)
        self.addCleanup(lambda: applogger.removeHandler(handler))


class TestMapperMatchRecords(TwoNodesBaseTest):
    @staticmethod
    def select_all_helper(mapper, table):
        """Helper method to get contents of a table in mapper."""
        with closing(mapper.con.cursor()) as cur:
            cur.execute(f'SELECT * FROM {table}')
            contents = cur.fetchall()
        return contents

    def test_exact_matches(self):
        mapper = Mapper(
            crosswalk_name='population',
            data=[['idx', 'population', 'idx1', 'idx2'],
                  ['A', 70, 'A', 'x'],
                  ['B', 80, 'B', 'y'],
                  ['C', 15, 'A', 'y']],
        )

        mapper.match_records(self.node1, 'left')

        self.assertEqual(
            self.select_all_helper(mapper, 'left_matches'),
            [(1, 1, 16.0, b'\x80', 1.0),
             (2, 2,  8.0, b'\x80', 1.0),
             (3, 3, 32.0, b'\x80', 1.0)],
        )

    def test_ambiguous_matches_over_limit(self):
        mapper = Mapper(
            crosswalk_name='population',
            data=[['idx', 'population', 'idx1', 'idx2'],
                  ['A', 70, 'A', 'x'],
                  ['A', 40, 'A', 'y'],
                  ['B', 80, 'B', '']],  # <- Matches to 2 records.
        )

        mapper.match_records(self.node2, 'right')  # <- match_limit dafaults to 1

        self.assertEqual(
            self.log_stream.getvalue(),
            ('WARNING: skipped 1 values that matched too many records\n'
             'WARNING: current match_limit is 1 but mapping includes '
             'values that match up to 2 records\n'),
        )

        self.assertEqual(
            self.select_all_helper(mapper, 'right_matches'),
            [(1, 1,  5.0, b'\xc0', 1.0),
             (2, 2, 15.0, b'\xc0', 1.0)],
            msg=('should only match two records, other records are '
                 'over the match limit'),
        )

    def test_ambiguous_matches_within_limit(self):
        mapper = Mapper(
            crosswalk_name='population',
            data=[['idx', 'population', 'idx1', 'idx2'],
                  ['A', 70, 'A', 'x'],
                  ['A', 40, 'A', 'y'],
                  ['B', 80, 'B', '']],  # <- Matches to 2 records.
        )

        mapper.match_records(self.node2, 'right', match_limit=2)

        self.assertEqual(
            self.select_all_helper(mapper, 'right_matches'),
            [(1, 1,  5.0, b'\xc0', 1.0),
             (2, 2, 15.0, b'\xc0', 1.0),
             (3, 3,  3.0, b'\x80', 0.375),   # <- Ambiguous, has different level (b'\x80').
             (3, 4,  5.0, b'\x80', 0.625)],  # <- Ambiguous, has different level (b'\x80').
        )

    def test_invalid_categories(self):
        mapper = Mapper(
            crosswalk_name='population',
            data=[['idx', 'population', 'idx1', 'idx2'],
                  ['A', 70, 'A', 'x'],
                  ['A', 40, 'A', 'y'],
                  ['B', 80,  '', 'y']],  # <- Invalid category.
        )

        mapper.match_records(self.node2, 'right')  # <- match_limit dafaults to 1

        self.assertEqual(
            self.log_stream.getvalue(),
            ('WARNING: skipped 1 values that used invalid categories:\n'
             '  idx2\n'),
        )

        self.assertEqual(
            self.select_all_helper(mapper, 'right_matches'),
            [(1, 1,  5.0, b'\xc0', 1.0),
             (2, 2, 15.0, b'\xc0', 1.0)],
            msg=('should only match two records, other records are '
                 'over the match limit'),
        )

    def test_unknown_columns(self):
        """Should log error-level message if unknown columns are given.

        The presence of unknown columns will prevent matching and users
        need to be informed why the match failed.
        """
        mapper = Mapper(
            crosswalk_name='population',
            data=[['idx', 'population', 'idx1', 'idx2', 'idx3'],
                  ['A', 70, 'A', 'x', 'z'],
                  ['B', 80, 'B', 'y', 'z'],
                  ['C', 15, 'A', 'y', 'z']],
        )

        mapper.match_records(self.node2, 'right')

        self.assertEqual(
            self.log_stream.getvalue(),
            ("ERROR: mapping contains columns not present in the node being "
             "matched: 'idx3'\n"),
        )

        self.assertEqual(
            self.select_all_helper(mapper, 'right_matches'),
            [],
            msg='should be empty, unknown columns will not match node indexes',
        )

    def test_missing_weight_exact_match(self):
        """Exact matches are OK even when weight is missing."""
        mapper = Mapper(
            crosswalk_name='population',
            data=[['idx', 'population', 'idx1', 'idx2'],
                  ['A', 70, 'A', 'x'],
                  ['A', 40, 'A', 'y'],
                  ['B', 80, 'B', 'x'],
                  ['B', 80, 'B', 'y']],
        )
        # Delete a weight record that's only involved in an exact match.
        self.node2.delete_weights('wght', idx1='B', idx2='x')

        mapper.match_records(self.node2, 'right', match_limit=2)

        self.assertEqual(
            self.select_all_helper(mapper, 'right_matches'),
            [(1, 1,  5.0, b'\xc0', 1.0),
             (2, 2, 15.0, b'\xc0', 1.0),
             (3, 3, None, b'\xc0', 1.0),  # <- Weight missing but exact match.
             (4, 4,  5.0, b'\xc0', 1.0)],
        )

    def test_missing_weight_ambiguous_match(self):
        """Ambiguous matches require weight values for all records.
        If one or more matched records is missing a weight, then the
        match must be skipped because there's no way to calculate a
        distribution.
        """
        mapper = Mapper(
            crosswalk_name='population',
            data=[['idx', 'population', 'idx1', 'idx2'],
                  ['A', 70, 'A', 'x'],  # <- exact match
                  ['A', 30, 'A', 'y'],  # <- exact match
                  ['B', 80, 'B', '']],  # <- ambiguous match
        )
        self.node2.delete_weights('wght', idx1='A', idx2='x')  # <- gets matched exactly
        self.node2.delete_weights('wght', idx1='B', idx2='y')  # <- gets matched ambiguously

        mapper.match_records(self.node2, 'right', match_limit=2)

        self.assertEqual(
            self.log_stream.getvalue(),
            ('WARNING: skipped 1 values that ambiguously matched to '
             'one or more records that have no associated weight\n'),
        )

        self.assertEqual(
            self.select_all_helper(mapper, 'right_matches'),
            [(1, 1, None, b'\xc0', 1.0),  # <- Weight missing but exact match.
             (2, 2, 15.0, b'\xc0', 1.0)],
        )

        # Above, the self.mapper_data record `['B', 80, 'B', '']` is not matched
        # to the right-side table because it's ambiguous AND one of the involved
        # index records has no corresponding weight (`B, y`, index_id 4). Also
        # notice that the self.mapper_data record `['A', 70, 'A', 'x']` IS matched
        # because it's an exact match (despite lacking a weight).

    def test_overlapping_not_allowed(self):
        mapper = Mapper(
            crosswalk_name='population',
            data=[['idx', 'population', 'idx1', 'idx2'],
                  ['A', 70, 'A', 'x'],
                  ['A', 40, 'A', 'y'],
                  ['B', 80, 'B', 'x'],
                  ['B', 80, 'B',  '']],  # <- Ambiguous mapping.
        )

        mapper.match_records(self.node2, 'right', match_limit=2)  # <- allow_overlapping defaults to False

        self.assertEqual(
            self.log_stream.getvalue(),
            ('WARNING: omitted 1 ambiguous matches that overlap with records '
             'that were already matched at a finer level of granularity\n'),
        )

        self.assertEqual(
            self.select_all_helper(mapper, 'right_matches'),
            [(1, 1,  5.0, b'\xc0', 1.0),
             (2, 2, 15.0, b'\xc0', 1.0),
             (3, 3,  3.0, b'\xc0', 1.0),
             (4, 4,  5.0, b'\x80', 1.0)],  # <- Only one record (overlap of 3 is omitted)
            msg='should omit the overlap with `B, x` (index_id 3)',
        )

    def test_overlapping_allowed(self):
        mapper = Mapper(
            crosswalk_name='population',
            data=[['idx', 'population', 'idx1', 'idx2'],
                  ['A', 70, 'A', 'x'],
                  ['A', 40, 'A', 'y'],
                  ['B', 80, 'B', 'x'],
                  ['B', 80, 'B',  '']],  # <- Ambiguous mapping.
        )

        mapper.match_records(self.node2, 'right', match_limit=2, allow_overlapping=True)

        self.assertEqual(
            self.log_stream.getvalue(),
            ('INFO: included 1 ambiguous matches that overlap with records '
             'that were also matched at a finer level of granularity\n'),
        )

        self.assertEqual(
            self.select_all_helper(mapper, 'right_matches'),
            [(1, 1,  5.0, b'\xc0', 1.0),
             (2, 2, 15.0, b'\xc0', 1.0),
             (3, 3,  3.0, b'\xc0', 1.0),
             (4, 3,  3.0, b'\x80', 0.375),  # <- Overlaps with exact match `3, 3`.
             (4, 4,  5.0, b'\x80', 0.625)],
            msg='should include the overlap with `B, x` (index_id 3)',
        )

    def test_duplicate_labels_not_always_overlapping(self):
        """If there are multiple records that use the same labels, they
        should be matched normally if they all use the same mapping
        level--they should not count as being overlapped.
        """
        mapper = Mapper(
            crosswalk_name='population',
            data=[['idx', 'population', 'idx1', 'idx2'],
                  ['A', 70, 'A', 'x'],
                  ['A', 40, 'A', 'y'],
                  ['B', 20, 'B', ''],   # <- Duplicate labels using same mapping level.
                  ['B', 40, 'B', '']],  # <- Duplicate labels using same mapping level.
        )

        mapper.match_records(self.node2, 'right', match_limit=2)

        self.assertEqual(
            self.select_all_helper(mapper, 'right_matches'),
            [(1, 1,  5.0, b'\xc0', 1.000),
             (2, 2, 15.0, b'\xc0', 1.000),
             (3, 3,  3.0, b'\x80', 0.375),   # <- Multiple matches.
             (3, 4,  5.0, b'\x80', 0.625),   # <- Multiple matches.
             (4, 3,  3.0, b'\x80', 0.375),   # <- Multiple matches.
             (4, 4,  5.0, b'\x80', 0.625)],  # <- Multiple matches.
        )


class TestGetRelations(TwoNodesBaseTest):
    def test_exact_matches(self):
        mapper = Mapper(
            crosswalk_name='population',
            data=[['idx', 'population', 'idx1', 'idx2'],
                  ['A', 10, 'A', 'x'],
                  ['A', 70, 'A', 'y'],
                  ['B', 20, 'B', 'x'],
                  ['B', 60, 'B', 'y'],
                  ['C', 30, 'C', 'x'],
                  ['C', 50, 'C', 'y']],
        )
        mapper.match_records(self.node1, 'left')
        mapper.match_records(self.node2, 'right')

        relations = mapper.get_relations(direction='->')  # <- Method under test.

        self.assertEqual(list(relations), [(1, 1, b'\xc0', 10.0),
                                           (1, 2, b'\xc0', 70.0),
                                           (2, 3, b'\xc0', 20.0),
                                           (2, 4, b'\xc0', 60.0),
                                           (3, 5, b'\xc0', 30.0),
                                           (3, 6, b'\xc0', 50.0)])

    def test_ambiguous_no_overlaps(self):
        mapper = Mapper(
            crosswalk_name='population',
            data=[['idx', 'population', 'idx1', 'idx2'],
                  ['A', 90, 'A',  ''],   # <- Matched to 2 right-side records.
                  ['B', 20, 'B', 'x'],   # <- Exact match.
                  ['B', 60, 'B', 'y'],   # <- Exact match.
                  ['C', 28, 'C',  ''],   # <- Matched to 1 right-side record (2-ambiguous, minus 1-exact overlap).
                  ['C',  7, 'C', 'y']],  # <- Exact match (overlaps the records matched on "C" alone).
        )
        mapper.match_records(self.node1, 'left')
        mapper.match_records(self.node2, 'right', match_limit=2)

        relations = mapper.get_relations(direction='->')  # <- Method under test.

        expected = [
            (1, 1, b'\x80', 22.5),
            (1, 2, b'\x80', 67.5),
            (2, 3, b'\xc0', 20.0),
            (2, 4, b'\xc0', 60.0),
            (3, 5, b'\x80', 28.0),  # <- Gets full weight, `3, 6` overlap omitted.
            (3, 6, b'\xc0',  7.0),  # <- `3, 6` already matched at finer granularity.
        ]
        self.assertEqual(list(relations), expected)

    def test_ambiguous_with_overlaps(self):
        mapper = Mapper(
            crosswalk_name='population',
            data=[['idx', 'population', 'idx1', 'idx2'],
                  ['A', 90, 'A',  ''],   # <- Matched to 2 right-side records.
                  ['B', 20, 'B', 'x'],   # <- Exact match.
                  ['B', 60, 'B', 'y'],   # <- Exact match.
                  ['C', 28, 'C',  ''],   # <- Matched to 1 right-side record (2-ambiguous, minus 1-exact overlap).
                  ['C',  7, 'C', 'y']],  # <- Exact match (overlaps the records matched on "C" alone).
        )
        mapper.match_records(self.node1, 'left')
        mapper.match_records(self.node2, 'right', match_limit=2, allow_overlapping=True)

        relations = mapper.get_relations(direction='->')  # <- Method under test.

        expected = [
            (1, 1, b'\x80', 22.5),
            (1, 2, b'\x80', 67.5),
            (2, 3, b'\xc0', 20.0),
            (2, 4, b'\xc0', 60.0),
            (3, 5, b'\x80', 10.4),  # <- Gets proportion of weight.
            (3, 6, b'\x80', 17.6),  # <- Gets proportion of weight, overlaps with exact match `3, 6`.
            (3, 6, b'\xc0',  7.0),  # <- Exact match overlapped by ambiguous match.
        ]
        self.assertEqual(list(relations), expected)

    def test_ambiguous_duplicate_mapping_labels(self):
        """Duplicate records should not count as overlaps if they
        share the same mapping level. When preparing a mappings, it's
        convinient to delete values that cannot be precisely matched
        but leave the original rows (rather than summing them before
        loading). Instead, the mapper should accept such values and
        sum them internally.
        """
        mapper = Mapper(
            crosswalk_name='population',
            data=[['idx', 'population', 'idx1', 'idx2'],
                  ['A', 30, 'A', ''],   # <- Duplicate labels using same mapping level.
                  ['A', 50, 'A', ''],   # <- Duplicate labels using same mapping level.
                  ['B', 20, 'B', ''],   # <- Duplicate labels using same mapping level.
                  ['B', 40, 'B', '']],  # <- Duplicate labels using same mapping level.
        )

        mapper.match_records(self.node1, 'left')
        mapper.match_records(self.node2, 'right', match_limit=2)

        relations = mapper.get_relations(direction='->')  # <- Method under test.

        expected = [
            (1, 1, b'\x80', 20.0),
            (1, 2, b'\x80', 60.0),
            (2, 3, b'\x80', 22.5),
            (2, 4, b'\x80', 37.5),
        ]
        self.assertEqual(list(relations), expected)
