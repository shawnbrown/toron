"""Tests for toron/node.py module."""

import sqlite3
import sys
import unittest
from contextlib import suppress
from unittest.mock import (
    Mock,
    call,
    sentinel,
)
if sys.version_info >= (3, 8):
    from typing import get_args
else:
    from typing_extensions import get_args

from toron._utils import ToronWarning
from toron.data_models import (
    Index,
    WeightGroup,
)
from toron.node import Node


class TestInstantiation(unittest.TestCase):
    def test_backend_implicit(self):
        """When no arguments are given, should create empty node."""
        node = Node()
        self.assertEqual(node._dal.backend, 'DAL1')

    def test_backend_explicit(self):
        """The ``backend`` can be given explicitly."""
        node = Node(backend='DAL1')
        self.assertEqual(node._dal.backend, 'DAL1')

    def test_backend_keyword_only(self):
        """The ``backend`` argument is keyword-only (not positional)."""
        with self.assertRaises(TypeError):
            node = Node('DAL1')  # Using positional argument.

    def test_backend_unknown(self):
        """Invalid ``backend`` values should raise an error."""
        with self.assertRaises(RuntimeError):
            node = Node(backend=None)

        with self.assertRaises(RuntimeError):
            node = Node(backend='DAL#')

    def test_kwds(self):
        """The ``**kwds`` are used to create a DataConnector."""
        node = Node(cache_to_drive=True)


class TestManagedConnectionCursorAndTransaction(unittest.TestCase):
    def test_managed_connection_type(self):
        """Connection manager should return appropriate type."""
        node = Node()  # Create node and get connection type (generic T1).
        connection_type = get_args(node._dal.DataConnector.__orig_bases__[0])[0]

        with node._managed_connection() as connection:
            pass

        self.assertIsInstance(connection, connection_type)

    def test_managed_connection_calls(self):
        """Connection manager should interact with connection methods."""
        node = Node()
        node._connector = Mock()

        with node._managed_connection() as connection:
            node._connector.assert_has_calls([
                call.acquire_connection(),  # <- Connection acquired.
            ])

        node._connector.assert_has_calls([
            call.acquire_connection(),
            call.release_connection(connection),  # <- Connection released.
        ])

    def test_managed_cursor_type(self):
        """Data cursor manager should return appropriate type."""
        node = Node()  # Create node and get cursor type (generic T2).
        cursor_type = get_args(node._dal.DataConnector.__orig_bases__[0])[1]

        with node._managed_connection() as connection:
            with node._managed_cursor(connection) as cursor:
                pass

        self.assertIsInstance(cursor, cursor_type)

    def test_managed_cursor_calls(self):
        """Cursor manager should interact with cursor methods."""
        node = Node()
        node._connector = Mock()

        # The acquire_connection() mock must return unique objects.
        node._connector.acquire_connection.side_effect = lambda: object()

        with node._managed_connection() as connection:
            with node._managed_cursor(connection) as cursor:
                node._connector.assert_has_calls([
                    call.acquire_cursor(connection),  # <- Cursor acquired.
                ])

            node._connector.assert_has_calls([
                call.acquire_cursor(connection),
                call.release_cursor(cursor),  # <- Cursor released.
            ])

    def test_managed_cursor_calls_implicit_connection(self):
        """Test ``_managed_cursor`` called without ``connection`` argument
        (should automatically create a connection internally).
        """
        node = Node()
        node._connector = Mock()
        dummy_connections = [sentinel.con1, sentinel.con2]
        node._connector.acquire_connection.side_effect = dummy_connections

        with node._managed_cursor() as cursor:  # <- No `connection` passed.
            node._connector.assert_has_calls([
                call.acquire_connection(),  # <- Connection acquired automatically.
                call.acquire_cursor(sentinel.con1),  # <- Cursor acquired.
            ])

        node._connector.assert_has_calls([
            call.release_cursor(cursor),  # <- Cursor released.
            call.release_connection(sentinel.con1),  # <- Connection released.
        ])

    def test_managed_transaction(self):
        """Should commit changes when no errors occur."""
        node = Node()
        node._connector = Mock()
        node._connector.acquire_connection.return_value = sentinel.con
        node._connector.acquire_cursor.return_value = sentinel.cur

        with node._managed_connection() as connection:
            with node._managed_cursor(connection) as cursor:
                with node._managed_transaction(cursor) as cursor:
                    node._connector.assert_has_calls([
                        call.acquire_connection(),
                        call.acquire_cursor(sentinel.con),
                        call.transaction_begin(sentinel.cur),  # <- BEGIN
                    ])

        node._connector.assert_has_calls([
            call.transaction_commit(sentinel.cur),  # <- COMMIT
            call.release_cursor(sentinel.cur),
            call.release_connection(sentinel.con),
        ])

    def test_managed_transaction_rollback(self):
        """Should roll-back changes when an error occurs."""
        node = Node()
        node._connector = Mock()
        node._connector.acquire_connection.return_value = sentinel.con
        node._connector.acquire_cursor.return_value = sentinel.cur

        with suppress(RuntimeError):
            with node._managed_connection() as connection:
                with node._managed_cursor(connection) as cursor:
                    with node._managed_transaction(cursor) as cursor:
                        raise RuntimeError  # <- Error inside the transaction.

        node._connector.assert_has_calls([
            call.acquire_connection(),
            call.acquire_cursor(sentinel.con),
            call.transaction_begin(sentinel.cur),
            call.transaction_rollback(sentinel.cur),  # <- ROLLBACK
            call.release_cursor(sentinel.cur),
            call.release_connection(sentinel.con),
        ])

    def test_managed_transaction_implicit_resources(self):
        """When called without args, should auto-acquire resources."""
        node = Node()
        node._connector = Mock()
        node._connector.acquire_connection.return_value = sentinel.con
        node._connector.acquire_cursor.return_value = sentinel.cur

        with node._managed_transaction() as cursor:
            pass

        node._connector.assert_has_calls([
            call.acquire_connection(),
            call.acquire_cursor(sentinel.con),
            call.transaction_begin(sentinel.cur),
            call.transaction_commit(sentinel.cur),  # <- COMMIT
            call.release_cursor(sentinel.cur),
            call.release_connection(sentinel.con),
        ])

    def test_managed_transaction_implicit_resources(self):
        node = Node()
        node._connector = Mock()
        node._connector.acquire_connection.return_value = sentinel.con
        node._connector.acquire_cursor.return_value = sentinel.cur

        with suppress(RuntimeError):
            with node._managed_transaction() as cursor:
                raise RuntimeError  # <- Error inside the transaction.

        node._connector.assert_has_calls([
            call.acquire_connection(),
            call.acquire_cursor(sentinel.con),
            call.transaction_begin(sentinel.cur),
            call.transaction_rollback(sentinel.cur),  # <- ROLLBACK
            call.release_cursor(sentinel.cur),
            call.release_connection(sentinel.con),
        ])


class TestIndexColumnMethods(unittest.TestCase):
    @staticmethod
    def get_cols_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            return node._dal.ColumnManager(cursor).get_columns()

    @staticmethod
    def add_cols_helper(node, *columns):  # <- Helper function.
        with node._managed_cursor() as cursor:
            manager = node._dal.ColumnManager(cursor)
            manager.add_columns(*columns)

    def test_add_index_columns(self):
        node = Node()

        node.add_index_columns('A', 'B')

        self.assertEqual(self.get_cols_helper(node), ('A', 'B'))

    def test_add_index_columns_atomic(self):
        """Adding columns should be an atomic operation (either all
        columns should be added or none should be added).
        """
        node = Node()

        with suppress(Exception):
            # Second 'baz' causes an error (cannot have duplicate names).
            node.add_index_columns('foo', 'bar', 'baz', 'baz')

        msg = 'should be empty tuple, no column names'
        self.assertEqual(self.get_cols_helper(node), (), msg=msg)

    def test_index_columns_property(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B')

        columns = node.index_columns  # Accessed as property attribute.

        self.assertEqual(columns, ('A', 'B'))

    def test_rename_index_columns(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B', 'C', 'D')

        if sqlite3.sqlite_version_info >= (3, 25, 0) or node._dal.backend != 'DAL1':
            node.rename_index_columns({'B': 'G', 'D': 'T'})
        else:
            import toron.dal1
            toron.dal1.legacy_rename_columns(node, {'B': 'G', 'D': 'T'})

        self.assertEqual(self.get_cols_helper(node), ('A', 'G', 'C', 'T'))

    def test_drop_index_columns(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B', 'C', 'D')

        if sqlite3.sqlite_version_info >= (3, 35, 5) or node._dal.backend != 'DAL1':
            node.drop_index_columns('B', 'D')
        else:
            import toron.dal1
            toron.dal1.legacy_drop_columns(node, 'B', 'D')

        self.assertEqual(self.get_cols_helper(node), ('A', 'C'))

    def test_drop_index_columns_all(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B', 'C')

        if node._dal.backend == 'DAL1' and sqlite3.sqlite_version_info < (3, 35, 5):
            self.skipTest('requires SQLite 3.35.5 or newer')

        regex = 'cannot remove all index columns'
        with self.assertRaisesRegex(RuntimeError, regex):
            node.drop_index_columns('A', 'B', 'C')


class TestIndexMethods(unittest.TestCase):
    @staticmethod
    def add_cols_helper(node, *columns):  # <- Helper function.
        with node._managed_cursor() as cursor:
            manager = node._dal.ColumnManager(cursor)
            manager.add_columns(*columns)

    @staticmethod
    def add_index_helper(node, data):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.IndexRepository(cursor)
            for row in data:
                repository.add(*row)

    @staticmethod
    def get_index_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.IndexRepository(cursor)
            return list(repository.get_all())

    def test_insert(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B')

        data = [('A', 'B'), ('foo', 'x'), ('bar', 'y')]
        node.insert_index(data)

        expected = [
            Index(0, '-', '-'),
            Index(1, 'foo', 'x'),
            Index(2, 'bar', 'y'),
        ]
        self.assertEqual(self.get_index_helper(node), expected)

    def test_insert_different_order(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B')

        data = [('B', 'A'), ('x', 'foo'), ('y', 'bar')]  # <- Different order.
        node.insert_index(data)

        expected = [
            Index(0, '-', '-'),
            Index(1, 'foo', 'x'),
            Index(2, 'bar', 'y'),
        ]
        self.assertEqual(self.get_index_helper(node), expected)

    def test_insert_invalid_columns(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B', 'C', 'D')

        regex = r"missing required columns: 'C', 'D'"
        with self.assertRaisesRegex(ValueError, regex):
            node.insert_index([('A', 'B'), ('foo', 'x'), ('bar', 'y')])

    def test_insert_duplicate_or_empty_strings(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B')

        data = [
            ('A', 'B'),
            ('foo', 'x'),
            ('foo', 'x'),  # <- Duplicate of previous record.
            ('bar', ''),   # <- Contains empty string.
            ('bar', 'y'),
            ('baz', 'z'),
        ]

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            node.insert_index(data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            'skipped 2 rows with duplicate labels or empty strings, loaded 3 rows',
        )

        # Check the loaded data.
        expected = [
            Index(0, '-', '-'),
            Index(1, 'foo', 'x'),
            Index(2, 'bar', 'y'),
            Index(3, 'baz', 'z'),
        ]
        self.assertEqual(self.get_index_helper(node), expected)

    def test_select(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B')
        data = [('foo', 'x'), ('foo', 'y'), ('bar', 'x'), ('bar', 'y')]
        self.add_index_helper(node, data)

        self.assertEqual(
            list(node.select_index(A='foo')),  # <- Filter on one column.
            [(1, 'foo', 'x'), (2, 'foo', 'y')],
        )

        self.assertEqual(
            list(node.select_index(header=True, B='x')),  # <- Include header.
            [('index_id', 'A', 'B'), (1, 'foo', 'x'), (3, 'bar', 'x')],
        )

        self.assertEqual(
            list(node.select_index(A='bar', B='x')),  # <- Filter on multiple columns.
            [(3, 'bar', 'x')],
        )

        self.assertEqual(
            list(node.select_index(index_id=4, A='bar')),  # <- Criteria includes `index_id`.
            [(4, 'bar', 'y')],
        )

        self.assertEqual(
            list(node.select_index(A='baz')),  # <- No matching value 'baz'.
            [],
        )

        self.assertEqual(
            list(node.select_index()),  # <- No criteria (returns all).
            [(0, '-', '-'),
             (1, 'foo', 'x'),
             (2, 'foo', 'y'),
             (3, 'bar', 'x'),
             (4, 'bar', 'y')],
        )


class TestNodeUpdateIndex(unittest.TestCase):
    @staticmethod
    def get_index_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.IndexRepository(cursor)
            return list(repository.get_all())

    @staticmethod
    def get_weight_helper(node):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM weight')
            return cursor.fetchall()

    @staticmethod
    def get_relation_helper(node):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM relation')
            return cursor.fetchall()

    def setUp(self):
        node = Node()
        with node._managed_cursor() as cursor:
            manager = node._dal.ColumnManager(cursor)
            manager.add_columns('A', 'B')

            repository = node._dal.IndexRepository(cursor)
            repository.add('foo', 'x')
            repository.add('bar', 'y')

            weight_group_repo = node._dal.WeightGroupRepository(cursor)
            weight_group_repo.add('group1')  # Adds weight_group_id 1.
            weight_repo = node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 175000)
            weight_repo.add(1, 2,  25000)

            crosswalk_repo = node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add('111-11-1111', None, 'other1')  # Adds crosswalk_id 1.
            relation_repo = node._dal.RelationRepository(cursor)
            relation_repo.add(1, 1, 1, 16350, 0.75, None)
            relation_repo.add(1, 1, 2, 5450,  0.25, None)
            relation_repo.add(1, 2, 2, 13050, 1.00, None)

        self.node = node

    def test_update_all_values(self):
        data = [('index_id', 'A', 'B'), (1, 'baz', 'z')]  # <- Updating columns A & B.
        self.node.update_index(data)
        expected = [Index(0, '-', '-'), Index(1, 'baz', 'z'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_update_different_order(self):
        """Columns should be matched on name, not positional order."""
        data = [('index_id', 'B', 'A'), (1, 'z', 'baz')]  # <- Different order (B then A)
        self.node.update_index(data)
        expected = [Index(0, '-', '-'), Index(1, 'baz', 'z'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_update_partial_values(self):
        """Update requires all label columns, raise error if missing."""
        data = [('index_id', 'B'), (2, 'xyz')]  # <- Missing column A.

        regex = "missing required columns: 'A'"
        with self.assertRaisesRegex(ValueError, regex):
            self.node.update_index(data)

        # Check values (unchanged).
        expected = [Index(0, '-', '-'), Index(1, 'foo', 'x'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_update_ignore_extra_cols(self):
        """When given extra columns, they are ignored when loading."""
        data = [('index_id', 'A', 'B', 'C'), (1, 'baz', 'z', 'zzz')]  # <- Column C not in index.
        self.node.update_index(data)
        expected = [Index(0, '-', '-'), Index(1, 'baz', 'z'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_update_non_matching_id(self):
        """If index_id did not exist, raise warning and skip to next.

        .. note::
            If the index_id *did* exist at the beginning of the update
            but it was merged by a previous row, then an exception
            should be raised and the transaction should be rolled back
            (this is checked later by the test case
            `test_merge_resulting_in_missing_index_id()`).
        """
        data = [
            ('index_id', 'A', 'B'),
            (4, 'baz', 'z'),  # <- No index_id 4!
            (2, 'bar', 'YYY'),
        ]

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.update_index(data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            'skipped 1 rows with non-matching index_id values, updated 1 rows',
        )

        # Check values (index 2 should be updated).
        expected = [Index(0, '-', '-'), Index(1, 'foo', 'x'), Index(2, 'bar', 'YYY')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_empty_string(self):
        """Should raise warning and skip to next for empty strings."""
        data = [
            ('index_id', 'A', 'B'),
            (1, 'bar', ''),  # <- Has empty string.
            (2, 'bar', 'YYY'),
        ]

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.update_index(data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            'skipped 1 rows with empty string values, updated 1 rows',
        )

        # Check values (index_id 2 updated).
        expected = [Index(0, '-', '-'), Index(1, 'foo', 'x'), Index(2, 'bar', 'YYY')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_no_index_id_column(self):
        """Must have 'index_id' to identify records when updating."""
        data = [('A', 'B'), ('baz', 'z')]  # <- No 'index_id' column.

        regex = "column 'index_id' required to update records"
        with self.assertRaisesRegex(ValueError, regex):
            self.node.update_index(data)

    def test_update_resulting_in_duplicate(self):
        """If updated labels are not unique, should merge records."""
        # The following data updates the labels of index_id 1 to `bar, y`.
        # But these are the same labels used for index_id 2. Because index
        # labels must be unique, this update should merge index_id 1 and
        # 2 into the same record. The final record should used the index_id
        # of the record being updated (in this case, 1).
        data = [('index_id', 'A', 'B'), (1, 'bar', 'y')]

        # Check that merge does not happen implicitly.
        with self.assertRaises(ValueError):
            self.node.update_index(data)  # <- Update creates duplicate labels.

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.update_index(data, merge_on_conflict=True)  # <- Update causes records to merge.

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            'merged 1 existing records with duplicate label values, updated 1 rows',
        )

        msg = 'Record index_id 2 should be merged with index_id 1.'
        expected = [Index(0, '-', '-'), Index(1, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected, msg=msg)

        msg = 'Weight records and values should be merged into one record.'
        expected = [(1, 1, 1, 200000.0)]
        self.assertEqual(self.get_weight_helper(self.node), expected, msg=msg)

        msg = 'Three relations merged into two, remaining relations have index_id 1.'
        expected = [(1, 1, 1, 1, 21800.0, 1.0, None), (2, 1, 2, 1, 13050.0, 1.0, None)]
        self.assertEqual(self.get_relation_helper(self.node), expected, msg=msg)

    def test_merge_resulting_in_missing_index_id(self):
        """Should raise error if attempting to update a record that was merged."""
        # When applying the first update, index_id 1 gets the same labels
        # as index_id 2 (bar, y) which triggers a merge of these two records.
        # Then when applying the second update, there is no index_id 2 (it
        # was just merged with 1). If a merge removes records that the user
        # was attempting to update, the results can be very confusing. Users
        # would be right to wonder why `baz, z` is not included in the newly
        # updated index. To prevent this confusing situation, attempting to
        # update a record that was previously merged should raise an exception
        # and any changes should be rolled-back.
        data = [('index_id', 'A', 'B'), (1, 'bar', 'y'), (2, 'baz', 'z')]

        regex = 'cannot update index_id 2, it was merged with another record on a previous row'
        with self.assertRaisesRegex(ValueError, regex):
            self.node.update_index(data, merge_on_conflict=True)

        # Check values (unchanged).
        expected = [Index(0, '-', '-'), Index(1, 'foo', 'x'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)


class TestNodeDeleteIndex(unittest.TestCase):
    @staticmethod
    def add_cols_helper(node, *columns):  # <- Helper function.
        with node._managed_cursor() as cursor:
            manager = node._dal.ColumnManager(cursor)
            manager.add_columns(*columns)

    @staticmethod
    def add_index_helper(node, data):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.IndexRepository(cursor)
            for row in data:
                repository.add(*row)

    @staticmethod
    def get_weight_helper(node):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM weight')
            return cursor.fetchall()

    @staticmethod
    def get_relation_helper(node):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM relation')
            return cursor.fetchall()

    @staticmethod
    def get_index_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.IndexRepository(cursor)
            return list(repository.get_all())

    def setUp(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B')
        self.add_index_helper(node, [('foo', 'x'), ('bar', 'y')])
        self.node = node

    def test_delete_index_only(self):
        data = [
            ('index_id', 'A', 'B'),
            (1, 'foo', 'x'),
            (2, 'bar', 'y'),
        ]
        self.node.delete_index(data)

        expected = [Index(0, '-', '-')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_delete_with_warning(self):
        data = [
            ('index_id', 'A', 'B'),
            (42, 'qux', 'a'),  # <- Id 42 does not exist in index.
            (1, 'foo', 'x'),
            (2, 'bar', 'zzz'),  # <- Labels don't match index record.
        ]

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.delete_index(data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            ('skipped 1 rows with non-matching index_id values, '
             'skipped 1 rows with mismatched labels, deleted 1 rows'),
        )

        # Check values (index_id 1 deleted).
        expected = [Index(0, '-', '-'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_delete_with_weights(self):
        """Should delete weight records associated with index_id."""
        with self.node._managed_cursor() as cursor:
            weight_group_repo = self.node._dal.WeightGroupRepository(cursor)
            weight_group_repo.add('group1')  # Adds weight_group_id 1.
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 175000)
            weight_repo.add(1, 2,  25000)

        data = [
            ('index_id', 'A', 'B'),
            (2, 'bar', 'y'),
        ]
        self.node.delete_index(data)

        expected = [Index(0, '-', '-'), Index(1, 'foo', 'x')]
        self.assertEqual(self.get_index_helper(self.node), expected)

        expected = [(1, 1, 1, 175000.0)]
        self.assertEqual(self.get_weight_helper(self.node), expected)

    def test_delete_with_relations(self):
        """Should delete relation records associated with index_id."""
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add('111-11-1111', None, 'other1')  # Adds crosswalk_id 1.
            relation_repo = self.node._dal.RelationRepository(cursor)
            relation_repo.add(1, 1, 1, 16350, 0.75, None)
            relation_repo.add(1, 1, 2, 5450,  0.25, None)
            relation_repo.add(1, 2, 2, 13050, 1.00, None)

        data = [
            ('index_id', 'A', 'B'),
            (2, 'bar', 'y'),
        ]
        self.node.delete_index(data)

        expected = [Index(0, '-', '-'), Index(1, 'foo', 'x')]
        self.assertEqual(self.get_index_helper(self.node), expected)

        expected = [(1, 1, 1, 1, 16350.0, 1.0, None)]  # <- Proportion is updated, too (was 0.75).
        self.assertEqual(self.get_relation_helper(self.node), expected)

    def test_delete_with_ambiguous_relations(self):
        """Should not delete records linked to ambiguous relations.

        NOTE: Ideally, this limitation should be removed in the future
        by re-mapping relations using their associated labels.
        """
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add('111-11-1111', None, 'other1')  # Adds crosswalk_id 1.
            relation_repo = self.node._dal.RelationRepository(cursor)
            relation_repo.add(1, 1, 1, 16350, 0.75, b'\x80')  # <- Ambiguous relations.
            relation_repo.add(1, 1, 2, 5450,  0.25, b'\x80')  # <- Ambiguous relations.
            relation_repo.add(1, 2, 2, 13050, 1.00, None)

        data = [('index_id', 'A', 'B'), (2, 'bar', 'y')]

        regex = 'associated crosswalk relations are ambiguous'
        with self.assertRaisesRegex(ValueError, regex):
            self.node.delete_index(data)

    def test_delete_using_interoperation(self):
        # Add more index rows so there are multiple records to select.
        self.add_index_helper(self.node, [('foo', 'qux'), ('foo', 'quux')])

        self.node.delete_index(A='foo')

        expected = [Index(0, '-', '-'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)


class TestNodeWeightGroupMethods(unittest.TestCase):
    @staticmethod
    def get_weight_group_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.WeightGroupRepository(cursor)
            return list(repository.get_all())

    @staticmethod
    def get_weight_helper(node):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM weight')
            return cursor.fetchall()

    def test_weight_groups_property(self):
        """The `node.weight_groups` property should be list of groups
        ordered by name.
        """
        node = Node()
        with node._managed_cursor() as cursor:
            weight_group_repo = node._dal.WeightGroupRepository(cursor)
            weight_group_repo.add('name_b')
            weight_group_repo.add('name_a', 'Group A', ['"[foo]"'], is_complete=True)
            weight_group_repo.add('name_c', 'Group C')

        expected = [
            WeightGroup(
                id=2,
                name='name_a',
                description='Group A',
                selectors=['"[foo]"'],
                is_complete=1,
            ),
            WeightGroup(
                id=1,
                name='name_b',
                description=None,
                selectors=None,
                is_complete=0,
            ),
            WeightGroup(
                id=3,
                name='name_c',
                description='Group C',
                selectors=None,
                is_complete=0,
            ),
        ]
        self.assertEqual(node.weight_groups, expected)

    def test_get_weight_group(self):
        node = Node()
        with node._managed_cursor() as cursor:
            weight_group_repo = node._dal.WeightGroupRepository(cursor)
            weight_group_repo.add('name_a', 'Group A')
            weight_group_repo.add('name_b', 'Group B')

        actual = node.get_weight_group('name_a')
        expected = WeightGroup(id=1, name='name_a', description='Group A', selectors=None)
        self.assertEqual(actual, expected)

        self.assertIsNone(node.get_weight_group('name_zzz'))

    def test_add_weight_group(self):
        node = Node()

        node.add_weight_group('name_a')  # <- Only `name` is required.
        node.add_weight_group(  # <- Defining all properties.
            name='name_b',
            description='Group B',
            selectors=['"[foo]"'],
            is_complete=True
        )

        expected = [
            WeightGroup(
                id=1,
                name='name_a',
                description=None,
                selectors=None,
                is_complete=False,
            ),
            WeightGroup(
                id=2,
                name='name_b',
                description='Group B',
                selectors=['"[foo]"'],
                is_complete=True,
            ),
        ]
        self.assertEqual(self.get_weight_group_helper(node), expected)

    def test_edit_weight_group(self):
        node = Node()
        with node._managed_cursor() as cursor:
            weight_group_repo = node._dal.WeightGroupRepository(cursor)
            weight_group_repo.add(
                'name_a',
                'Group A',
                ['"[foo]"'],
                is_complete=False,
            )

        node.edit_weight_group('name_a', name='NameA', is_complete=True)
        expected = [
            WeightGroup(
                id=1,
                name='NameA',  # <- Value changed.
                description='Group A',
                selectors=['"[foo]"'],
                is_complete=True,  # <- Value changed.
            ),
        ]
        self.assertEqual(self.get_weight_group_helper(node), expected)

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            node.edit_weight_group('name_x', description='Description of X.')

        self.assertEqual(str(cm.warning), "no weight group named 'name_x'")

    def test_drop_weight_group(self):
        node = Node()
        with node._managed_cursor() as cursor:
            manager = node._dal.ColumnManager(cursor)
            index_repo = node._dal.IndexRepository(cursor)
            weight_group_repo = node._dal.WeightGroupRepository(cursor)
            weight_repo = node._dal.WeightRepository(cursor)

            # Add index columns and records.
            manager.add_columns('A', 'B')
            index_repo.add('foo', 'x')
            index_repo.add('bar', 'y')
            index_repo.add('baz', 'z')

            # Add weight group and associated weights.
            weight_group_repo.add('name_a')
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        node.drop_weight_group('name_a')

        msg = 'weight group and associated weights should be deleted'
        self.assertEqual(self.get_weight_group_helper(node), [], msg=msg)
        self.assertEqual(self.get_weight_helper(node), [], msg=msg)

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            node.drop_weight_group('name_x')

        self.assertEqual(str(cm.warning), "no weight group named 'name_x'")


class TestNodeWeightMethods(unittest.TestCase):
    def setUp(self):
        node = Node()
        with node._managed_cursor() as cursor:
            col_manager = node._dal.ColumnManager(cursor)
            index_repo = node._dal.IndexRepository(cursor)
            weight_group_repo = node._dal.WeightGroupRepository(cursor)

            # Add index columns and records.
            col_manager.add_columns('A', 'B')
            index_repo.add('foo', 'x')
            index_repo.add('bar', 'y')
            index_repo.add('bar', 'z')

            # Add weight_group_id 1.
            weight_group_repo.add('weight1')

        self.node = node

    def get_weights_helper(self):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with self.node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM weight')
            return cursor.fetchall()

    def test_select(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        weights = self.node.select_weights('weight1', header=True)
        expected = [
            ('index_id', 'A', 'B', 'weight1'),
            (1, 'foo', 'x', 10.0),
            (2, 'bar', 'y', 25.0),
            (3, 'bar', 'z', 15.0),
        ]
        self.assertEqual(list(weights), expected)

        # Test with selection `header=False` and `A='bar'`.
        weights = self.node.select_weights('weight1', header=False, A='bar')
        expected = [
            (2, 'bar', 'y', 25.0),
            (3, 'bar', 'z', 15.0),
        ]
        self.assertEqual(list(weights), expected)

        # Test with selection `header=True` and `A='NOMATCH'`.
        weights = self.node.select_weights('weight1', header=True, A='NOMATCH')
        expected = [('index_id', 'A', 'B', 'weight1')]
        msg = 'header row only, when there are no matches'
        self.assertEqual(list(weights), expected, msg=msg)

        # Test with selection `header=False` and `A='NOMATCH'`.
        weights = self.node.select_weights('weight1', header=False, A='NOMATCH')
        self.assertEqual(list(weights), [], msg='iterator should be empty')

    def test_insert_by_label(self):
        data = [
            ('A', 'B', 'weight1'),
            ('foo', 'x', 10.0),
            ('bar', 'y', 25.0),
            ('bar', 'z', 15.0),
        ]
        self.node.insert_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_by_label_different_order(self):
        data = [
            ('B', 'A', 'weight1'),
            ('x', 'foo', 10.0),
            ('y', 'bar', 25.0),
            ('z', 'bar', 15.0),
        ]
        self.node.insert_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_by_label_extra_columns(self):
        data = [
            ('A', 'B', 'C', 'weight1'),
            ('foo', 'x', 'a', 10.0),
            ('bar', 'y', 'b', 25.0),
            ('bar', 'z', 'c', 15.0),
        ]
        self.node.insert_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_by_index_and_label(self):
        data = [
            ('index_id', 'A', 'B', 'weight1'),
            (1, 'foo', 'x', 10.0),
            (2, 'bar', 'y', 25.0),
            (3, 'bar', 'z', 15.0),
        ]
        self.node.insert_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_by_index_and_label_extra_columns(self):
        data = [
            ('index_id', 'A', 'B', 'C', 'weight1'),
            (1, 'foo', 'x', 'a', 10.0),
            (2, 'bar', 'y', 'b', 25.0),
            (3, 'bar', 'z', 'c', 15.0),
        ]
        self.node.insert_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_by_index_and_label_different_order(self):
        data = [
            ('B', 'weight1', 'A', 'index_id'),
            ('x', 10.0, 'foo', 1),
            ('y', 25.0, 'bar', 2),
            ('z', 15.0, 'bar', 3),
        ]
        self.node.insert_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_warnings_with_index_id(self):
        data = [
            ('index_id', 'A', 'B', 'weight1'),
            (9, 'foo', 'x', 10.0),    # <- No matching index.
            (2, 'bar', 'YYY', 25.0),  # <- Mismatched labels.
            (3, 'bar', 'z', 15.0),    # <- OK (gets inserted)
        ]

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.insert_weights('weight1', data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            ('skipped 1 rows with non-matching index_id values, '
             'skipped 1 rows with mismatched labels, '
             'loaded 1 rows'),
        )

        # Check inserted records (only one).
        self.assertEqual(self.get_weights_helper(), [(1, 1, 3, 15.0)])

    def test_insert_warnings_not_index_id(self):
        data = [
            ('A', 'B', 'weight1'),
            ('foo', 'XXX', 10.0),  # <- No matching labels.
            ('bar', 'YYY', 25.0),  # <- No matching labels.
            ('bar', 'z', 15.0),    # <- OK (gets inserted)
        ]

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.insert_weights('weight1', data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            ('skipped 2 rows with labels that match no index, '
             'loaded 1 rows'),
        )

        # Check inserted records (only one).
        self.assertEqual(self.get_weights_helper(), [(1, 1, 3, 15.0)])

    def test_update(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        data = [
            ('index_id', 'A', 'B','weight1'),
            (2, 'bar', 'y', 555.0),
        ]
        self.node.update_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 555.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_update_different_order(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        data = [
            ('B', 'index_id', 'A', 'weight1'),
            ('y', 2, 'bar', 555.0),
        ]
        self.node.update_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 555.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_update_different_order_add_new(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)

        data = [
            ('B', 'index_id', 'A', 'weight1'),
            ('x', 1, 'foo', 111.0),
            ('y', 2, 'bar', 222.0),
            ('z', 3, 'bar', 333.0),  # <- Does not previously exist.
        ]
        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.update_weights('weight1', data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            ('inserted 1 rows that did not previously exist, '
             'updated 2 rows'),
        )

        expected = [
            (1, 1, 1, 111.0),  # <- Updated.
            (2, 1, 2, 222.0),  # <- Updated.
            (3, 1, 3, 333.0),  # <- Inserted (new record).
        ]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_update_missing_and_mismatched(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        data = [
            ('index_id', 'A', 'B','weight1'),
            (2, 'bar', 'YYY', 444.0),  # <- Mismatch.
            (9, 'bar', 'z', 555.0),    # <- No index_id 9.
        ]

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.update_weights('weight1', data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            ('skipped 1 rows with non-matching index_id values, '
             'skipped 1 rows with mismatched labels, '
             'updated 0 rows'),
        )

        # Check that values are unchanged.
        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_delete(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        data = [
            ('index_id', 'A', 'B'),
            (1, 'foo', 'x'),
            (2, 'bar', 'y'),
        ]
        self.node.delete_weights('weight1', data)
        expected = [(3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

        # Test with weight column (can be present but is ignored).
        data = [
            ('index_id', 'A', 'B', 'weight1'),
            (3, 'bar', 'z', 15.0),
        ]
        self.node.delete_weights('weight1', data)
        self.assertEqual(self.get_weights_helper(), [])

    def test_delete_warnings(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)

        data = [
            ('index_id', 'A', 'B'),
            (7, 'foo', 'x'),    # <- No index match.
            (2, 'bar', 'YYY'),  # <- Label mismatch.
            (3, 'bar', 'z'),    # <- No matching weight.
        ]
        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.delete_weights('weight1', data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            ('skipped 1 rows with non-matching index_id values, '
             'skipped 1 rows with mismatched labels, '
             'skipped 1 rows with no matching weights, '
             'deleted 0 rows'),
        )

        # Check weights (unchanged--only two weights were added).
        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_delete_criteria(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        # Test single criteria (matches 2 rows).
        self.node.delete_weights('weight1', A='bar')
        expected = [(1, 1, 1, 10.0)]
        self.assertEqual(self.get_weights_helper(), expected)

        # Test multiple criteria (matches 1 row).
        self.node.delete_weights('weight1', A='foo', B='x')
        self.assertEqual(self.get_weights_helper(), [])
