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


class TestColumnMethods(unittest.TestCase):
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
            toron.dal1.legacy_delete_columns(node, 'B', 'D')

        self.assertEqual(self.get_cols_helper(node), ('A', 'C'))

    def test_drop_index_columns_all(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B', 'C')

        if node._dal.backend == 'DAL1' and sqlite3.sqlite_version_info < (3, 35, 5):
            self.skipTest('requires SQLite 3.35.5 or newer')

        regex = 'cannot remove all index columns'
        with self.assertRaisesRegex(RuntimeError, regex):
            node.drop_index_columns('A', 'B', 'C')
