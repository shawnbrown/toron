"""Tests for ColumnManager class."""

import sqlite3
import unittest

from toron.dal1.data_connector import DataConnector
from toron.data_models import BaseColumnManager
from toron.dal1.column_manager import (
    ColumnManager,
    verify_foreign_key_check,
    legacy_rename_columns,
    legacy_delete_columns,
)
from toron.node import Node


class TestColumnManager(unittest.TestCase):
    @property
    def concrete_class(self):
        return ColumnManager

    def setUp(self):
        connector = DataConnector()
        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

    def test_inheritance(self):
        """Should subclass from appropriate abstract base class."""
        manager = ColumnManager(self.cursor)
        self.assertTrue(isinstance(manager, BaseColumnManager))

    def assertColumnsEqual(self, table_name, expected_columns, msg=None):
        self.cursor.execute(f"PRAGMA main.table_info('{table_name}')")
        actual_columns = [row[1] for row in self.cursor.fetchall()]
        self.assertEqual(actual_columns, expected_columns, msg=msg)

    def assertRecordsEqual(self, table_name, expected_records, msg=None):
        self.cursor.execute(f"SELECT * FROM {table_name}")
        actual_records = self.cursor.fetchall()
        self.assertEqual(actual_records, expected_records, msg=msg)

    def test_add_columns(self):
        manager = ColumnManager(self.cursor)

        manager.add_columns('foo', 'bar')

        self.assertColumnsEqual('node_index', ['index_id', 'foo', 'bar'])
        self.assertColumnsEqual('location', ['_location_id', 'foo', 'bar'])
        self.assertColumnsEqual('structure', ['_structure_id', '_granularity', 'foo', 'bar'])

    def test_add_columns_special_chars(self):
        manager = ColumnManager(self.cursor)

        manager.add_columns('x "y"')  # <- Check special characters (space and quotes).

        self.assertColumnsEqual('node_index', ['index_id', 'x "y"'])
        self.assertColumnsEqual('location', ['_location_id', 'x "y"'])
        self.assertColumnsEqual('structure', ['_structure_id', '_granularity', 'x "y"'])

    def test_get_columns(self):
        # Only add to node_index for testing.
        self.cursor.execute('ALTER TABLE node_index ADD COLUMN "foo"')
        self.cursor.execute('ALTER TABLE node_index ADD COLUMN "bar"')
        manager = ColumnManager(self.cursor)

        actual = manager.get_columns()

        self.assertEqual(actual, ('foo', 'bar'), msg='should be label columns only, no index_id')

    def test_get_columns_empty(self):
        manager = ColumnManager(self.cursor)

        actual = manager.get_columns()

        self.assertEqual(actual, tuple(), msg='should be empty tuple when no label columns')

    @unittest.skipIf(sqlite3.sqlite_version_info < (3, 25, 0), 'requires 3.25.0 or newer')
    def test_rename_columns(self):
        manager = ColumnManager(self.cursor)
        manager.add_columns('foo', 'bar')
        self.cursor.executescript("""
            INSERT INTO node_index VALUES (NULL, 'a', 'x');
            INSERT INTO node_index VALUES (NULL, 'b', 'y');
            INSERT INTO node_index VALUES (NULL, 'c', 'z');
        """)

        manager.rename_columns({'foo': 'qux', 'bar': 'quux'})

        self.assertColumnsEqual('node_index', ['index_id', 'qux', 'quux'])
        self.assertColumnsEqual('location', ['_location_id', 'qux', 'quux'])
        self.assertColumnsEqual('structure', ['_structure_id', '_granularity', 'qux', 'quux'])
        self.assertRecordsEqual(
            'node_index',
            [(0, '-', '-'), (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'z')],
        )

    @unittest.skipIf(sqlite3.sqlite_version_info < (3, 35, 5), 'requires 3.35.5 or newer')
    def test_delete_columns(self):
        manager = ColumnManager(self.cursor)
        manager.add_columns('foo', 'bar', 'baz', 'qux')
        self.cursor.executescript("""
            INSERT INTO node_index VALUES (NULL, 'a', 'x', '111', 'one');
            INSERT INTO node_index VALUES (NULL, 'b', 'y', '222', 'two');
            INSERT INTO node_index VALUES (NULL, 'c', 'z', '333', 'three');
        """)

        manager.delete_columns('bar')  # <- Delete 1 column.

        self.assertColumnsEqual('node_index', ['index_id', 'foo', 'baz', 'qux'])
        self.assertColumnsEqual('location', ['_location_id', 'foo', 'baz', 'qux'])
        self.assertColumnsEqual('structure', ['_structure_id', '_granularity', 'foo', 'baz', 'qux'])
        self.assertRecordsEqual(
            'node_index',
            [(0, '-', '-', '-'), (1, 'a', '111', 'one'), (2, 'b', '222', 'two'), (3, 'c', '333', 'three')],
        )

        manager.delete_columns('baz', 'qux')  # <- Delete 2 more columns at the same time.

        self.assertColumnsEqual('node_index', ['index_id', 'foo'])
        self.assertColumnsEqual('location', ['_location_id', 'foo'])
        self.assertColumnsEqual('structure', ['_structure_id', '_granularity', 'foo'])
        self.assertRecordsEqual(
            'node_index',
            [(0, '-'), (1, 'a'), (2, 'b'), (3, 'c')],
        )

    @unittest.skipIf(sqlite3.sqlite_version_info < (3, 35, 5), 'requires 3.35.5 or newer')
    def test_delete_columns_all(self):
        manager = ColumnManager(self.cursor)
        manager.add_columns('foo', 'bar')

        regex = 'cannot delete all columns'
        with self.assertRaisesRegex(RuntimeError, regex):
            manager.delete_columns('foo', 'bar')

    def test_delete_columns_legacy_message(self):
        """On SQLite versions older than 3.35.5, should raise error."""
        if sqlite3.sqlite_version_info >= (3, 35, 5):
            return  # <- EXIT!

        manager = ColumnManager(self.cursor)
        manager.add_columns('foo', 'bar')

        regex = 'requires SQLite 3.35.5 or newer'
        with self.assertRaisesRegex(Exception, regex):
            manager.delete_columns('bar')


class TestVerifyForeignKeyCheck(unittest.TestCase):
    def setUp(self):
        connection = sqlite3.connect(':memory:')
        self.addCleanup(connection.close)

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

        self.cursor.executescript("""
            CREATE TABLE foo (
                foo_id INTEGER PRIMARY KEY,
                foo_value TEXT
            );
            CREATE TABLE bar (
                bar_id INTEGER PRIMARY KEY,
                foo_id INTEGER,
                bar_value NUMERIC,
                FOREIGN KEY (foo_id) REFERENCES foo(foo_id)
            );
        """)

    def test_key_references_good(self):
        self.cursor.executescript("""
            INSERT INTO foo (foo_id, foo_value) VALUES (1, 'qux');
            INSERT INTO foo (foo_id, foo_value) VALUES (2, 'quux');

            INSERT INTO bar (foo_id, bar_value) VALUES (1, 5.0);
            INSERT INTO bar (foo_id, bar_value) VALUES (2, 20.0);
            INSERT INTO bar (foo_id, bar_value) VALUES (1, 15.0);
            INSERT INTO bar (foo_id, bar_value) VALUES (2, 25.0);
        """)
        try:
            verify_foreign_key_check(self.cursor)
        except Exception as err:
            self.fail(f'should pass without error, got {err!r}')

    def test_key_violations(self):
        self.cursor.executescript("""
            INSERT INTO foo (foo_id, foo_value) VALUES (1, 'qux');
            INSERT INTO foo (foo_id, foo_value) VALUES (2, 'quux');

            INSERT INTO bar (foo_id, bar_value) VALUES (1, 5.0);
            INSERT INTO bar (foo_id, bar_value) VALUES (2, 20.0);
            INSERT INTO bar (foo_id, bar_value) VALUES (3, 15.0); /* <- key violation */
            INSERT INTO bar (foo_id, bar_value) VALUES (4, 25.0); /* <- key violation */
        """)
        regex = 'unexpected foreign key violations'
        with self.assertRaisesRegex(RuntimeError, regex):
            verify_foreign_key_check(self.cursor)


class TestLegacyUpdateColumns(unittest.TestCase):
    def assertColumnsEqual(self, table_name, expected_columns, msg=None):
        self.cursor.execute(f"PRAGMA main.table_info('{table_name}')")
        actual_columns = [row[1] for row in self.cursor.fetchall()]
        self.assertEqual(actual_columns, expected_columns, msg=msg)

    def assertRecordsEqual(self, table_name, expected_records, msg=None):
        self.cursor.execute(f"SELECT * FROM {table_name}")
        actual_records = self.cursor.fetchall()
        self.assertEqual(actual_records, expected_records, msg=msg)

    def setUp(self):
        self.node = Node()
        connection = self.node._connector.acquire_connection()
        self.addCleanup(lambda: self.node._connector.release_connection(connection))
        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

    def test_rename_columns(self):
        manager = ColumnManager(self.cursor)
        manager.add_columns('foo', 'bar')
        self.cursor.executescript("""
            INSERT INTO node_index VALUES (NULL, 'a', 'x');
            INSERT INTO node_index VALUES (NULL, 'b', 'y');
            INSERT INTO node_index VALUES (NULL, 'c', 'z');
        """)

        legacy_rename_columns(self.node, {'foo': 'qux', 'bar': 'quux'})

        self.assertColumnsEqual('node_index', ['index_id', 'qux', 'quux'])
        self.assertColumnsEqual('location', ['_location_id', 'qux', 'quux'])
        self.assertColumnsEqual('structure', ['_structure_id', '_granularity', 'qux', 'quux'])
        self.assertRecordsEqual(
            'node_index',
            [(0, '-', '-'), (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'z')],
        )

    def test_rename_columns_bad_transaction_state(self):
        manager = ColumnManager(self.cursor)
        manager.add_columns('foo', 'bar')

        self.cursor.execute('BEGIN TRANSACTION')
        self.addCleanup(lambda: self.cursor.execute('ROLLBACK TRANSACTION'))

        regex = 'existing transaction'
        with self.assertRaisesRegex(RuntimeError, regex):
            legacy_rename_columns(self.node, {'foo': 'qux', 'bar': 'quux'})

    def test_legacy_delete_columns(self):
        manager = ColumnManager(self.cursor)
        manager.add_columns('foo', 'bar', 'baz', 'qux')
        self.cursor.executescript("""
            INSERT INTO node_index VALUES (NULL, 'a', 'x', '111', 'one');
            INSERT INTO node_index VALUES (NULL, 'b', 'y', '222', 'two');
            INSERT INTO node_index VALUES (NULL, 'c', 'z', '333', 'three');
        """)

        legacy_delete_columns(self.node, 'bar')  # <- Delete 1 column.

        self.assertColumnsEqual('node_index', ['index_id', 'foo', 'baz', 'qux'])
        self.assertColumnsEqual('location', ['_location_id', 'foo', 'baz', 'qux'])
        self.assertColumnsEqual('structure', ['_structure_id', '_granularity', 'foo', 'baz', 'qux'])
        self.assertRecordsEqual(
            'node_index',
            [(0, '-', '-', '-'), (1, 'a', '111', 'one'), (2, 'b', '222', 'two'), (3, 'c', '333', 'three')],
        )

        legacy_delete_columns(self.node, 'baz', 'qux')  # <- Delete 2 more columns at the same time.

        self.assertColumnsEqual('node_index', ['index_id', 'foo'])
        self.assertColumnsEqual('location', ['_location_id', 'foo'])
        self.assertColumnsEqual('structure', ['_structure_id', '_granularity', 'foo'])
        self.assertRecordsEqual(
            'node_index',
            [(0, '-'), (1, 'a'), (2, 'b'), (3, 'c')],
        )

    def test_legacy_delete_columns_all(self):
        manager = ColumnManager(self.cursor)
        manager.add_columns('foo', 'bar')

        regex = 'cannot delete all columns'
        with self.assertRaisesRegex(RuntimeError, regex):
            legacy_delete_columns(self.node, 'foo', 'bar')
