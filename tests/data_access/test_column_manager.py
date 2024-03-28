"""Tests for toron/_data_access/column_manager.py module."""

import sqlite3
import unittest
from abc import ABC, abstractmethod
from types import SimpleNamespace

from toron._data_access.data_connector import DataConnector
from toron._data_access.base_classes import BaseColumnManager
from toron._data_access.column_manager import (
    verify_foreign_key_check,
    ColumnManager,
)


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


class Bases(SimpleNamespace):
    """Wrapping TestCase base classes to prevent test discovery."""

    class TestColumnManager(ABC, unittest.TestCase):
        @property
        @abstractmethod
        def concrete_class(self):
            """The concrete class to be tested."""
            return NotImplemented

        def setUp(self):
            connector = DataConnector()
            resource = connector.acquire_resource()
            self.addCleanup(lambda: connector.release_resource(resource))

            self.cursor = resource.cursor()

        def test_inheritance(self):
            """Should subclass from appropriate abstract base class."""
            self.assertTrue(issubclass(self.concrete_class, BaseColumnManager))

        @abstractmethod
        def test_add_columns(self):
            ...

        @abstractmethod
        def test_get_columns(self):
            ...

        @abstractmethod
        def test_update_columns(self):
            ...

        @abstractmethod
        def test_delete_columns(self):
            ...


class TestColumnManager(Bases.TestColumnManager):
    @property
    def concrete_class(self):
        return ColumnManager

    def get_columns(self, table_name):
        self.cursor.execute(f"PRAGMA main.table_info('{table_name}')")
        return [row[1] for row in self.cursor.fetchall()]

    def test_add_columns(self):
        manager = ColumnManager(self.cursor)

        msg = 'before adding label columns, should only contain functional columns'
        self.assertEqual(self.get_columns('node_index'), ['index_id'], msg=msg)
        self.assertEqual(self.get_columns('location'), ['_location_id'], msg=msg)
        self.assertEqual(self.get_columns('structure'), ['_structure_id', '_granularity'], msg=msg)

        manager.add_columns('foo', 'bar')
        manager.add_columns('x "y"')  # <- Check special characters (space and quotes).

        self.assertEqual(self.get_columns('node_index'), ['index_id', 'foo', 'bar', 'x "y"'])
        self.assertEqual(self.get_columns('location'), ['_location_id', 'foo', 'bar', 'x "y"'])
        self.assertEqual(self.get_columns('structure'), ['_structure_id', '_granularity', 'foo', 'bar', 'x "y"'])

    def test_get_columns(self):
        manager = ColumnManager(self.cursor)

        actual = manager.get_columns()
        self.assertEqual(actual, tuple(), msg='should be empty tuple when no label columns')

        # Only add to node_index for testing.
        self.cursor.executescript("""
            ALTER TABLE node_index ADD COLUMN "foo";
            ALTER TABLE node_index ADD COLUMN "bar";
        """)

        actual = manager.get_columns()
        self.assertEqual(actual, ('foo', 'bar'), msg='should be label columns only, no index_id')

    def test_update_columns(self):
        manager = ColumnManager(self.cursor)
        manager.add_columns('foo', 'bar')
        self.cursor.executescript("""
            INSERT INTO node_index VALUES (NULL, 'a', 'x');
            INSERT INTO node_index VALUES (NULL, 'b', 'y');
            INSERT INTO node_index VALUES (NULL, 'c', 'z');
        """)

        manager.update_columns({'foo': 'qux', 'bar': 'quux'})

        self.assertEqual(self.get_columns('node_index'), ['index_id', 'qux', 'quux'])
        self.assertEqual(self.get_columns('location'), ['_location_id', 'qux', 'quux'])
        self.assertEqual(self.get_columns('structure'), ['_structure_id', '_granularity', 'qux', 'quux'])

        self.cursor.execute('SELECT * FROM node_index')
        self.assertEqual(
            self.cursor.fetchall(),
            [(0, '-', '-'), (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'z')],
        )

    def test_update_columns_bad_transaction_state(self):
        manager = ColumnManager(self.cursor)
        manager.add_columns('foo', 'bar')

        self.cursor.execute('BEGIN TRANSACTION')
        self.addCleanup(lambda: self.cursor.execute('ROLLBACK TRANSACTION'))

        regex = 'existing transaction'
        with self.assertRaisesRegex(RuntimeError, regex):
            manager.update_columns({'foo': 'qux', 'bar': 'quux'})

    def test_delete_columns(self):
        manager = ColumnManager(self.cursor)
        manager.add_columns('foo', 'bar', 'baz', 'qux')
        self.cursor.executescript("""
            INSERT INTO node_index VALUES (NULL, 'a', 'x', '111', 'one');
            INSERT INTO node_index VALUES (NULL, 'b', 'y', '222', 'two');
            INSERT INTO node_index VALUES (NULL, 'c', 'z', '333', 'three');
        """)

        manager.delete_columns('bar')

        self.assertEqual(self.get_columns('node_index'), ['index_id', 'foo', 'baz', 'qux'])
        self.assertEqual(self.get_columns('location'), ['_location_id', 'foo', 'baz', 'qux'])
        self.assertEqual(self.get_columns('structure'), ['_structure_id', '_granularity', 'foo', 'baz', 'qux'])

        self.cursor.execute('SELECT * FROM node_index')
        self.assertEqual(
            self.cursor.fetchall(),
            [(0, '-', '-', '-'), (1, 'a', '111', 'one'), (2, 'b', '222', 'two'), (3, 'c', '333', 'three')],
        )

        manager.delete_columns('baz', 'qux')

        self.assertEqual(self.get_columns('node_index'), ['index_id', 'foo'])
        self.assertEqual(self.get_columns('location'), ['_location_id', 'foo'])
        self.assertEqual(self.get_columns('structure'), ['_structure_id', '_granularity', 'foo'])

        self.cursor.execute('SELECT * FROM node_index')
        self.assertEqual(
            self.cursor.fetchall(),
            [(0, '-'), (1, 'a'), (2, 'b'), (3, 'c')],
        )

        regex = 'cannot delete all columns'
        with self.assertRaisesRegex(RuntimeError, regex):
            manager.delete_columns('foo')
