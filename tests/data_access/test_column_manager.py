"""Tests for toron/_data_access/column_manager.py module."""

import sqlite3
import unittest
from abc import ABC, abstractmethod
from types import SimpleNamespace

from toron._data_access.data_connector import DataConnector
from toron._data_access.base_classes import BaseColumnManager
from toron._data_access.column_manager import ColumnManager


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

    def test_add_columns(self):
        manager = ColumnManager(self.cursor)

        self.cursor.execute(f"PRAGMA main.table_info('node_index')")
        actual = [row[1] for row in self.cursor.fetchall()]
        self.assertEqual(actual, ['index_id'], msg='only "index_id", no label columns')

        manager.add_columns('foo', 'bar')

        self.cursor.execute(f"PRAGMA main.table_info('node_index')")
        actual = [row[1] for row in self.cursor.fetchall()]
        self.assertEqual(actual, ['index_id', 'foo', 'bar'])

    def test_get_columns(self):
        manager = ColumnManager(self.cursor)

        actual = manager.get_columns()
        self.assertEqual(actual, tuple(), msg='should be empty tuple when no label columns')

        self.cursor.execute("ALTER TABLE node_index ADD COLUMN 'foo'")
        self.cursor.execute("ALTER TABLE node_index ADD COLUMN 'bar'")
        actual = manager.get_columns()
        self.assertEqual(actual, ('foo', 'bar'), msg='should be label columns only, no index_id')

    @unittest.skip('not implemented')
    def test_update_columns(self):
        raise NotImplementedError

    @unittest.skip('not implemented')
    def test_delete_columns(self):
        raise NotImplementedError
