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

        self.assertEqual(self.get_columns('node_index'), ['index_id', 'foo', 'bar'])
        self.assertEqual(self.get_columns('location'), ['_location_id', 'foo', 'bar'])
        self.assertEqual(self.get_columns('structure'), ['_structure_id', '_granularity', 'foo', 'bar'])

    def test_get_columns(self):
        manager = ColumnManager(self.cursor)

        actual = manager.get_columns()
        self.assertEqual(actual, tuple(), msg='should be empty tuple when no label columns')

        # Only add to node_index for testing.
        self.cursor.executescript("""
            ALTER TABLE node_index ADD COLUMN 'foo';
            ALTER TABLE node_index ADD COLUMN 'bar';
        """)

        actual = manager.get_columns()
        self.assertEqual(actual, ('foo', 'bar'), msg='should be label columns only, no index_id')

    @unittest.skip('not implemented')
    def test_update_columns(self):
        raise NotImplementedError

    @unittest.skip('not implemented')
    def test_delete_columns(self):
        raise NotImplementedError
