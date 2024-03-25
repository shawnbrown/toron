"""Tests for toron/_data_access/index_repository.py module."""

import unittest
from abc import ABC, abstractmethod
from types import SimpleNamespace

from toron._data_access.data_connector import DataConnector
from toron._data_access.base_classes import Index, BaseIndexRepository
from toron._data_access.index_repository import IndexRepository


class Bases(SimpleNamespace):
    """Wrapping TestCase base classes to prevent test discovery."""

    class TestIndexRepository(ABC, unittest.TestCase):
        @property
        @abstractmethod
        def repository_class(self):
            """The concrete class to be tested."""
            return NotImplemented

        def setUp(self):
            connector = DataConnector()
            resource = connector.acquire_resource()
            self.addCleanup(lambda: connector.release_resource(resource))

            self.cursor = resource.cursor()

        def test_inheritance(self):
            """Should subclass from appropriate abstract base class."""
            self.assertTrue(issubclass(self.repository_class, BaseIndexRepository))

        @abstractmethod
        def test_add(self):
            ...

        @abstractmethod
        def test_get(self):
            ...

        @abstractmethod
        def test_update(self):
            ...

        @abstractmethod
        def test_delete(self):
            ...

        #@abstractmethod
        #def test_get_all(self):
        #    ...

        #@abstractmethod
        #def test_find(self):
        #    ...

        @abstractmethod
        def test_add_columns(self):
            ...

        @abstractmethod
        def test_get_columns(self):
            ...

        #@abstractmethod
        #def test_update_columns(self):
        #    ...

        #@abstractmethod
        #def test_delete_columns(self):
        #    ...


class TestIndexRepository(Bases.TestIndexRepository):
    @property
    def repository_class(self):
        return IndexRepository

    def test_add(self):
        repository = IndexRepository(self.cursor)
        self.cursor.executescript("""
            DROP INDEX IF EXISTS unique_nodeindex_index;
            ALTER TABLE node_index ADD COLUMN A TEXT NOT NULL CHECK (A != '') DEFAULT '-';
            ALTER TABLE node_index ADD COLUMN B TEXT NOT NULL CHECK (B != '') DEFAULT '-';
            CREATE UNIQUE INDEX unique_nodeindex_index ON node_index(A, B);
        """)

        repository.add('foo', 'bar')

        self.cursor.execute('SELECT * FROM node_index')
        self.assertEqual(
            self.cursor.fetchall(), [(0, '-', '-'), (1, 'foo', 'bar')],
        )

        msg = "should not add ('foo', 'bar') again, duplicates not allowed"
        with self.assertRaises(ValueError, msg=msg):
            repository.add('foo', 'bar')

        msg = "adding ('foo', '') should fail, empty strings not allowed"
        with self.assertRaises(ValueError, msg=msg):
            repository.add('foo', '')

    @unittest.skip('not implemented')
    def test_get(self):
        raise NotImplementedError

    @unittest.skip('not implemented')
    def test_update(self):
        raise NotImplementedError

    @unittest.skip('not implemented')
    def test_delete(self):
        raise NotImplementedError

    #def test_get_all(self):
    #    raise NotImplementedError

    #def test_find(self):
    #    raise NotImplementedError

    def test_add_columns(self):
        repository = IndexRepository(self.cursor)

        self.cursor.execute(f"PRAGMA main.table_info('node_index')")
        actual = [row[1] for row in self.cursor.fetchall()]
        self.assertEqual(actual, ['index_id'], msg='should start with "index_id"')

        repository.add_columns('foo', 'bar')

        self.cursor.execute(f"PRAGMA main.table_info('node_index')")
        actual = [row[1] for row in self.cursor.fetchall()]
        self.assertEqual(actual, ['index_id', 'foo', 'bar'])

    def test_get_columns(self):
        repository = IndexRepository(self.cursor)

        actual = repository.get_columns()
        self.assertEqual(actual, tuple(), msg='should be empty tuple when no label columns')

        self.cursor.execute("ALTER TABLE node_index ADD COLUMN 'foo'")
        self.cursor.execute("ALTER TABLE node_index ADD COLUMN 'bar'")
        actual = repository.get_columns()
        self.assertEqual(actual, ('foo', 'bar'), msg='should be label columns only, no index_id')

    #def test_update_columns(self):
    #    raise NotImplementedError

    #def test_delete_columns(self):
    #    raise NotImplementedError
