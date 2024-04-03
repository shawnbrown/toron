"""Tests for toron/_data_access/index_repository.py module."""

import sqlite3
import unittest

from toron._data_access.data_connector import DataConnector
from toron._data_models import Index, BaseIndexRepository
from toron._data_access.repositories import IndexRepository


class TestIndexRepository(unittest.TestCase):
    @property
    def repository_class(self):
        return IndexRepository

    def setUp(self):
        connector = DataConnector()
        resource = connector.acquire_resource()
        self.addCleanup(lambda: connector.release_resource(resource))

        self.cursor = resource.cursor()
        self.addCleanup(self.cursor.close)

    def test_inheritance(self):
        """Should subclass from appropriate abstract base class."""
        self.assertTrue(issubclass(IndexRepository, BaseIndexRepository))

    def test_add(self):
        repository = IndexRepository(self.cursor)
        self.cursor.executescript("""
            DROP INDEX IF EXISTS unique_index_label_columns;
            ALTER TABLE node_index ADD COLUMN "A" TEXT NOT NULL CHECK ("A" != '') DEFAULT '-';
            ALTER TABLE node_index ADD COLUMN "B" TEXT NOT NULL CHECK ("B" != '') DEFAULT '-';
            CREATE UNIQUE INDEX unique_index_label_columns ON node_index("A", "B");
        """)

        repository.add('foo', 'bar')

        self.cursor.execute('SELECT * FROM node_index')
        self.assertEqual(
            self.cursor.fetchall(), [(0, '-', '-'), (1, 'foo', 'bar')],
        )

        msg = "should not add ('foo', 'bar') again, duplicates not allowed"
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add('foo', 'bar')

        msg = "adding ('foo', '') should fail, empty strings not allowed"
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add('foo', '')

    def test_get(self):
        repository = IndexRepository(self.cursor)
        self.cursor.executescript("""
            DROP INDEX IF EXISTS unique_index_label_columns;
            ALTER TABLE node_index ADD COLUMN "A" TEXT NOT NULL CHECK ("A" != '') DEFAULT '-';
            ALTER TABLE node_index ADD COLUMN "B" TEXT NOT NULL CHECK ("B" != '') DEFAULT '-';
            CREATE UNIQUE INDEX unique_index_label_columns ON node_index("A", "B");
            INSERT INTO node_index VALUES (1, 'foo', 'bar');
            INSERT INTO node_index VALUES (2, 'foo', 'baz');
        """)

        self.assertEqual(repository.get(0), Index(0, '-', '-'))
        self.assertEqual(repository.get(1), Index(1, 'foo', 'bar'))
        self.assertEqual(repository.get(2), Index(2, 'foo', 'baz'))
        self.assertIsNone(repository.get(3), msg='should be None if no matching id')

    def test_update(self):
        repository = IndexRepository(self.cursor)
        self.cursor.executescript("""
            DROP INDEX IF EXISTS unique_index_label_columns;
            ALTER TABLE node_index ADD COLUMN "A" TEXT NOT NULL CHECK ("A" != '') DEFAULT '-';
            ALTER TABLE node_index ADD COLUMN "B" TEXT NOT NULL CHECK ("B" != '') DEFAULT '-';
            CREATE UNIQUE INDEX unique_index_label_columns ON node_index("A", "B");
            INSERT INTO node_index VALUES (1, 'foo', 'bar');
        """)

        repository.update(Index(1, 'qux', 'quux'))
        self.cursor.execute('SELECT * FROM node_index')
        records = self.cursor.fetchall()
        self.assertEqual(records, [(0, '-', '-'), (1, 'qux', 'quux')])

        with self.assertRaises(sqlite3.OperationalError, msg='2 columns but only 1 value'):
            repository.update(Index(1, 'corge'))

        repository.update(Index(2, 'corge', 'blerg'))  # <- No index_id 2 exists.
        self.cursor.execute('SELECT * FROM node_index')
        records = self.cursor.fetchall()
        msg = 'there is no index_id 2, records should be unchanged'
        self.assertEqual(records, [(0, '-', '-'), (1, 'qux', 'quux')], msg=msg)

        msg = 'should fail to modify undefined record (index_id 0)'
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.update(Index(0, 'x', 'x'))

    def test_delete(self):
        repository = IndexRepository(self.cursor)
        self.cursor.executescript("""
            DROP INDEX IF EXISTS unique_index_label_columns;
            ALTER TABLE node_index ADD COLUMN "A" TEXT NOT NULL CHECK ("A" != '') DEFAULT '-';
            ALTER TABLE node_index ADD COLUMN "B" TEXT NOT NULL CHECK ("B" != '') DEFAULT '-';
            CREATE UNIQUE INDEX unique_index_label_columns ON node_index("A", "B");
            INSERT INTO node_index VALUES (1, 'foo', 'bar');
            INSERT INTO node_index VALUES (2, 'foo', 'baz');
        """)

        repository.delete(2)
        self.cursor.execute('SELECT * FROM node_index')
        self.assertEqual(
            self.cursor.fetchall(),
            [(0, '-', '-'), (1, 'foo', 'bar')],
        )

        repository.delete(1)
        self.cursor.execute('SELECT * FROM node_index')
        self.assertEqual(
            self.cursor.fetchall(),
            [(0, '-', '-')],
        )

        msg = 'should fail to delete undefined record (index_id 0)'
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.delete(0)

    #def test_get_all(self):
    #    raise NotImplementedError

    #def test_find(self):
    #    raise NotImplementedError
