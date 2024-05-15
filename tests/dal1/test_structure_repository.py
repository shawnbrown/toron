"""Tests for StructureRepository class."""

import sqlite3
import unittest

from toron.dal1.data_connector import DataConnector
from toron.data_models import Structure, BaseStructureRepository
from toron.dal1.repositories import StructureRepository


class TestStructureRepository(unittest.TestCase):
    def setUp(self):
        connector = DataConnector()
        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

        self.cursor.executescript("""
            DROP INDEX IF EXISTS unique_structure_label_columns;
            ALTER TABLE structure ADD COLUMN "A" INTEGER NOT NULL CHECK ("A" IN (0, 1)) DEFAULT 0;
            ALTER TABLE structure ADD COLUMN "B" INTEGER NOT NULL CHECK ("B" IN (0, 1)) DEFAULT 0;
            ALTER TABLE structure ADD COLUMN "C" INTEGER NOT NULL CHECK ("C" IN (0, 1)) DEFAULT 0;
            CREATE UNIQUE INDEX unique_structure_label_columns ON structure("A", "B", "C");
        """)

    def test_inheritance(self):
        """Should subclass from appropriate abstract base class."""
        self.assertTrue(issubclass(StructureRepository, BaseStructureRepository))

    def assertRecords(self, expected_records, msg=None):
        self.cursor.execute(f'SELECT * FROM structure')
        actual_records = self.cursor.fetchall()
        self.assertEqual(actual_records, expected_records, msg=msg)

    def test_add(self):
        repository = StructureRepository(self.cursor)

        repository.add(None, 0, 0, 0)
        repository.add(None, 1, 1, 0)
        repository.add(None, 1, 1, 1)

        self.assertRecords([(1, None, 0, 0, 0), (2, None, 1, 1, 0), (3, None, 1, 1, 1)])

        msg = "should not add (1, 1, 1) again, duplicates not allowed"
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add(None, 1, 1, 1)

        msg = "NULL values not allowed in label columns"
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add(None, 1, None, 1)

    def test_get(self):
        repository = StructureRepository(self.cursor)
        self.cursor.executescript("""
            INSERT INTO structure VALUES (1, NULL, 0, 0, 0);
            INSERT INTO structure VALUES (2, NULL, 1, 1, 0);
            INSERT INTO structure VALUES (3, NULL, 1, 1, 1);
        """)

        self.assertEqual(repository.get(1), Structure(1, None, 0, 0, 0))
        self.assertEqual(repository.get(2), Structure(2, None, 1, 1, 0))
        self.assertEqual(repository.get(3), Structure(3, None, 1, 1, 1))
        self.assertIsNone(repository.get(37), msg='should be None if no matching id')
        self.assertIsNone(repository.get(0), msg='should be None if no matching id')

    def test_get_all(self):
        repository = StructureRepository(self.cursor)
        self.cursor.executescript("""
            INSERT INTO structure VALUES (1, 0.0, 0, 0, 0);
            INSERT INTO structure VALUES (2, 7.0, 1, 1, 0);
            INSERT INTO structure VALUES (3, NULL, 1, 0, 0);
            INSERT INTO structure VALUES (4, 9.0, 1, 1, 1);
        """)

        self.assertEqual(
            repository.get_all(),
            [Structure(4, 9.0,  1, 1, 1),
             Structure(2, 7.0,  1, 1, 0),
             Structure(1, 0.0,  0, 0, 0),
             Structure(3, None, 1, 0, 0)]
        )

    def test_update(self):
        repository = StructureRepository(self.cursor)
        self.cursor.executescript("""
            INSERT INTO structure VALUES (1, NULL, 0, 0, 0);
            INSERT INTO structure VALUES (2, NULL, 1, 1, 0);
            INSERT INTO structure VALUES (3, NULL, 1, 1, 1);
        """)

        repository.update(Structure(3, 9.25, 1, 1, 1))
        self.cursor.execute('SELECT * FROM structure')
        records = self.cursor.fetchall()
        self.assertEqual(records, [(1, None, 0, 0, 0), (2, None, 1, 1, 0), (3, 9.25, 1, 1, 1)])

        with self.assertRaises(sqlite3.OperationalError, msg='4 columns but only 2 values'):
            repository.update(Structure(3, 9.25, 1))

        repository.update(Structure(7, 3.5, 0, 0, 1))  # <- No _structure_id 7 exists.
        self.cursor.execute('SELECT * FROM structure')
        records = self.cursor.fetchall()
        msg = 'there is no _structure_id 7, records should be unchanged'
        self.assertEqual(records, [(1, None, 0, 0, 0), (2, None, 1, 1, 0), (3, 9.25, 1, 1, 1)], msg=msg)

    def test_delete(self):
        repository = StructureRepository(self.cursor)
        self.cursor.executescript("""
            INSERT INTO structure VALUES (1, 0.0, 0, 0, 0);
            INSERT INTO structure VALUES (2, 9.25, 1, 1, 1);
        """)

        repository.delete(1)
        self.assertRecords([(2, 9.25, 1, 1, 1)])

        repository.delete(2)
        self.assertRecords([])

        try:
            repository.delete(42)
        except Exception as err:
            self.fail(f'deleting non-existant ids should not raise errors, got {err!r}')
