"""Tests for toron/_data_access/location_repository.py module."""

import sqlite3
import unittest

from toron._data_access.data_connector import DataConnector
from toron._data_models import Location, BaseLocationRepository
from toron._data_access.repositories import LocationRepository


class TestLocationRepository(unittest.TestCase):
    def setUp(self):
        connector = DataConnector()
        resource = connector.acquire_resource()
        self.addCleanup(lambda: connector.release_resource(resource))

        self.cursor = resource.cursor()
        self.addCleanup(self.cursor.close)

        self.cursor.executescript("""
            DROP INDEX IF EXISTS unique_location_label_columns;
            ALTER TABLE location ADD COLUMN "A" TEXT NOT NULL DEFAULT '';
            ALTER TABLE location ADD COLUMN "B" TEXT NOT NULL DEFAULT '';
            CREATE UNIQUE INDEX unique_location_label_columns ON location("A", "B");
        """)

    def test_inheritance(self):
        """Should subclass from appropriate abstract base class."""
        self.assertTrue(issubclass(LocationRepository, BaseLocationRepository))

    def assertRecords(self, expected_records, msg=None):
        self.cursor.execute(f'SELECT * FROM location')
        actual_records = self.cursor.fetchall()
        self.assertEqual(actual_records, expected_records, msg=msg)

    def test_add(self):
        repository = LocationRepository(self.cursor)

        repository.add('foo', 'bar')
        repository.add('foo', '')  # <- Empty strings are allowed.

        self.assertRecords([(1, 'foo', 'bar'), (2, 'foo', '')])

        msg = "should not add ('foo', '') again, duplicates not allowed"
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add('foo', '')

        msg = "NULL values not allowed in location table"
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add('foo', None)

    def test_get(self):
        repository = LocationRepository(self.cursor)
        self.cursor.executescript("""
            INSERT INTO location VALUES (1, 'foo', 'bar');
            INSERT INTO location VALUES (2, 'foo', 'baz');
            INSERT INTO location VALUES (3, 'foo', '');
        """)

        self.assertEqual(repository.get(1), Location(1, 'foo', 'bar'))
        self.assertEqual(repository.get(2), Location(2, 'foo', 'baz'))
        self.assertEqual(repository.get(3), Location(3, 'foo', ''))
        self.assertIsNone(repository.get(37), msg='should be None if no matching id')
        self.assertIsNone(repository.get(0), msg='should be None if no matching id')

    def test_update(self):
        repository = LocationRepository(self.cursor)
        self.cursor.executescript("""
            INSERT INTO location VALUES (1, 'foo', 'bar');
            INSERT INTO location VALUES (2, 'foo', 'baz');
        """)

        repository.update(Location(1, 'qux', 'quux'))
        self.cursor.execute('SELECT * FROM location')
        records = self.cursor.fetchall()
        self.assertEqual(records, [(1, 'qux', 'quux'), (2, 'foo', 'baz')])

        with self.assertRaises(sqlite3.OperationalError, msg='2 columns but only 1 value'):
            repository.update(Location(1, 'corge'))

        repository.update(Location(3, 'corge', 'blerg'))  # <- No _location_id 3 exists.
        self.cursor.execute('SELECT * FROM location')
        records = self.cursor.fetchall()
        msg = 'there is no _location_id 3, records should be unchanged'
        self.assertEqual(records, [(1, 'qux', 'quux'), (2, 'foo', 'baz')], msg=msg)

    def test_delete(self):
        repository = LocationRepository(self.cursor)
        self.cursor.executescript("""
            INSERT INTO location VALUES (1, 'foo', 'bar');
            INSERT INTO location VALUES (2, 'foo', 'baz');
        """)

        repository.delete(2)
        self.assertRecords([(1, 'foo', 'bar')])

        repository.delete(1)
        self.assertRecords([])

        try:
            repository.delete(42)
        except Exception as err:
            self.fail(f'deleting non-existant ids should not raise errors, got {err!r}')
