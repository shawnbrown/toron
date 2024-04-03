"""Tests for toron/_data_access/property_repository.py module."""

import unittest

from toron._data_access.data_connector import DataConnector
from toron._data_access.repositories import PropertyRepository


class TestPropertyRepository(unittest.TestCase):
    def setUp(self):
        connector = DataConnector()
        resource = connector.acquire_resource()
        self.addCleanup(lambda: connector.release_resource(resource))

        self.cursor = resource.cursor()
        self.addCleanup(self.cursor.close)

    def test_add(self):
        repository = PropertyRepository(self.cursor)

        repository.add('foo', 'bar')

        self.cursor.execute("SELECT * FROM property WHERE key='foo'")
        self.assertEqual(self.cursor.fetchall(), [('foo', 'bar')])

    def test_get(self):
        repository = PropertyRepository(self.cursor)
        self.cursor.execute("INSERT INTO property VALUES ('foo', '\"bar\"')")

        value = repository.get('foo')

        self.assertEqual(value, 'bar')

    def test_update(self):
        repository = PropertyRepository(self.cursor)
        self.cursor.execute("INSERT INTO property VALUES ('foo', '\"bar\"')")

        repository.update('foo', 'baz')

        self.cursor.execute("SELECT * FROM property WHERE key='foo'")
        self.assertEqual(self.cursor.fetchall(), [('foo', 'baz')])

    def test_delete(self):
        repository = PropertyRepository(self.cursor)
        self.cursor.execute("INSERT INTO property VALUES ('foo', '\"bar\"')")

        repository.delete('foo')

        self.cursor.execute("SELECT * FROM property WHERE key='foo'")
        self.assertEqual(self.cursor.fetchall(), [])
