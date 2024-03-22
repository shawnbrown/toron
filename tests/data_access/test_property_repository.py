"""Tests for toron/_data_access/property_repository.py module."""

import unittest
from abc import ABC, abstractmethod
from types import SimpleNamespace

from toron._data_access.data_connector import DataConnector
from toron._data_access.base_classes import BasePropertyRepository
from toron._data_access.property_repository import PropertyRepository


class Bases(SimpleNamespace):
    """Wrapping TestCase base classes to prevent test discovery."""

    class TestPropertyRepository(ABC, unittest.TestCase):
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
            self.assertTrue(issubclass(self.repository_class, BasePropertyRepository))

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

        def test_initial_properties(self):
            """Before adding any new properties, a newly-created node
            should have three pre-set keys:

            * toron_schema_version
            * toron_app_version
            * unique_id
            """
            repository = self.repository_class(self.cursor)

            self.assertIsNotNone(repository.get('toron_schema_version'))
            self.assertIsNotNone(repository.get('toron_app_version'))
            self.assertIsNotNone(repository.get('unique_id'))

        def test_integration(self):
            """Test interoperation of add, get, update, and delete."""
            repository = self.repository_class(self.cursor)

            value = {'foo': ['bar', 1234, 1234.5, True, False, None]}
            repository.add('foo', value)
            self.assertEqual(repository.get('foo'), value)

            value = {'baz': 42, 'qux': [True, False]}
            repository.update('foo', value)
            self.assertEqual(repository.get('foo'), value)

            repository.delete('foo')
            self.assertIsNone(repository.get('foo'))


class TestPropertyRepository(Bases.TestPropertyRepository):
    @property
    def repository_class(self):
        return PropertyRepository

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
