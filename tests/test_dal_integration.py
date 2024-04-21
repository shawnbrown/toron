
import os
import tempfile
import unittest
from abc import ABC, abstractmethod
from contextlib import closing, suppress


#######################################################################
# Abstract Test Cases
#######################################################################

from toron.data_models import (
    BaseDataConnector,
    BasePropertyRepository,
)


class DataConnectorBaseTest(ABC):
    @property
    @abstractmethod
    def connector_class(self):
        """The concrete class to be tested."""
        return NotImplemented

    def test_inheritance(self):
        """Should subclass from BaseDataConnector."""
        self.assertTrue(issubclass(self.connector_class, BaseDataConnector))

    def test_instantiation(self):
        """Without args, should create an empty node structure."""
        try:
            connector = self.connector_class()
        except Exception:
            self.fail('should instantiate with no args')

    def test_unique_id(self):
        """Each node should get a unique ID value."""
        connector1 = self.connector_class()
        connector2 = self.connector_class()
        self.assertNotEqual(connector1.unique_id, connector2.unique_id)

    def test_acquire_release_interoperation(self):
        """The acquire and release methods should interoperate."""
        connector = self.connector_class()
        try:
            connection = connector.acquire_connection()
            connector.release_connection(connection)
        except Exception:
            self.fail('acquired connection should be releasable')

    def test_to_file(self):
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            file_path = os.path.join(tmpdir, 'mynode.toron')
            self.assertFalse(os.path.exists(file_path))

            connector = self.connector_class()
            connector.to_file(file_path, fsync=True)
            self.assertTrue(os.path.exists(file_path))

            file_size = os.path.getsize(file_path)
            self.assertGreater(file_size, 0, msg='file should not be empty')

    def test_from_file(self):
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            file_path = os.path.join(tmpdir, 'mynode.toron')
            original = self.connector_class()
            original.to_file(file_path)

            loadedfromfile = self.connector_class.from_file(file_path)
            self.assertEqual(original.unique_id, loadedfromfile.unique_id)

    def test_from_file_missing(self):
        """Should raise FileNotFoundError if file doesn't exist."""
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            file_path = os.path.join(tmpdir, 'does_not_exist.toron')

            with self.assertRaises(FileNotFoundError):
                self.connector_class.from_file(file_path)

    def test_from_file_unknown_format(self):
        """Should raise RuntimeError if file uses unknown format."""
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            file_path = os.path.join(tmpdir, 'unknown_format.xyz')
            with closing(open(file_path, 'w')) as f:
                f.write('Hello World\n')

            with self.assertRaises(RuntimeError):
                self.connector_class.from_file(file_path)


class ColumnManagerBaseTest(ABC):
    @property
    @abstractmethod
    def connector_class(self):
        ...

    @property
    @abstractmethod
    def manager_class(self):
        ...

    def setUp(self):
        connector = self.connector_class()

        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        cursor = connector.acquire_cursor(connection)
        self.addCleanup(lambda: connector.release_cursor(cursor))

        self.manager = self.manager_class(cursor)


class PropertyRepositoryBaseTest(ABC):
    @property
    @abstractmethod
    def connector_class(self):
        ...

    @property
    @abstractmethod
    def repository_class(self):
        ...

    def setUp(self):
        connector = self.connector_class()
        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        cursor = connector.acquire_cursor(connection)
        self.addCleanup(lambda: connector.release_cursor(cursor))

        self.repository = self.repository_class(cursor)

    def test_inheritance(self):
        """Should subclass from appropriate abstract base class."""
        self.assertTrue(issubclass(self.repository_class, BasePropertyRepository))

    def test_initial_properties(self):
        """Before adding any new properties, a newly-created node
        should have three pre-set keys:

        * toron_schema_version
        * toron_app_version
        * unique_id
        """
        repository = self.repository

        self.assertIsNotNone(repository.get('toron_schema_version'))
        self.assertIsNotNone(repository.get('toron_app_version'))
        self.assertIsNotNone(repository.get('unique_id'))

    def test_integration(self):
        """Test interoperation of add, get, update, and delete."""
        repository = self.repository

        value = {'foo': ['bar', 1234, 1234.5, True, False, None]}
        repository.add('foo', value)
        self.assertEqual(repository.get('foo'), value)

        value = {'baz': 42, 'qux': [True, False]}
        repository.update('foo', value)
        self.assertEqual(repository.get('foo'), value)

        repository.delete('foo')
        self.assertIsNone(repository.get('foo'))


#######################################################################
# Concrete Test Cases for SQLite Backend
#######################################################################

from toron import dal1


class TestDataConnectorDAL1(DataConnectorBaseTest, unittest.TestCase):
    @property
    def connector_class(self):
        return dal1.DataConnector


class ColumnManagerDAL1(ColumnManagerBaseTest, unittest.TestCase):
    @property
    def connector_class(self):
        return dal1.DataConnector

    @property
    def manager_class(self):
        return dal1.ColumnManager


class PropertyRepositoryDAL1(PropertyRepositoryBaseTest, unittest.TestCase):
    @property
    def connector_class(self):
        return dal1.DataConnector

    @property
    def repository_class(self):
        return dal1.PropertyRepository
