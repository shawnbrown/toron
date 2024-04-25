"""Tests for data models and data model integration.

Currently this module is focused on integration tests while most of
the behavior is covered by unit tests in the dal1/ folder. But in the
future, it may be best to move or adapt relevant tests to this module
as well.
"""

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
    Index, BaseIndexRepository,
    Weight, BaseWeightRepository,
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


class IndexRepositoryBaseTest(ABC):
    @property
    @abstractmethod
    def connector_class(self):
        ...

    @property
    @abstractmethod
    def manager_class(self):
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

        self.manager = self.manager_class(cursor)

        self.repository = self.repository_class(cursor)

    def test_inheritance(self):
        """Must inherit from appropriate abstract base class."""
        self.assertTrue(issubclass(self.repository_class, BaseIndexRepository))

    def test_integration(self):
        """Test add(), get(), update() and delete() interaction."""
        self.manager.add_columns('A', 'B')

        self.repository.add('foo', 'x')
        self.repository.add('bar', 'y')

        self.assertEqual(self.repository.get(1), Index(1, 'foo', 'x'))
        self.assertEqual(self.repository.get(2), Index(2, 'bar', 'y'))

        self.repository.update(Index(2, 'bar', 'z'))
        self.assertEqual(self.repository.get(2), Index(2, 'bar', 'z'))

        self.repository.delete(2)
        self.assertIsNone(self.repository.get(2))

    def test_add_duplicate_value(self):
        """Attempting to add duplicate values should raise ValueError."""
        self.manager.add_columns('A', 'B')
        self.repository.add('foo', 'bar')

        msg = "should not add ('foo', 'bar') again, duplicates not allowed"
        with self.assertRaises(ValueError, msg=msg):
            self.repository.add('foo', 'bar')

    def test_add_empty_string(self):
        """Attempting to add empty strings should raise ValueError."""
        self.manager.add_columns('A', 'B')

        msg = "adding ('foo', '') should fail, empty strings not allowed"
        with self.assertRaises(ValueError, msg=msg):
            self.repository.add('foo', '')

    def test_get_all(self):
        self.manager.add_columns('A', 'B')
        self.repository.add('foo', 'x')
        self.repository.add('bar', 'y')

        results = self.repository.get_all()

        expected = [
            Index(id=0, values=('-', '-')),
            Index(id=1, values=('foo', 'x')),
            Index(id=2, values=('bar', 'y')),
        ]
        self.assertEqual(list(results), expected)

    def test_find_by_label(self):
        self.manager.add_columns('A', 'B')
        self.repository.add('foo', 'x')
        self.repository.add('foo', 'y')
        self.repository.add('bar', 'x')
        self.repository.add('bar', 'y')

        results = self.repository.find_by_label({'A': 'foo'})
        expected = [Index(1, 'foo', 'x'), Index(2, 'foo', 'y')]
        self.assertEqual(list(results), expected)

        results = self.repository.find_by_label({'B': 'x'})
        expected = [Index(1, 'foo', 'x'), Index(3, 'bar', 'x')]
        self.assertEqual(list(results), expected)

        results = self.repository.find_by_label({'A': 'bar', 'B': 'x'})
        expected = [Index(3, 'bar', 'x')]
        self.assertEqual(list(results), expected)

        regex = 'find_by_label requires at least 1 criteria value, got 0'
        with self.assertRaisesRegex(ValueError, regex):
            results = self.repository.find_by_label(dict())  # <- Empty dict.


class WeightRepositoryBaseTest(ABC):
    @property
    @abstractmethod
    def dal(self):
        ...

    def setUp(self):
        connector = self.dal.DataConnector()
        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        cursor = connector.acquire_cursor(connection)
        self.addCleanup(lambda: connector.release_cursor(cursor))

        self.manager = self.dal.ColumnManager(cursor)
        self.index_repo = self.dal.IndexRepository(cursor)

        self.manager.add_columns('A', 'B')
        self.index_repo.add('foo', 'x')
        self.index_repo.add('bar', 'y')
        self.index_repo.add('baz', 'z')

        self.weight_group_repo = self.dal.WeightGroupRepository(cursor)

        self.repository = self.dal.WeightRepository(cursor)
        self.weight_group_repo.add('population')  # Adds weight_group_id 1.
        self.weight_group_repo.add('square_miles')  # Adds weight_group_id 2.

        self.repository.add(1, 1, 175000)
        self.repository.add(1, 2,  25000)
        self.repository.add(1, 3, 100000)

        self.repository.add(2, 1, 583.75)
        self.repository.add(2, 2, 416.25)
        self.repository.add(2, 3, 500.0)

    def test_inheritance(self):
        """Must inherit from appropriate abstract base class."""
        self.assertTrue(issubclass(self.dal.WeightRepository, BaseWeightRepository))

    def test_find_by_index_id(self):
        results = self.repository.find_by_index_id(1)
        expected = [
            Weight(id=1, weight_group_id=1, index_id=1, value=175000.0),
            Weight(id=4, weight_group_id=2, index_id=1, value=583.75),
        ]
        self.assertEqual(list(results), expected)

        results = self.repository.find_by_index_id(99)  # No index_id 99
        self.assertEqual(list(results), [], msg='should return empty iterator')


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


class IndexRepositoryDAL1(IndexRepositoryBaseTest, unittest.TestCase):
    @property
    def connector_class(self):
        return dal1.DataConnector

    @property
    def manager_class(self):
        return dal1.ColumnManager

    @property
    def repository_class(self):
        return dal1.IndexRepository


class WeightRepositoryDAL1(WeightRepositoryBaseTest, unittest.TestCase):
    @property
    def dal(self):
        return dal1


class PropertyRepositoryDAL1(PropertyRepositoryBaseTest, unittest.TestCase):
    @property
    def connector_class(self):
        return dal1.DataConnector

    @property
    def repository_class(self):
        return dal1.PropertyRepository
