
import os
import tempfile
import unittest
from abc import ABC, abstractmethod
from contextlib import closing


#######################################################################
# Abstract Test Cases
#######################################################################

from toron._data_access.base_classes import BaseDataConnector


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
            resource = connector.acquire_resource()
            connector.release_resource(resource)
        except Exception:
            self.fail('acquired resource should be releasable')

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


#######################################################################
# Concrete Test Cases for SQLite Backend
#######################################################################

from toron._data_access.data_connector import DataConnector


class TestDataConnectorSqlite(DataConnectorBaseTest, unittest.TestCase):
    @property
    def connector_class(self):
        return DataConnector
