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

    @unittest.skip('not implemented')
    def test_add(self):
        raise NotImplementedError

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

    @unittest.skip('not implemented')
    def test_add_columns(self):
        raise NotImplementedError

    @unittest.skip('not implemented')
    def test_get_columns(self):
        raise NotImplementedError

    #def test_update_columns(self):
    #    raise NotImplementedError

    #def test_delete_columns(self):
    #    raise NotImplementedError
