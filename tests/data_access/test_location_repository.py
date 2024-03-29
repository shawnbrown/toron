"""Tests for toron/_data_access/location_repository.py module."""

import sqlite3
import unittest
from abc import ABC, abstractmethod
from types import SimpleNamespace

from toron._data_access.data_connector import DataConnector
from toron._data_access.base_classes import Location, BaseLocationRepository


class Bases(SimpleNamespace):
    """Wrapping TestCase base classes to prevent test discovery."""

    class TestLocationRepository(ABC, unittest.TestCase):
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
            self.assertTrue(issubclass(self.repository_class, BaseLocationRepository))

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
        #def test_filter_by_structure(self):
        #    ...
