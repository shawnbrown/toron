"""Tests for toron/_data_access/data_connector.py module."""

import tempfile
import unittest
from abc import ABC, abstractmethod
from types import SimpleNamespace

from toron._data_access.base_classes import BaseDataConnector
from toron._data_access.data_connector import DataConnector


class Bases(SimpleNamespace):
    """Wrapping TestCase base classes to prevent test discovery."""

    class TestDataConnector(ABC, unittest.TestCase):
        @property
        @abstractmethod
        def connector_class(self):
            return NotImplemented

        def test_inheritance(self):
            self.assertTrue(issubclass(self.connector_class, BaseDataConnector))


class TestDataConnector(Bases.TestDataConnector):
    @property
    def connector_class(self):
        return DataConnector

    def test_current_working_path(self):
        connector = DataConnector()  # <- Creates in-memory database.
        self.assertIsNone(connector._current_working_path)

        connector = DataConnector(cache_to_drive=True)  # <- Creates on-drive database.
        tempdir = tempfile.gettempdir()
        self.assertTrue(connector._current_working_path.startswith(tempdir))
        self.assertTrue(connector._current_working_path.endswith('.toron'))
