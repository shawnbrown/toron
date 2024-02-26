"""Tests for toron/_data_access/data_connector.py module."""

import tempfile
import unittest

from toron._data_access.base_classes import BaseDataConnector
from toron._data_access.data_connector import DataConnector


class TestDataConnector(unittest.TestCase):
    def test_inheritance(self):
        self.assertTrue(issubclass(DataConnector, BaseDataConnector))

    def test_current_working_path(self):
        connector = DataConnector()  # <- Creates in-memory database.
        self.assertIsNone(connector._current_working_path)

        connector = DataConnector(cache_to_drive=True)  # <- Creates on-drive database.
        tempdir = tempfile.gettempdir()
        self.assertTrue(connector._current_working_path.startswith(tempdir))
        self.assertTrue(connector._current_working_path.endswith('.toron'))
