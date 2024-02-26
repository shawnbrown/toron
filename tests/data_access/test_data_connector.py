"""Tests for toron/_data_access/data_connector.py module."""

import unittest

from toron._data_access.base_classes import BaseDataConnector
from toron._data_access.data_connector import DataConnector


class TestDataConnector(unittest.TestCase):
    def test_inheritance(self):
        self.assertTrue(issubclass(DataConnector, BaseDataConnector))
