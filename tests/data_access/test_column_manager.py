"""Tests for toron/_data_access/column_manager.py module."""

import sqlite3
import unittest
from abc import ABC, abstractmethod
from types import SimpleNamespace

from toron._data_access.base_classes import BaseColumnManager
from toron._data_access.column_manager import ColumnManager


class Bases(SimpleNamespace):
    """Wrapping TestCase base classes to prevent test discovery."""

    class TestColumnManager(ABC, unittest.TestCase):
        @property
        @abstractmethod
        def concrete_class(self):
            """The concrete class to be tested."""
            return NotImplemented

        def test_inheritance(self):
            """Should subclass from appropriate abstract base class."""
            self.assertTrue(issubclass(self.concrete_class, BaseColumnManager))


class TestColumnManager(Bases.TestColumnManager):
    @property
    def concrete_class(self):
        return ColumnManager
