
import unittest
from abc import ABC, abstractmethod
from types import SimpleNamespace

from toron._data_access.base_classes import Index, BaseIndexRepository


class Bases(SimpleNamespace):
    """Wrapping TestCase base classes to prevent test discovery."""

    class TestIndexRepository(ABC, unittest.TestCase):
        @property
        @abstractmethod
        def repository_class(self):
            """The concrete class to be tested."""
            return NotImplemented

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
