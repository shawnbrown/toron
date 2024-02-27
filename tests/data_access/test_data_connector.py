"""Tests for toron/_data_access/data_connector.py module."""

import os
import tempfile
import unittest
from abc import ABC, abstractmethod
from types import SimpleNamespace

from toron._data_access.base_classes import BaseDataConnector
from toron._data_access.data_connector import (
    _cleanup_leftover_temp_files,
    make_sqlite_uri_filepath,
    DataConnector,
)


class TestMakeSqliteUriFilepath(unittest.TestCase):
    def test_cases_without_mode(self):
        self.assertEqual(
            make_sqlite_uri_filepath('mynode.toron', mode=None),
            'file:mynode.toron',
        )
        self.assertEqual(
            make_sqlite_uri_filepath('my?node.toron', mode=None),
            'file:my%3Fnode.toron',
        )
        self.assertEqual(
            make_sqlite_uri_filepath('path///to//mynode.toron', mode=None),
            'file:path/to/mynode.toron',
        )

    def test_cases_with_mode(self):
        self.assertEqual(
            make_sqlite_uri_filepath('mynode.toron', mode='ro'),
            'file:mynode.toron?mode=ro',
        )
        self.assertEqual(
            make_sqlite_uri_filepath('my?node.toron', mode='rw'),
            'file:my%3Fnode.toron?mode=rw',
        )
        self.assertEqual(
            make_sqlite_uri_filepath('path///to//mynode.toron', mode='rwc'),
            'file:path/to/mynode.toron?mode=rwc',
        )

    def test_windows_specifics(self):
        if os.name != 'nt':
            return

        path = r'path\to\mynode.toron'
        expected = 'file:path/to/mynode.toron'
        self.assertEqual(make_sqlite_uri_filepath(path, mode=None), expected)

        path = r'C:\path\to\my node.toron'
        expected = 'file:/C:/path/to/my%20node.toron'
        self.assertEqual(make_sqlite_uri_filepath(path, mode=None), expected)

        path = r'C:\path\to\myno:de.toron'  # <- Errant ":".
        expected = 'file:/C:/path/to/myno%3Ade.toron'
        self.assertEqual(make_sqlite_uri_filepath(path, mode=None), expected)

        path = r'C:mynode.toron'  # <- Relative to CWD on C: drive (not simply C:\mynode.toron).
        c_drive_cwd = os.path.dirname(os.path.abspath(path))
        expected = f'file:/{c_drive_cwd}/mynode.toron'.replace('\\', '/').replace('//', '/')
        self.assertEqual(make_sqlite_uri_filepath(path, mode=None), expected)


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

    def test_tempfile_cleanup(self):
        connector = DataConnector(cache_to_drive=True)
        working_path = connector._current_working_path

        self.assertTrue(os.path.exists(working_path))

        connector.__del__()  # Call magic method directly only for testing.
        self.assertFalse(os.path.exists(working_path))

    def test_atexit_cleanup(self):
        connector = DataConnector(cache_to_drive=True)
        connector._cleanup_funcs = []  # <- Clear cleanup funcs for testing.
        working_path = connector._current_working_path

        self.assertTrue(os.path.exists(working_path))

        _cleanup_leftover_temp_files()
        self.assertFalse(os.path.exists(working_path))
