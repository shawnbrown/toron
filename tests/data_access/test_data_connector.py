"""Tests for toron/_data_access/data_connector.py module."""

import os
import sqlite3
import tempfile
import unittest
from abc import ABC, abstractmethod
from contextlib import closing
from types import SimpleNamespace

from toron._data_access.base_classes import BaseDataConnector
from toron._data_access.data_connector import (
    _cleanup_leftover_temp_files,
    make_sqlite_uri_filepath,
    get_sqlite_connection,
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


class TestGetSqlite3Connection(unittest.TestCase):
    def test_in_memory(self):
        con = get_sqlite_connection(':memory:')

        with closing(con):
            self.assertEqual(con.execute('SELECT 123').fetchall(), [(123,)])

    def test_implicit_tempfile(self):
        con = get_sqlite_connection('')

        with closing(con):
            self.assertEqual(con.execute('SELECT 123').fetchall(), [(123,)])

    def test_explicit_tempfile(self):
        with closing(tempfile.NamedTemporaryFile(delete=False)) as temp_f:
            database_path = os.path.abspath(temp_f.name)
            self.addCleanup(lambda: os.unlink(database_path))

        con = get_sqlite_connection(database_path)

        with closing(con):
            self.assertEqual(con.execute('SELECT 123').fetchall(), [(123,)])

    def test_read_only_access_mode(self):
        with closing(tempfile.NamedTemporaryFile(delete=False)) as temp_f:
            database_path = os.path.abspath(temp_f.name)
            self.addCleanup(lambda: os.unlink(database_path))

        con = get_sqlite_connection(database_path, access_mode='ro')

        with self.assertRaises(sqlite3.OperationalError):
            with closing(con):
                con.execute('CREATE TABLE t1(a, b)')  # Should raise error.


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

    def test_in_memory_database(self):
        connector = DataConnector()  # <- In-memory database.
        self.assertIsNone(connector._current_working_path)
        self.assertIsInstance(connector._in_memory_connection, sqlite3.Connection)

    def test_on_drive_database(self):
        connector = DataConnector(cache_to_drive=True)  # <- On-drive database.
        self.assertTrue(connector._current_working_path.startswith(tempfile.gettempdir()))
        self.assertTrue(connector._current_working_path.endswith('.toron'))
        self.assertIsNone(connector._in_memory_connection)

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