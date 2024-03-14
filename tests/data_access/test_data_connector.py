"""Tests for toron/_data_access/data_connector.py module."""

import gc
import os
import sqlite3
import tempfile
import unittest
from abc import ABC, abstractmethod
from contextlib import closing
from types import SimpleNamespace

from toron._data_access.base_classes import BaseDataConnector
from toron._data_access.data_connector import (
    make_sqlite_uri_filepath,
    get_sqlite_connection,
    ToronSqlite3Connection,
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

    def test_factory_subclass(self):
        class MyConnection(sqlite3.Connection):
            pass

        con = get_sqlite_connection(':memory:', factory=MyConnection)

        with closing(con):
            self.assertIsInstance(con, MyConnection)

    def test_factory_error(self):
        class BadConnection(object):
            def __init__(self, *args, **kwds):
                pass

        msg = 'should fail when *factory* is not a subclass of sqlite3.Connection'
        with self.assertRaises(TypeError, msg=msg):
            get_sqlite_connection(':memory:', factory=BadConnection)


class TestToronSqlite3Connection(unittest.TestCase):
    def test_closing_error(self):
        con = ToronSqlite3Connection(':memory:')
        self.addCleanup(super(con.__class__, con).close)

        regex = 'cannot close directly'
        with self.assertRaisesRegex(RuntimeError, regex):
            con.close()

    def test_closing_success(self):
        con = ToronSqlite3Connection(':memory:')

        try:
            super(con.__class__, con).close()
        except Exception:
            self.fail('should close via superclass close() method')


class Bases(SimpleNamespace):
    """Wrapping TestCase base classes to prevent test discovery."""

    class TestDataConnector(ABC, unittest.TestCase):
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

        del connector  # Delete connector and explicitly trigger full
        gc.collect()   # garbage collection.

        self.assertFalse(os.path.exists(working_path))

    def test_acquire_resource_in_memory(self):
        """Connection should be same instance as _in_memory_connection."""
        connector = DataConnector()
        con = connector.acquire_resource()  # <- Method under test.
        self.addCleanup(super(ToronSqlite3Connection, con).close)

        self.assertIs(con, connector._in_memory_connection)
        self.assertIsNone(connector._current_working_path)

    def test_acquire_resource_on_drive(self):
        """Connection's file should match _current_working_path."""
        connector = DataConnector(cache_to_drive=True)
        con = connector.acquire_resource()  # <- Method under test.
        self.addCleanup(super(ToronSqlite3Connection, con).close)

        cur = con.execute('PRAGMA database_list')
        _, _, file = cur.fetchone()  # Row contains `seq`, `name`, and `file`.

        self.assertEqual(
            os.path.realpath(file),
            os.path.realpath(connector._current_working_path),
            msg='should be the same file',
        )
        self.assertIsNone(connector._in_memory_connection)

    def test_release_resource_in_memory(self):
        """Connection to temporary database should remain open."""
        connector = DataConnector()
        con = connector.acquire_resource()
        connector.release_resource(con)  # <- Method under test.

        try:
            con.execute('SELECT 1')
        except sqlite3.ProgrammingError as err:
            if 'closed database' not in str(err):
                raise  # Re-raise error if it's something else.
            self.fail('the connection should not be closed')

    def test_release_resource_on_drive(self):
        """Connection to persistent database should be closed."""
        connector = DataConnector(cache_to_drive=True)
        con = connector.acquire_resource()
        connector.release_resource(con)  # <- Method under test.

        regex = 'closed database'
        with self.assertRaisesRegex(sqlite3.ProgrammingError, regex):
            con.execute('SELECT 1')
