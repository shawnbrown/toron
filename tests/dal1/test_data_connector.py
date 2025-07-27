"""Tests for DataConnector class."""

import gc
import os
import sqlite3
import stat
import sys
import tempfile
import unittest
from contextlib import closing
from unittest.mock import Mock, call

from toron.dal1.data_connector import (
    make_sqlite_uri_filepath,
    get_sqlite_connection,
    verify_permissions,
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

    def test_nonfile_path(self):
        """Non-file resources should fail immediately."""
        with tempfile.TemporaryDirectory(prefix='toron-') as dirname:
            with self.assertRaises(sqlite3.OperationalError):
                get_sqlite_connection(dirname)

    def test_nondatabase_file(self):
        """Non-database files should fail."""
        with tempfile.NamedTemporaryFile(prefix='toron-', delete=False) as f:
            f.write(b'\xff' * 64)  # Write 64 bytes of 1s.
        self.addCleanup(lambda: os.unlink(f.name))

        # Returns connection but doesn't fail immediately.
        con = get_sqlite_connection(f.name)
        self.addCleanup(con.close)

        # Fails when attempting to interact with connection.
        with self.assertRaises(sqlite3.DatabaseError):
            con.execute('PRAGMA main.user_version')


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


class TestVerifyPermissions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory(prefix='toron-')
        if sys.version_info >= (3, 8, 0):
            cls.addClassCleanup(cls.temp_dir.cleanup)

        cls.rw_path = os.path.join(cls.temp_dir.name, 'readwrite.toron')
        open(cls.rw_path, 'w').close()

        cls.ro_path = os.path.join(cls.temp_dir.name, 'readonly.toron')
        open(cls.ro_path, 'w').close()
        os.chmod(cls.ro_path, stat.S_IRUSR)  # Make sure file is read-only.

        # Define path but don't create a file (file should not exist).
        cls.new_path = os.path.join(cls.temp_dir.name, 'new_file.toron')

    if sys.version_info < (3, 8, 0):
        @classmethod
        def tearDownClass(cls):
            # Make file read-write (old bug https://github.com/python/cpython/issues/70847)
            os.chmod(cls.ro_path, stat.S_IRUSR | stat.S_IWUSR)
            cls.temp_dir.cleanup()

    def test_readonly_required(self):
        try:
            verify_permissions(self.ro_path, required_permissions='ro')
        except Exception:
            self.fail("requiring 'ro' on read-only file should not fail")

        with self.assertRaises(PermissionError):
            verify_permissions(self.rw_path, required_permissions='ro')

        with self.assertRaises(FileNotFoundError):
            verify_permissions(self.new_path, required_permissions='ro')

    def test_readwrite_required(self):
        try:
            verify_permissions(self.rw_path, required_permissions='rw')
        except Exception:
            self.fail("requiring 'rw' on read-write file should not fail")

        with self.assertRaises(PermissionError):
            verify_permissions(self.ro_path, required_permissions='rw')

        try:
            verify_permissions(self.new_path, required_permissions='rw')
        except Exception:
            self.fail("requiring 'rw' to create a new file should not fail")

    def test_none_required(self):
        try:
            verify_permissions(self.rw_path, required_permissions=None)
        except Exception:
            self.fail('requiring None on read-write file should not fail')

        try:
            verify_permissions(self.ro_path, required_permissions=None)
        except Exception:
            self.fail('requiring None on read-only file should not fail')

        try:
            verify_permissions(self.new_path, required_permissions=None)
        except Exception:
            self.fail('requiring None on a new file should not fail')

    def test_bad_permissions(self):
        regex = "must be 'ro', 'rw', or None"
        with self.assertRaisesRegex(ValueError, regex):
            verify_permissions(self.rw_path, required_permissions='badpermissions')


class TestDataConnector(unittest.TestCase):
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

    def test_acquire_connection_in_memory(self):
        """Connection should be same instance as _in_memory_connection."""
        connector = DataConnector()
        con = connector.acquire_connection()  # <- Method under test.
        self.addCleanup(super(ToronSqlite3Connection, con).close)

        self.assertIs(con, connector._in_memory_connection)
        self.assertIsNone(connector._current_working_path)

    def test_acquire_connection_on_drive(self):
        """Connection's file should match _current_working_path."""
        connector = DataConnector(cache_to_drive=True)
        con = connector.acquire_connection()  # <- Method under test.
        try:
            cur = con.execute('PRAGMA database_list')
            _, _, file = cur.fetchone()  # Row contains `seq`, `name`, and `file`.

            self.assertEqual(
                os.path.realpath(file),
                os.path.realpath(connector._current_working_path),
                msg='should be the same file',
            )
            self.assertIsNone(connector._in_memory_connection)
        finally:
            super(ToronSqlite3Connection, con).close()

    def test_release_connection_in_memory(self):
        """Connection to temporary database should remain open."""
        connector = DataConnector()
        con = connector.acquire_connection()
        connector.release_connection(con)  # <- Method under test.

        try:
            con.execute('SELECT 1')
        except sqlite3.ProgrammingError as err:
            if 'closed database' not in str(err):
                raise  # Re-raise error if it's something else.
            self.fail('the connection should not be closed')

    def test_release_connection_on_drive(self):
        """Connection to persistent database should be closed."""
        connector = DataConnector(cache_to_drive=True)
        con = connector.acquire_connection()
        connector.release_connection(con)  # <- Method under test.

        regex = 'closed database'
        with self.assertRaisesRegex(sqlite3.ProgrammingError, regex):
            con.execute('SELECT 1')


class TestToFile(unittest.TestCase):
    def setUp(cls):
        cls.temp_dir = tempfile.TemporaryDirectory(prefix='toron-')
        cls.addCleanup(cls.temp_dir.cleanup)

    def test_file_permissions(self):
        """Check that `to_file()` saves with default permissions.

        Internally, ``to_file()`` uses ``tempfile.NamedTemporaryFile``
        which does not use the default permissions so we need to make
        sure that this is being handled properly.
        """
        # Create sample file using process' default permissions.
        sample_path = os.path.join(self.temp_dir.name, 'sample_file.txt')
        with open(sample_path, 'w') as f:
            f.write('Hello World')

        # Create node file.
        node_path = os.path.join(self.temp_dir.name, 'node_file.toron')
        DataConnector().to_file(node_path)

        self.assertEqual(
            oct(os.stat(node_path).st_mode),
            oct(os.stat(sample_path).st_mode),
            msg='node file should have the same drive permissions as the sample',
        )


class TestFromLiveData(unittest.TestCase):
    def setUp(cls):
        cls.temp_dir = tempfile.TemporaryDirectory(prefix='toron-')
        cls.addCleanup(cls.temp_dir.cleanup)

        if sys.version_info < (3, 7, 17):
            # Fix for old bug https://github.com/python/cpython/issues/70847
            def make_files_readwrite():
                root_dir = cls.temp_dir.name
                for f in os.listdir(root_dir):
                    f_path = os.path.join(root_dir, f)
                    os.chmod(f_path, stat.S_IRUSR | stat.S_IWUSR)

            cls.addCleanup(make_files_readwrite)

    def test_new_file_readwrite(self):
        """In read-write mode, nodes can be created directly on drive."""
        new_path = os.path.join(self.temp_dir.name, 'new_node.toron')
        self.assertFalse(os.path.isfile(new_path))

        connector = DataConnector.attach_to_file(new_path, 'rw')
        del connector
        gc.collect()  # Explicitly trigger full garbage collection.

        msg = 'file must persist on drive'
        self.assertTrue(os.path.isfile(new_path), msg=msg)

    def test_new_file_readonly(self):
        """In read-only mode, nodes must already exist--cannot be created."""
        new_path = os.path.join(self.temp_dir.name, 'new_node.toron')
        self.assertFalse(os.path.isfile(new_path))

        with self.assertRaises(FileNotFoundError):
            connector = DataConnector.attach_to_file(new_path, 'ro')

    def test_existing_readwrite(self):
        file_path = os.path.join(self.temp_dir.name, 'mynode.toron')
        DataConnector().to_file(file_path)  # Create a new node and save to drive.

        try:
            connector = DataConnector.attach_to_file(file_path, 'rw')
        except Exception:
            self.fail("read-write file should open with 'rw' permissions")

        os.chmod(file_path, stat.S_IRUSR)  # Set to read-only.
        regex = 'should be read-write but has read-only permissions'
        with self.assertRaisesRegex(PermissionError, regex):
            connector = DataConnector.attach_to_file(file_path, 'rw')

    def test_existing_readonly(self):
        file_path = os.path.join(self.temp_dir.name, 'mynode.toron')
        DataConnector().to_file(file_path)  # Create a new node and save to drive.

        regex = 'should be read-only but has read-write permissions'
        with self.assertRaisesRegex(PermissionError, regex):
            connector = DataConnector.attach_to_file(file_path, 'ro')

        os.chmod(file_path, stat.S_IRUSR)  # Set to read-only.
        try:
            connector = DataConnector.attach_to_file(file_path, 'ro')
        except Exception:
            self.fail("read-only file should open with 'ro' permissions")

    def test_acquire_connection_readonly(self):
        """Connections from a read-only connector should remain
        read-only even if drive permissions on the underlying file
        are changed after instantiation.
        """
        # Create a new node, save to drive, and set permissions to read-only.
        file_path = os.path.join(self.temp_dir.name, 'mynode.toron')
        DataConnector().to_file(file_path)
        os.chmod(file_path, stat.S_IRUSR)

        # Create a connector requiring read-only permissions.
        connector = DataConnector.attach_to_file(file_path, required_permissions='ro')

        # Change on-drive permissions to read-write.
        os.chmod(file_path, stat.S_IRUSR | stat.S_IWUSR)

        # Try to write data to the node (should fail even though the
        # on-drive permissions now allow writing).
        con = connector.acquire_connection()
        try:
            regex='attempt to write a readonly database'
            with self.assertRaisesRegex(sqlite3.OperationalError, regex):
                con.execute("INSERT INTO property VALUES ('my_key', '\"my_value\"')")
        finally:
            connector.release_connection(con)


class TestTransactionMethods(unittest.TestCase):
    def setUp(self):
        self.connector = DataConnector()

    def test_transaction_begin(self):
        cursor = Mock()
        self.connector.transaction_begin(cursor)
        self.assertEqual(cursor.mock_calls, [call.execute('BEGIN TRANSACTION')])

    def test_transaction_rollback(self):
        cursor = Mock()
        self.connector.transaction_rollback(cursor)
        self.assertEqual(cursor.mock_calls, [call.execute('ROLLBACK TRANSACTION')])

    def test_transaction_commit(self):
        cursor = Mock()
        self.connector.transaction_commit(cursor)
        self.assertEqual(cursor.mock_calls, [call.execute('COMMIT TRANSACTION')])
