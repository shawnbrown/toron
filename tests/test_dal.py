"""Tests for toron/_dal.py module."""

import gc
import itertools
import os
import json
import sqlite3
import stat
import tempfile
import unittest
from collections import OrderedDict
from stat import S_IRUSR, S_IWUSR
from textwrap import dedent

from .common import get_column_names
from .common import TempDirTestCase

from toron._schema import get_connection
from toron._schema import _schema_script
from toron._schema import _add_functions_and_triggers
from toron._selectors import SimpleSelector
from toron._dal import DataAccessLayer
from toron._dal import DataAccessLayerPre24
from toron._dal import DataAccessLayerPre25
from toron._dal import DataAccessLayerPre35
from toron._dal import dal_class
from toron._dal import _temp_files_to_delete_atexit
from toron._utils import (
    ToronError,
    ToronWarning,
    QuantityIterator,
)


SQLITE_VERSION_INFO = sqlite3.sqlite_version_info


def get_dal_filepath(dal):
    """Helper function returns path of DAL's db file (if any)."""
    if hasattr(dal, '_connection'):
        con = dal._connection
    elif dal._absolute_working_path:
        con = sqlite3.connect(dal._absolute_working_path)
    else:
        raise Exception(f'cannot get connection from data access layer: {dal}')

    cur = con.execute('PRAGMA database_list')
    _, name, file = cur.fetchone()  # Row contains `seq`, `name`, and `file`.
    if name != 'main':
        raise Exception(f"expected 'main' database: got {name!r}")
    return file


class TestDataAccessLayerInit(TempDirTestCase):
    def setUp(self):
        self.addCleanup(self.cleanup_temp_files)

    def test_load_in_memory(self):
        dal = dal_class()  # <- Loads into memory.

        # Check file path of underlying database (should be blank).
        filepath = get_dal_filepath(dal)
        self.assertEqual(filepath, '', msg='expecting empty string for in-memory DAL')

        # Check for DAL functionality.
        result = dal.get_data(['toron_schema_version'])
        expected = {'toron_schema_version': '0.1.0'}
        self.assertEqual(result, expected)

    def test_cache_to_drive(self):
        dal = dal_class(cache_to_drive=True)  # <- Writes to temporary file.

        # Check file path of underlying database.
        filepath = get_dal_filepath(dal)
        tempdir = tempfile.gettempdir().replace('\\', '\\\\')  # Escape any "\" chars for regex.
        regex = f'^{tempdir}.+\\.toron$'
        self.assertRegex(filepath, regex, msg='expecting tempfile path for on-drive DAL')

        # Check for DAL functionality.
        result = dal.get_data(['toron_schema_version'])
        expected = {'toron_schema_version': '0.1.0'}
        self.assertEqual(result, expected)


class TestUniqueId(TempDirTestCase):
    """On creation, each node should get its own unique id value."""
    def setUp(self):
        self.addCleanup(self.cleanup_temp_files)

    def test_unique_id_basics(self):
        """Check basic unique id behavior."""
        dal1 = dal_class()
        dal2 = dal_class()

        # Currently Toron unique IDs are generated as UUIDs.
        uuid_regex = r'[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}'
        self.assertRegex(dal1.unique_id, uuid_regex)
        self.assertRegex(dal2.unique_id, uuid_regex)

        self.assertNotEqual(dal1.unique_id, dal2.unique_id)

    def test_persistence(self):
        """A node's unique ID must only be generated once (at creation)."""
        file_path = 'tempnode.toron'

        dal = dal_class()
        initial_value = dal.unique_id
        dal.to_file(file_path)  # Node is persisted to drive.
        del dal

        # Load node from drive.
        dal = dal_class.from_file(file_path)
        reloaded_value = dal.unique_id

        msg = 'Unique ID from reloaded node should match the initial unique ID'
        self.assertEqual(initial_value, reloaded_value, msg=msg)


class TestDataAccessLayerFromFile(TempDirTestCase):
    def setUp(self):
        self.existing_path = 'existing_node.toron'
        con = get_connection(self.existing_path, 'readwrite')
        params = ('testkey', '"testval"')
        con.execute("INSERT INTO main.property(key, value) VALUES(?, ?)", params)
        con.close()
        self.addCleanup(self.cleanup_temp_files)

    def test_load_in_memory(self):
        # Load data from file.
        dal = dal_class.from_file(self.existing_path)  # <- Defaults to in-memory.

        # Check that `dal` is using an in-memory connection.
        filepath = get_dal_filepath(dal)
        self.assertEqual(filepath, '', msg='should be empty string for in-memory db')

        # For in-memory connections, path is unused.
        self.assertIsNone(dal._absolute_working_path)

        # Check that node contains test value.
        value = dal.get_data(['testkey'])
        expected = {'testkey': 'testval'}
        self.assertEqual(value, expected)

    def test_cache_to_drive(self):
        # Load data from file.
        dal = dal_class.from_file(self.existing_path, cache_to_drive=True)  # <- Cached in temp file.

        # Check that `dal` is using an on-drive connection.
        filepath = get_dal_filepath(dal)
        self.assertRegex(filepath, '.toron$', msg="temp file should use '.toron' suffix")

        # For on-drive connections, path and node are used.
        self.assertIsNotNone(dal._absolute_working_path)
        self.assertEqual(dal._required_permissions, 'readwrite')

        # Check that node contains test value.
        value = dal.get_data(['testkey'])
        expected = {'testkey': 'testval'}
        self.assertEqual(value, expected)

    def test_nonexistent_file(self):
        with self.assertRaises(ToronError):
            dal_class.from_file('nonexistent_file.toron')

    def test_del_behavior(self):
        dal = dal_class.from_file(self.existing_path, cache_to_drive=True)
        path = dal._absolute_working_path

        self.assertIn(path, _temp_files_to_delete_atexit)

        dal.__del__()
        self.assertNotIn(path, _temp_files_to_delete_atexit)

    def test_atexit_behavior(self):
        class DummyDataAccessLayer(dal_class):
            def __del__(self):
                pass  # <- Dummy method takes no action.

        dal = DummyDataAccessLayer.from_file(self.existing_path, cache_to_drive=True)
        path = dal._absolute_working_path

        dal.__del__()
        self.assertIn(path, _temp_files_to_delete_atexit)

        # The `_delete_leftover_temp_files()` function will raise
        # a RuntimeWarning after tests complete if a file cannot be
        # removed.


class TestDataAccessLayerOpen(TempDirTestCase):
    def setUp(self):
        self.existing_path = 'existing_node.toron'
        get_connection(self.existing_path, None).close()  # Create empty Toron node file.
        self.addCleanup(self.cleanup_temp_files)

        os.chmod(self.existing_path, S_IRUSR)  # Set to read-only.
        self.addCleanup(lambda: os.chmod(self.existing_path, S_IRUSR|S_IWUSR))  # Revert to read-write after test.

    def test_readwrite_new(self):
        """In readwrite mode, nodes can be created directly on drive."""
        new_path = 'new_node.toron'
        self.assertFalse(os.path.isfile(new_path))

        dal = dal_class.open(new_path, 'readwrite')
        with dal._transaction() as cur:
            pass  # Dummy transaction to test connectivity.
        del dal
        gc.collect()  # Explicitly trigger full garbage collection.

        msg = 'data should persist as a file on drive'
        self.assertTrue(os.path.isfile(new_path), msg=msg)

    def test_readwrite_existing(self):
        self.assertTrue(os.path.isfile(self.existing_path))

        msg = "required 'readwrite' will fail when file is 'readonly'"
        with self.assertRaises(PermissionError, msg=msg):
            dal_class.open(self.existing_path, 'readwrite')

        os.chmod(self.existing_path, S_IRUSR|S_IWUSR)  # Set read-write permissions.
        dal = dal_class.open(self.existing_path, 'readwrite')
        with dal._transaction() as cur:
            pass  # Dummy transaction to test connectivity.

    def test_readonly_new(self):
        """In readonly mode, nodes must already exist--cannot be created."""
        new_path = 'new_node.toron'
        self.assertFalse(os.path.isfile(new_path))

        with self.assertRaises(ToronError):
            dal_class.open(new_path)  # <- Defaults to required_permissions='readonly'.

    def test_readonly_existing(self):
        self.assertTrue(os.path.isfile(self.existing_path))
        dal = dal_class.open(self.existing_path)  # <- Defaults to mode='readonly'.
        with dal._transaction() as cur:
            pass  # Dummy transaction to test connectivity.

    def test_bad_permissions(self):
        with self.assertRaises(ToronError):
            dal_class.open(self.existing_path, 'badpermissions')


class TestDataAccessLayerToFile(TempDirTestCase):
    def setUp(self):
        self.addCleanup(self.cleanup_temp_files)

    @staticmethod
    def make_dummy_dal(**properties):
        dal = dal_class()
        con = dal._get_connection()
        sql = "INSERT INTO main.property (key, value) VALUES(?, ?)"
        params = ((k, json.dumps(v)) for k, v in properties.items())
        con.executemany(sql, params)
        return dal

    def test_from_memory(self):
        dal1 = self.make_dummy_dal(testkey='testvalue')

        file_path = 'mynode.toron'

        self.assertFalse(os.path.isfile(file_path))

        dal1.to_file(file_path)
        self.assertTrue(os.path.isfile(file_path))

        dal2 = dal_class.from_file(file_path)
        expected = dal2.get_data(['testkey'])
        self.assertEqual(expected, {'testkey': 'testvalue'})

    def test_replace_existing(self):
        file_path = 'mynode.toron'

        # Create a new file.
        with open(file_path, 'wt') as f:
            f.write('original content\n')

        # Verify that file exists.
        self.assertTrue(os.path.isfile(file_path))

        # Create a dummy DAL and overwrite the existing file.
        dal1 = self.make_dummy_dal(testkey='was overwritten')
        dal1.to_file(file_path)  # <- Should overwrite existing file.

        # Verify that file was overwritten.
        dal2 = dal_class.from_file(file_path)
        expected = dal2.get_data(['testkey'])
        self.assertEqual(expected, {'testkey': 'was overwritten'})

    def test_readonly_failure(self):
        """If the destination file is read-only, should fail."""
        file_path = 'mynode.toron'

        # Create a new file and set it to read-only permissions.
        with open(file_path, 'wt') as f:
            f.write('original content\n')
        os.chmod(file_path, stat.S_IREAD)

        # Re-enable write permissions during clean-up.
        self.addCleanup(lambda: os.chmod(file_path, stat.S_IWRITE))

        # Verify read-only status.
        self.assertFalse(os.access(file_path, os.W_OK), msg='expecting read-only')

        # Check that method raises an error if destination is read-only.
        regex = "The file '.*mynode.toron' is read-only."
        with self.assertRaisesRegex(PermissionError, regex):
            dal = self.make_dummy_dal(testkey='testvalue')
            dal.to_file(file_path)  # <- Method under test.

        # Verify that existing file is unchanged.
        with open(file_path) as f:
            self.assertEqual(f.read(), 'original content\n')


class TestTransaction(TempDirTestCase):
    """Tests for the _transaction() context manager.

    When DAL is backed with a file on-drive, the _transaction()
    method should establish its own connection and then close this
    connection once it is finished.
    """
    def setUp(self):
        self.addCleanup(self.cleanup_temp_files)

    def assertCursorOpen(self, cursor, msg=None):
        try:
            cursor.execute('SELECT 1')
        except sqlite3.ProgrammingError:
            self.fail(msg or 'cursor is not open')
        cursor.fetchall()  # Discard query result.

    def assertCursorClosed(self, cursor, msg=None):
        try:
            cursor.execute('SELECT 1')
        except sqlite3.ProgrammingError:
            return
        cursor.fetchall()  # Discard query result.
        self.fail(msg or 'cursor is not closed')

    def assertConnectionOpen(self, connection, msg=None):
        try:
            cur = connection.cursor()
        except sqlite3.ProgrammingError:
            self.fail(msg or 'connection is not open')
        cur.close()  # Close unused cursor.

    def assertConnectionClosed(self, connection, msg=None):
        try:
            cur = connection.cursor()
        except sqlite3.ProgrammingError:
            return
        cur.close()  # Close unused cursor.
        self.fail(msg or 'connection is not closed')

    def test_transaction_commit(self):
        dal = dal_class()

        with dal._transaction() as cursor:
            cursor.execute("""INSERT INTO property VALUES ('key1', '"value1"')""")

        con = dal._get_connection()
        result = con.execute("SELECT * FROM property WHERE key='key1'").fetchone()
        msg = 'successful transaction should commit changes to database'
        self.assertEqual(result, ('key1', 'value1'), msg=msg)

    def test_transaction_rollback(self):
        dal = dal_class()

        with self.assertRaises(sqlite3.IntegrityError):
            with dal._transaction() as cursor:
                cursor.execute("""INSERT INTO property VALUES ('key1', '"value1"')""")  # <- Success.
                cursor.execute("""INSERT INTO property VALUES ('key2', 'bad json')""")  # <- Failure.

        con = dal._get_connection()
        result = con.execute("SELECT * FROM property WHERE key='key1'").fetchone()
        msg = 'a failed transaction should rollback all changes to the database'
        self.assertEqual(result, None, msg=msg)

    def test_tempfile_on_drive(self):
        """When using an on-drive file, _transaction() should establish
        its own connection and then close this connection once it is
        finished.
        """
        dal = dal_class(cache_to_drive=True)  # Create new tempfile.

        cm = dal._transaction()  # Instantiate context manager.

        cur = cm.__enter__()  # Enter context (returns cursor).
        self.assertCursorOpen(cur)

        con = cur.connection
        self.assertConnectionOpen(con)

        cm.__exit__(None, None, None)  # Exit context.
        self.assertCursorClosed(cur)
        self.assertConnectionClosed(con)

    def test_existing_file_on_drive(self):
        """When using an on-drive file, _transaction() should establish
        its own connection and then close this connection once it is
        finished.
        """
        dal_class().to_file('mynode.toron')  # Create file on drive.
        dal = dal_class.open('mynode.toron', 'readwrite')  # Open existing file.

        cm = dal._transaction()  # Instantiate context manager.

        cur = cm.__enter__()  # Enter context (returns cursor).
        self.assertCursorOpen(cur)

        con = cur.connection
        self.assertConnectionOpen(con)

        cm.__exit__(None, None, None)  # Exit context.
        self.assertCursorClosed(cur)
        self.assertConnectionClosed(con)

    def test_persistent_inmemory_connection(self):
        """When given a in-memory Connection, transaction() should use
        the connection as provided and leave it open when finished.
        """
        dal = dal_class()

        cm = dal._transaction()  # Instantiate context manager.

        cur = cm.__enter__()  # Enter context (returns cursor).
        self.assertCursorOpen(cur)

        con = cur.connection
        self.assertConnectionOpen(con)

        cm.__exit__(None, None, None)  # Exit context.
        self.assertCursorClosed(cur)
        self.assertConnectionOpen(con, msg='connection should remain open')


class TestAddIndexColumnsMakeSql(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.con = get_connection(':memory:', None)
        self.cur = self.con.cursor()
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def test_add_index_columns_to_new(self):
        """Add columns to new/empty node database."""
        statements = DataAccessLayer._add_index_columns_make_sql(self.cur, ['state', 'county'])
        expected = [
            'DROP INDEX IF EXISTS main.unique_nodeindex_index',
            'DROP INDEX IF EXISTS main.unique_location_index',
            'DROP INDEX IF EXISTS main.unique_structure_index',
            'ALTER TABLE main.node_index ADD COLUMN "state" TEXT NOT NULL CHECK ("state" != \'\') DEFAULT \'-\'',
            'ALTER TABLE main.location ADD COLUMN "state" TEXT NOT NULL DEFAULT \'\'',
            'ALTER TABLE main.structure ADD COLUMN "state" INTEGER CHECK ("state" IN (0, 1)) DEFAULT 0',
            'ALTER TABLE main.node_index ADD COLUMN "county" TEXT NOT NULL CHECK ("county" != \'\') DEFAULT \'-\'',
            'ALTER TABLE main.location ADD COLUMN "county" TEXT NOT NULL DEFAULT \'\'',
            'ALTER TABLE main.structure ADD COLUMN "county" INTEGER CHECK ("county" IN (0, 1)) DEFAULT 0',
            'CREATE UNIQUE INDEX main.unique_nodeindex_index ON node_index("state", "county")',
            'CREATE UNIQUE INDEX main.unique_location_index ON location("state", "county")',
            'CREATE UNIQUE INDEX main.unique_structure_index ON structure("state", "county")',
        ]
        self.assertEqual(statements, expected)

    def test_add_index_columns_to_exsting(self):
        """Add columns to database with existing node index columns."""
        # Add initial index columns.
        statements = DataAccessLayer._add_index_columns_make_sql(self.cur, ['state', 'county'])
        for stmnt in statements:
            self.cur.execute(stmnt)

        # Add attitional index columns.
        statements = DataAccessLayer._add_index_columns_make_sql(self.cur, ['tract', 'block'])
        expected = [
            'DROP INDEX IF EXISTS main.unique_nodeindex_index',
            'DROP INDEX IF EXISTS main.unique_location_index',
            'DROP INDEX IF EXISTS main.unique_structure_index',
            'ALTER TABLE main.node_index ADD COLUMN "tract" TEXT NOT NULL CHECK ("tract" != \'\') DEFAULT \'-\'',
            'ALTER TABLE main.location ADD COLUMN "tract" TEXT NOT NULL DEFAULT \'\'',
            'ALTER TABLE main.structure ADD COLUMN "tract" INTEGER CHECK ("tract" IN (0, 1)) DEFAULT 0',
            'ALTER TABLE main.node_index ADD COLUMN "block" TEXT NOT NULL CHECK ("block" != \'\') DEFAULT \'-\'',
            'ALTER TABLE main.location ADD COLUMN "block" TEXT NOT NULL DEFAULT \'\'',
            'ALTER TABLE main.structure ADD COLUMN "block" INTEGER CHECK ("block" IN (0, 1)) DEFAULT 0',
            'CREATE UNIQUE INDEX main.unique_nodeindex_index ON node_index("state", "county", "tract", "block")',
            'CREATE UNIQUE INDEX main.unique_location_index ON location("state", "county", "tract", "block")',
            'CREATE UNIQUE INDEX main.unique_structure_index ON structure("state", "county", "tract", "block")',
        ]
        self.assertEqual(statements, expected)

    def test_no_columns_to_add(self):
        """When there are no new columns to add, should return empty list."""
        # Add initial index columns.
        statements = DataAccessLayer._add_index_columns_make_sql(self.cur, ['state', 'county'])
        for stmnt in statements:
            self.cur.execute(stmnt)

        # When there are no new columns to add, should return empty list.
        statements = DataAccessLayer._add_index_columns_make_sql(self.cur, ['state', 'county'])  # <- Columns already exist.
        self.assertEqual(statements, [])

    def test_duplicate_column_input(self):
        regex = 'duplicate column name: "county"'
        with self.assertRaisesRegex(ValueError, regex):
            DataAccessLayer._add_index_columns_make_sql(self.cur, ['state', 'county', 'county'])

    def test_normalization_duplicate_column_input(self):
        regex = 'duplicate column name: "county"'
        with self.assertRaisesRegex(ValueError, regex):
            columns = [
                'state',
                'county    ',  # <- Normalized to "county", collides with duplicate.
                'county',
            ]
            DataAccessLayer._add_index_columns_make_sql(self.cur, columns)

    def test_normalization_collision_with_existing(self):
        """Columns should be checked for collisions after normalizing."""
        # Add initial index columns.
        for stmnt in DataAccessLayer._add_index_columns_make_sql(self.cur, ['state', 'county']):
            self.cur.execute(stmnt)

        # Prepare attitional index columns.
        columns = [
            'state     ',  # <- Normalized to "state", which then gets skipped.
            'county    ',  # <- Normalized to "county", which then gets skipped.
            'tract     ',
        ]
        statements = DataAccessLayer._add_index_columns_make_sql(self.cur, columns)

        expected = [
            'DROP INDEX IF EXISTS main.unique_nodeindex_index',
            'DROP INDEX IF EXISTS main.unique_location_index',
            'DROP INDEX IF EXISTS main.unique_structure_index',
            'ALTER TABLE main.node_index ADD COLUMN "tract" TEXT NOT NULL CHECK ("tract" != \'\') DEFAULT \'-\'',
            'ALTER TABLE main.location ADD COLUMN "tract" TEXT NOT NULL DEFAULT \'\'',
            'ALTER TABLE main.structure ADD COLUMN "tract" INTEGER CHECK ("tract" IN (0, 1)) DEFAULT 0',
            'CREATE UNIQUE INDEX main.unique_nodeindex_index ON node_index("state", "county", "tract")',
            'CREATE UNIQUE INDEX main.unique_location_index ON location("state", "county", "tract")',
            'CREATE UNIQUE INDEX main.unique_structure_index ON structure("state", "county", "tract")',
        ]
        msg = 'should only add "tract" because "state" and "county" already exist'
        self.assertEqual(statements, expected, msg=msg)

    def test_column_id_collision(self):
        regex = 'column name not allowed: "_location_id"'
        with self.assertRaisesRegex(ValueError, regex):
            DataAccessLayer._add_index_columns_make_sql(self.cur, ['state', '_location_id'])


class TestAddColumns(unittest.TestCase):
    def test_add_index_columns(self):
        """Check that columns are added to appropriate tables."""
        dal = dal_class()
        dal.set_data({'add_index_columns': ['state', 'county']})  # <- Add columns.

        con = dal._connection

        columns = get_column_names(con, 'node_index')
        self.assertEqual(columns, ['index_id', 'state', 'county'])

        columns = get_column_names(con, 'location')
        self.assertEqual(columns, ['_location_id', 'state', 'county'])

        columns = get_column_names(con, 'structure')
        self.assertEqual(columns, ['_structure_id', '_granularity', 'state', 'county'])

        con = dal._get_connection()
        cur = con.execute('SELECT * FROM structure')
        actual = {row[2:] for row in cur.fetchall()}
        self.assertEqual(actual, {(0, 0), (1, 1)})

    def test_set_data_order(self):
        """The set_data() method should run 'add_index_columns' items first."""
        dal = dal_class()

        mapping = OrderedDict([
            ('structure', [{'state'}, {'county'}, {'state', 'county'}]),
            ('add_index_columns', ['state', 'county']),
        ])

        try:
            dal.set_data(mapping)  # <- Should pass without error.
        except ToronError as err:
            if 'must first add columns' not in str(err):
                raise
            msg = "should run 'add_index_columns' first, regardless of mapping order"
            self.fail(msg)


class TestRenameIndexColumnsApplyMapper(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()
        self.dal.set_data({'add_index_columns': ['state', 'county', 'town']})
        self.con = self.dal._connection
        self.cur = self.con.cursor()
        self.addCleanup(self.cur.close)

    def test_mapper_callable(self):
        mapper = str.upper  # <- Callable mapper.
        result = self.dal._rename_index_columns_apply_mapper(self.cur, mapper)
        column_names, new_column_names = result  # Unpack result tuple
        self.assertEqual(column_names, ['"state"', '"county"', '"town"'])
        self.assertEqual(new_column_names, ['"STATE"', '"COUNTY"', '"TOWN"'])

    def test_mapper_dict(self):
        mapper = {'state': 'stusab', 'town': 'place'}  # <- Dict mapper.
        result = self.dal._rename_index_columns_apply_mapper(self.cur, mapper)
        column_names, new_column_names = result  # Unpack result tuple
        self.assertEqual(column_names, ['"state"', '"county"', '"town"'])
        self.assertEqual(new_column_names, ['"stusab"', '"county"', '"place"'])

    def test_mapper_bad_type(self):
        mapper = ['state', 'stusab']  # <- Bad mapper type.
        with self.assertRaises(ValueError):
            result = self.dal._rename_index_columns_apply_mapper(self.cur, mapper)

    def test_name_collision(self):
        regex = 'column name collisions: "(state|town)"->"XXXX", "(town|state)"->"XXXX"'
        with self.assertRaisesRegex(ValueError, regex):
            mapper = {'state': 'XXXX', 'county': 'COUNTY', 'town': 'XXXX'}
            result = self.dal._rename_index_columns_apply_mapper(self.cur, mapper)

    def test_name_collision_from_normalization(self):
        regex = 'column name collisions: "(state|town)"->"A B", "(town|state)"->"A B"'
        with self.assertRaisesRegex(ValueError, regex):
            mapper = {'state': 'A\t\tB', 'town': 'A    B    '}  # <- Gets normalized.
            result = self.dal._rename_index_columns_apply_mapper(self.cur, mapper)


class TestRenameIndexColumnsMakeSql(unittest.TestCase):
    def setUp(self):
        self.column_names = ['"state"', '"county"', '"town"']
        self.new_column_names = ['"stusab"', '"county"', '"place"']

    @unittest.skipIf(SQLITE_VERSION_INFO < (3, 25, 0), 'requires 3.25.0 or newer')
    def test_native_rename_index_column_support(self):
        """Test native RENAME COLUMN statements."""
        sql = DataAccessLayer._rename_index_columns_make_sql(self.column_names, self.new_column_names)
        expected = [
            'ALTER TABLE main.node_index RENAME COLUMN "state" TO "stusab"',
            'ALTER TABLE main.location RENAME COLUMN "state" TO "stusab"',
            'ALTER TABLE main.structure RENAME COLUMN "state" TO "stusab"',
            'ALTER TABLE main.node_index RENAME COLUMN "town" TO "place"',
            'ALTER TABLE main.location RENAME COLUMN "town" TO "place"',
            'ALTER TABLE main.structure RENAME COLUMN "town" TO "place"',
        ]
        self.assertEqual(sql, expected)

    def test_pre25_without_native_rename(self):
        """Test legacy column-rename statements for workaround procedure."""
        sql = DataAccessLayerPre25._rename_index_columns_make_sql(self.column_names, self.new_column_names)
        expected = [
            'CREATE TABLE main.new_nodeindex(index_id INTEGER PRIMARY KEY AUTOINCREMENT, "stusab" TEXT NOT NULL CHECK ("stusab" != \'\') DEFAULT \'-\', "county" TEXT NOT NULL CHECK ("county" != \'\') DEFAULT \'-\', "place" TEXT NOT NULL CHECK ("place" != \'\') DEFAULT \'-\')',
            'INSERT INTO main.new_nodeindex SELECT index_id, "state", "county", "town" FROM main.node_index',
            'DROP TABLE main.node_index',
            'ALTER TABLE main.new_nodeindex RENAME TO node_index',
            'CREATE TABLE main.new_location(_location_id INTEGER PRIMARY KEY, "stusab" TEXT NOT NULL DEFAULT \'\', "county" TEXT NOT NULL DEFAULT \'\', "place" TEXT NOT NULL DEFAULT \'\')',
            'INSERT INTO main.new_location SELECT _location_id, "state", "county", "town" FROM main.location',
            'DROP TABLE main.location',
            'ALTER TABLE main.new_location RENAME TO location',
            'CREATE TABLE main.new_structure(_structure_id INTEGER PRIMARY KEY, _granularity REAL, "stusab" INTEGER CHECK ("stusab" IN (0, 1)) DEFAULT 0, "county" INTEGER CHECK ("county" IN (0, 1)) DEFAULT 0, "place" INTEGER CHECK ("place" IN (0, 1)) DEFAULT 0)',
            'INSERT INTO main.new_structure SELECT _structure_id, _granularity, "state", "county", "town" FROM main.structure',
            'DROP TABLE main.structure',
            'ALTER TABLE main.new_structure RENAME TO structure',
            'CREATE UNIQUE INDEX main.unique_nodeindex_index ON node_index("stusab", "county", "place")',
            'CREATE UNIQUE INDEX main.unique_location_index ON location("stusab", "county", "place")',
            'CREATE UNIQUE INDEX main.unique_structure_index ON structure("stusab", "county", "place")',
        ]
        self.assertEqual(sql, expected)


class TestRenameIndexColumns(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()
        self.dal.set_data({'add_index_columns': ['state', 'county', 'town']})
        self.dal.add_index_records([
            ('state', 'county', 'town'),
            ('CA', 'SAN DIEGO', 'CORONADO'),
            ('IN', 'GRANT', 'MARION'),
            ('CA', 'MARIN', 'SAN RAFAEL'),
            ('CA', 'MARIN', 'SAUSALITO'),
            ('AR', 'MILLER', 'TEXARKANA'),
            ('TX', 'BOWIE', 'TEXARKANA'),
        ])
        self.con = self.dal._connection
        self.cur = self.con.cursor()
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def run_rename_test(self, rename_index_columns_func):
        columns_before_rename = get_column_names(self.cur, 'node_index')
        self.assertEqual(columns_before_rename, ['index_id', 'state', 'county', 'town'])

        data_before_rename = \
            self.cur.execute('SELECT state, county, town FROM node_index').fetchall()

        mapper = {'state': 'stusab', 'town': 'place'}
        rename_index_columns_func(self.dal, mapper)  # <- Rename columns!

        columns_after_rename = get_column_names(self.cur, 'node_index')
        self.assertEqual(columns_after_rename, ['index_id', 'stusab', 'county', 'place'])

        data_after_rename = \
            self.cur.execute('SELECT stusab, county, place FROM node_index').fetchall()

        self.assertEqual(data_before_rename, data_after_rename)

    @unittest.skipIf(SQLITE_VERSION_INFO < (3, 25, 0), 'requires 3.25.0 or newer')
    def test_rename_index_columns(self):
        """Test the native RENAME COLUMN implementation."""
        self.run_rename_test(DataAccessLayer.rename_index_columns)

    def test_legacy_rename_index_columns(self):
        """Test the alternate legacy implementation."""
        self.run_rename_test(DataAccessLayerPre25.rename_index_columns)

    def test_data_access_layer_rename_index_columns(self):
        """Test the assigned 'dal_class' class."""
        self.run_rename_test(dal_class.rename_index_columns)


class TestRemoveIndexColumnsMakeSql(unittest.TestCase):
    def setUp(self):
        self.column_names = ['"state"', '"county"', '"mcd"', '"place"']
        self.columns_to_remove = ['"mcd"', '"place"']

    @unittest.skipIf(SQLITE_VERSION_INFO < (3, 35, 0), 'requires 3.35.0 or newer')
    def test_native_delete_column_support(self):
        sql_stmnts = DataAccessLayer._remove_index_columns_make_sql(self.column_names, self.columns_to_remove)
        expected = [
            'DROP INDEX IF EXISTS main.unique_nodeindex_index',
            'DROP INDEX IF EXISTS main.unique_location_index',
            'DROP INDEX IF EXISTS main.unique_structure_index',
            'ALTER TABLE main.node_index DROP COLUMN "mcd"',
            'ALTER TABLE main.location DROP COLUMN "mcd"',
            'ALTER TABLE main.structure DROP COLUMN "mcd"',
            'ALTER TABLE main.node_index DROP COLUMN "place"',
            'ALTER TABLE main.location DROP COLUMN "place"',
            'ALTER TABLE main.structure DROP COLUMN "place"',
            'CREATE UNIQUE INDEX main.unique_nodeindex_index ON node_index("state", "county")',
            'CREATE UNIQUE INDEX main.unique_location_index ON location("state", "county")',
            'CREATE UNIQUE INDEX main.unique_structure_index ON structure("state", "county")',
        ]
        self.assertEqual(sql_stmnts, expected)

    def test_pre35_without_native_drop(self):
        """Check SQL of column removal procedure for legacy SQLite."""
        sql_stmnts = DataAccessLayerPre35._remove_index_columns_make_sql(self.column_names, self.columns_to_remove)
        expected = [
            'CREATE TABLE main.new_nodeindex(index_id INTEGER PRIMARY KEY AUTOINCREMENT, "state" TEXT NOT NULL CHECK ("state" != \'\') DEFAULT \'-\', "county" TEXT NOT NULL CHECK ("county" != \'\') DEFAULT \'-\')',
            'INSERT INTO main.new_nodeindex SELECT index_id, "state", "county" FROM main.node_index',
            'DROP TABLE main.node_index',
            'ALTER TABLE main.new_nodeindex RENAME TO node_index',
            'CREATE TABLE main.new_location(_location_id INTEGER PRIMARY KEY, "state" TEXT NOT NULL DEFAULT \'\', "county" TEXT NOT NULL DEFAULT \'\')',
            'INSERT INTO main.new_location SELECT _location_id, "state", "county" FROM main.location',
            'DROP TABLE main.location',
            'ALTER TABLE main.new_location RENAME TO location',
            'CREATE TABLE main.new_structure(_structure_id INTEGER PRIMARY KEY, _granularity REAL, "state" INTEGER CHECK ("state" IN (0, 1)) DEFAULT 0, "county" INTEGER CHECK ("county" IN (0, 1)) DEFAULT 0)',
            'INSERT INTO main.new_structure SELECT _structure_id, _granularity, "state", "county" FROM main.structure',
            'DROP TABLE main.structure',
            'ALTER TABLE main.new_structure RENAME TO structure',
            'CREATE UNIQUE INDEX main.unique_nodeindex_index ON node_index("state", "county")',
            'CREATE UNIQUE INDEX main.unique_location_index ON location("state", "county")',
            'CREATE UNIQUE INDEX main.unique_structure_index ON structure("state", "county")',
        ]
        self.assertEqual(sql_stmnts, expected)


class TestRemoveIndexColumnsMixin(object):
    class_under_test = None  # When subclassing, assign DAL class to test.

    def setUp(self):
        self.dal = self.class_under_test()

        con = self.dal._get_connection()
        self.addCleanup(con.close)

        self.cur = con.cursor()
        self.addCleanup(self.cur.close)

        self.dal.set_data({'add_index_columns': ['state', 'county', 'mcd', 'place']})
        self.dal.add_discrete_categories([
            {'state'},
            {'state', 'county'},
            {'state', 'county', 'mcd'},
        ])

        data = [
            ('state', 'county', 'mcd', 'place', 'population'),
            ('AZ', 'Graham', 'Safford', 'Cactus Flats', 1524),
            ('CA', 'Los Angeles', 'Newhall', 'Val Verde', 2399),
            ('CA', 'Riverside', 'Corona', 'Coronita', 2639),
            ('CA', 'San Benito', 'Hollister', 'Ridgemark', 3212),
            ('IN', 'LaPorte', 'Kankakee', 'Rolling Prairie', 562),
            ('MO', 'Cass', 'Raymore', 'Belton', 6259),
            ('OH', 'Franklin', 'Washington', 'Dublin', 40734),
            ('PA', 'Somerset', 'Somerset', 'Somerset', 6048),
            ('TX', 'Denton', 'Denton', 'Denton', 102631),
            ('TX', 'Cass', 'Atlanta', 'Queen City', 1397),
        ]
        self.dal.add_index_records(data)
        self.dal.add_weights(data, name='population', selectors=None)

    def test_initial_fixture_state(self):
        # Check initial categories.
        data = self.dal.get_data(['discrete_categories'])
        expected = [
            {'state'},
            {'state', 'county'},
            {'state', 'county', 'mcd'},
            {'state', 'county', 'mcd', 'place'},  # <- whole space
        ]
        self.assertEqual(data['discrete_categories'], expected)

        # Check initial structure table.
        self.cur.execute('SELECT * FROM main.structure')
        actual = {row[2:] for row in self.cur.fetchall()}
        expected = {
            (0, 0, 0, 0),  # <- Empty set.
            (1, 0, 0, 0),  # <- {'state'}
            (1, 1, 0, 0),  # <- {'state', 'county'}
            (1, 1, 1, 0),  # <- {'state', 'county', 'mcd'}
            (1, 1, 1, 1),  # <- whole space
        }
        self.assertEqual(actual, expected)

    def test_remove_index_columns(self):
        self.dal.remove_index_columns(['mcd', 'place'])  # <- Method under test.

        # Check rebuilt categories.
        data = self.dal.get_data(['discrete_categories'])
        self.assertEqual(data['discrete_categories'], [{'state'}, {'state', 'county'}])

        # Check rebuild structure table.
        self.cur.execute('SELECT * FROM main.structure')
        actual = {row[2:] for row in self.cur.fetchall()}
        self.assertEqual(actual, {(0, 0), (1, 0), (1, 1)})

        # Check index records and weights.
        actual = self.cur.execute('''
            SELECT a.*, b.weight_value
            FROM node_index a
            JOIN weight b USING (index_id)
            JOIN weighting c USING (weighting_id)
            WHERE c.name='population'
        ''').fetchall()

        expected = [
            (1, 'AZ', 'Graham', 1524),
            (2, 'CA', 'Los Angeles', 2399),
            (3, 'CA', 'Riverside', 2639),
            (4, 'CA', 'San Benito', 3212),
            (5, 'IN', 'LaPorte', 562),
            (6, 'MO', 'Cass', 6259),
            (7, 'OH', 'Franklin', 40734),
            (8, 'PA', 'Somerset', 6048),
            (9, 'TX', 'Denton', 102631),
            (10, 'TX', 'Cass', 1397),
        ]

        self.assertEqual(actual, expected)

    def test_nonmatching_names(self):
        """Non-matching column names should be ignored."""
        self.dal.remove_index_columns(['nomatch1', 'nomatch2'])  # <- Method under test.

    def test_category_violation(self):
        regex = "cannot remove, categories are undefined for remaining columns: 'place'"
        with self.assertRaisesRegex(ToronError, regex):
            self.dal.remove_index_columns(['mcd'])  # <- Method under test.

        regex = "cannot remove, categories are undefined for remaining columns: 'mcd', 'place'"
        with self.assertRaisesRegex(ToronError, regex):
            self.dal.remove_index_columns(['county'])  # <- Method under test.

    def test_granularity_violation(self):
        regex = 'cannot remove, columns are needed to preserve granularity'
        with self.assertRaisesRegex(ToronError, regex):
            self.dal.remove_index_columns(['county', 'mcd', 'place'])  # <- Method under test.

    def test_strategy_restructure(self):
        """The 'restructure' strategy should override category error."""
        self.dal.remove_index_columns(['mcd'], preserve_structure=False)  # <- Method under test.

        # Check rebuilt categories.
        data = self.dal.get_data(['discrete_categories'])
        self.assertEqual(
            data['discrete_categories'],
            [{'state'}, {'county', 'state'}, {'county', 'state', 'place'}],
        )

        # Check rebuilt structure.
        self.cur.execute('SELECT * FROM structure')
        actual = {row[2:] for row in self.cur.fetchall()}
        expected = {
            (0, 0, 0),
            (1, 0, 0),
            (1, 1, 0),
            (1, 1, 1),
        }
        self.assertEqual(actual, expected)

        # Check index records and weights.
        actual = self.cur.execute('''
            SELECT a.*, b.weight_value
            FROM node_index a
            JOIN weight b USING (index_id)
            JOIN weighting c USING (weighting_id)
            WHERE c.name='population'
        ''').fetchall()

        expected = [
            (1, 'AZ', 'Graham', 'Cactus Flats', 1524),
            (2, 'CA', 'Los Angeles', 'Val Verde', 2399),
            (3, 'CA', 'Riverside', 'Coronita', 2639),
            (4, 'CA', 'San Benito', 'Ridgemark', 3212),
            (5, 'IN', 'LaPorte', 'Rolling Prairie', 562),
            (6, 'MO', 'Cass', 'Belton', 6259),
            (7, 'OH', 'Franklin', 'Dublin', 40734),
            (8, 'PA', 'Somerset', 'Somerset', 6048),
            (9, 'TX', 'Denton', 'Denton', 102631),
            (10, 'TX', 'Cass', 'Queen City', 1397),
        ]
        self.assertEqual(actual, expected)

    def test_strategy_coarsen_weights(self):
        """The 'coarsen' strategy should override granularity error."""
        # Get hash of node before coarsening.
        hash_before_coarsening = self.dal._get_data_property(self.cur, 'index_hash')

        # Coarsen the dataset (run the method under test).
        self.dal.remove_index_columns(['county', 'mcd', 'place'], preserve_granularity=False)  # <- Method under test.

        # Check that dataset was altered as expected.
        actual = self.cur.execute('''
            SELECT a.*, b.weight_value
            FROM node_index a
            JOIN weight b USING (index_id)
            JOIN weighting c USING (weighting_id)
            WHERE c.name='population'
        ''').fetchall()
        expected = [
            (1, 'AZ', 1524),
            (2, 'CA', 8250),  # <- Aggregate sum of 3 records.
            (5, 'IN', 562),
            (6, 'MO', 6259),
            (7, 'OH', 40734),
            (8, 'PA', 6048),
            (9, 'TX', 104028),  # <- Aggregate sum of 2 records.
        ]
        self.assertEqual(actual, expected)

        # Check that 'index_hash' was recomputed for the coarsened dataset.
        hash_after_coarsening = self.dal._get_data_property(self.cur, 'index_hash')
        self.assertNotEqual(
            hash_before_coarsening,
            hash_after_coarsening,
            msg='When a node is coarsened, its index hash should be updated.',
        )

    def test_strategy_coarsen_weights_and_quantities(self):
        """The 'coarsen' strategy should override granularity error."""
        data = [
            ('state', 'county',      'attr', 'count'),
            ('AZ',    'Graham',      'foo',  1000),
            ('CA',    'Los Angeles', 'foo',  2000),
            ('CA',    'Riverside',   'bar',  2000),
            ('CA',    'San Benito',  'foo',  3000),
            ('IN',    'LaPorte',     'bar',  500),
            ('MO',    'Cass',        'foo',  6000),
            ('OH',    'Franklin',    'bar',  40000),
            ('PA',    'Somerset',    'baz',  6000),
            ('TX',    'Denton',      'baz',  100000),
            ('TX',    'Cass',        'baz',  1000),
        ]
        self.dal.add_quantities(data, 'count')

        self.dal.remove_index_columns(    # <- Method under test.
            ['county', 'mcd', 'place'],
            preserve_granularity=False,
        )

        actual = self.cur.execute('''
            SELECT a.*, b.attributes, b.quantity_value
            FROM location a
            JOIN quantity b USING (_location_id)
            ORDER BY a._location_id, b.attributes
        ''').fetchall()
        expected = [
            (1, 'AZ', {'attr': 'foo'}, 1000),
            (2, 'CA', {'attr': 'bar'}, 2000),  # <- Gets new _location_id.
            (2, 'CA', {'attr': 'foo'}, 5000),  # <- Combined (2000 + 3000)
            (5, 'IN', {'attr': 'bar'}, 500),
            (6, 'MO', {'attr': 'foo'}, 6000),
            (7, 'OH', {'attr': 'bar'}, 40000),
            (8, 'PA', {'attr': 'baz'}, 6000),
            (9, 'TX', {'attr': 'baz'}, 101000),  # <- Combined (100000 + 1000)
        ]
        self.assertEqual(expected, actual)

    def test_strategy_coarsenrestructure(self):
        """The 'coarsenrestructure' strategy should override both
        granularity and category errors.

        Note: The example result used in this test is nonsensical but
        it does serve to validate the strategy behavior.
        """
        self.dal.remove_index_columns(  # <- Method under test.
            ['state', 'mcd', 'place'],
            preserve_structure=False,
            preserve_granularity=False,
        )

        actual = self.cur.execute('''
            SELECT a.*, b.weight_value
            FROM node_index a
            JOIN weight b USING (index_id)
            JOIN weighting c USING (weighting_id)
            WHERE c.name='population'
        ''').fetchall()

        expected = [
            (1, 'Graham', 1524),
            (2, 'Los Angeles', 2399),
            (3, 'Riverside', 2639),
            (4, 'San Benito', 3212),
            (5, 'LaPorte', 562),
            (6, 'Cass', 7656),  # <- Aggregate sum of 2 records.
            (7, 'Franklin', 40734),
            (8, 'Somerset', 6048),
            (9, 'Denton', 102631),
        ]

        self.assertEqual(actual, expected)

    def test_coarsening_incomplete_weight(self):
        data = [
            ('index_id', 'state', 'new_count'),
            (1, 'AZ', 253),
            # 2 missing.
            # 3 missing.
            (4, 'CA', 121),
            (5, 'IN', 25),
            (6, 'MO', 528),
            (7, 'OH', 7033),
            (8, 'PA', 407),
            (9, 'TX', 6214),
            # 10 missing.
        ]
        self.dal.add_weights(data, name='new_count', selectors=['[foo="bar"]'])

        self.cur.execute("SELECT is_complete FROM weighting WHERE name='new_count'")
        actual = self.cur.fetchone()[0]
        msg = "should be False/0 because it's incomplete"
        self.assertEqual(actual, False, msg=msg)

        self.dal.remove_index_columns(['county', 'mcd', 'place'], preserve_granularity=False)  # <- Method under test.

        self.cur.execute("SELECT is_complete FROM weighting WHERE name='new_count'")
        actual = self.cur.fetchone()[0]
        msg = 'should be True/1 (complete) after coarsening'
        self.assertEqual(actual, True, msg=msg)

        actual = set(self.cur.execute('''
            SELECT a.*, b.weight_value
            FROM node_index a
            JOIN weight b USING (index_id)
            JOIN weighting c USING (weighting_id)
            WHERE c.name='new_count'
        ''').fetchall())

        expected = {
            (1, 'AZ', 253),
            (2, 'CA', 121),  # <- Remaining record uses `index_id` 2.
            # 3 aggregated together with 2.
            # 4 aggregated together with 2.
            (5, 'IN', 25),
            (6, 'MO', 528),
            (7, 'OH', 7033),
            (8, 'PA', 407),
            (9, 'TX', 6214),  # <- Remaining record uses `index_id` 9.
            # 10 aggregated together with 9.
        }
        self.assertEqual(actual, expected)


@unittest.skipIf(SQLITE_VERSION_INFO < (3, 35, 0), 'requires 3.35.0 or newer')
class TestRemoveIndexColumns(TestRemoveIndexColumnsMixin, unittest.TestCase):
    class_under_test = DataAccessLayer


class TestRemoveIndexColumnsLegacy(TestRemoveIndexColumnsMixin, unittest.TestCase):
    class_under_test = DataAccessLayerPre24


class TestRemoveIndexColumnsWithEdgesMixin(object):
    class_under_test = None  # When subclassing, assign DAL class to test.

    def setUp(self):
        self.dal = self.class_under_test()

        con = self.dal._get_connection()
        self.addCleanup(con.close)

        self.cur = con.cursor()
        self.addCleanup(self.cur.close)

    @staticmethod
    def load_data(dal, data, index_cols, categories, weight_col):
        """Helper function to load node data."""
        dal.set_data({'add_index_columns': index_cols})
        dal.add_discrete_categories(categories)
        dal.add_index_records(data)
        dal.add_weights(data, name=weight_col, selectors=None)

    @staticmethod
    def load_edge(dal, data, edge_id, name, unique_id, filename_hint, complete):
        """Helper function to insert edge and relation data."""
        con = dal._get_connection()
        cur = con.cursor()

        # Insert edge record.
        cur.execute("""
            INSERT INTO main.edge (
                edge_id,
                name,
                other_unique_id,
                other_filename_hint,
                is_locally_complete
            )
            VALUES (?, ?, ?, ?, ?)
        """, (edge_id, name, unique_id, filename_hint, complete))

        # Insert relation records.
        parameters = [(edge_id,) + row for row in data]
        cur.executemany("""
            INSERT INTO main.relation (
                edge_id,
                other_index_id,
                index_id,
                relation_value,
                proportion,
                mapping_level
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, parameters)


class TestAddIndexRecordsMakeSql(unittest.TestCase):
    def setUp(self):
        self.con = get_connection(':memory:', None)
        self.cur = self.con.cursor()

        for stmnt in DataAccessLayer._add_index_columns_make_sql(self.cur, ['state', 'county', 'town']):
            self.cur.execute(stmnt)

        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def test_simple_case(self):
        """Insert columns that match index columns."""
        columns = ['state', 'county', 'town']
        sql = DataAccessLayer._add_index_records_make_sql(self.cur, columns)
        expected = 'INSERT INTO main.node_index ("state", "county", "town") VALUES (?, ?, ?)'
        self.assertEqual(sql, expected)

    def test_differently_ordered_columns(self):
        """Order should reflect given *columns* not table order."""
        columns = ['town', 'county', 'state']  # <- Reverse order from table cols.
        sql = DataAccessLayer._add_index_records_make_sql(self.cur, columns)
        expected = 'INSERT INTO main.node_index ("town", "county", "state") VALUES (?, ?, ?)'
        self.assertEqual(sql, expected)

    def test_subset_of_columns(self):
        """Insert fewer columns than exist in the index table."""
        columns = ['state', 'county']  # <- Does not include "town", and that's OK.
        sql = DataAccessLayer._add_index_records_make_sql(self.cur, columns)
        expected = 'INSERT INTO main.node_index ("state", "county") VALUES (?, ?)'
        self.assertEqual(sql, expected)

    def test_bad_column_value(self):
        regex = 'invalid column name: "region"'
        with self.assertRaisesRegex(sqlite3.OperationalError, regex):
            DataAccessLayer._add_index_records_make_sql(self.cur, ['state', 'region'])


class TestAddIndexRecords(unittest.TestCase):
    def test_add_index_records(self):
        dal = dal_class()
        dal.set_data({'add_index_columns': ['state', 'county']})  # <- Add columns.
        data = [
            ('state', 'county'),  # <- Header row.
            ('IA', 'POLK'),
            ('IN', 'LA PORTE'),
            ('MN', 'HENNEPIN '),
        ]
        dal.add_index_records(data)

        con = dal._connection
        result = con.execute('SELECT * FROM node_index').fetchall()
        expected = [
            (0, '-', '-'),
            (1, 'IA', 'POLK'),
            (2, 'IN', 'LA PORTE'),
            (3, 'MN', 'HENNEPIN '),
        ]
        self.assertEqual(result, expected)

    def test_add_index_records_column_subset(self):
        """Omitted columns should get default value ('-')."""
        dal = dal_class()
        dal.set_data({'add_index_columns': ['state', 'county']})  # <- Add columns.

        # Index rows include "state" but not "county".
        data = [
            ('state',),  # <- Header row.
            ('IA',),
            ('IN',),
            ('MN',),
        ]
        dal.add_index_records(data)

        con = dal._connection
        result = con.execute('SELECT * FROM node_index').fetchall()
        expected = [
            (0, '-', '-'),
            (1, 'IA', '-'),  # <- "county" gets default '-'
            (2, 'IN', '-'),  # <- "county" gets default '-'
            (3, 'MN', '-'),  # <- "county" gets default '-'
        ]
        self.assertEqual(result, expected)

    def test_add_index_records_column_superset(self):
        """Surplus columns should be filtered-out before loading."""
        dal = dal_class()
        dal.set_data({'add_index_columns': ['state', 'county']})  # <- Add columns.

        # Lable rows include unknown columns "region" and "group".
        data = [
            ('region', 'state', 'group',  'county'),  # <- Header row.
            ('WNC',    'IA',    'GROUP2', 'POLK'),
            ('ENC',    'IN',    'GROUP7', 'LA PORTE'),
            ('WNC',    'MN',    'GROUP1', 'HENNEPIN '),
        ]
        dal.add_index_records(data)

        con = dal._connection
        result = con.execute('SELECT * FROM node_index').fetchall()
        expected = [
            (0, '-', '-'),
            (1, 'IA', 'POLK'),
            (2, 'IN', 'LA PORTE'),
            (3, 'MN', 'HENNEPIN '),
        ]
        self.assertEqual(result, expected)

    @unittest.expectedFailure
    def test_no_columns_added(self):
        """Specify behavior when attempting to add records before
        index columns have been defined.
        """
        raise NotImplementedError


class TestIndexRecords(unittest.TestCase):
    def setUp(self):
        self.header = ('state', 'town', 'neighborhood')

        self.data = [
            ('IL', 'Chicago', 'River North'),
            ('IL', 'Chicago', 'Streeterville'),
            ('IL', 'Chicago', 'The Loop'),
            ('IL', 'Springfield', 'Downtown'),
            ('IL', 'Springfield', 'Harvard Park'),
            ('IL', 'Springfield', 'Lincoln Park'),
            ('CA', 'Los Angeles', 'Bel Air'),
            ('CA', 'Los Angeles', 'Hollywood'),
            ('CA', 'Los Angeles', 'Venice'),
            ('CA', 'San Francisco', 'Mid-Market'),
            ('CA', 'San Francisco', 'Mission District'),
            ('CA', 'San Francisco', 'Russian Hill'),
        ]

        self.dal = dal_class()
        self.dal.set_data({'add_index_columns': self.header})
        self.dal.add_index_records([self.header] + self.data)

    def test_no_args(self):
        results = self.dal.index_records()
        data = [row[1:] for row in results]  # Slice-off index_id.
        self.assertEqual(data, [('-', '-', '-')] + self.data)

    def test_where_args(self):
        results = self.dal.index_records(state='IL', town='Chicago')
        data = [row[1:] for row in results]  # Slice-off index_id.
        expected = [
            ('IL', 'Chicago', 'River North'),
            ('IL', 'Chicago', 'Streeterville'),
            ('IL', 'Chicago', 'The Loop'),
        ]
        self.assertEqual(data, expected)

    def test_where_index_id(self):
        results = self.dal.index_records(index_id=3)
        data = list(results)
        expected = [(3, 'IL', 'Chicago', 'The Loop')]
        self.assertEqual(data, expected)

    def test_where_index_id_string(self):
        results = self.dal.index_records(index_id='3')  # <- '3' as string
        data = list(results)
        expected = [(3, 'IL', 'Chicago', 'The Loop')]
        self.assertEqual(data, expected)

    def test_bad_column(self):
        with self.assertRaises(KeyError):
            results = self.dal.index_records(country='US')  # no "country" column

    def test_index_records_grouped(self):
        results = self.dal.index_records_grouped([
            {'state': 'CA', 'town': 'Los Angeles'},
            {'state': 'CA', 'town': 'San Francisco'},
        ])

        # Check first item.
        key, group = next(results)
        expected_key = {'state': 'CA', 'town': 'Los Angeles'}
        expected_group = [
            (7, 'CA', 'Los Angeles', 'Bel Air'),
            (8, 'CA', 'Los Angeles', 'Hollywood'),
            (9, 'CA', 'Los Angeles', 'Venice')
        ]
        self.assertEqual(key, expected_key)
        self.assertEqual(list(group), expected_group)

        # Check second item.
        key, group = next(results)
        expected_key = {'state': 'CA', 'town': 'San Francisco'}
        expected_group = [
            (10, 'CA', 'San Francisco', 'Mid-Market'),
            (11, 'CA', 'San Francisco', 'Mission District'),
            (12, 'CA', 'San Francisco', 'Russian Hill'),
        ]
        self.assertEqual(key, expected_key)
        self.assertEqual(list(group), expected_group)


class TestAddWeightsGetNewId(unittest.TestCase):
    def setUp(self):
        self.con = get_connection(':memory:', None)
        self.cur = self.con.cursor()
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def run_func_test(self, func):
        name = 'myname'
        selectors = ['[category="stuff"]']
        description = 'My description.'

        weighting_id = func(self.cur, name, selectors=selectors, description=description)  # <- Test the function.

        actual = self.cur.execute('SELECT * FROM weighting').fetchall()
        expected = [(1, name, description, [SimpleSelector('category', '=', 'stuff')], 0)]
        self.assertEqual(actual, expected)

        msg = 'retrieved weighting_id should be same as returned from function'
        retrieved_weighting_id = actual[0][0]
        self.assertEqual(retrieved_weighting_id, weighting_id, msg=msg)

    @unittest.skipIf(SQLITE_VERSION_INFO < (3, 35, 0), 'requires 3.35.0 or newer')
    def test_with_returning_clause(self):
        self.run_func_test(DataAccessLayer._add_weights_get_new_id)

    def test_pre35_without_returning_clause(self):
        self.run_func_test(DataAccessLayerPre35._add_weights_get_new_id)


class TestAddWeightsMakeSql(unittest.TestCase):
    def setUp(self):
        self.con = get_connection(':memory:', None)
        self.cur = self.con.cursor()

        for stmnt in DataAccessLayer._add_index_columns_make_sql(self.cur, ['state', 'county', 'town']):
            self.cur.execute(stmnt)

        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def test_all_columns(self):
        columns = ['state', 'county', 'town']
        sql = DataAccessLayer._add_weights_make_sql(self.cur, columns)
        expected = """
            INSERT INTO main.weight (weighting_id, index_id, weight_value)
            SELECT ? AS weighting_id, index_id, ? AS weight_value
            FROM main.node_index
            WHERE "state"=? AND "county"=? AND "town"=?
            GROUP BY "state", "county", "town"
            HAVING COUNT(*)=1
        """
        self.assertEqual(
            dedent(sql).strip(),
            dedent(expected).strip(),
        )

    def test_subset_of_columns(self):
        columns = ['state', 'county']
        sql = DataAccessLayer._add_weights_make_sql(self.cur, columns)
        expected = """
            INSERT INTO main.weight (weighting_id, index_id, weight_value)
            SELECT ? AS weighting_id, index_id, ? AS weight_value
            FROM main.node_index
            WHERE "state"=? AND "county"=?
            GROUP BY "state", "county"
            HAVING COUNT(*)=1
        """
        self.assertEqual(
            dedent(sql).strip(),
            dedent(expected).strip(),
        )

    def test_invalid_column(self):
        regex = 'invalid column name: "region"'
        with self.assertRaisesRegex(sqlite3.OperationalError, regex):
            columns = ['state', 'county', 'region']
            sql = DataAccessLayer._add_weights_make_sql(self.cur, columns)


class TestAddWeightsSetIsComplete(unittest.TestCase):
    def setUp(self):
        self.con = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES, isolation_level=None)
        self.con.executescript(_schema_script)  # Create database schema.
        _add_functions_and_triggers(self.con)
        self.cur = self.con.cursor()

        self.columns = ['label_a', 'label_b']
        for stmnt in dal_class._add_index_columns_make_sql(self.cur, self.columns):
            self.cur.execute(stmnt)
        sql = dal_class._add_index_records_make_sql(self.cur, self.columns)
        iterator = [
            ('X', '001'),
            ('Y', '001'),
            ('Z', '002'),
        ]
        self.cur.executemany(sql, iterator)

        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def test_complete(self):
        weighting_id = dal_class._add_weights_get_new_id(self.cur, 'tot10', ['[category="census"]'])

        # Insert weight records.
        iterator = [
            (weighting_id, 12, 'X', '001'),
            (weighting_id, 35, 'Y', '001'),
            (weighting_id, 20, 'Z', '002'),
        ]
        sql = dal_class._add_weights_make_sql(self.cur, self.columns)
        self.cur.executemany(sql, iterator)

        dal_class._add_weights_set_is_complete(self.cur, weighting_id)  # <- Update is_complete!

        # Check is_complete flag.
        self.cur.execute('SELECT is_complete FROM weighting WHERE weighting_id=?', (weighting_id,))
        result = self.cur.fetchone()
        self.assertEqual(result, (1,), msg='weighting is complete, should be 1')

    def test_incomplete(self):
        weighting_id = dal_class._add_weights_get_new_id(self.cur, 'tot10', ['[category="census"]'])

        # Insert weight records.
        iterator = [
            (weighting_id, 12, 'X', '001'),
            (weighting_id, 35, 'Y', '001'),
        ]
        sql = dal_class._add_weights_make_sql(self.cur, self.columns)
        self.cur.executemany(sql, iterator)

        dal_class._add_weights_set_is_complete(self.cur, weighting_id)  # <- Update is_complete!

        # Check is_complete flag.
        self.cur.execute('SELECT is_complete FROM weighting WHERE weighting_id=?', (weighting_id,))
        result = self.cur.fetchone()
        self.assertEqual(result, (0,), msg='weighting is incomplete, should be 0')


class TestAddWeights(unittest.TestCase):
    """Tests for dal.add_weights() method."""
    def setUp(self):
        self.dal = dal_class()
        self.dal.set_data({'add_index_columns': ['state', 'county', 'tract']})
        self.dal.add_index_records([
            ('state', 'county', 'tract'),
            ('12', '001', '000200'),
            ('12', '003', '040101'),
            ('12', '003', '040102'),
            ('12', '005', '000300'),
            ('12', '007', '000200'),
            ('12', '011', '010401'),
            ('12', '011', '010601'),
            ('12', '017', '450302'),
            ('12', '019', '030202'),
        ])

        con = self.dal._connection
        self.cursor = con.cursor()
        self.addCleanup(con.close)
        self.addCleanup(self.cursor.close)

    def test_full_column_match(self):
        data = [
            ('state', 'county', 'tract', 'pop10'),
            ('12', '001', '000200', 110),
            ('12', '003', '040101', 212),
            ('12', '003', '040102', 17),
            ('12', '005', '000300', 10),
            ('12', '007', '000200', 414),
            ('12', '011', '010401', 223),
            ('12', '011', '010601', 141),
            ('12', '017', '450302', 183),
            ('12', '019', '030202', 62),
        ]
        self.dal.add_weights(data, name='pop10', selectors=None)

        self.cursor.execute('SELECT * FROM weighting')
        self.assertEqual(
            self.cursor.fetchall(),
            [(1, 'pop10', None, None, 1)],  # <- is_complete is 1
        )

        self.cursor.execute("""
            SELECT state, county, tract, weight_value
            FROM node_index
            NATURAL JOIN weight
            WHERE weighting_id=1
        """)
        expected = set(data[1:])  # Slice-off header and convert to set.
        self.assertEqual(set(self.cursor.fetchall()), expected)

    def test_missing_and_null(self):
        """Check that missing records and records with None weight
        are not inserted into the weight table.
        """
        data = [
            ('state', 'county', 'tract', 'pop10'),
            ('12', '001', '000200', 110),
            ('12', '003', '040101', 212),
            ('12', '003', '040102', 17),
            # Omits three middle records:
            #    ('12', '005', '000300', 10)
            #    ('12', '007', '000200', 414)
            #    ('12', '011', '010401', 223)
            # Following records with None weight are not inserted:
            ('12', '011', '010601', None),  # <- Not inserted!
            ('12', '017', '450302', None),  # <- Not inserted!
            ('12', '019', '030202', None),  # <- Not inserted!
        ]
        self.dal.add_weights(data, name='pop10', selectors=None)  # <- Method under test.

        self.cursor.execute('SELECT * FROM weighting')
        self.assertEqual(
            self.cursor.fetchall(),
            [(1, 'pop10', None, None, 0)],  # <- is_complete is 0
        )

        self.cursor.execute("""
            SELECT state, county, tract, weight_value
            FROM node_index
            NATURAL JOIN weight
            WHERE weighting_id=1
        """)

        # Expected result only includes first three records.
        expected = {
            ('12', '003', '040101', 212.0),
            ('12', '003', '040102', 17.0),
            ('12', '001', '000200', 110.0),
        }
        self.assertEqual(set(self.cursor.fetchall()), expected)

    def test_skip_non_unique_matches(self):
        """Should only insert weights that match to a single record."""
        weights = [
            ('state', 'county', 'pop10'),
            ('12', '001', 110),
            ('12', '003', 229),  # <- Matches multiple records.
            ('12', '005', 10),
            ('12', '007', 414),
            ('12', '011', 364),  # <- Matches multiple records.
            ('12', '017', 183),
            ('12', '019', 62),
        ]
        self.dal.add_weights(weights, name='pop10', selectors=None)

        self.cursor.execute('SELECT * FROM weighting')
        self.assertEqual(
            self.cursor.fetchall(),
            [(1, 'pop10', None, None, 0)],  # <- is_complete is 0
        )

        # Get loaded weights.
        self.cursor.execute("""
            SELECT state, county, weight_value
            FROM node_index
            JOIN weight USING (index_id)
            WHERE weighting_id=1
        """)
        result = self.cursor.fetchall()

        expected = [
            ('12', '001', 110),
            #('12', '003', 229),  <- Not included because no unique match.
            ('12', '005', 10),
            ('12', '007', 414),
            #('12', '011', 364),  <- Not included because no unique match.
            ('12', '017', 183),
            ('12', '019', 62),
        ]
        self.assertEqual(set(result), set(expected))

    @unittest.expectedFailure
    def test_match_by_index_id(self):
        raise NotImplementedError

    @unittest.expectedFailure
    def test_mismatched_columns_and_index_id(self):
        raise NotImplementedError

    def test_default_weighting_implicit(self):
        default_weighting = self.dal._get_data_property(self.cursor, 'default_weighting')
        self.assertIsNone(default_weighting, msg='before adding weight, no default')

        # Add first weighting (auto-assigns 'default_weighting').
        self.dal.add_weights(
            data=[
                ('state', 'county', 'tract', 'pop20'),
                ('12', '001', '000200', 110),
                ('12', '003', '040101', 212),
            ],
            name='pop20',
            selectors=None,
            # (implicit default handling, *make_default* not passed)
        )
        result = self.dal._get_data_property(self.cursor, 'default_weighting')
        self.assertEqual(result, 'pop20', msg='gets first weighting as default')

        # Add second weighting (existing default unchanged).
        self.dal.add_weights(
            data=[
                ('state', 'county', 'tract', 'vap20'),
                ('12', '001', '000200', 110),
                ('12', '003', '040101', 212),
            ],
            name='vap20',
            selectors=None,
            # (implicit default handling, *make_default* not passed)
        )
        result = self.dal._get_data_property(self.cursor, 'default_weighting')
        self.assertEqual(result, 'pop20', msg='default already exists, unchanged')

    def test_default_weighting_explicit_false(self):
        default_weighting = self.dal._get_data_property(self.cursor, 'default_weighting')
        self.assertIsNone(default_weighting, msg='before adding weight, no default')

        self.dal.add_weights(
            data=[
                ('state', 'county', 'tract', 'pop20'),
                ('12', '001', '000200', 110),
                ('12', '003', '040101', 212),
            ],
            name='pop20',
            selectors=None,
            make_default=False,  # <- Explicitly False!
        )
        result = self.dal._get_data_property(self.cursor, 'default_weighting')
        self.assertIsNone(default_weighting, msg='no default set, explicitly False')

    def test_default_weighting_explicit_true(self):
        default_weighting = self.dal._get_data_property(self.cursor, 'default_weighting')
        self.assertIsNone(default_weighting, msg='before adding weight, no default')

        # Add first weighting (auto-assigns 'default_weighting').
        self.dal.add_weights(
            data=[
                ('state', 'county', 'tract', 'pop20'),
                ('12', '001', '000200', 110),
                ('12', '003', '040101', 212),
            ],
            name='pop20',
            selectors=None,
            make_default=True,  # <- Explicitly True!
        )
        result = self.dal._get_data_property(self.cursor, 'default_weighting')
        self.assertEqual(result, 'pop20', msg='explicitly True sets as default')

        # Add second weighting (existing default unchanged).
        self.dal.add_weights(
            data=[
                ('state', 'county', 'tract', 'vap20'),
                ('12', '001', '000200', 110),
                ('12', '003', '040101', 212),
            ],
            name='vap20',
            selectors=None,
            make_default=True,  # <- Explicitly True!
        )
        result = self.dal._get_data_property(self.cursor, 'default_weighting')
        self.assertEqual(result, 'vap20', msg='explicitly True replaces previous default')


class TestWeightRecords(unittest.TestCase):
    """Tests for dal.weighting_records() method."""
    def setUp(self):
        data = [
            ['idx1', 'idx2', 'myweight'],
            ['A', 'x', 30],
            ['A', 'y', 15],
            ['B', 'x', 20],
            ['B', 'y', 70],
        ]
        self.dal = dal_class()
        self.dal.set_data({'add_index_columns': ['idx1', 'idx2']})
        self.dal.add_index_records(data)
        self.dal.add_weights(data, name='myweight', selectors=None)

        con = self.dal._connection
        self.cursor = con.cursor()
        self.addCleanup(con.close)
        self.addCleanup(self.cursor.close)

    def test_explicit_name(self):
        records = self.dal.weight_records('myweight')  # <- Method under test.
        expected = [(0, None), (1, 30.0), (2, 15.0), (3, 20.0), (4, 70.0)]
        self.assertEqual(set(records), set(expected))

    def test_implicit_name(self):
        """When no name is given, should use default_weighting."""
        records = self.dal.weight_records()  # <- Method under test.
        expected = [(0, None), (1, 30.0), (2, 15.0), (3, 20.0), (4, 70.0)]
        self.assertEqual(set(records), set(expected))

    def test_filtered_records(self):
        records = self.dal.weight_records(idx1='B')  # <- Method under test.
        expected = [(3, 20.0), (4, 70.0)]
        self.assertEqual(set(records), set(expected))

        records = self.dal.weight_records(idx1='B', idx2='x')  # <- Method under test.
        expected = [(3, 20.0)]
        self.assertEqual(set(records), set(expected))


class TestAddQuantitiesGetLocationId(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()
        self.dal.set_data({'add_index_columns': ['state', 'county', 'tract']})
        con = self.dal._connection
        self.cursor = con.cursor()
        self.addCleanup(con.close)
        self.addCleanup(self.cursor.close)

    def test_insert_values(self):
        self.cursor.execute('SELECT * FROM location')
        msg = 'table should start out empty'
        self.assertEqual(self.cursor.fetchall(), [], msg=msg)

        values_a = {'state': '12', 'county': '001', 'tract': '000200'}
        values_b = {'state': '12', 'county': '', 'tract': ''}

        # Should insert new record.
        location_id = self.dal._add_quantities_get_location_id(self.cursor, values_a)
        self.assertEqual(location_id, 1)

        # Should insert new record.
        location_id = self.dal._add_quantities_get_location_id(self.cursor, values_b)
        self.assertEqual(location_id, 2)

        self.cursor.execute('SELECT * FROM location')
        expected = [
            (1, '12', '001', '000200'),
            (2, '12', '', ''),
        ]
        msg = 'two records should have been inserted'
        self.assertEqual(self.cursor.fetchall(), expected, msg=msg)

    def test_select_existing_record(self):
        self.cursor.execute('SELECT * FROM location')
        msg = 'table should start out empty'
        self.assertEqual(self.cursor.fetchall(), [], msg=msg)

        values = {'state': '12', 'county': '001', 'tract': '000200'}

        # Should insert new record.
        location_id = self.dal._add_quantities_get_location_id(self.cursor, values)
        self.assertEqual(location_id, 1)

        # Should select existing record.
        location_id = self.dal._add_quantities_get_location_id(self.cursor, values)
        self.assertEqual(location_id, 1)

        self.cursor.execute('SELECT * FROM location')
        expected = [
            (1, '12', '001', '000200'),
        ]
        msg = 'only one record should have been inserted'
        self.assertEqual(self.cursor.fetchall(), expected, msg=msg)

    def test_multiple_match_error(self):
        # Insert records.
        self.dal._add_quantities_get_location_id(
            self.cursor, {'state': '12', 'county': '001', 'tract': '000200'}
        )
        self.dal._add_quantities_get_location_id(
            self.cursor, {'state': '12', 'county': '', 'tract': ''}
        )

        # Test failure when values match multiple records.
        values = {'state': '12'}
        with self.assertRaises(RuntimeError):
            location_id = self.dal._add_quantities_get_location_id(self.cursor, values)


class TestAddQuantities(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()
        self.dal.set_data({'add_index_columns': ['state', 'county']})
        con = self.dal._connection
        self.cursor = con.cursor()
        self.addCleanup(con.close)
        self.addCleanup(self.cursor.close)

    sample_location_records = [
        (1, 'OH', 'BUTLER'),
        (2, 'OH', 'FRANKLIN')
    ]

    sample_quantity_records = [
        (1, 1, {'census': 'TOT_MALE'}, 180140),
        (2, 1, {'census': 'TOT_FEMALE'}, 187990),
        (3, 2, {'census': 'TOT_MALE'}, 566499),
        (4, 2, {'census': 'TOT_FEMALE'}, 596915)
    ]

    def test_header(self):
        data = [
            ('state', 'county', 'census', 'counts'),
            ('OH', 'BUTLER', 'TOT_MALE', 180140),
            ('OH', 'BUTLER', 'TOT_FEMALE', 187990),
            ('OH', 'FRANKLIN', 'TOT_MALE', 566499),
            ('OH', 'FRANKLIN', 'TOT_FEMALE', 596915),
        ]
        self.dal.add_quantities(data, 'counts')  # <- Method under test.

        records = self.cursor.execute('SELECT * FROM location').fetchall()
        self.assertEqual(records, self.sample_location_records)

        records = self.cursor.execute('SELECT * FROM quantity').fetchall()
        self.assertEqual(records, self.sample_quantity_records)

    def test_dict_rows(self):
        data = [
            {'state': 'OH', 'county': 'BUTLER', 'census': 'TOT_MALE', 'counts': 180140},
            {'state': 'OH', 'county': 'BUTLER', 'census': 'TOT_FEMALE', 'counts': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'census': 'TOT_MALE', 'counts': 566499},
            {'state': 'OH', 'county': 'FRANKLIN', 'census': 'TOT_FEMALE', 'counts': 596915},
        ]
        self.dal.add_quantities(data, 'counts')  # <- Method under test.

        records = self.cursor.execute('SELECT * FROM location').fetchall()
        self.assertEqual(records, self.sample_location_records)

        records = self.cursor.execute('SELECT * FROM quantity').fetchall()
        self.assertEqual(records, self.sample_quantity_records)

    def test_non_mapping_non_sequence(self):
        """Given *data* must contain dict-rows or sequence-rows."""
        data = [
            {'state', 'county', 'census', 'counts'},   # <- set (non-sequence)
            {'OH', 'BUTLER', 'TOT_MALE', 180140},      # <- set (non-sequence)
            {'OH', 'BUTLER', 'TOT_FEMALE', 187990},    # <- set (non-sequence)
            {'OH', 'FRANKLIN', 'TOT_MALE', 566499},    # <- set (non-sequence)
            {'OH', 'FRANKLIN', 'TOT_FEMALE', 596915},  # <- set (non-sequence)
        ]
        with self.assertRaises(TypeError):
            self.dal.add_quantities(data, 'counts')  # <- Method under test.

    def test_ignore_underscore_attrs(self):
        """The '_dummy' column should not be loaded as an attribute
        beucase it starts with an underscore.
        """
        data = [
            ('state', 'county', 'census', '_dummy', 'counts'),
            ('OH', 'BUTLER', 'TOT_MALE', 'A', 180140),
            ('OH', 'BUTLER', 'TOT_FEMALE', 'B', 187990),
            ('OH', 'FRANKLIN', 'TOT_MALE', 'C', 566499),
            ('OH', 'FRANKLIN', 'TOT_FEMALE', 'D', 596915),
        ]
        self.dal.add_quantities(data, 'counts')  # <- Method under test.

        records = self.cursor.execute('SELECT * FROM location').fetchall()
        self.assertEqual(records, self.sample_location_records)

        records = self.cursor.execute('SELECT * FROM quantity').fetchall()
        self.assertEqual(records, self.sample_quantity_records)

    def test_ignore_empty_string_attrs(self):
        """The empty string column ('') should not be loaded as an attribute."""
        data = [
            ('state', 'county', 'census', '', 'counts'),
            ('OH', 'BUTLER', 'TOT_MALE', 'A', 180140),
            ('OH', 'BUTLER', 'TOT_FEMALE', 'B', 187990),
            ('OH', 'FRANKLIN', 'TOT_MALE', 'C', 566499),
            ('OH', 'FRANKLIN', 'TOT_FEMALE', 'D', 596915),
        ]
        self.dal.add_quantities(data, 'counts')  # <- Method under test.

        records = self.cursor.execute('SELECT * FROM location').fetchall()
        self.assertEqual(records, self.sample_location_records)

        records = self.cursor.execute('SELECT * FROM quantity').fetchall()
        self.assertEqual(records, self.sample_quantity_records)

    def test_explicit_attributes(self):
        """Only include specified attributes (if given)."""
        data = [
            ('state', 'county', 'census', 'dummy', 'counts'),
            ('OH', 'BUTLER', 'TOT_MALE', 'A', 180140),
            ('OH', 'BUTLER', 'TOT_FEMALE', 'B', 187990),
            ('OH', 'FRANKLIN', 'TOT_MALE', 'C', 566499),
            ('OH', 'FRANKLIN', 'TOT_FEMALE', 'D', 596915),
        ]
        self.dal.add_quantities(data, 'counts', attributes=['census'])  # <- Method under test.

        records = self.cursor.execute('SELECT * FROM location').fetchall()
        self.assertEqual(records, self.sample_location_records)

        records = self.cursor.execute('SELECT * FROM quantity').fetchall()
        self.assertEqual(records, self.sample_quantity_records)

    def test_no_attribute_values(self):
        data = [
            ('state', 'county', 'census', 'counts'),
            ('OH', 'BUTLER', '', 180140),  # <- Not loaded (no attributes).
            ('OH', 'BUTLER', 'TOT_FEMALE', 187990),
            ('OH', 'FRANKLIN', '', 566499),  # <- Not loaded (no attributes).
            ('OH', 'FRANKLIN', 'TOT_FEMALE', 596915),
        ]
        with self.assertWarnsRegex(ToronWarning, 'skipped 2 rows.*inserted 2 rows'):
            self.dal.add_quantities(data, 'counts')  # <- Method under test.

        records = self.cursor.execute('SELECT * FROM quantity').fetchall()
        expected_quantity_records = [
            (1, 1, {'census': 'TOT_FEMALE'}, 187990),
            (2, 2, {'census': 'TOT_FEMALE'}, 596915)
        ]
        self.assertEqual(records, expected_quantity_records)

    def test_no_attribute_columns(self):
        data = [
            ('state', 'county', '_badname', 'counts'),  # <- Column '_badname' not attribute.
            ('OH', 'BUTLER', 'TOT_MALE', 180140),
            ('OH', 'BUTLER', 'TOT_FEMALE', 187990),
        ]
        with self.assertWarnsRegex(ToronWarning, 'skipped 2 rows.*inserted 0 rows'):
           self.dal.add_quantities(data, 'counts')  # <- Method under test.
        records = self.cursor.execute('SELECT * FROM quantity').fetchall()
        self.assertEqual(records, [])

        data = [
            ('state', 'county', 'counts'),  # <- No attribute column at all.
            ('OH', 'FRANKLIN', 566499),
            ('OH', 'FRANKLIN', 596915),
        ]
        with self.assertWarnsRegex(ToronWarning, 'skipped 2 rows.*inserted 0 rows'):
            self.dal.add_quantities(data, 'counts')  # <- Method under test.
        records = self.cursor.execute('SELECT * FROM quantity').fetchall()
        self.assertEqual(records, [])

    def test_no_quantity_values(self):
        data = [
            ('state', 'county', 'census', 'counts'),
            ('OH', 'BUTLER', 'TOT_MALE', 0),  # <- Zero should be included.
            ('OH', 'BUTLER', 'TOT_FEMALE', ''),  # <- Not loaded (empty string).
            ('OH', 'FRANKLIN', 'TOT_MALE', 566499),
            ('OH', 'FRANKLIN', 'TOT_FEMALE', None),  # <- Not loaded (None).
        ]
        with self.assertWarnsRegex(ToronWarning, 'skipped 2 rows.*inserted 2 rows'):
            self.dal.add_quantities(data, 'counts')  # <- Method under test.

        records = self.cursor.execute('SELECT * FROM quantity').fetchall()
        expected_quantity_records = [
            (1, 1, {'census': 'TOT_MALE'}, 0),
            (2, 2, {'census': 'TOT_MALE'}, 566499),
        ]
        self.assertEqual(records, expected_quantity_records)


class TestFormatSelectParams(unittest.TestCase):
    def setUp(self):
        self.where = OrderedDict([('state', 'IL'), ('town', 'Chicago')])

    def test_using_all_defaults(self):
        result = dal_class._format_select_params(self.where)
        self.assertIsInstance(result, tuple)

        where_expr, parameters = result
        self.assertEqual(where_expr, '"state"=:autoparam1 AND "town"=:autoparam2')
        self.assertEqual(parameters, {'autoparam1': 'IL', 'autoparam2': 'Chicago'})

    def test_using_all_defaults_int(self):
        where_expr, parameters = dal_class._format_select_params(
            {'index_id': 3}
        )
        self.assertEqual(where_expr, '"index_id"=:autoparam1')
        self.assertEqual(parameters, {'autoparam1': 3})

    def test_connecting_op(self):
        where_expr, parameters = dal_class._format_select_params(
            self.where, connecting_op='OR'
        )
        self.assertEqual(where_expr, '"state"=:autoparam1 OR "town"=:autoparam2')
        self.assertEqual(parameters, {'autoparam1': 'IL', 'autoparam2': 'Chicago'})

    def test_start_num(self):
        where_expr, parameters = dal_class._format_select_params(
            self.where, start_num=5
        )
        self.assertEqual(where_expr, '"state"=:autoparam5 AND "town"=:autoparam6')
        self.assertEqual(parameters, {'autoparam5': 'IL', 'autoparam6': 'Chicago'})

    def test_table_qualifier(self):
        where_expr, parameters = dal_class._format_select_params(
            self.where, table_qualifier='mytable'
        )
        self.assertEqual(where_expr, 'mytable."state"=:autoparam1 AND mytable."town"=:autoparam2')
        self.assertEqual(parameters, {'autoparam1': 'IL', 'autoparam2': 'Chicago'})

    def test_single_item(self):
        where_expr, parameters = dal_class._format_select_params(
            {'state': 'IL'}
        )
        self.assertEqual(where_expr, '"state"=:autoparam1')
        self.assertEqual(parameters, {'autoparam1': 'IL'})

    def test_empty_dict(self):
        where_expr, parameters = dal_class._format_select_params({})
        self.assertEqual(where_expr, '')
        self.assertEqual(parameters, {})

    def test_integration(self):
        con = sqlite3.connect(':memory:')
        con.executescript("""
            CREATE TABLE mytable(
                "state" TEXT,
                "town" TEXT,
                "neighborhood" TEXT
            );
            INSERT INTO mytable
            VALUES
                ('IL', 'Chicago', 'River North'),
                ('IL', 'Chicago', 'Streeterville'),
                ('IL', 'Chicago', 'The Loop'),
                ('IL', 'Springfield', 'Downtown'),
                ('IL', 'Springfield', 'Harvard Park'),
                ('IL', 'Springfield', 'Lincoln Park')
        """)

        where_expr, parameters = dal_class._format_select_params(self.where)

        sql = f'SELECT * FROM mytable WHERE {where_expr}'
        results = con.execute(sql, parameters).fetchall()

        expected = [
            ('IL', 'Chicago', 'River North'),
            ('IL', 'Chicago', 'Streeterville'),
            ('IL', 'Chicago', 'The Loop'),
        ]
        self.assertEqual(results, expected)


class TestGetRawQuantities(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()
        self.dal.set_data({'add_index_columns': ['state', 'county']})
        con = self.dal._connection
        self.cursor = con.cursor()
        self.addCleanup(con.close)
        self.addCleanup(self.cursor.close)

        data = [
            ('state', 'county',   'census',     'counts'),
            ('OH',    'BUTLER',   'TOT_MALE',   180140),
            ('OH',    'BUTLER',   'TOT_FEMALE', 187990),
            ('OH',    'FRANKLIN', 'TOT_MALE',   566499),
            ('OH',    'FRANKLIN', 'TOT_FEMALE', 596915),
            ('OH',    '',         'TOT_ALL',    368130),
            ('OH',    '',         'TOT_ALL',    1163414),
        ]
        self.dal.add_quantities(data, 'counts')  # <- Method under test.

    @staticmethod
    def take(iterable, n):  # <- Helper function.
        """Return first n items of the iterable as a list."""
        return list(itertools.islice(iterable, n))

    def test_get_all(self):
        result = self.dal.get_raw_quantities()
        expected = [
            {'state': 'OH', 'county': 'BUTLER',   'census': 'TOT_MALE',   'value': 180140},
            {'state': 'OH', 'county': 'BUTLER',   'census': 'TOT_FEMALE', 'value': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'census': 'TOT_MALE',   'value': 566499},
            {'state': 'OH', 'county': 'FRANKLIN', 'census': 'TOT_FEMALE', 'value': 596915},
            {'state': 'OH', 'county': '',         'census': 'TOT_ALL',    'value': 368130},
            {'state': 'OH', 'county': '',         'census': 'TOT_ALL',    'value': 1163414},
        ]
        self.assertEqual(list(result), expected)

    def test_where_args_for_location(self):
        result = self.dal.get_raw_quantities(state='OH', county='BUTLER')
        expected = [
            {'state': 'OH', 'county': 'BUTLER', 'census': 'TOT_MALE',   'value': 180140},
            {'state': 'OH', 'county': 'BUTLER', 'census': 'TOT_FEMALE', 'value': 187990},
        ]
        self.assertEqual(list(result), expected)

    def test_where_args_for_attribute(self):
        result = self.dal.get_raw_quantities(census='TOT_ALL')
        expected = [
            {'state': 'OH', 'county': '', 'census': 'TOT_ALL', 'value': 368130},
            {'state': 'OH', 'county': '', 'census': 'TOT_ALL', 'value': 1163414},
        ]
        self.assertEqual(list(result), expected)

    def test_where_args_for_location_and_attribute(self):
        result = self.dal.get_raw_quantities(county='FRANKLIN', census='TOT_MALE')
        expected = [
            {'state': 'OH', 'county': 'FRANKLIN', 'census': 'TOT_MALE', 'value': 566499},
        ]
        self.assertEqual(list(result), expected)

    def test_multiple_cursors(self):
        iterable1 = self.dal.get_raw_quantities()
        iterable2 = self.dal.get_raw_quantities()

        # First 4 items from iterable1.
        result = self.take(iterable1, 4)
        expected = [
            {'state': 'OH', 'county': 'BUTLER',   'census': 'TOT_MALE',   'value': 180140},
            {'state': 'OH', 'county': 'BUTLER',   'census': 'TOT_FEMALE', 'value': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'census': 'TOT_MALE',   'value': 566499},
            {'state': 'OH', 'county': 'FRANKLIN', 'census': 'TOT_FEMALE', 'value': 596915},
        ]
        self.assertEqual(result, expected)

        # First 3 items from iterable2.
        result = self.take(iterable2, 3)
        expected = [
            {'state': 'OH', 'county': 'BUTLER',   'census': 'TOT_MALE',   'value': 180140},
            {'state': 'OH', 'county': 'BUTLER',   'census': 'TOT_FEMALE', 'value': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'census': 'TOT_MALE',   'value': 566499},
        ]
        self.assertEqual(result, expected)

        # Remaining items from iterable1.
        result = list(iterable1)
        expected = [
            {'state': 'OH', 'county': '',         'census': 'TOT_ALL',    'value': 368130},
            {'state': 'OH', 'county': '',         'census': 'TOT_ALL',    'value': 1163414},
        ]
        self.assertEqual(result, expected)

        # Deleting iterable2 before it's entirely consumed should
        # not raise an error warning or otherwise cause problems.
        del iterable2

    def test_generator_userfunction_interaction(self):
        """Closing a generator early should not raise an exception."""
        # Get quantities filtered using `census` attribute--this creates
        # a user-defined function.
        generator = self.dal.get_raw_quantities(census='TOT_ALL')

        # Start iterating over generator but close before StopIteration.
        next(generator)  # Fetch one result (start iteration).
        try:
            generator.close()  # Close generator early (before it's exhausted).
        except sqlite3.OperationalError as err:
            self.fail(str(err))


class TestDeleteRawQuantities(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()
        self.dal.set_data({'add_index_columns': ['state', 'county']})
        con = self.dal._connection
        self.cursor = con.cursor()
        self.addCleanup(con.close)
        self.addCleanup(self.cursor.close)

        data = [
            ('state', 'county',   'census',     'counts'),
            ('OH',    'BUTLER',   'TOT_MALE',   180140),
            ('OH',    'BUTLER',   'TOT_FEMALE', 187990),
            ('OH',    'FRANKLIN', 'TOT_MALE',   566499),
            ('OH',    'FRANKLIN', 'TOT_FEMALE', 596915),
            ('OH',    '',         'TOT_ALL',    1531544),
        ]
        self.dal.add_quantities(data, 'counts')  # <- Method under test.

    def assertRemainingQuantities(self, expected):
        result = self.cursor.execute('SELECT * FROM quantity').fetchall()
        self.assertEqual(result, expected)

    def assertRemainingLocations(self, expected):
        result = self.cursor.execute('SELECT * FROM location').fetchall()
        self.assertEqual(result, expected)

    def test_delete_by_location(self):
        self.dal.delete_raw_quantities(county='FRANKLIN')

        self.assertRemainingQuantities([
            (1, 1, {'census': 'TOT_MALE'},   180140),
            (2, 1, {'census': 'TOT_FEMALE'}, 187990),
            (5, 3, {'census': 'TOT_ALL'},    1531544)
        ])

        self.assertRemainingLocations([
            (1, 'OH', 'BUTLER'),
            (3, 'OH', ''),
        ])

    def test_delete_by_attribute(self):
        self.dal.delete_raw_quantities(census='TOT_ALL')

        self.assertRemainingQuantities([
            (1, 1, {'census': 'TOT_MALE'},   180140),
            (2, 1, {'census': 'TOT_FEMALE'}, 187990),
            (3, 2, {'census': 'TOT_MALE'},   566499),
            (4, 2, {'census': 'TOT_FEMALE'}, 596915),
        ])

        self.assertRemainingLocations([
            (1, 'OH', 'BUTLER'),
            (2, 'OH', 'FRANKLIN'),
        ])

    def test_delete_by_location_and_attribute(self):
        self.dal.delete_raw_quantities(county='FRANKLIN', census='TOT_MALE')

        self.assertRemainingQuantities([
            (1, 1, {'census': 'TOT_MALE'},   180140),
            (2, 1, {'census': 'TOT_FEMALE'}, 187990),
            (4, 2, {'census': 'TOT_FEMALE'}, 596915),
            (5, 3, {'census': 'TOT_ALL'},    1531544),
        ])

        self.assertRemainingLocations([
            (1, 'OH', 'BUTLER'),
            (2, 'OH', 'FRANKLIN'),
            (3, 'OH', ''),
        ])

    def test_delete_all_records(self):
        self.dal.delete_raw_quantities(state='OH')
        self.assertRemainingQuantities(expected=[])
        self.assertRemainingLocations(expected=[])

    def test_no_rows_deleted(self):
        expected_quantities = [
            (1, 1, {'census': 'TOT_MALE'},   180140),
            (2, 1, {'census': 'TOT_FEMALE'}, 187990),
            (3, 2, {'census': 'TOT_MALE'},   566499),
            (4, 2, {'census': 'TOT_FEMALE'}, 596915),
            (5, 3, {'census': 'TOT_ALL'},    1531544),
        ]
        expected_locations = [
            (1, 'OH', 'BUTLER'),
            (2, 'OH', 'FRANKLIN'),
            (3, 'OH', ''),
        ]

        self.dal.delete_raw_quantities(state='OH', county='NO-MATCH')
        self.assertRemainingQuantities(expected_quantities)
        self.assertRemainingLocations(expected_locations)

        self.dal.delete_raw_quantities(census='NO-MATCH')
        self.assertRemainingQuantities(expected_quantities)
        self.assertRemainingLocations(expected_locations)

        self.dal.delete_raw_quantities(county='NO-MATCH', census='NO-MATCH')
        self.assertRemainingQuantities(expected_quantities)
        self.assertRemainingLocations(expected_locations)

        self.dal.delete_raw_quantities(census='NO-MATCH')
        self.assertRemainingQuantities(expected_quantities)
        self.assertRemainingLocations(expected_locations)

    def test_missing_kwds(self):
        msg = 'should fail if no arguments are passed to function'
        with self.assertRaises(TypeError, msg=msg):
            self.dal.delete_raw_quantities()


class TestDisaggregateHelpers(unittest.TestCase):
    def test_disaggregate_make_sql_constraints(self):
        columns = ['"A"', '"B"', '"C"', '"D"']  # <- Should be normalized identifiers.
        expected = """t2."A"=t3."A" AND t2."B"='' AND t2."C"=t3."C" AND t2."D"=''"""

        bitmask = [1, 0, 1, 0]
        result = dal_class._disaggregate_make_sql_constraints(columns, bitmask, 't2', 't3')
        self.assertEqual(result, expected)

        bitmask_trailing_zeros = [1, 0, 1, 0, 0, 0]
        result = dal_class._disaggregate_make_sql_constraints(columns, bitmask_trailing_zeros, 't2', 't3')
        self.assertEqual(result, expected, msg='extra trailing zeros are OK')

        bitmask_truncated = [1, 0, 1]
        result = dal_class._disaggregate_make_sql_constraints(columns, bitmask_truncated, 't2', 't3')
        self.assertEqual(result, expected, msg='bitmask shorter than columns is OK')

        bad_bitmask = [1, 0, 1, 0, 1]
        with self.assertRaises(ValueError, msg='final "1" does not match any column'):
            dal_class._disaggregate_make_sql_constraints(columns, bad_bitmask, 't2', 't3')

    def test_disaggregate_make_sql(self):
        columns = ['"A"', '"B"', '"C"', '"D"']  # <- Should be normalized identifiers.
        bitmask = [1, 0, 1, 0]
        match_selector_func = 'USER_FUNC_NAME'
        result = DataAccessLayer._disaggregate_make_sql(columns, bitmask, match_selector_func)
        expected = """
            SELECT
                t3.index_id,
                t1.attributes,
                t1.quantity_value * IFNULL(
                    (t4.weight_value / SUM(t4.weight_value) OVER (PARTITION BY t1.quantity_id)),
                    (1.0 / COUNT(1) OVER (PARTITION BY t1.quantity_id))
                ) AS quantity_value
            FROM main.quantity t1
            JOIN main.location t2 USING (_location_id)
            JOIN main.node_index t3 ON (t2."A"=t3."A" AND t2."B"='' AND t2."C"=t3."C" AND t2."D"='')
            JOIN main.weight t4 ON (
                t3.index_id=t4.index_id
                AND t4.weighting_id=USER_FUNC_NAME(t1.attributes)
            )
        """
        self.assertEqual(result, expected)

        bitmask = [0, 0, 0, 0]  # <- Bitmask is all 0s.
        result = DataAccessLayer._disaggregate_make_sql(columns, bitmask, match_selector_func)
        self.assertIn("""JOIN main.node_index t3 ON (t2."A"='' AND t2."B"='' AND t2."C"='' AND t2."D"='')""", result)

    def test_disaggregate_make_sql_filter_attrs_func(self):
        """Providing a *filter_attrs_func* argument adds a WHERE clause."""
        columns = ['"A"', '"B"', '"C"', '"D"']  # <- Should be normalized identifiers.
        bitmask = [1, 0, 1, 0]
        match_selector_func = 'USER_FUNC_NAME1'
        filter_attrs_func = 'USER_FUNC_NAME2'
        result = DataAccessLayer._disaggregate_make_sql(columns, bitmask, match_selector_func, filter_attrs_func)
        expected = """
            SELECT
                t3.index_id,
                t1.attributes,
                t1.quantity_value * IFNULL(
                    (t4.weight_value / SUM(t4.weight_value) OVER (PARTITION BY t1.quantity_id)),
                    (1.0 / COUNT(1) OVER (PARTITION BY t1.quantity_id))
                ) AS quantity_value
            FROM main.quantity t1
            JOIN main.location t2 USING (_location_id)
            JOIN main.node_index t3 ON (t2."A"=t3."A" AND t2."B"='' AND t2."C"=t3."C" AND t2."D"='')
            JOIN main.weight t4 ON (
                t3.index_id=t4.index_id
                AND t4.weighting_id=USER_FUNC_NAME1(t1.attributes)
            )
            WHERE USER_FUNC_NAME2(t1.attributes)=1
        """
        self.maxDiff = None
        self.assertEqual(result, expected)


class TestStaticDisaggregate(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()

        connection = self.dal._get_connection()
        self.addCleanup(connection.close)

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

        columns = ['col1', 'col2']
        self.dal.set_data({'add_index_columns': columns})

        categories = [{'col1'}]
        self.dal.add_discrete_categories(categories)

        records = [
            ('col1', 'col2'),
            ('A',    'x'),
            ('A',    'y'),
            ('B',    'x'),
            ('B',    'y'),
            ('C',    'x'),
            ('C',    'y'),
            ('C',    'z'),
        ]
        self.dal.add_index_records(records)

        weighting = [
            ('col1', 'col2', 'weight'),
            ('A',    'x',    20),
            ('A',    'y',    30),
            ('B',    'x',    15),
            ('B',    'y',    60),
            ('C',    'x',    0),
            ('C',    'y',    0),
            ('C',    'z',    0),
        ]
        self.dal.add_weights(weighting, name='weight', selectors=['[attr1]'])

    @staticmethod
    def make_hashable(iterable):
        """Helper function to make disaggregation rows hashable."""
        func = lambda a, b, c: (a, frozenset(b.items()), c)
        return {func(*row) for row in iterable}

    def test_disaggregate(self):
        # Add data for test.
        data = [
            ('col1', 'col2', 'attr1', 'value'),
            ('A',    'x',    'foo',   18),
            ('A',    'y',    'foo',   29),
            ('B',    'x',    'foo',   22),
            ('B',    'y',    'foo',   70),

            ('A',    '',     'bar',   15),
            ('B',    '',     'bar',   20),

            ('',     '',     'baz',   25),
        ]
        self.dal.add_quantities(data, 'value')

        results = self.dal.static_disaggregate()
        expected = [
            (1, {'attr1': 'foo'}, 18.0),  # index (1, 'A', 'x')
            (2, {'attr1': 'foo'}, 29.0),  # index (2, 'A', 'y')
            (3, {'attr1': 'foo'}, 22.0),  # index (3, 'B', 'x')
            (4, {'attr1': 'foo'}, 70.0),  # index (4, 'B', 'y')

            (1, {'attr1': 'bar'}, 6.0),   # index (1, 'A', 'x')
            (2, {'attr1': 'bar'}, 9.0),   # index (2, 'A', 'y')
            (3, {'attr1': 'bar'}, 4.0),   # index (3, 'B', 'x')
            (4, {'attr1': 'bar'}, 16.0),  # index (4, 'B', 'y')

            (1, {'attr1': 'baz'}, 4.0),   # index (1, 'A', 'x')
            (2, {'attr1': 'baz'}, 6.0),   # index (2, 'A', 'y')
            (3, {'attr1': 'baz'}, 3.0),   # index (3, 'B', 'x')
            (4, {'attr1': 'baz'}, 12.0),  # index (4, 'B', 'y')
            (5, {'attr1': 'baz'}, 0),     # index (5, 'C', 'x')
            (6, {'attr1': 'baz'}, 0),     # index (6, 'C', 'y')
            (7, {'attr1': 'baz'}, 0),     # index (7, 'C', 'z')
        ]

        results = self.make_hashable(results)
        expected = self.make_hashable(expected)
        self.assertEqual(results, expected)

    def test_disaggregate2(self):
        # Add data for test.
        data = [
            ('col1', 'col2', 'attr1', 'value'),
            ('A',    'x',    'foo',   18),
            ('A',    'y',    'foo',   29),
            ('B',    'x',    'foo',   22),
            ('B',    'y',    'foo',   70),

            ('A',    '',     'foo',   15),
            ('B',    '',     'foo',   20),

            ('',     '',     'foo',   25),
        ]
        self.dal.add_quantities(data, 'value')

        results = self.dal.static_disaggregate()
        expected = [
            (1, {'attr1': 'foo'}, 28.0),  # <- 18 + 6 + 4
            (2, {'attr1': 'foo'}, 44.0),  # <- 29 + 9 + 6
            (3, {'attr1': 'foo'}, 29.0),  # <- 22 + 4 + 3
            (4, {'attr1': 'foo'}, 98.0),  # <- 70 + 16 + 12
            (5, {'attr1': 'foo'}, 0),  # <- 0 + 0 + 0
            (6, {'attr1': 'foo'}, 0),  # <- 0 + 0 + 0
            (7, {'attr1': 'foo'}, 0),  # <- 0 + 0 + 0
        ]

        results = self.make_hashable(results)
        expected = self.make_hashable(expected)
        self.assertEqual(results, expected)

    def test_fallback_behavior(self):
        """Check weighting fallback behavior--first try static
        weighting but when static weighting for group is zero,
        use uniform distribution.
        """
        # Add data for test.
        data = [
            ('col1', 'col2', 'attr1', 'value'),
            ('A',    'x',    'foo',   4),       # <- Should use static weight (1-to-1 no disaggregation).
            ('A',    'y',    'foo',   12),      # <- Should use static weight (1-to-1 no disaggregation).

            ('A',    '',     'foo',   15),      # <- Should use static weighting.
            ('A',    '',     'bar',   20),      # <- Should use static weighting.
            ('B',    '',     'bar',   18.75),   # <- Should use static weighting.
            ('C',    '',     'baz',   9),       # <- Should use uniform weighting (weights for 'C' are all zero).

            ('',     '',     'bar',   25),      # <- Should use static weighting.
        ]
        self.dal.add_quantities(data, 'value')

        results = self.dal.static_disaggregate()
        expected = [
            (1, {'attr1': 'foo'}, 10.0),  # index (1, 'A', 'x')
            (1, {'attr1': 'bar'}, 12.0),  # index (1, 'A', 'x')
            (2, {'attr1': 'foo'}, 21.0),  # index (2, 'A', 'y')
            (2, {'attr1': 'bar'}, 18.0),  # index (2, 'A', 'y')
            (3, {'attr1': 'bar'}, 6.75),  # index (3, 'B', 'x')
            (4, {'attr1': 'bar'}, 27.0),  # index (4, 'B', 'y')
            (5, {'attr1': 'bar'}, 0.0),   # index (5, 'C', 'x')
            (5, {'attr1': 'baz'}, 3.0),   # index (5, 'C', 'x')
            (6, {'attr1': 'bar'}, 0.0),   # index (6, 'C', 'y')
            (6, {'attr1': 'baz'}, 3.0),   # index (6, 'C', 'y')
            (7, {'attr1': 'bar'}, 0.0),   # index (7, 'C', 'z')
            (7, {'attr1': 'baz'}, 3.0),   # index (7, 'C', 'z')
        ]
        results = self.make_hashable(results)
        expected = self.make_hashable(expected)
        self.assertEqual(results, expected)

    def test_disaggregate_where_kwds(self):
        # Add data for test.
        data = [
            ('col1', 'col2', 'attr1', 'value'),
            ('A',    'x',    'foo',   18),
            ('A',    'y',    'foo',   29),
            ('B',    'x',    'foo',   22),
            ('B',    'y',    'foo',   70),

            ('A',    '',     'bar',   15),
            ('B',    '',     'bar',   20),

            ('',     '',     'baz',   25),
        ]
        self.dal.add_quantities(data, 'value')

        # Filter by index value.
        results = self.dal.static_disaggregate(col1='B')
        expected = [
            (3, {'attr1': 'foo'}, 22.0),  # index (3, 'B', 'x')
            (4, {'attr1': 'foo'}, 70.0),  # index (4, 'B', 'y')
            (3, {'attr1': 'bar'}, 4.0),   # index (3, 'B', 'x')
            (4, {'attr1': 'bar'}, 16.0),  # index (4, 'B', 'y')
            (3, {'attr1': 'baz'}, 3.0),   # index (3, 'B', 'x')
            (4, {'attr1': 'baz'}, 12.0),  # index (4, 'B', 'y')
        ]
        results = self.make_hashable(results)
        expected = self.make_hashable(expected)
        self.assertEqual(results, expected)

        # Filter by attribute value.
        results = self.dal.static_disaggregate(attr1='baz')
        expected = [
            (1, {'attr1': 'baz'}, 4.0),   # index (1, 'A', 'x')
            (2, {'attr1': 'baz'}, 6.0),   # index (2, 'A', 'y')
            (3, {'attr1': 'baz'}, 3.0),   # index (3, 'B', 'x')
            (4, {'attr1': 'baz'}, 12.0),  # index (4, 'B', 'y')
            (5, {'attr1': 'baz'}, 0),     # index (5, 'C', 'x')
            (6, {'attr1': 'baz'}, 0),     # index (6, 'C', 'y')
            (7, {'attr1': 'baz'}, 0),     # index (7, 'C', 'z')
        ]
        results = self.make_hashable(results)
        expected = self.make_hashable(expected)
        self.assertEqual(results, expected)

        # Filter by index and attribute values.
        results = self.dal.static_disaggregate(col1='B', attr1='baz')
        expected = [
            (3, {'attr1': 'baz'}, 3.0),   # index (3, 'B', 'x')
            (4, {'attr1': 'baz'}, 12.0),  # index (4, 'B', 'y')
        ]
        results = self.make_hashable(results)
        expected = self.make_hashable(expected)
        self.assertEqual(results, expected)


class TestAdaptiveDisaggregate(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()

        connection = self.dal._get_connection()
        self.addCleanup(connection.close)

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

        columns = ['col1', 'col2']
        self.dal.set_data({'add_index_columns': columns})

        categories = [{'col1'}]
        self.dal.add_discrete_categories(categories)

        records = [
            ('col1', 'col2'),
            ('A',    'x'),
            ('A',    'y'),
            ('B',    'x'),
            ('B',    'y'),
            ('C',    'x'),
            ('C',    'y'),
            ('C',    'z'),
        ]
        self.dal.add_index_records(records)

        weighting = [
            ('col1', 'col2', 'weight'),
            ('A',    'x',     8),
            ('A',    'y',     16),
            ('B',    'x',     5),
            ('B',    'y',     10),
            ('C',    'x',     0),
            ('C',    'y',     0),
            ('C',    'z',     0),
        ]
        self.dal.add_weights(weighting, name='weight', selectors=['[attr1]'])

    @staticmethod
    def make_hashable(iterable):
        """Helper function to make disaggregation rows hashable."""
        func = lambda a, b, c: (a, frozenset(b.items()), c)
        return {func(*row) for row in iterable}

    def test_adaptive_disaggregate_make_sql(self):
        self.maxDiff = None

        normalized_columns = ['"A"', '"B"', '"C"', '"D"']
        bitmask = [1, 0, 1, 0]
        match_selector_func = 'UserFuncName'
        adaptive_weight_table = 'AdaptiveWeightTable'

        result = DataAccessLayer._adaptive_disaggregate_make_sql(
            normalized_columns,
            bitmask,
            match_selector_func,
            adaptive_weight_table,
        )
        expected = """
            SELECT
                t3.index_id,
                t1.attributes,
                t1.quantity_value * COALESCE(
                    (COALESCE(t5.weight_value, 0.0) / SUM(t5.weight_value) OVER (PARTITION BY t1.quantity_id)),
                    (t4.weight_value / SUM(t4.weight_value) OVER (PARTITION BY t1.quantity_id)),
                    (1.0 / COUNT(1) OVER (PARTITION BY t1.quantity_id))
                ) AS quantity_value
            FROM main.quantity t1
            JOIN main.location t2 USING (_location_id)
            JOIN main.node_index t3 ON (t2."A"=t3."A" AND t2."B"='' AND t2."C"=t3."C" AND t2."D"='')
            JOIN main.weight t4 ON (
                t3.index_id=t4.index_id
                AND t4.weighting_id=UserFuncName(t1.attributes)
            )
            LEFT JOIN (
                SELECT
                    t5sub.index_id,
                    user_json_object_keep(t5sub.attributes) AS attrs_subset,
                    SUM(t5sub.quantity_value) AS weight_value
                FROM AdaptiveWeightTable t5sub
                GROUP BY t5sub.index_id, user_json_object_keep(t5sub.attributes)
            ) t5 ON (
                t3.index_id=t5.index_id
                AND t5.attrs_subset=user_json_object_keep(t1.attributes)
            )
            UNION ALL
            SELECT index_id, attributes, quantity_value FROM AdaptiveWeightTable
        """
        self.assertEqual(result.strip(), expected.strip())

        bitmask = [0, 0, 0, 0]  # <- Bitmask is all 0s.
        result = DataAccessLayer._adaptive_disaggregate_make_sql(
            normalized_columns,
            [0, 0, 0, 0],  # <- Different bitmask.
            match_selector_func,
            adaptive_weight_table,
        )
        self.assertIn("""JOIN main.node_index t3 ON (t2."A"='' AND t2."B"='' AND t2."C"='' AND t2."D"='')""", result)

    def test_adaptive_disaggregate_make_sql_filter_attrs_func(self):
        result = DataAccessLayer._adaptive_disaggregate_make_sql(
            normalized_columns=['"A"', '"B"', '"C"', '"D"'],
            bitmask=[1, 0, 1, 0],
            match_selector_func='UserFuncName',
            adaptive_weight_table='AdaptiveWeightTable',
            filter_attrs_func='USER_FUNC_NAME2',
        )
        expected = """
            SELECT
                t3.index_id,
                t1.attributes,
                t1.quantity_value * COALESCE(
                    (COALESCE(t5.weight_value, 0.0) / SUM(t5.weight_value) OVER (PARTITION BY t1.quantity_id)),
                    (t4.weight_value / SUM(t4.weight_value) OVER (PARTITION BY t1.quantity_id)),
                    (1.0 / COUNT(1) OVER (PARTITION BY t1.quantity_id))
                ) AS quantity_value
            FROM main.quantity t1
            JOIN main.location t2 USING (_location_id)
            JOIN main.node_index t3 ON (t2."A"=t3."A" AND t2."B"='' AND t2."C"=t3."C" AND t2."D"='')
            JOIN main.weight t4 ON (
                t3.index_id=t4.index_id
                AND t4.weighting_id=UserFuncName(t1.attributes)
            )
            LEFT JOIN (
                SELECT
                    t5sub.index_id,
                    user_json_object_keep(t5sub.attributes) AS attrs_subset,
                    SUM(t5sub.quantity_value) AS weight_value
                FROM AdaptiveWeightTable t5sub
                GROUP BY t5sub.index_id, user_json_object_keep(t5sub.attributes)
            ) t5 ON (
                t3.index_id=t5.index_id
                AND t5.attrs_subset=user_json_object_keep(t1.attributes)
            )
            WHERE USER_FUNC_NAME2(t1.attributes)=1
            UNION ALL
            SELECT index_id, attributes, quantity_value FROM AdaptiveWeightTable
        """
        self.maxDiff = None
        self.assertEqual(result.strip(), expected.strip())

    def test_adaptive_disaggregate(self):
        # Add data for test.
        data = [
            ('col1', 'col2', 'attr1', 'value'),
            ('A',    'x',    'foo',   20),  # <- 1st group: uses weights from weight table.
            ('A',    'y',    'foo',   30),  # <- 1st group: uses weights from weight table.
            ('B',    'x',    'foo',   15),  # <- 1st group: uses weights from weight table.
            ('B',    'y',    'foo',   60),  # <- 1st group: uses weights from weight table.

            ('A',    '',     'foo',   20),  # <- 2nd group: uses 1st group as weighting layer.
            ('B',    '',     'foo',   15),  # <- 2nd group: uses 1st group as weighting layer.

            ('',     '',     'foo',   25),  # <- 3rd group: uses 1st group + 2nd group as weighting layer.
        ]
        self.dal.add_quantities(data, 'value')

        results = self.dal.adaptive_disaggregate()
        expected = [
            (1, {"attr1": "foo"}, 32.375),   # index (1, 'A', 'x')
            (2, {"attr1": "foo"}, 48.5625),  # index (2, 'A', 'y')
            (3, {"attr1": "foo"}, 20.8125),  # index (3, 'B', 'x')
            (4, {"attr1": "foo"}, 83.25),    # index (4, 'B', 'y')
            (5, {"attr1": "foo"}, 0.0),      # index (5, 'C', 'x')
            (6, {"attr1": "foo"}, 0.0),      # index (6, 'C', 'y')
            (7, {"attr1": "foo"}, 0.0),      # index (7, 'C', 'z')
        ]
        results = self.make_hashable(results)
        expected = self.make_hashable(expected)
        self.assertEqual(results, expected)

    def test_partial_coverage_for_adaptive_weights(self):
        # Add data for test.
        data = [
            ('col1', 'col2', 'attr1', 'value'),
            #('A',    'x',    ...,     ...),  <- Not included!
            ('A',    'y',    'foo',   30),
            ('B',    'x',    'foo',   15),
            #('B',    'y',    ...,     ...),  <- Not included!
            ('A',    '',     'foo',   20),
            ('B',    '',     'foo',   15),
            ('',     '',     'foo',   25),
        ]
        self.dal.add_quantities(data, 'value')

        results = self.dal.adaptive_disaggregate()
        expected = [
            (1, {"attr1": "foo"}, 0.0),  # <- Adaptive weight is 0 here.
            (2, {"attr1": "foo"}, 65.625),
            (3, {"attr1": "foo"}, 39.375),
            (4, {"attr1": "foo"}, 0.0),  # <- Adaptive weight is 0 here.
            (5, {"attr1": "foo"}, 0.0),  # <- Static and adaptive weight are 0.
            (6, {"attr1": "foo"}, 0.0),  # <- Static and adaptive weight are 0.
            (7, {"attr1": "foo"}, 0.0),  # <- Static and adaptive weight are 0.
        ]
        results = self.make_hashable(results)
        expected = self.make_hashable(expected)
        self.assertEqual(results, expected)

    def test_fallback_behavior(self):
        """Check weighting fallback behavior--first try adaptive weighting,
        when not available use static weighting, and when static weighting
        is zero use uniform distribution.
        """
        # Add data for test.
        data = [
            ('col1', 'col2', 'attr1', 'value'),
            ('A',    'x',    'foo',   4),     # <- Should use static weight (1-to-1 no disaggregation).
            ('A',    'y',    'foo',   12),    # <- Should use static weight (1-to-1 no disaggregation).

            ('A',    '',     'foo',   8),     # <- Should use adaptive weight (from 1st group).
            ('A',    '',     'bar',   12),    # <- Should use static weighting (no 'bar' in 1st group).
            ('B',    '',     'foo',   12),    # <- Should use static weighting (no 'B' in 1st group).
            ('C',    '',     'foo',   9),     # <- Should use uniform weighting (weights for 'C' are all zero).

            ('',     '',     'bar',   18),    # <- Should use adaptive weighting (to other areas with 'bar' weights).
            ('',     '',     'baz',   9.75),  # <- Should use static weighting (no 'baz' in 1st or 2nd groups).
        ]
        self.dal.add_quantities(data, 'value')

        results = self.dal.adaptive_disaggregate()
        expected = [
            # index (1, 'A', 'x'):
            (1, {'attr1': 'foo'}, 6.0),
            (1, {'attr1': 'bar'}, 10.0),
            (1, {'attr1': 'baz'}, 2.0),
            # index (2, 'A', 'y'):
            (2, {'attr1': 'foo'}, 18.0),
            (2, {'attr1': 'bar'}, 20.0),
            (2, {'attr1': 'baz'}, 4.0),
            # index (3, 'B', 'x'):
            (3, {'attr1': 'foo'}, 4.0),
            (3, {'attr1': 'bar'}, 0.0),
            (3, {'attr1': 'baz'}, 1.25),
            # index (4, 'B', 'y'):
            (4, {'attr1': 'foo'}, 8.0),
            (4, {'attr1': 'bar'}, 0.0),
            (4, {'attr1': 'baz'}, 2.5),
            # index (5, 'C', 'x'):
            (5, {'attr1': 'foo'}, 3.0),
            (5, {'attr1': 'bar'}, 0.0),
            (5, {'attr1': 'baz'}, 0.0),
            # index (6, 'C', 'y'):
            (6, {'attr1': 'foo'}, 3.0),
            (6, {'attr1': 'bar'}, 0.0),
            (6, {'attr1': 'baz'}, 0.0),
            # index (7, 'C', 'z'):
            (7, {'attr1': 'foo'}, 3.0),
            (7, {'attr1': 'bar'}, 0.0),
            (7, {'attr1': 'baz'}, 0.0),
        ]
        results = self.make_hashable(results)
        expected = self.make_hashable(expected)
        self.assertEqual(results, expected)

    def test_disaggregate_where_kwds(self):
        # Add data for test.
        data = [
            ('col1', 'col2', 'attr1', 'value'),
            ('A',    'x',    'foo',   4),
            ('A',    'y',    'foo',   12),

            ('A',    '',     'foo',   8),
            ('A',    '',     'bar',   12),
            ('B',    '',     'foo',   12),
            ('C',    '',     'foo',   9),

            ('',     '',     'bar',   18),
            ('',     '',     'baz',   9.75),
        ]
        self.dal.add_quantities(data, 'value')

        # Filter by index value.
        results = self.dal.adaptive_disaggregate(col1='B')
        expected = [
            (3, {'attr1': 'foo'}, 4.0),   # index (3, 'B', 'x')
            (3, {'attr1': 'bar'}, 0.0),   # index (3, 'B', 'x')
            (3, {'attr1': 'baz'}, 1.25),  # index (3, 'B', 'x')
            (4, {'attr1': 'foo'}, 8.0),   # index (4, 'B', 'y')
            (4, {'attr1': 'bar'}, 0.0),   # index (4, 'B', 'y')
            (4, {'attr1': 'baz'}, 2.5),   # index (4, 'B', 'y')
        ]
        results = self.make_hashable(results)
        expected = self.make_hashable(expected)
        self.assertEqual(results, expected)

        # Filter by attribute value.
        results = self.dal.adaptive_disaggregate(attr1='baz')
        expected = [
            (1, {'attr1': 'baz'}, 2.0),   # index (1, 'A', 'x')
            (2, {'attr1': 'baz'}, 4.0),   # index (2, 'A', 'y')
            (3, {'attr1': 'baz'}, 1.25),  # index (3, 'B', 'x')
            (4, {'attr1': 'baz'}, 2.5),   # index (4, 'B', 'y')
            (5, {'attr1': 'baz'}, 0.0),   # index (5, 'C', 'x')
            (6, {'attr1': 'baz'}, 0.0),   # index (6, 'C', 'y')
            (7, {'attr1': 'baz'}, 0.0),   # index (7, 'C', 'z')
        ]
        results = self.make_hashable(results)
        expected = self.make_hashable(expected)
        self.assertEqual(results, expected)

        # Filter by index and attribute values.
        results = self.dal.adaptive_disaggregate(col1='B', attr1='baz')
        expected = [
            (3, {'attr1': 'baz'}, 1.25),  # index (3, 'B', 'x')
            (4, {'attr1': 'baz'}, 2.5),   # index (4, 'B', 'y')
        ]
        results = self.make_hashable(results)
        expected = self.make_hashable(expected)
        self.assertEqual(results, expected)

    def test_disaggregate_match_attrs_keys(self):
        """Adaptive disagg using `match_attrs_keys` argument."""
        # Add data for test.
        data = [
            ('col1', 'col2', 'attr1', 'attr2', 'value'),
            ('A',    'x',    'foo',   'a',     20),
            ('A',    'y',    'foo',   'b',     30),
            ('B',    'x',    'foo',   'c',     15),
            ('B',    'y',    'foo',   'a',     60),

            ('A',    '',     'foo',   'a',     20),
            ('B',    '',     'foo',   'b',     15),

            ('',     '',     'foo',   'a',     25),
        ]
        self.dal.add_quantities(data, 'value')

        match_attrs_keys = ['attr1']  # <- Match using only "attr1" (ignores "attr2").
        results = self.dal.adaptive_disaggregate(match_attrs_keys)

        expected = [
            (1, {'attr1': 'foo', 'attr2': 'a'}, 32.375),   # index (1, 'A', 'x')
            (2, {'attr1': 'foo', 'attr2': 'a'}, 18.5625),  # index (2, 'A', 'y')
            (2, {'attr1': 'foo', 'attr2': 'b'}, 30.0),     # index (2, 'A', 'y')
            (3, {'attr1': 'foo', 'attr2': 'b'}, 3.0),      # index (3, 'B', 'x')
            (3, {'attr1': 'foo', 'attr2': 'c'}, 15.0),     # index (3, 'B', 'x')
            (3, {'attr1': 'foo', 'attr2': 'a'}, 2.8125),   # index (3, 'B', 'x')
            (4, {'attr1': 'foo', 'attr2': 'a'}, 71.25),    # index (4, 'B', 'y')
            (4, {'attr1': 'foo', 'attr2': 'b'}, 12.0),     # index (4, 'B', 'y')
            (5, {'attr1': 'foo', 'attr2': 'a'}, 0.0),      # index (5, 'C', 'x')
            (6, {'attr1': 'foo', 'attr2': 'a'}, 0.0),      # index (6, 'C', 'y')
            (7, {'attr1': 'foo', 'attr2': 'a'}, 0.0),      # index (7, 'C', 'z')
        ]
        results = self.make_hashable(results)
        expected = self.make_hashable(expected)
        self.assertEqual(results, expected)


class TestGetAndSetDataProperty(unittest.TestCase):
    class_under_test = dal_class  # Use auto-assigned DAL class.

    def setUp(self):
        self.dal = self.class_under_test()

        connection = self.dal._get_connection()
        self.addCleanup(connection.close)

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

        self.cursor.execute('''
            INSERT INTO property
            VALUES
                ('a', '{"x": 1, "y": 2}'),
                ('b', '"xyz"'),
                ('c', '0.1875')
        ''')

    def test_get_property_parse_json(self):
        """JSON values should be parsed into objects."""
        value = self.dal._get_data_property(self.cursor, 'a')  # <- Method under test.
        self.assertEqual(value, {'x': 1, 'y': 2})

        value = self.dal._get_data_property(self.cursor, 'b')  # <- Method under test.
        self.assertEqual(value, 'xyz')

        value = self.dal._get_data_property(self.cursor, 'c')  # <- Method under test.
        self.assertEqual(value, 0.1875)

    def test_get_property_missing_key(self):
        """Value should be None when key does not exist."""
        value = self.dal._get_data_property(self.cursor, 'd')  # <- Method under test.
        self.assertIsNone(value)

    def test_set_property(self):
        """Objects should be serialized as JSON formatted strings."""
        self.dal._set_data_property(self.cursor, 'e', [1, 'two', 3.1875])  # <- Method under test.
        self.cursor.execute("SELECT value FROM property WHERE key='e'")
        self.assertEqual(self.cursor.fetchall(), [([1, 'two', 3.1875],)])

    def test_set_property_update_existing(self):
        """Objects that already exist should get updated."""
        self.dal._set_data_property(self.cursor, 'a', [1, 2])  # <- Method under test.

        self.cursor.execute("SELECT value FROM property WHERE key='a'")
        self.assertEqual(self.cursor.fetchall(), [([1, 2],)])

    def test_set_property_value_is_none(self):
        """When value is None, record should be deleted."""
        get_results_sql = "SELECT * FROM property WHERE key IN ('a', 'b', 'c')"

        self.dal._set_data_property(self.cursor, 'a', None)  # <- Method under test.

        self.cursor.execute(get_results_sql)
        self.assertEqual(self.cursor.fetchall(), [('b', 'xyz'), ('c', 0.1875)])

        self.dal._set_data_property(self.cursor, 'b', None)  # <- Method under test.

        self.cursor.execute(get_results_sql)
        self.assertEqual(self.cursor.fetchall(), [('c', 0.1875),])

    def test_set_property_key_is_new_value_is_none(self):
        """Should not insert record when value is None."""
        self.dal._set_data_property(self.cursor, 'f', None)  # <- Method under test.

        self.cursor.execute("SELECT * FROM property WHERE key='f'")
        self.assertEqual(self.cursor.fetchall(), [])


@unittest.skipIf(SQLITE_VERSION_INFO < (3, 24, 0), 'requires 3.24.0 or newer')
class TestGetAndSetDataPropertyLatest(TestGetAndSetDataProperty):
    class_under_test = DataAccessLayer  # Use latest DAL class.


class TestGetAndSetDataPropertyPre24(TestGetAndSetDataProperty):
    class_under_test = DataAccessLayerPre24  # Use legacy DAL class.


class TestGetColumnNames(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()

    def test_get_names(self):
        self.dal.set_data({'add_index_columns': ['A', 'B', 'C']})
        data = self.dal.get_data(['index_columns'])  # <- Method under test.
        self.assertEqual(data, {'index_columns': ['A', 'B', 'C']})

    def test_no_columns_added(self):
        """Should return empty list when no columns have been added."""
        data = self.dal.get_data(['index_columns'])  # <- Method under test.
        self.assertEqual(data, {'index_columns': []})


class TestGetAndSetDiscreteCategories(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()

        connection = self.dal._get_connection()
        self.addCleanup(connection.close)

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

    def test_get_categories(self):
        self.cursor.execute('''
            INSERT INTO property
            VALUES ('discrete_categories', '[["A"], ["A", "B"], ["A", "B", "C"]]')
        ''')
        data = self.dal.get_data(['discrete_categories'])  # <- Method under test.
        expected = {'discrete_categories': [{"A"}, {"A", "B"}, {"A", "B", "C"}]}
        self.assertEqual(data, expected, msg='should get a list of sets')

    def test_get_categories_none_defined(self):
        """If no discrete categories, should return empty list."""
        self.cursor.execute("DELETE FROM property WHERE key='discrete_categories'")
        data = self.dal.get_data(['discrete_categories'])  # <- Method under test.
        self.assertEqual(data, {'discrete_categories': []})

    def test_set_categories(self):
        self.dal.set_data({'add_index_columns': ['A', 'B', 'C']})

        categories = [{'A'}, {'B'}, {'C'}]
        self.dal.add_discrete_categories(categories)  # <- Method under test.

        self.cursor.execute("SELECT value FROM property WHERE key='discrete_categories'")
        result = self.cursor.fetchone()[0]
        self.assertEqual(result, [['A'], ['B'], ['C']])

        self.cursor.execute("SELECT * FROM structure")
        result = {tup[2:] for tup in self.cursor.fetchall()}
        expected = {
            (0, 0, 0), (1, 0, 0), (0, 1, 0), (0, 0, 1),
            (1, 1, 0), (1, 0, 1), (0, 1, 1), (1, 1, 1),
        }
        self.assertEqual(result, expected)

    def test_set_categories_implicit_whole(self):
        """The "whole space" category should be added if not covered
        by a union of existing categories.
        """
        self.dal.set_data({'add_index_columns': ['A', 'B', 'C']})

        categories = [{'A'}, {'B'}]
        self.dal.add_discrete_categories(categories)  # <- Method under test.

        self.cursor.execute("SELECT value FROM property WHERE key='discrete_categories'")
        actual = [set(x) for x in self.cursor.fetchone()[0]]
        expected = [
            {'A'},
            {'B'},
            {'A', 'B', 'C'},  # <- The "whole space" category should be automatically added.
        ]
        self.assertEqual(actual, expected)

    def test_get_and_set_categories(self):
        self.dal.set_data({'add_index_columns': ['A', 'B', 'C']})

        categories = [{'A'}, {'A', 'B'}, {'A', 'B', 'C'}]

        self.dal.add_discrete_categories(categories)  # <- Set!!!
        data = self.dal.get_data(['discrete_categories'])  # <- Get!!!

        self.assertEqual(data['discrete_categories'], categories)


class TestGetProperties(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()

        connection = self.dal._get_connection()
        self.addCleanup(connection.close)

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

        self.cursor.execute('''
            INSERT INTO property
            VALUES
                ('a', '{"x": 1, "y": 2}'),
                ('b', '"xyz"'),
                ('c', '0.1875')
        ''')

    def test_get_properties(self):
        data = self.dal.get_data(['a', 'b'])  # <- Method under test.
        self.assertEqual(data, {'a': {'x': 1, 'y': 2}, 'b': 'xyz'})

    def test_unknown_key(self):
        """Unknown keys should get None values."""
        data = self.dal.get_data(['c', 'd'])  # <- Method under test.
        self.assertEqual(data, {'c': 0.1875, 'd': None})


class TestSetStructure(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()
        self.connection = self.dal._get_connection()
        self.cursor = self.connection.cursor()
        self.addCleanup(self.connection.close)
        self.addCleanup(self.cursor.close)

    def test_insert_structure(self):
        self.dal.set_data({'add_index_columns': ['state', 'county', 'town']})
        structure = [set(),
                     {'state'},
                     {'state', 'county'},
                     {'state', 'county', 'town'}]

        DataAccessLayer._set_data_structure(self.cursor, structure)  # <- Method under test.

        self.cursor.execute('SELECT state, county, town FROM main.structure')
        actual = self.cursor.fetchall()
        expected = [(0, 0, 0),  # <- set()
                    (1, 0, 0),  # <- {'state'}
                    (1, 1, 0),  # <- {'state', 'county'}
                    (1, 1, 1)]  # <- {'state', 'county', 'town'}
        self.assertEqual(actual, expected)

    def test_replace_existing(self):
        self.dal.set_data({'add_index_columns': ['A', 'B', 'C']})
        structure = [set(), {'A', 'B'}, {'A', 'B', 'C'}]
        DataAccessLayer._set_data_structure(self.cursor, structure)

        structure = [set(), {'A'}, {'B'}, {'A', 'B'}, {'A', 'B', 'C'}]
        DataAccessLayer._set_data_structure(self.cursor, structure)  # <- Method under test.

        self.cursor.execute('SELECT A, B, C FROM main.structure')
        actual = self.cursor.fetchall()
        expected = [(0, 0, 0),  # <- set()
                    (1, 0, 0),  # <- {'A'}
                    (0, 1, 0),  # <- {'B'}
                    (1, 1, 0),  # <- {'A', 'B'}
                    (1, 1, 1)]  # <- {'A', 'B', 'C'}
        self.assertEqual(set(actual), set(expected))


class TestRefreshGranularity(unittest.TestCase):
    maxDiff = None

    def test_refresh_granularity_sql(self):
        sql = DataAccessLayer._refresh_granularity_sql(['A', 'B', 'C'])
        expected = """
            WITH
                subset (cardinality) AS (
                    SELECT CAST(COUNT(*) AS REAL)
                    FROM main.node_index
                    WHERE index_id > 0
                    GROUP BY "A", "B", "C"
                ),
                summand (uncertainty) AS (
                    SELECT ((subset.cardinality / :partition_cardinality)
                            * LOG2(subset.cardinality))
                    FROM subset
                ),
                granularity (value) AS (
                    SELECT LOG2(:partition_cardinality) - SUM(uncertainty)
                    FROM summand
                )
            UPDATE main.structure
            SET _granularity = (SELECT value FROM granularity)
            WHERE _structure_id=:structure_id
        """
        self.assertEqual(sql, expected)

    def test_refresh_granularity_sql_no_columns(self):
        sql = DataAccessLayer._refresh_granularity_sql([])
        expected = """
            WITH
                subset (cardinality) AS (
                    SELECT CAST(COUNT(*) AS REAL)
                    FROM main.node_index
                    WHERE index_id > 0
                ),
                summand (uncertainty) AS (
                    SELECT ((subset.cardinality / :partition_cardinality)
                            * LOG2(subset.cardinality))
                    FROM subset
                ),
                granularity (value) AS (
                    SELECT LOG2(:partition_cardinality) - SUM(uncertainty)
                    FROM summand
                )
            UPDATE main.structure
            SET _granularity = (SELECT value FROM granularity)
            WHERE _structure_id=:structure_id
        """
        self.assertEqual(sql, expected)

    def test_refresh_granularity_sql_execute(self):
        # Prepare DAL to test.
        dal = dal_class()
        dal.set_data({'add_index_columns': ['A', 'B', 'C']})
        dal.add_discrete_categories([{'A', 'B'}])
        connection = dal._connection
        dal.add_index_records([
            ['A',  'B',  'C'],
            ['a1', 'b1', 'c1'],
            ['a1', 'b1', 'c2'],
            ['a1', 'b2', 'c3'],
            ['a1', 'b2', 'c4'],
            ['a2', 'b3', 'c5'],
            ['a2', 'b3', 'c6'],
        ])

        # Prepare SQL statement.
        sql = dal._refresh_granularity_sql(['A', 'B'])  # <- Method under test.

        # Define parameters and execute SQL.
        node_cardnality = 6
        structure_id = 2
        connection.execute(sql, (node_cardnality, structure_id))

        # Check for expected result.
        calculated = connection.execute(
            'SELECT _granularity FROM structure WHERE _structure_id=2'
        ).fetchall()[0][0]
        expected = 1.584962500721156
        self.assertAlmostEqual(calculated, expected, places=7)

    def test_refresh_granularity(self):
        """Test for DataAccessLayer._refresh_granularity() method."""
        dal = dal_class()
        dal.set_data({'add_index_columns': ['A', 'B', 'C']})
        dal.add_discrete_categories([{'A'}, {'A', 'B'}])
        cur = dal._connection.cursor()

        columns = ['A',  'B',  'C']
        data = [
            ['a1', 'b1', 'c1'],
            ['a1', 'b1', 'c2'],
            ['a1', 'b2', 'c3'],
            ['a1', 'b2', 'c4'],
            ['a2', 'b3', 'c5'],
            ['a2', 'b3', 'c6'],
            ['a2', 'b4', 'c7'],
            ['a2', 'b4', 'c8'],
        ]
        sql = dal._add_index_records_make_sql(cur, ['A', 'B', 'C'])
        cur.executemany(sql, data)

        dal._refresh_granularity(cur)  # <- Method under test.

        cur.execute('SELECT * FROM main.structure')
        structure_records = cur.fetchall()
        expected = [
            (1, 0.0, 0, 0, 0),  # <- 0.0 granularity for {} (0, 0, 0)
            (2, 1.0, 1, 0, 0),  # <- 1.0 granularity for {A} (1, 0, 0)
            (3, 2.0, 1, 1, 0),  # <- 2.0 granularity for {A, B} (1, 1, 0)
            (4, 3.0, 1, 1, 1),  # <- 3.0 granularity for {A, B, C} (1, 1, 1)
        ]
        self.assertEqual(structure_records, expected)


class TestRefreshIndexHash(unittest.TestCase):
    maxDiff = None

    def test_refresh_index_hash(self):
        """Test for DataAccessLayer._refresh_index_hash() method."""
        dal = dal_class()
        dal.set_data({'add_index_columns': ['A', 'B', 'C']})
        cur = dal._connection.cursor()

        columns = ['A',  'B',  'C']
        data = [
            ['a1', 'b1', 'c1'],
            ['a1', 'b1', 'c2'],
            ['a1', 'b2', 'c3'],
            ['a1', 'b2', 'c4'],
        ]
        sql = dal._add_index_records_make_sql(cur, columns)
        cur.executemany(sql, data)

        dal._refresh_index_hash(cur)  # <- Method under test.

        cur.execute("SELECT value FROM main.property WHERE key='index_hash'")
        index_hash = cur.fetchone()[0]  # Hash of index_id values "1|2|3|4".
        expected = '8e96dc5e83d405a518a3a93fcbaa8f6a21fd909fa989f73635fe74a093615f39'
        self.assertEqual(index_hash, expected)


class TestAddEdgeGetNewId(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()

        con = self.dal._get_connection()
        self.addCleanup(con.close)

        self.cur = con.cursor()
        self.addCleanup(self.cur.close)

    def test_create_new_edge(self):
        edge_id = self.dal._add_edge_get_new_id(
            cursor=self.cur,
            unique_id='00000000-0000-0000-0000-000000000000',
            name='edge 1',
            description='Edge number one.',
            selectors=['[foo="bar"]'],
            filename_hint='other-node-1.toron',
        )
        self.assertEqual(edge_id, 1)

        self.cur.execute('SELECT * FROM main.edge')
        actual = self.cur.fetchall()
        expected = [(
            1,                                      # edge_id
            'edge 1',                               # name
            'Edge number one.',                     # description
            [SimpleSelector('foo', '=', 'bar')],    # selectors
            None,                                   # user_properties
            '00000000-0000-0000-0000-000000000000', # other_unique_id
            'other-node-1.toron',                   # other_filename_hint
            None,                                   # other_index_hash
            0,                                      # is_locally_complete
            1,                                      # is_default
        )]
        self.assertEqual(actual, expected)

    def test_create_new_edge_minimal_args(self):
        edge_id = self.dal._add_edge_get_new_id(
            self.cur, '00000000-0000-0000-0000-000000000000', 'edge 1',
        )
        self.assertEqual(edge_id, 1)

        self.cur.execute('SELECT * FROM main.edge')
        actual = self.cur.fetchall()
        expected = [(
            1,                                      # edge_id
            'edge 1',                               # name
            None,                                   # description
            None,                                   # selectors
            None,                                   # user_properties
            '00000000-0000-0000-0000-000000000000', # other_unique_id
            None,                                   # other_filename_hint
            None,                                   # other_index_hash
            0,                                      # is_locally_complete
            1,                                      # is_default
        )]
        self.assertEqual(actual, expected)

    def test_edge_name_conflict(self):
        """Should fail if trying to load a new edge using the same name
        as an existing edge from the same node.
        """
        # Create new edge.
        new_edge_id = self.dal._add_edge_get_new_id(
            self.cur, '0000-00-00-00-000000', 'edge 1'
        )

        # Try to create a new edge with the same name.
        with self.assertRaises(ToronError):
            new_edge_id = self.dal._add_edge_get_new_id(
                self.cur, '0000-00-00-00-000000', 'edge 1'
            )


class TestAddEdgeRelations(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()

        con = self.dal._get_connection()
        self.addCleanup(con.close)

        self.cur = con.cursor()
        self.addCleanup(self.cur.close)

        self.dal.set_data({'add_index_columns': ['A', 'B', 'C']})
        data = [
            ['A', 'B', 'C'],
            ['a1', 'b1', 'c1'],
            ['a1', 'b1', 'c2'],
            ['a1', 'b2', 'c3'],
            ['a1', 'b2', 'c4'],
        ]
        self.dal.add_index_records(data)
        self.edge_id = self.dal._add_edge_get_new_id(
            self.cur,
            '00000000-0000-0000-0000-000000000000',
            'edge 1',
        )

    def test_add_edge_relations(self):
        self.dal._add_edge_relations(
            cursor=self.cur,
            edge_id=self.edge_id,
            relations=[
                (6, 1, 110.0, None),
                (7, 2, 120.0, None),
                (8, 3, 130.0, None),
                (9, 4, 140.0, None),
                (0, 0,   0.0, None),
            ],
        )

        results = sorted(self.cur.execute("""
            SELECT edge_id, other_index_id, index_id, relation_value
            FROM main.relation
        """))

        expected = [
            (1, 0, 0,   0.0),
            (1, 6, 1, 110.0),
            (1, 7, 2, 120.0),
            (1, 8, 3, 130.0),
            (1, 9, 4, 140.0),
        ]
        self.assertEqual(results, expected)

    def test_update_relations(self):
        self.dal._add_edge_relations(
            cursor=self.cur,
            edge_id=self.edge_id,
            relations=[
                (6, 1, 110.0, None),
                (7, 2, 120.0, None),
                (8, 3, 130.0, None),
                (9, 4, 140.0, None),
                (0, 0,   0.0, None),
            ],
        )

        self.dal._add_edge_relations(  # <- UPDATE EXISTING RELATIONS!
            cursor=self.cur,
            edge_id=self.edge_id,
            relations=[
                (8, 3, 980.0, None),
                (9, 4, 990.0, None),
            ],
        )

        results = sorted(self.cur.execute("""
            SELECT edge_id, other_index_id, index_id, relation_value
            FROM main.relation
        """))

        expected = [
            (1, 0, 0,   0.0),
            (1, 6, 1, 110.0),
            (1, 7, 2, 120.0),
            (1, 8, 3, 980.0),  # <- Updated value!
            (1, 9, 4, 990.0),  # <- Updated value!
        ]
        self.assertEqual(results, expected)

    def test_auto_add_undefined_relation(self):
        self.dal._add_edge_relations(
            cursor=self.cur,
            edge_id=self.edge_id,
            relations=[(6, 1, 110.0, None),
                       (7, 2, 120.0, None),
                       (8, 3, 130.0, None),
                       (9, 4, 140.0, None)],
        )

        results = sorted(self.cur.execute(
            'SELECT edge_id, other_index_id, index_id, relation_value FROM main.relation'
        ))

        expected = [
            (1, 0, 0,   0.0),  # <- Automatically added.
            (1, 6, 1, 110.0),
            (1, 7, 2, 120.0),
            (1, 8, 3, 130.0),
            (1, 9, 4, 140.0),
        ]
        self.assertEqual(results, expected)

    def test_force_undefined_relation_weight(self):
        """Weight value for undefined relation should always be zero."""
        self.dal._add_edge_relations(
            cursor=self.cur,
            edge_id=self.edge_id,
            relations=[(6, 1, 110.0, None),
                       (7, 2, 120.0, None),
                       (0, 0, 999.0, None)],  # <- Undefined relation with weight 999.
        )

        results = sorted(self.cur.execute(
            'SELECT edge_id, other_index_id, index_id, relation_value FROM main.relation'
        ))

        expected = [
            (1, 0, 0,   0.0),  # <- Undefined relation always gets weight value of 0.0.
            (1, 6, 1, 110.0),
            (1, 7, 2, 120.0),
        ]
        self.assertEqual(results, expected)


class TestRefreshProportions(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()

        con = self.dal._get_connection()
        self.addCleanup(con.close)

        self.cur = con.cursor()
        self.addCleanup(self.cur.close)

        self.dal.set_data({'add_index_columns': ['A', 'B', 'C']})
        data = [
            ['A', 'B', 'C'],
            ['a1', 'b1', 'c1'],  # index_id 1
            ['a1', 'b1', 'c2'],  # index_id 2
            ['a1', 'b2', 'c3'],  # index_id 3
            ['a1', 'b2', 'c4'],  # index_id 4
        ]
        self.dal.add_index_records(data)

    def add_edge_and_relations(self, relations):
        """Helper function to add edge and relations."""
        edge_id = self.dal._add_edge_get_new_id(
            self.cur,
            '00000000-0000-0000-0000-000000000000',
            'edge 1',
        )
        self.dal._add_edge_relations(
            cursor=self.cur,
            edge_id=edge_id,
            relations=relations,
        )
        return edge_id

    def get_relations(self, edge_id):
        """Helper function to get relations from database."""
        self.cur.execute("""
            SELECT other_index_id, index_id, relation_value, proportion
            FROM relation
        """)
        return self.cur.fetchall()

    def test_refresh_proportions(self):
        edge_id = self.add_edge_and_relations([
            (6, 1, 117.0, None),
            (6, 2,  91.0, None),
            (7, 3, 110.0, None),
            (7, 4,  50.0, None),
            (8, 3,  97.0, None),
            (8, 4,   0.0, None),
            (0, 0,   0.0, None),
        ])
        self.dal._refresh_proportions(self.cur, edge_id)  # <- Method under test.

        results = self.get_relations(edge_id)
        expected = [
            (6, 1, 117.0, 0.5625),
            (6, 2,  91.0, 0.4375),
            (7, 3, 110.0, 0.6875),
            (7, 4,  50.0, 0.3125),
            (8, 3,  97.0,    1.0),
            (8, 4,   0.0,    0.0),
            (0, 0,   0.0,    1.0),
        ]
        self.assertEqual(results, expected)

    def test_zero_weight_relations(self):
        """When the sum of a relation's values are 0, divide evenly."""
        edge_id = self.add_edge_and_relations([
            (6, 1, 0.0, None),
            (6, 2, 0.0, None),
            (6, 3, 0.0, None),
            (6, 4, 0.0, None),
            (7, 3, 0.0, None),
            (7, 4, 0.0, None),
            (8, 4, 0.0, None),
            (9, 1, 7.5, None),
            (9, 2, 0.0, None),
            (9, 3, 0.0, None),
            (9, 4, 4.5, None),
            (0, 0, 0.0, None),
        ])
        self.dal._refresh_proportions(self.cur, edge_id)  # <- Method under test.

        results = self.get_relations(edge_id)
        expected = (0, 0, 0.0, 1.0)
        self.assertIn(expected, results)

        expected = [
            # For incoming ID 6, there are four relations and their values
            # are all 0.0. When the sum of all values in a relation are 0,
            # they are evenly proportioned.
            (6, 1, 0.0, 0.25),
            (6, 2, 0.0, 0.25),
            (6, 3, 0.0, 0.25),
            (6, 4, 0.0, 0.25),
            # For incoming ID 7, there are two relations and their values
            # are both 0.0. These relations get evenly proportioned.
            (7, 3, 0.0, 0.5),
            (7, 4, 0.0, 0.5),
            # For incoming ID 8, there is a single relation whose value
            # is 0.0. In this case, an "evenly proportioned" weight simply
            # means assigning it all to this one relation (1.0).
            (8, 4, 0.0, 1.0),
            # For incoming ID 9, there are four relations--two of the
            # relations have a value of 0.0 but the sum of all values
            # is greater than zero. Since the sum is greater than 0,
            # the proportions are assigned normally according to value
            # and the zero-weight relations get a proportion of 0.0.
            (9, 1, 7.5, 0.625),
            (9, 2, 0.0, 0.0),
            (9, 3, 0.0, 0.0),
            (9, 4, 4.5, 0.375),
            # Undefined-to-undefined always has proportion of 1.0.
            (0, 0, 0.0, 1.0),
        ]
        self.assertEqual(results, expected)

    def test_handling_for_undefined_points(self):
        edge_id = self.add_edge_and_relations([
            (6, 0, 117.0, None),  # 6 -> 0 (defined-to-undefined)
            (6, 2,  91.0, None),  # 6 -> 2 (defined-to-defined)
            (7, 0, 110.0, None),  # 7 -> 0 (defined-to-undefined)
            (7, 4,  50.0, None),  # 7 -> 4 (defined-to-defined)
            (0, 3,  97.0, None),  # 0 -> 3 (undefined-to-defined)
            (0, 4,   0.0, None),  # 0 -> 4 (undefined-to-defined)
        ])
        self.dal._refresh_proportions(self.cur, edge_id)  # <- Method under test.

        results = self.get_relations(edge_id)
        expected = (0, 0, 0.0, 1.0)
        self.assertIn(expected, results)

        expected = [
            (6, 0, 117.0, 0.5625),  # <- Defined-to-undefined is unaffected.
            (6, 2,  91.0, 0.4375),
            (7, 0, 110.0, 0.6875),  # <- Defined-to-undefined is unaffected.
            (7, 4,  50.0, 0.3125),
            (0, 3,  97.0, 0.0),  # <- Undefined-to-defined always gets 0 regardless of value.
            (0, 4,   0.0, 0.0),  # <- Undefined-to-defined always gets 0 regardless of value.
            (0, 0,   0.0, 1.0),  # <- Undefined-to-undefined always gets 1.0.
        ]
        self.assertEqual(results, expected)


class TestRefreshOtherIndexHash(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()

        con = self.dal._get_connection()
        self.addCleanup(con.close)

        self.cur = con.cursor()
        self.addCleanup(self.cur.close)

        self.dal.set_data({'add_index_columns': ['A', 'B', 'C']})
        data = [
            ['A', 'B', 'C'],
            ['a1', 'b1', 'c1'],
            ['a1', 'b1', 'c2'],
            ['a1', 'b2', 'c3'],
            ['a1', 'b2', 'c4'],
        ]
        self.dal.add_index_records(data)

        # Add first edge.
        edge_id = self.dal._add_edge_get_new_id(
            self.cur, '00000000-0000-0000-0000-000000000000', 'edge 1',
        )
        self.dal._add_edge_relations(
            cursor=self.cur,
            edge_id=edge_id,
            relations=[(6, 1, 110.0, None),
                       (7, 2, 120.0, None),
                       (8, 3, 130.0, None),
                       (9, 4, 140.0, None)],
        )

        # Add second edge (same distinct other_index_id values as "edge 3")
        edge_id = self.dal._add_edge_get_new_id(
            self.cur, '00000000-0000-0000-0000-000000000000', 'edge 2',
        )
        self.dal._add_edge_relations(
            cursor=self.cur,
            edge_id=edge_id,
            relations=[(1, 1, 110.0, None),
                       (1, 2, 120.0, None),
                       (2, 3, 130.0, None),
                       (2, 4, 140.0, None)],
        )

        # Add third edge (same distinct other_index_id values as "edge 2").
        edge_id = self.dal._add_edge_get_new_id(
            self.cur, '00000000-0000-0000-0000-000000000000', 'edge 3',
        )
        self.dal._add_edge_relations(
            cursor=self.cur,
            edge_id=edge_id,
            relations=[(1, 1, 110.0, None), (2, 2, 120.0, None)],
        )

    def test_refresh_single_edge(self):
        self.dal._refresh_other_index_hash(self.cur, 1)  # <- Method under test.

        self.cur.execute('SELECT edge_id, other_index_hash FROM main.edge')
        results = self.cur.fetchall()
        expected = [
            (1, 'a717d3ef0e9800283fedbd8d865df1b441e504ad79a72d9f14c32a28d5fab7b2'),
            (2, None),
            (3, None),
        ]
        self.assertEqual(results, expected)

    def test_refresh_specified_edges(self):
        self.dal._refresh_other_index_hash(self.cur, [1, 2])  # <- Method under test.

        self.cur.execute('SELECT edge_id, other_index_hash FROM main.edge')
        results = self.cur.fetchall()
        expected = [
            (1, 'a717d3ef0e9800283fedbd8d865df1b441e504ad79a72d9f14c32a28d5fab7b2'),
            (2, '22074227d8462b39403011e0bc4c5e7a3f1ee1bae54ae2deb0943dece537f93f'),
            (3, None),
        ]
        self.assertEqual(results, expected)

    def test_refresh_all_edges(self):
        """When no edge_ids are given, all hashes should get refreshed."""
        self.dal._refresh_other_index_hash(self.cur)  # <- Method under test.

        self.cur.execute('SELECT edge_id, other_index_hash FROM main.edge')
        results = self.cur.fetchall()
        expected = [
            (1, 'a717d3ef0e9800283fedbd8d865df1b441e504ad79a72d9f14c32a28d5fab7b2'),
            (2, '22074227d8462b39403011e0bc4c5e7a3f1ee1bae54ae2deb0943dece537f93f'),
            (3, '22074227d8462b39403011e0bc4c5e7a3f1ee1bae54ae2deb0943dece537f93f'),
        ]
        self.assertEqual(results, expected)

        # NOTE: Edges 2 and 3 have the same hash digest because they
        # have the same DISTINCT "other_index_id" values.


class TestRefreshIsLocallyComplete(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()

        con = self.dal._get_connection()
        self.addCleanup(con.close)

        self.cur = con.cursor()
        self.addCleanup(self.cur.close)

        self.dal.set_data({'add_index_columns': ['A', 'B', 'C']})
        data = [
            ['A', 'B', 'C'],
            ['a1', 'b1', 'c1'],  # <- index_id=1
            ['a1', 'b1', 'c2'],  # <- index_id=2
            ['a1', 'b2', 'c3'],  # <- index_id=3
            ['a1', 'b2', 'c4'],  # <- index_id=4
        ]
        self.dal.add_index_records(data)

        # Add first edge.
        edge_id = self.dal._add_edge_get_new_id(
            self.cur, '00000000-0000-0000-0000-000000000000', 'edge 1',
        )
        self.dal._add_edge_relations(
            cursor=self.cur,
            edge_id=edge_id,
            relations=[(1, 1, 110.0, None),
                       (2, 2, 120.0, None),
                       (3, 3, 130.0, None),
                       (4, 4, 140.0, None)],  # <- Complete.
        )

        # Add second edge
        edge_id = self.dal._add_edge_get_new_id(
            self.cur, '00000000-0000-0000-0000-000000000000', 'edge 2',
        )
        self.dal._add_edge_relations(
            cursor=self.cur,
            edge_id=edge_id,
            relations=[(1, 1, 110.0, None),
                       (2, 2, 120.0, None),
                       (3, 3, 100.0, None),
                       (4, 3,  30.0, None)],  # <- Not complete (missing 4).
        )

    def test_complete(self):
        self.dal._refresh_is_locally_complete(self.cur, 1)  # <- Method under test.

        self.cur.execute('SELECT is_locally_complete FROM main.edge WHERE edge_id=1')
        is_locally_complete = self.cur.fetchone()[0]
        self.assertEqual(is_locally_complete, 1)

    def test_not_complete(self):
        self.dal._refresh_is_locally_complete(self.cur, 2)  # <- Method under test.

        self.cur.execute('SELECT is_locally_complete FROM main.edge WHERE edge_id=2')
        is_locally_complete = self.cur.fetchone()[0]
        self.assertEqual(is_locally_complete, 0)


class TestAddEdge(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()

        con = self.dal._get_connection()
        self.addCleanup(con.close)

        self.cur = con.cursor()
        self.addCleanup(self.cur.close)

        self.dal.set_data({'add_index_columns': ['A', 'B', 'C']})
        data = [
            ['A', 'B', 'C'],
            ['a1', 'b1', 'c1'],  # <- index_id=1
            ['a1', 'b1', 'c2'],  # <- index_id=2
            ['a1', 'b2', 'c3'],  # <- index_id=3
            ['a1', 'b2', 'c4'],  # <- index_id=4
        ]
        self.dal.add_index_records(data)

    def test_add_complete_edge(self):
        self.dal.add_incoming_edge(
            unique_id='00000000-0000-0000-0000-000000000000',
            name='edge 1',
            relations=[(1, 1, 110.0, None),
                       (2, 2, 120.0, None),
                       (3, 3, 130.0, None),
                       (4, 4, 140.0, None)],
            description='Edge one description.',
            selectors=['[foo="bar"]'],
            filename_hint='other-file.toron',
        )

        # Check "edge" record.
        results = self.cur.execute('SELECT * FROM main.edge').fetchall()
        expected = [(
            1,
            'edge 1',
            'Edge one description.',
            [SimpleSelector('foo', '=', 'bar')],
            None,
            '00000000-0000-0000-0000-000000000000',
            'other-file.toron',
            '8e96dc5e83d405a518a3a93fcbaa8f6a21fd909fa989f73635fe74a093615f39',
            1,  # <- Is locally complete.
            1,
        )]
        self.assertEqual(results, expected)

        # Check relation records.
        results = self.cur.execute('SELECT * FROM main.relation').fetchall()
        expected = [
            (1, 1, 1, 1, 110.0, 1.0, None),
            (2, 1, 2, 2, 120.0, 1.0, None),
            (3, 1, 3, 3, 130.0, 1.0, None),
            (4, 1, 4, 4, 140.0, 1.0, None),
            (5, 1, 0, 0,   0.0, 1.0, None),
        ]
        self.assertEqual(results, expected)

    def test_default_implicit_handling(self):
        """The first edge between two nodes implicitly receives the
        default flag unless it's explicitly set to False.
        """
        # Add incoming edges and let default flag be assigned implicitly.
        self.dal.add_incoming_edge('1111-11-11-11-111111', 'edge 1', relations=[])
        self.dal.add_incoming_edge('2222-22-22-22-222222', 'edge 1', relations=[])
        self.dal.add_incoming_edge('2222-22-22-22-222222', 'edge 2', relations=[])

        # Check implemented behavior.
        self.cur.execute('SELECT other_unique_id, name, is_default FROM main.edge')
        expected = [
            ('1111-11-11-11-111111', 'edge 1', 1),     # <- Has flag!
            ('2222-22-22-22-222222', 'edge 1', 1),     # <- Has flag!
            ('2222-22-22-22-222222', 'edge 2', None),  # <- Was second, does not have flag.
        ]
        self.assertEqual(set(self.cur.fetchall()), set(expected))

    def test_default_explicit_handling(self):
        """If an edge added later is explicitly assigned the default
        flag, then the flag should be removed from the edge that had
        it previously.
        """
        # Add two edges, setting default flag for both (last edge that was
        # explicitly set to True should be the only one that's True).
        self.dal.add_incoming_edge(
            '1111-11-11-11-111111', 'edge 1', relations=[], make_default=True
        )
        self.dal.add_incoming_edge(
            '1111-11-11-11-111111', 'edge 2', relations=[], make_default=True
        )
        self.dal.add_incoming_edge(
            '2222-22-22-22-222222', 'edge 1', relations=[], make_default=False
        )

        # Check implemented behavior.
        self.cur.execute('SELECT other_unique_id, name, is_default FROM main.edge')
        expected = [
            ('1111-11-11-11-111111', 'edge 1', None),  # <- No longer has flag.
            ('1111-11-11-11-111111', 'edge 2', 1),     # <- Now has the flag!
            ('2222-22-22-22-222222', 'edge 1', None),  # <- Never had the flag!
        ]
        self.assertEqual(set(self.cur.fetchall()), set(expected))

    def test_default_implicit_handling_with_no_flags(self):
        """Additional edges should never implicitly receive the default
        flag even if no existing edge has the flag.
        """
        # Two edges, first gets explicit False, second gets implicit False.
        self.dal.add_incoming_edge(
            '1111-11-11-11-111111', 'edge 1', relations=[], make_default=False
        )
        self.dal.add_incoming_edge(
            '1111-11-11-11-111111', 'edge 2', relations=[]  # <- No explicit is_default.
        )

        # Check implemented behavior.
        self.cur.execute('SELECT other_unique_id, name, is_default FROM main.edge')
        expected = [
            ('1111-11-11-11-111111', 'edge 1', None),  # <- Does not have default flag.
            ('1111-11-11-11-111111', 'edge 2', None),  # <- Does not have default flag.
        ]
        self.assertEqual(set(self.cur.fetchall()), set(expected))

    def test_add_incomplete_edge(self):
        self.dal.add_incoming_edge(
            unique_id='00000000-0000-0000-0000-000000000000',
            name='edge 1',
            relations=[(1, 1, 110.0, None),
                       (2, 2, 120.0, None),
                       (3, 3, 100.0, None),
                       (4, 3,  30.0, None)],
        )

        # Check "edge" record.
        results = self.cur.execute('SELECT * FROM main.edge').fetchall()
        expected = [(
            1,
            'edge 1',
            None,
            None,
            None,
            '00000000-0000-0000-0000-000000000000',
            None,
            '8e96dc5e83d405a518a3a93fcbaa8f6a21fd909fa989f73635fe74a093615f39',
            0,  # <- Not locally complete.
            1,
        )]
        self.assertEqual(results, expected)

        # Check relation records.
        results = self.cur.execute('SELECT * FROM main.relation').fetchall()
        expected =  [
            (1, 1, 1, 1, 110.0, 1.0, None),
            (2, 1, 2, 2, 120.0, 1.0, None),
            (3, 1, 3, 3, 100.0, 1.0, None),
            (4, 1, 4, 3,  30.0, 1.0, None),
            (5, 1, 0, 0,   0.0, 1.0, None),
        ]
        self.assertEqual(results, expected)


class TestEditIncomingEdge(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()

        con = self.dal._get_connection()
        self.addCleanup(con.close)

        self.cur = con.cursor()
        self.addCleanup(self.cur.close)

        self.dal.set_data({'add_index_columns': ['A', 'B', 'C']})
        data = [
            ['A', 'B', 'C'],
            ['a1', 'b1', 'c1'],  # <- index_id=1
            ['a1', 'b1', 'c2'],  # <- index_id=2
            ['a1', 'b2', 'c3'],  # <- index_id=3
            ['a1', 'b2', 'c4'],  # <- index_id=4
        ]
        self.dal.add_index_records(data)

        self.dal.add_incoming_edge(
            unique_id='0000-00-00-00-000000',
            name='edge 1',
            relations=[(1, 1, 11.0, None),
                       (2, 2, 12.0, None),
                       (3, 3, 13.0, None),
                       (4, 4, 14.0, None)],
            description='A description of Edge One.',
            selectors=['[foo="bar"]'],
            filename_hint='other-file.toron',
            #is_default=,
        )

    def test_update_multiple(self):
        self.dal.edit_incoming_edge(
            '0000-00-00-00-000000',
            'edge 1',
            description='An updated description for Edge One.',
            selectors=['[foo="baz"]'],
            filename_hint='updated-file-name.toron',
            is_default=False,
        )

        results = self.cur.execute('SELECT * FROM main.edge').fetchall()
        expected = [(
            1,
            'edge 1',
            'An updated description for Edge One.',
            [SimpleSelector('foo', '=', 'baz')],
            None,
            '0000-00-00-00-000000',
            'updated-file-name.toron',
            '8e96dc5e83d405a518a3a93fcbaa8f6a21fd909fa989f73635fe74a093615f39',
            1,
            None,
        )]
        self.assertEqual(results, expected)

    def test_update_multiple_to_none(self):
        self.dal.edit_incoming_edge(
            '0000-00-00-00-000000',
            'edge 1',
            description=None,
            selectors=None,
            filename_hint=None,
            is_default=None,
        )

        results = self.cur.execute('SELECT * FROM main.edge').fetchall()
        expected = [(
            1,
            'edge 1',
            None,
            None,
            None,
            '0000-00-00-00-000000',
            None,
            '8e96dc5e83d405a518a3a93fcbaa8f6a21fd909fa989f73635fe74a093615f39',
            1,
            None,
        )]
        self.assertEqual(results, expected)

    def test_update_description(self):
        self.dal.edit_incoming_edge(
            '0000-00-00-00-000000',
            'edge 1',
            description='An updated description for Edge One.',
        )

        results = self.cur.execute('SELECT * FROM main.edge').fetchall()
        expected = [(
            1,
            'edge 1',
            'An updated description for Edge One.',  # <- Updated description.
            [SimpleSelector('foo', '=', 'bar')],
            None,
            '0000-00-00-00-000000',
            'other-file.toron',
            '8e96dc5e83d405a518a3a93fcbaa8f6a21fd909fa989f73635fe74a093615f39',
            1,
            1,
        )]
        self.assertEqual(results, expected)

    def test_update_selectors(self):
        self.dal.edit_incoming_edge(
            '0000-00-00-00-000000',
            'edge 1',
            selectors=['[foo="baz"]'],
        )

        results = self.cur.execute('SELECT * FROM main.edge').fetchall()
        expected = [(
            1,
            'edge 1',
            'A description of Edge One.',
            [SimpleSelector('foo', '=', 'baz')],  # <- Updated selectors.
            None,
            '0000-00-00-00-000000',
            'other-file.toron',
            '8e96dc5e83d405a518a3a93fcbaa8f6a21fd909fa989f73635fe74a093615f39',
            1,
            1,
        )]
        self.assertEqual(results, expected)

    def test_update_filename_hint(self):
        self.dal.edit_incoming_edge(
            '0000-00-00-00-000000',
            'edge 1',
            filename_hint='updated-file-name.toron',
        )

        results = self.cur.execute('SELECT * FROM main.edge').fetchall()
        expected = [(
            1,
            'edge 1',
            'A description of Edge One.',
            [SimpleSelector('foo', '=', 'bar')],
            None,
            '0000-00-00-00-000000',
            'updated-file-name.toron',  # <- Updated filename_hint.
            '8e96dc5e83d405a518a3a93fcbaa8f6a21fd909fa989f73635fe74a093615f39',
            1,
            1,
        )]
        self.assertEqual(results, expected)

    def test_update_is_default(self):
        # Add a second edge for testing.
        self.dal.add_incoming_edge(
            '0000-00-00-00-000000',
            'edge 2',
            relations=[],
        )

        def get_results():  # <- Helper function.
            self.cur.execute('SELECT edge_id, is_default FROM main.edge')
            return set(self.cur.fetchall())

        # Check current 'is_default' state. Since 'edge 1' was already
        # set to default, the newly added 'edge 2' did not receive this
        # flag.
        expected = {
            (1, True),  # <- edge_id 1 is default (True)
            (2, None),
        }
        self.assertEqual(get_results(), expected)

        # Change default to 'edge 2' (from 'edge 1').
        self.dal.edit_incoming_edge(  # <- Method under test.
            '0000-00-00-00-000000',
            'edge 2',
            is_default=True,
        )
        expected = {
            (1, None),  # <- edge_id 1 is now NULL/None
            (2, True),  # <- edge_id 2 is now default (True)
        }
        self.assertEqual(get_results(), expected)

        # Disable default flag for 'edge 2' (no default)
        self.dal.edit_incoming_edge(  # <- Method under test.
            '0000-00-00-00-000000',
            'edge 2',
            is_default=False,
        )
        expected = {
            (1, None),  # <- No default edge!
            (2, None),  # <- No default edge!
        }
        self.assertEqual(get_results(), expected)


class TestTranslate(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class()

        con = self.dal._get_connection()
        self.addCleanup(con.close)

        self.cur = con.cursor()
        self.addCleanup(self.cur.close)

        self.dal.set_data({'add_index_columns': ['A', 'B', 'C']})
        data = [
            ['A', 'B', 'C'],
            ['a1', 'b1', 'c1'],  # <- index_id=1
            ['a1', 'b1', 'c2'],  # <- index_id=2
            ['a1', 'b2', 'c3'],  # <- index_id=3
            ['a1', 'b2', 'c4'],  # <- index_id=4
        ]
        self.dal.add_index_records(data)

        self.dal.add_incoming_edge(
            unique_id='00000000-0000-0000-0000-000000000000',
            name='edge 1',
            relations=[
                (1, 1,  39.0, None),  # proportion: 0.6
                (1, 2,  26.0, None),  # proportion: 0.4
                (2, 2,  16.0, None),  # proportion: 1.0
                (3, 2,  50.0, None),  # proportion: 0.250
                (3, 3,  25.0, None),  # proportion: 0.125
                (3, 4, 125.0, None),  # proportion: 0.625
                (4, 3,  64.0, None),  # proportion: 1.0
                (5, 3,  19.0, None),  # proportion: 0.38
                (5, 4,  31.0, None),  # proportion: 0.62
                (0, 0,   0.0, None),  # proportion: 1.0
            ],
            description='Edge one description.',
            selectors=['[foo="bar"]'],
            filename_hint='other-file.toron',
        )

        self.dal.add_incoming_edge(
            unique_id='00000000-0000-0000-0000-000000000000',
            name='edge 2',
            relations=[
                (1, 1, 32.0,  None),  # proportion: 0.5
                (1, 2, 32.0,  None),  # proportion: 0.5
                (2, 2, 15.0,  None),  # proportion: 1.0
                (3, 2, 85.5,  None),  # proportion: 0.333984375
                (3, 3, 85.25, None),  # proportion: 0.3330078125
                (3, 4, 85.25, None),  # proportion: 0.3330078125
                (4, 3, 64.0,  None),  # proportion: 1.0
                (5, 3, 50.0,  None),  # proportion: 0.5
                (5, 4, 50.0,  None),  # proportion: 0.5
                (0, 0,  0.0,  None),  # proportion: 1.0
            ],
            description='Edge two description.',
            selectors=['[foo]'],
            filename_hint='other-file.toron',
        )
        #print(self.cur.execute('SELECT * FROM main.relation').fetchall())

    def test_translate_generator(self):
        quantities = QuantityIterator(
            '00000000-0000-0000-0000-000000000000',
            [
                (1, {'foo': 'bar'}, 100),
                (2, {'foo': 'bar'}, 100),
                (3, {'foo': 'bar'}, 100),
                (4, {'foo': 'bar'}, 100),
                (5, {'foo': 'bar'}, 100),
            ],
        )
        results = self.dal._translate_generator(self.cur, quantities)

        expected = [
            (1, {'foo': 'bar'}, 60.0),
            (2, {'foo': 'bar'}, 40.0),
            (2, {'foo': 'bar'}, 100.0),
            (2, {'foo': 'bar'}, 25.0),
            (3, {'foo': 'bar'}, 12.5),
            (4, {'foo': 'bar'}, 62.5),
            (3, {'foo': 'bar'}, 100.0),
            (3, {'foo': 'bar'}, 38.0),
            (4, {'foo': 'bar'}, 62.0),
        ]
        self.assertEqual(list(results), expected)

    def test_simple_case(self):
        quantities = QuantityIterator(
            '00000000-0000-0000-0000-000000000000',
            [
                (1, {'foo': 'bar'}, 100),
                (2, {'foo': 'bar'}, 100),
                (3, {'foo': 'bar'}, 100),
                (4, {'foo': 'bar'}, 100),
                (5, {'foo': 'bar'}, 100),
            ],
        )
        results = self.dal.translate(quantities)

        self.assertIsInstance(results, QuantityIterator)
        self.assertNotEqual(
            quantities.unique_id,
            results.unique_id,
            msg='results should NOT match previous unique_id',
        )
        self.assertEqual(
            results.unique_id,
            self.dal.unique_id,
            msg='results should match unique_id of node',
        )
        expected = [
            (1, {'foo': 'bar'}, 60),
            (2, {'foo': 'bar'}, 165),
            (3, {'foo': 'bar'}, 150.5),
            (4, {'foo': 'bar'}, 124.5),
        ]
        self.assertEqual(list(results), expected)

    def test_handling_multiple_edges(self):
        """Check that quantities are translated using appropriate edges.

        Quantities should be matched by their attributes to the edge
        with the greatest unique specificity or the default edge if
        there is no unique match.
        """
        quantities = QuantityIterator(
            '00000000-0000-0000-0000-000000000000',
            [
                # Attributes {'foo': 'bar'} match 'edge 1' ([foo="bar"])
                # and 'edge 2' ([foo]), but 'edge 1' is used because it
                # has a greater specificity.
                (1, {'foo': 'bar'}, 100),
                (2, {'foo': 'bar'}, 100),
                (3, {'foo': 'bar'}, 100),
                (4, {'foo': 'bar'}, 100),
                (5, {'foo': 'bar'}, 100),

                # Attributes {'foo': 'baz'} match 'edge 2' ([foo]).
                (1, {'foo': 'baz'}, 100),
                (2, {'foo': 'baz'}, 100),
                (3, {'foo': 'baz'}, 100),
                (4, {'foo': 'baz'}, 100),
                (5, {'foo': 'baz'}, 100),
            ],
        )
        results = self.dal.translate(quantities)

        expected = [
            (1, {'foo': 'bar'}, 60.0),          # <- Edge 1
            (1, {'foo': 'baz'}, 50.0),          # <- Edge 2
            (2, {'foo': 'bar'}, 165.0),         # <- Edge 1
            (2, {'foo': 'baz'}, 183.3984375),   # <- Edge 2
            (3, {'foo': 'bar'}, 150.5),         # <- Edge 1
            (3, {'foo': 'baz'}, 183.30078125),  # <- Edge 2
            (4, {'foo': 'bar'}, 124.5),         # <- Edge 1
            (4, {'foo': 'baz'}, 83.30078125),   # <- Edge 2
        ]
        self.assertEqual(list(results), expected)
