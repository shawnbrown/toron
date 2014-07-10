# -*- coding: utf-8 -*-
import decimal
import glob
import os
import sqlite3
import sys
import tempfile

from gpn.tests import _unittest as unittest

from gpn.partition import _create_partition
from gpn.partition import _Connector
from gpn.partition import Partition
from gpn.partition import IN_MEMORY
from gpn.partition import TEMP_FILE
from gpn.partition import READ_ONLY


try:
    callable  # Removed from 3.0 and 3.1, added back in 3.2.
except NameError:
    def callable(obj):
        parent_types = type(obj).__mro__
        return any('__call__' in typ.__dict__ for typ in parent_types)


class MkdtempTestCase(unittest.TestCase):
    # TestCase changes cwd to temporary location.  After testing,
    # removes files and restores original cwd.
    @classmethod
    def setUpClass(cls):
        cls._orig_dir = os.getcwd()
        cls._temp_dir = tempfile.mkdtemp()  # Requires mkdtemp--cannot
        os.chdir(cls._temp_dir)             # use TemporaryDirectory.

    @classmethod
    def tearDownClass(cls):
        os.chdir(cls._orig_dir)
        os.rmdir(cls._temp_dir)

    def setUp(self):
        self._no_class_fixtures = not hasattr(self, '_temp_dir')
        if self._no_class_fixtures:
            self.setUpClass.__func__(self)

    def tearDown(self):
        self._remove_tempfiles()
        if self._no_class_fixtures:
            self.tearDownClass.__func__(self)

    def _remove_tempfiles(self):
        for path in glob.glob(os.path.join(self._temp_dir, '*')):
            os.remove(path)


class TestConnector(MkdtempTestCase):
    def _get_tables(self, database):
        """Return tuple of expected tables and actual tables for given
        SQLite database."""
        if callable(database):
            connection = database()
        else:
            connection = sqlite3.connect(database)
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        actual_tables = set(x[0] for x in cursor)
        connection.close()
        expected_tables = set([
            'cell', 'hierarchy', 'label', 'cell_label', 'partition',
            'edge', 'edge_weight', 'relation', 'relation_weight', 'property',
            'sqlite_sequence'
        ])
        return expected_tables, actual_tables

    def test_path_to_uri(self):
        # Basic path translation.
        uri = _Connector._path_to_uri('foo')
        self.assertEqual('file:foo', uri)

        uri = _Connector._path_to_uri('/foo')
        self.assertEqual('file:/foo', uri)

        uri = _Connector._path_to_uri('foo/../bar/')
        self.assertEqual('file:bar', uri)

        uri = _Connector._path_to_uri('/foo/../bar/')
        self.assertEqual('file:/bar', uri)

        # Query parameters.
        uri = _Connector._path_to_uri('foo', mode='ro')
        self.assertEqual('file:foo?mode=ro', uri)

        uri = _Connector._path_to_uri('foo', mode=None)
        self.assertEqual('file:foo', uri, 'None values must be removed.')

        uri = _Connector._path_to_uri('foo', mode='ro', cache='shared')
        self.assertEqual('file:foo?cache=shared&mode=ro', uri)

        # Special characters.
        uri = _Connector._path_to_uri('/foo?/bar#')
        self.assertEqual('file:/foo%3F/bar%23', uri)

        uri = _Connector._path_to_uri('foo', other='foo?bar#')
        self.assertEqual('file:foo?other=foo%3Fbar%23', uri)

    @unittest.skipUnless(os.name == 'nt', 'Windows-only path tests.')
    def test_win_path_to_uri(self):
        uri = _Connector._path_to_uri(r'foo\bar')
        self.assertEqual('file:foo/bar', uri)

        uri = _Connector._path_to_uri(r'C:\foo\bar')
        self.assertEqual('file:///C:/foo/bar', uri)

        uri = _Connector._path_to_uri(r'C:foo\bar')
        self.assertEqual('file:///C:/foo/bar', uri)

    def test_existing_database(self):
        """Existing database should load without errors."""
        global _create_partition

        database = 'partition_database'
        connection = sqlite3.connect(database)
        cursor = connection.cursor()
        cursor.executescript(_create_partition)  # Creating database.
        connection.close()

        connect = _Connector(database)  # Existing database.
        connection = connect()
        self.assertIsInstance(connection, sqlite3.Connection)

    def test_new_database(self):
        """If named database does not exist, it should be created."""
        database = 'partition_database'

        self.assertFalse(os.path.exists(database))  # File should not exist.

        connect = _Connector(database)
        self.assertTrue(os.path.exists(database))  # Now, file should exist.

        # Check that file contains expected tables.
        expected_tables, actual_tables = self._get_tables(database)
        self.assertSetEqual(expected_tables, actual_tables)

    def test_temp_file_database(self):
        connect = _Connector(mode=TEMP_FILE)
        filename = connect._temp_path

        # Check that database contains expected tables.
        expected_tables, actual_tables = self._get_tables(filename)
        self.assertSetEqual(expected_tables, actual_tables)

        # Make sure that temp file is removed up when object is deleted.
        self.assertTrue(os.path.exists(filename))  # Should exist.
        del connect
        self.assertFalse(os.path.exists(filename))  # Should not exist.

    def test_in_memory_temp_database(self):
        """In-memory database."""
        connect = _Connector(mode=IN_MEMORY)
        self.assertIsNone(connect._temp_path)
        self.assertIsInstance(connect._memory_conn, sqlite3.Connection)

        # Check that database contains expected tables.
        expected_tables, actual_tables = self._get_tables(connect)
        self.assertSetEqual(expected_tables, actual_tables)

        second_connect = _Connector(mode=IN_MEMORY)
        msg = 'Multiple in-memory connections must be independent.'
        self.assertIsNot(connect._memory_conn, second_connect._memory_conn, msg)

    def test_bad_sqlite_structure(self):
        """SQLite databases with unexpected table structure should fail."""
        filename = 'unknown_database.db'
        connection = sqlite3.connect(filename)
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE foo (bar, baz)')
        connection.close()

        # Attempt to load a non-Partition SQLite database.
        def wrong_database():
            connect = _Connector(filename)
        self.assertRaises(Exception, wrong_database)

    def test_wrong_file_type(self):
        """Non-SQLite files should fail to load."""
        filename = 'test.txt'
        fh = open(filename, 'w')
        fh.write('This is a text file.')
        fh.close()

        # Attempt to load non-SQLite file.
        def wrong_file_type():
            connect = _Connector(wrong_file_type)


class TestSqlDataModel(MkdtempTestCase):
    def setUp(self):
        self._partition = Partition()
        self.connection = self._partition._connect()
        super(self.__class__, self).setUp()

    def test_foreign_keys(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'region', 0)")

        def foreign_key_constraint():
            cursor.execute("INSERT INTO label VALUES (1, 2, 'Midwest')")
        self.assertRaises(sqlite3.IntegrityError, foreign_key_constraint)

    def test_cell_defaults(self):
        cursor = self.connection.cursor()
        cursor.execute('INSERT INTO cell DEFAULT VALUES')
        cursor.execute('SELECT * FROM cell')
        self.assertEqual([(1, 0)], cursor.fetchall())

    def test_label_autoincrement(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'region', 0)")
        cursor.executescript("""
            INSERT INTO label VALUES (NULL, 1, 'Midwest');
            INSERT INTO label VALUES (NULL, 1, 'Northeast');
            INSERT INTO label VALUES (4,    1, 'South');  /* <- Explicit id. */
            INSERT INTO label VALUES (NULL, 1, 'West');
        """)
        cursor.execute('SELECT * FROM label')
        expected = [(1, 1, 'Midwest'),
                    (2, 1, 'Northeast'),
                    (4, 1, 'South'),
                    (5, 1, 'West')]
        self.assertEqual(expected, cursor.fetchall())

    def test_label_unique_constraint(self):
        """Labels must be unique within their hierarchy level."""
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'region', 0)")

        def unique_constraint():
            cursor.executescript("""
                INSERT INTO label VALUES (NULL, 1, 'Midwest');
                INSERT INTO label VALUES (NULL, 1, 'Midwest');
            """)
        self.assertRaises(sqlite3.IntegrityError, unique_constraint)

    def test_cell_label_foreign_key(self):
        """Mismatched hierarchy_id/label_id pairs must fail."""
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'region', 0)")
        cursor.execute("INSERT INTO hierarchy VALUES (2, 'state',  1)")
        cursor.execute("INSERT INTO cell VALUES (1, 0)")
        cursor.execute("INSERT INTO label VALUES (1, 1, 'Midwest')")
        cursor.execute("INSERT INTO label VALUES (2, 2, 'Ohio')")

        def foreign_key_constraint():
            cursor.execute("INSERT INTO cell_label VALUES (1, 1, 1, 2)")
        self.assertRaises(sqlite3.IntegrityError, foreign_key_constraint)

    def test_cell_label_unique_constraint(self):
        """Cells must never have two labels from the same hierarchy level."""
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'region', 0)")
        cursor.execute("INSERT INTO hierarchy VALUES (2, 'state',  1)")
        cursor.execute("INSERT INTO cell VALUES (1, 0)")
        cursor.execute("INSERT INTO label VALUES (1, 1, 'Midwest')")
        cursor.execute("INSERT INTO cell_label VALUES (1, 1, 1, 1)")

        def unique_constraint():
            cursor.execute("INSERT INTO cell_label VALUES (2, 1, 1, 1)")
        self.assertRaises(sqlite3.IntegrityError, unique_constraint)

    def test_cell_label_trigger(self):
        """Each cell_id must be associated with a unique combination of
        label_ids.

        """
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'region', 0)")
        cursor.execute("INSERT INTO label VALUES (1, 1, 'Midwest')")

        cursor.execute("INSERT INTO hierarchy VALUES (2, 'state',  1)")
        cursor.execute("INSERT INTO label VALUES (2, 2, 'Ohio')")
        cursor.execute("INSERT INTO label VALUES (3, 2, 'Indiana')")

        cursor.execute("INSERT INTO cell VALUES (1, 0)")
        cursor.execute("INSERT INTO cell_label VALUES (1, 1, 1, 1)")
        cursor.execute("INSERT INTO cell_label VALUES (2, 1, 2, 2)")

        cursor.execute("INSERT INTO cell VALUES (2, 0)")
        cursor.execute("INSERT INTO cell_label VALUES (3, 2, 1, 1)")
        cursor.execute("INSERT INTO cell_label VALUES (4, 2, 2, 3)")

        def duplicate_combination():
            # Insert label_id combination that conflicts with cell_id 1.
            cursor.execute("INSERT INTO cell VALUES (3, 0)")
            cursor.execute("INSERT INTO cell_label VALUES (5, 3, 1, 1)")
            cursor.execute("INSERT INTO cell_label VALUES (6, 3, 2, 2)")
        regex = 'violates unique label-combination constraint'
        self.assertRaisesRegex(sqlite3.IntegrityError, regex,
                               duplicate_combination)

    def test_textnum_decimal_type(self):
        """Decimal type values should be adapted as strings for TEXTNUM
        columns.  Fetched TEXTNUM values should be converted to Decimal
        types.

        """
        cursor = self.connection.cursor()
        cursor.execute('CREATE TEMPORARY TABLE test (weight TEXTNUM)')
        cursor.execute('INSERT INTO test VALUES (?)', (decimal.Decimal('1.1'),))
        cursor.execute('INSERT INTO test VALUES (?)', (decimal.Decimal('2.2'),))

        cursor.execute('SELECT * FROM test')
        expected = [(decimal.Decimal('1.1'),), (decimal.Decimal('2.2'),)]
        msg = 'TEXTNUM values must be converted to Decimal type.'
        self.assertEqual(expected, cursor.fetchall(), msg)


class TestPartition(MkdtempTestCase):
    def test_existing_partition(self):
        """Existing partition should load without errors."""
        global _create_partition

        filename = 'existing_partition'
        connection = sqlite3.connect(filename)
        cursor = connection.cursor()
        cursor.executescript(_create_partition)  # Creating existing partition.
        connection.close()

        ptn = Partition(filename)  # Use existing file.

    @unittest.skip('Temporarily while removing URI Filename requirement.')
    @unittest.skipUnless(sys.version_info >= (3, 4), 'Only supported on 3.4.')
    def test_read_only_partition(self):
        """Existing partition should load without errors."""
        global _create_partition

        filename = 'existing_partition'
        connection = sqlite3.connect(filename)
        cursor = connection.cursor()
        cursor.executescript(_create_partition)  # Creating existing partition.
        connection.close()

        def read_only():
            ptn = Partition(filename, mode=READ_ONLY)
            connection = ptn._connect()
            cursor = connection.cursor()
            cursor.execute('INSERT INTO cell DEFAULT VALUES')
        self.assertRaises(sqlite3.OperationalError, read_only)

    def test_new_partition(self):
        filename = 'new_partition'

        self.assertFalse(os.path.exists(filename))
        ptn = Partition(filename)  # Create new file.
        del ptn
        self.assertTrue(os.path.exists(filename))

    def test_temporary_partition(self):
        # In memory.
        ptn = Partition()
        self.assertIsNone(ptn._connect._temp_path)
        self.assertIsNotNone(ptn._connect._memory_conn)

        # On disk.
        ptn = Partition(mode=TEMP_FILE)
        self.assertIsNotNone(ptn._connect._temp_path)
        self.assertIsNone(ptn._connect._memory_conn)


if __name__ == '__main__':
    unittest.main()
