# -*- coding: utf-8 -*-
import decimal
import os
import sqlite3

from gpn.tests import _unittest as unittest
from gpn.tests.common import MkdtempTestCase

from gpn.connector import _create_partition
from gpn.connector import _normalize_args_for_trigger
from gpn.connector import _null_clause_for_trigger
from gpn.connector import _where_clause_for_trigger
from gpn.connector import _insert_trigger
from gpn.connector import _update_trigger
from gpn.connector import _delete_trigger
from gpn.connector import _foreign_key_triggers
from gpn.connector import _Connector
from gpn.connector import IN_MEMORY
from gpn.connector import TEMP_FILE
from gpn.connector import READ_ONLY


try:
    callable  # Removed from 3.0 and 3.1, added back in 3.2.
except NameError:
    def callable(obj):
        parent_types = type(obj).__mro__
        return any('__call__' in typ.__dict__ for typ in parent_types)


class TestTriggerFunctions(unittest.TestCase):
    """Trigger functions are used to build 'foreign key constraint'
    triggers for older versions of SQLite that don't enforce foreign
    keys natively.

    """
    def test_normalize_args(self):
        """Args should be normalized as a sequence of objects."""
        # Single key.
        args = _normalize_args_for_trigger('foo_id', 'id', True)
        self.assertEqual((['foo_id'], ['id'], [True]), args)

        # Multiple keys.
        args = _normalize_args_for_trigger(child_key=['foo_id1', 'foo_id2'],
                                           parent_key=['id1', 'id2'],
                                           not_null=[True, False])
        expected = (
            ['foo_id1', 'foo_id2'],
            ['id1', 'id2'],
            [True, False]
        )
        self.assertEqual(expected, args)

        # Multiple keys with not_null expansion.
        args = _normalize_args_for_trigger(child_key=['foo_id1', 'foo_id2'],
                                           parent_key=['id1', 'id2'],
                                           not_null=True)
        expected = (
            ['foo_id1', 'foo_id2'],
            ['id1', 'id2'],
            [True, True]  # not_null converted to list of equal size
        )
        self.assertEqual(expected, args)

    def test_null_clause(self):
        """If foreign key is not "NOT NULL", must add additional clause."""
        # Single key, not_null=True.
        null_clause = _null_clause_for_trigger(['foo_id'], [True], 'NEW')
        self.assertEqual('', null_clause)

        # not_null=False
        null_clause = _null_clause_for_trigger(['foo_id'], [False], 'NEW')
        self.assertEqual('NEW.foo_id IS NOT NULL\n             AND ', null_clause)

        # Multiple keys, nulls allowed for all.
        null_clause = _null_clause_for_trigger(['foo_id1', 'foo_id2'], [False, False], 'NEW')
        expected = 'NEW.foo_id1 IS NOT NULL AND NEW.foo_id2 IS NOT NULL\n             AND '
        self.assertEqual(expected, null_clause)

        # Multiple keys, nulls allowed for some but not others.
        null_clause = _null_clause_for_trigger(['foo_id1', 'foo_id2'], [True, False], 'NEW')
        expected = 'NEW.foo_id2 IS NOT NULL\n             AND '
        self.assertEqual(expected, null_clause)

    def test_where_clause(self):
        """Where clause must work for single and composite foreign key triggers.
        Should be 'child_key=NEW.parent_key' or 'parent_key=OLD.child_key'.

        """
        where_clause = _where_clause_for_trigger(['foo_id'], ['id'], 'NEW')
        self.assertEqual('foo_id=NEW.id', where_clause)

        where_clause = _where_clause_for_trigger(['foo_id1', 'foo_id2'],
                                                 ['id1', 'id2'],
                                                 'NEW')
        self.assertEqual('foo_id1=NEW.id1 AND foo_id2=NEW.id2', where_clause)

    def test_insert_trigger(self):
        kwds = {'name': 'fki_bar_foo_id',
                'child': 'bar',
                'null_clause': '',
                'parent': 'foo',
                'where_clause': 'id=NEW.foo_id'}
        trigger_sql = _insert_trigger(**kwds)

        expected = ("CREATE TEMPORARY TRIGGER IF NOT EXISTS fki_bar_foo_id\n"
                    "BEFORE INSERT ON main.bar FOR EACH ROW\n"
                    "WHEN (SELECT 1 FROM main.foo WHERE id=NEW.foo_id) IS NULL\n"
                    "BEGIN\n"
                    "    SELECT RAISE(ABORT, 'FOREIGN KEY constraint failed');\n"
                    "END;")
        self.assertEqual(expected, trigger_sql)

    def test_update_trigger(self):
        kwds = {'name': 'fku_bar_foo_id',
                'child': 'bar',
                'null_clause': '',
                'parent': 'foo',
                'where_clause': 'id=NEW.foo_id'}
        trigger_sql = _update_trigger(**kwds)

        expected = ("CREATE TEMPORARY TRIGGER IF NOT EXISTS fku_bar_foo_id\n"
                    "BEFORE UPDATE ON main.bar FOR EACH ROW\n"
                    "WHEN (SELECT 1 FROM main.foo WHERE id=NEW.foo_id) IS NULL\n"
                    "BEGIN\n"
                    "    SELECT RAISE(ABORT, 'FOREIGN KEY constraint failed');\n"
                    "END;")
        self.assertEqual(expected, trigger_sql)

    def test_delete_trigger(self):
        kwds = {'name': 'fkd_bar_foo_id',
                'child': 'bar',
                'null_clause': '',
                'parent': 'foo',
                'where_clause': 'foo_id=OLD.id'}
        trigger_sql = _delete_trigger(**kwds)


        expected = ("CREATE TEMPORARY TRIGGER IF NOT EXISTS fkd_bar_foo_id\n"
                    "BEFORE DELETE ON main.foo FOR EACH ROW\n"
                    "WHEN (SELECT 1 FROM main.bar WHERE foo_id=OLD.id) IS NOT NULL\n"
                    "BEGIN\n"
                    "    SELECT RAISE(ABORT, 'FOREIGN KEY constraint failed');\n"
                    "END;")
        self.assertEqual(expected, trigger_sql)

    def test_trigger_actions(self):
        """Actions that violate foreign key constraints must fail."""
        connection = sqlite3.connect(':memory:')
        cursor = connection.cursor()
        create_table_sql = """
            create table foo (
                id INTEGER NOT NULL PRIMARY KEY
            );
            CREATE TABLE bar (
                id INTEGER NOT NULL PRIMARY KEY,
                foo_id INTEGER NOT NULL /* CONSTRAINT fk_foo_id REFERENCES foo(id) */
            );
        """
        # Create tables, get sql, and create foreign key triggers.
        cursor.executescript(create_table_sql)
        create_triggers_sql = _foreign_key_triggers('foobar', 'bar', 'foo_id', 'foo', 'id')
        cursor.executescript(create_triggers_sql)

        # Insert test values.
        cursor.execute('INSERT INTO foo VALUES (1)')
        cursor.execute('INSERT INTO foo VALUES (2)')
        cursor.execute('INSERT INTO bar VALUES (1, 1)')
        cursor.execute('INSERT INTO bar VALUES (2, 2)')

        def insert_failure():
            cursor.execute('INSERT INTO bar VALUES (3, 3)')
        regex = 'FOREIGN KEY constraint failed'
        self.assertRaisesRegex(sqlite3.IntegrityError, regex, insert_failure)

        def update_failure():
            cursor.execute('UPDATE bar SET foo_id=3 WHERE id=2')
        regex = 'FOREIGN KEY constraint failed'
        self.assertRaisesRegex(sqlite3.IntegrityError, regex, update_failure)

        def delete_failure():
            cursor.execute('DELETE FROM foo WHERE id=1')
        regex = 'FOREIGN KEY constraint failed'
        self.assertRaisesRegex(sqlite3.IntegrityError, regex, delete_failure)


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

    def test_existing_database(self):
        """Existing database should load without errors."""
        global _create_partition

        database = 'partition_database'
        connection = sqlite3.connect(database)
        cursor = connection.cursor()
        cursor.execute('PRAGMA synchronous=OFF')
        cursor.executescript(_create_partition)  # Creating database.
        cursor.execute('PRAGMA synchronous=FULL')
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
        """Tempfile should be removed when object is garbage collected."""
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

    def test_read_only_database(self):
        """Read-only connections should fail on INSERT, UPDATE, etc."""
        global _create_partition

        database = 'partition_database'
        connection = sqlite3.connect(database)
        cursor = connection.cursor()
        cursor.execute('PRAGMA synchronous=OFF')
        cursor.executescript(_create_partition)  # Creating database.
        cursor.executescript("""
            INSERT INTO cell VALUES (1, 0);
            INSERT INTO cell VALUES (2, 0);
            INSERT INTO cell VALUES (3, 0);
        """)
        cursor.execute('PRAGMA synchronous=FULL')
        connection.close()

        connect = _Connector(database, mode=READ_ONLY)
        connection = connect()
        cursor = connection.cursor()
        def insert_into():
            cursor.execute('INSERT INTO cell VALUES (4, 0)')
        self.assertRaises(sqlite3.OperationalError, insert_into)

        def update():
            cursor.execute('UPDATE cell SET partial=1 WHERE cell_id=3')
        self.assertRaises(sqlite3.OperationalError, update)

        def drop_table():
            cursor.execute('DROP TABLE cell')
        self.assertRaises(sqlite3.OperationalError, drop_table)

        def alter_table():
            cursor.execute('ALTER TABLE cell ADD COLUMN other TEXT')
        self.assertRaises(sqlite3.OperationalError, alter_table)

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
        regex = 'File - .* - is not a valid partition.'
        self.assertRaisesRegex(Exception, regex, wrong_database)

    def test_wrong_file_type(self):
        """Non-SQLite files should fail to load."""
        filename = 'test.txt'
        fh = open(filename, 'w')
        fh.write('This is a text file.')
        fh.close()

        # Attempt to load non-SQLite file.
        def wrong_file_type():
            connect = _Connector(filename)
        regex = 'File - .* - is not a valid partition.'
        self.assertRaisesRegex(Exception, regex, wrong_file_type)


class TestSqlDataModel(unittest.TestCase):
    def setUp(self):
        self._connect = _Connector(mode=IN_MEMORY)
        self.connection = self._connect()

    def test_foreign_keys(self):
        """Foreign key constraints should be enforced."""
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'region', 0)")

        def foreign_key_constraint():
            cursor.execute("INSERT INTO label VALUES (1, 2, 'Midwest')")
        self.assertRaises(sqlite3.IntegrityError, foreign_key_constraint)

    def test_cell_defaults(self):
        """Should be possible to insert records in to cell using defaults vals."""
        cursor = self.connection.cursor()
        cursor.execute('INSERT INTO cell DEFAULT VALUES')
        cursor.execute('SELECT * FROM cell')
        self.assertEqual([(1, 0)], cursor.fetchall())

    def test_label_autoincrement(self):
        """Label_id should auto-increment despite being in a composite key."""
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

        self.connection.commit()

        def insert_duplicate():
            # Insert label_id combination that conflicts with cell_id 1.
            cursor.execute("INSERT INTO cell VALUES (3, 0)")
            cursor.execute("INSERT INTO cell_label VALUES (5, 3, 1, 1)")
            cursor.execute("INSERT INTO cell_label VALUES (6, 3, 2, 2)")
        regex = 'violates unique label-combination constraint'
        self.assertRaisesRegex(sqlite3.IntegrityError, regex, insert_duplicate)

        def update_duplicate():
            # Update label_id creating conflict with cell_id 1.
            cursor.execute("UPDATE cell_label SET label_id=2 WHERE cell_label_id=4")
        regex = 'violates unique label-combination constraint'
        self.assertRaisesRegex(sqlite3.IntegrityError, regex, update_duplicate)

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


if __name__ == '__main__':
    unittest.main()
