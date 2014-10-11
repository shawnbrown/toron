# -*- coding: utf-8 -*-
import decimal
import os
import sqlite3
import re

from gpn.tests import _unittest as unittest
from gpn.tests.common import MkdtempTestCase

from gpn.connector import _schema_items
from gpn.connector import _normalize_args_for_trigger
from gpn.connector import _null_clause_for_trigger
from gpn.connector import _where_clause_for_trigger
from gpn.connector import _insert_trigger
from gpn.connector import _update_trigger
from gpn.connector import _delete_trigger
from gpn.connector import _foreign_key_triggers
from gpn.connector import _read_only_triggers
from gpn.connector import _Connector
from gpn.connector import _SharedConnection
from gpn.connector import IN_MEMORY
from gpn.connector import TEMP_FILE
from gpn.connector import READ_ONLY


try:
    callable  # Removed from 3.0 and 3.1, added back in 3.2.
except NameError:
    def callable(obj):
        parent_types = type(obj).__mro__
        return any('__call__' in typ.__dict__ for typ in parent_types)


class TestForeignKeyTriggers(unittest.TestCase):
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

        regex = 'FOREIGN KEY constraint failed'

        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute('INSERT INTO bar VALUES (3, 3)')

        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute('UPDATE bar SET foo_id=3 WHERE id=2')

        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute('DELETE FROM foo WHERE id=1')


class TestReadOnlyTriggers(unittest.TestCase):
    """Trigger functions are used provide partial read-only support for
    older versions of SQLite that don't implement the query_only PRAGMA.

    """
    def test_read_only_syntax(self):
        trigger_sql = _read_only_triggers('readonlyfoo', 'foo')
        expected = (
            "CREATE TEMPORARY TRIGGER IF NOT EXISTS roi_readonlyfoo\n"
            "BEFORE INSERT ON main.foo FOR EACH ROW\n"
            "BEGIN\n"
            "    SELECT RAISE(ABORT, 'attempt to write a readonly database');\n"
            "END;\n"
            "\n"
            "CREATE TEMPORARY TRIGGER IF NOT EXISTS rou_readonlyfoo\n"
            "BEFORE UPDATE ON main.foo FOR EACH ROW\n"
            "BEGIN\n"
            "    SELECT RAISE(ABORT, 'attempt to write a readonly database');\n"
            "END;\n"
            "\n"
            "CREATE TEMPORARY TRIGGER IF NOT EXISTS rod_readonlyfoo\n"
            "BEFORE DELETE ON main.foo FOR EACH ROW\n"
            "BEGIN\n"
            "    SELECT RAISE(ABORT, 'attempt to write a readonly database');\n"
            "END;"
        )
        self.assertEqual(expected, trigger_sql)

    def test_trigger_actions(self):
        """Actions that violate foreign key constraints must fail."""
        connection = sqlite3.connect(':memory:')
        cursor = connection.cursor()
        create_table = 'CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY);'
        cursor.executescript(create_table)

        # Test values.
        cursor.execute('INSERT INTO foo VALUES (1)')
        cursor.execute('INSERT INTO foo VALUES (2)')
        cursor.execute('INSERT INTO foo VALUES (3)')

        # Set read-only mode.
        create_triggers_sql = _read_only_triggers('readonlyfoo', 'foo')
        cursor.executescript(create_triggers_sql)

        regex = 'attempt to write a readonly database'

        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute('INSERT INTO foo VALUES (4)')

        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute('UPDATE foo SET id=5 WHERE id=2')

        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute('DELETE FROM foo WHERE id=1')


class TestSchemaItems(unittest.TestCase):
    def test_names(self):
        """_schema_items name must match CREATE statement."""
        for name, operation in _schema_items:
            pat = 'CREATE (?:TABLE|INDEX|TRIGGER) (\w+)'
            match = re.search(pat, operation)
            statement, found = match.group(0, 1)

            msg = 'Names must match - uses "%s" but operation contains "%s".'
            self.assertEqual(name, found, msg % (name, statement))


class TestConnector(MkdtempTestCase):
    def _make_database(self, filename):
        global _schema_items
        self._existing_node = filename
        connection = sqlite3.connect(self._existing_node)
        cursor = connection.cursor()
        cursor.execute('PRAGMA synchronous=OFF')
        for _, operation in _schema_items:
            cursor.execute(operation)
        cursor.execute('PRAGMA synchronous=FULL')
        connection.close()

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
            'cell', 'hierarchy', 'label', 'cell_label', 'node', 'edge',
            'weight', 'relation', 'relation_weight', 'property',
            'sqlite_sequence'
        ])
        return expected_tables, actual_tables

    def test_existing_database(self):
        """Existing database should load without errors."""
        database = 'node_database'
        self._make_database(database)

        connect = _Connector(database)  # Existing database.
        connection = connect()
        self.assertIsInstance(connection, sqlite3.Connection)

    def test_existing_database_subdirectory(self):
        os.mkdir('subdir')
        database = 'subdir/node_database'
        self._make_database(database)

        connect = _Connector(database)  # Existing database.
        connection = connect()
        self.assertIsInstance(connection, sqlite3.Connection)

    def test_new_database(self):
        """If named database does not exist, it should be created."""
        database = 'node_database'

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

    def test_partial_read_only_support(self):
        """Read-only connections should fail on INSERT, UPDATE, and DELETE."""
        database = 'node_database'
        self._make_database(database)
        connection = sqlite3.connect(database)
        cursor = connection.cursor()
        cursor.execute('PRAGMA synchronous=OFF')
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

        regex = 'attempt to write a readonly database'

        with self.assertRaisesRegex((sqlite3.OperationalError,
                                     sqlite3.IntegrityError), regex):
            cursor.execute('INSERT INTO cell VALUES (4, 0)')

        with self.assertRaisesRegex((sqlite3.OperationalError,
                                     sqlite3.IntegrityError), regex):
            cursor.execute('UPDATE cell SET partial=1 WHERE cell_id=3')

        with self.assertRaisesRegex((sqlite3.OperationalError,
                                     sqlite3.IntegrityError), regex):
            cursor.execute('DELETE FROM cell WHERE cell_id=1')

    #@unittest.skipUnless(sqlite3.sqlite_version_info < (3, 8, 0),
    #    'Should raise a warning if SQLite is older than version 3.8.0.')
    #def test_partial_read_only_warning(self):
    #    return NotImplemented

    @unittest.skipIf(sqlite3.sqlite_version_info < (3, 8, 0),
        'The query_only PRAGMA was added to SQLite in version 3.8.0')
    def test_full_read_only_support(self):
        """Read-only connections should also fail on DROP, ALTER, etc."""
        database = 'node_database'
        self._make_database(database)

        connect = _Connector(database, mode=READ_ONLY)
        connection = connect()
        cursor = connection.cursor()

        with self.assertRaises(sqlite3.OperationalError):
            cursor.execute('ALTER TABLE cell ADD COLUMN other TEXT')

        with self.assertRaises(sqlite3.OperationalError):
            cursor.execute('DROP TABLE cell')

    def test_bad_sqlite_structure(self):
        """SQLite databases with unexpected table structure should fail."""
        filename = 'unknown_database.db'
        connection = sqlite3.connect(filename)
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE foo (bar, baz)')
        connection.close()

        # Attempt to load a non-Node SQLite database.
        regex = 'File - .* - is not a valid node.'
        with self.assertRaisesRegex(Exception, regex):
            connect = _Connector(filename)

    def test_wrong_file_type(self):
        """Non-SQLite files should fail to load."""
        filename = 'test.txt'
        fh = open(filename, 'w')
        fh.write('This is a text file.')
        fh.close()

        # Attempt to load non-SQLite file.
        regex = 'File - .* - is not a valid node.'
        with self.assertRaisesRegex(Exception, regex):
            connect = _Connector(filename)


class TestSharedConnection(unittest.TestCase):
    def setUp(self):
        conn = sqlite3.connect(':memory:', factory=_SharedConnection)
        conn.execute('CREATE TABLE mytable (col1 INTEGER PRIMARY KEY)')
        conn.execute('INSERT INTO mytable VALUES (1)')
        conn.execute('INSERT INTO mytable VALUES (2)')
        conn.commit()
        self.connection = conn

    def test_commit_and_remains_usable(self):
        """Test commit behavior, should remain usable after 'false' close."""
        with self.connection as conn:
            conn.execute('INSERT INTO mytable VALUES (3)')
        self.connection.rollback()  # Rollback uncomitted changes!
        cursor = self.connection.cursor()
        cursor.execute('SELECT * FROM mytable')
        msg = 'Must commit changes when exiting context manager.'
        self.assertEqual([(1,), (2,), (3,)], cursor.fetchall(), msg)

        # Test usability.
        self.connection.close()  # Must remain usable afterwards.
        self.connection.execute('INSERT INTO mytable VALUES (4)')

    def test_connection_close(self):
        conn = self.connection
        conn.isolation_level = None
        cursor = self.connection.cursor()
        cursor.execute('BEGIN TRANSACTION')
        cursor.execute('INSERT INTO mytable VALUES (3)')
        conn.close()  # <- Call close on connection!

        # Check isolation_level.
        msg = ("Connection's isolation level should be reset to original "
               "value (empty string).")
        self.assertEqual('', conn.isolation_level, msg)

        # Check rollback.
        cursor = self.connection.cursor()
        cursor.execute('SELECT * FROM mytable')
        self.assertEqual([(1,), (2,)], cursor.fetchall())

    def test_cursor_close(self):
        """Returned cursor should also use 'false' close."""
        conn = self.connection
        conn.isolation_level = None
        cursor = self.connection.cursor()
        cursor.execute('BEGIN TRANSACTION')
        cursor.execute('INSERT INTO mytable VALUES (3)')
        cursor.close()  # <- Call close on cursor!

        # Check rollback.
        cursor = self.connection.cursor()
        cursor.execute('SELECT * FROM mytable')
        self.assertEqual([(1,), (2,)], cursor.fetchall())

    def test_close_parent(self):
        """Calling close_parent should close the connection permanently."""
        conn = self.connection
        conn.execute('INSERT INTO mytable VALUES (3)')

        conn.close_parent()
        with self.assertRaises(sqlite3.ProgrammingError):
            conn.execute('INSERT INTO mytable VALUES (4)')


class TestSqlDataModel(unittest.TestCase):
    def setUp(self):
        self._connect = _Connector(mode=IN_MEMORY)
        self.connection = self._connect()

    def test_foreign_keys(self):
        """Foreign key constraints should be enforced."""
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'region', 0)")

        with self.assertRaises(sqlite3.IntegrityError):
            cursor.execute("INSERT INTO label VALUES (1, 2, 'Midwest')")

    def test_cell_defaults(self):
        """Should be possible to insert records in to cell using defaults vals."""
        cursor = self.connection.cursor()
        cursor.execute('INSERT INTO cell DEFAULT VALUES')
        cursor.execute('SELECT * FROM cell')
        self.assertEqual([(1, 0)], cursor.fetchall())

    def test_hierarchy_check(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'country', 0)")
        cursor.execute("INSERT INTO hierarchy VALUES (2, 'region', 1)")

        regex = '(CHECK )?constraint failed(: hierarchy)?'

        # Attempt insert with "cell_id" as hierarchy_value.
        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute("INSERT INTO hierarchy VALUES (3, 'cell_id', 2)")

        # Attempt insert with hierarchy_value containing a dot (".").
        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute("INSERT INTO hierarchy VALUES (3, 'sta.te', 2)")

        cursor.execute('SELECT * FROM hierarchy')
        expected = [(1, 'country', 0), (2, 'region', 1)]
        self.assertEqual(expected, cursor.fetchall())

    def test_label_autoincrement(self):
        """Label_id should auto-increment despite being in a composite key."""
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'country', 0)")
        cursor.execute("INSERT INTO hierarchy VALUES (2, 'region', 1)")
        cursor.executescript("""
            INSERT INTO label VALUES (NULL, 1, 'United States');
            INSERT INTO label VALUES (NULL, 2, 'Midwest');
            INSERT INTO label VALUES (NULL, 2, 'Northeast');
            INSERT INTO label VALUES (5,    2, 'South');  /* <- Explicit id. */
            INSERT INTO label VALUES (NULL, 2, 'West');
        """)
        cursor.execute('SELECT * FROM label')
        expected = [(1, 1, 'United States'),
                    (2, 2, 'Midwest'),
                    (3, 2, 'Northeast'),
                    (5, 2, 'South'),
                    (6, 2, 'West')]
        self.assertEqual(expected, cursor.fetchall())

    def test_label_unique_constraint(self):
        """Labels must be unique within their hierarchy level."""
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'region', 0)")

        with self.assertRaises(sqlite3.IntegrityError):
            cursor.executescript("""
                INSERT INTO label VALUES (NULL, 1, 'Midwest');
                INSERT INTO label VALUES (NULL, 1, 'Midwest');
            """)

    def test_rootlabel_constraint(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'country', 0)")
        cursor.execute("INSERT INTO label VALUES (1, 1, 'UNMAPPED')")
        cursor.execute("INSERT INTO label VALUES (2, 1, 'United States')")
        cursor.execute("INSERT INTO hierarchy VALUES (2, 'region', 1)")
        cursor.execute("INSERT INTO label VALUES (3, 2, 'UNMAPPED')")
        cursor.execute("INSERT INTO label VALUES (4, 2, 'Northeast')")
        cursor.execute("INSERT INTO label VALUES (5, 2, 'Midwest')")

        regex = 'root hierarchy cannot have multiple values'

        # Check insert trigger on `label` table.
        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute("INSERT INTO label VALUES (6, 1, 'Germany')")

        # Check update trigger on `label` table.
        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute("UPDATE label SET label_value='Japan' WHERE label_id=1")

        # Check update trigger on `hierarchy` table.
        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute("UPDATE hierarchy SET hierarchy_level=2 WHERE hierarchy_id=1")

        # Check delete trigger on `hierarchy` table.
        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute("DELETE FROM label WHERE hierarchy_id=1")
            cursor.execute("DELETE FROM hierarchy WHERE hierarchy_id=1")

    def test_cell_label_foreign_key(self):
        """Mismatched hierarchy_id/label_id pairs must fail."""
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'region', 0)")
        cursor.execute("INSERT INTO hierarchy VALUES (2, 'state',  1)")
        cursor.execute("INSERT INTO cell VALUES (1, 0)")
        cursor.execute("INSERT INTO label VALUES (1, 1, 'Midwest')")
        cursor.execute("INSERT INTO label VALUES (2, 2, 'Ohio')")

        with self.assertRaises(sqlite3.IntegrityError):
            cursor.execute("INSERT INTO cell_label VALUES (1, 1, 1, 2)")

    def test_cell_label_unique_constraint(self):
        """Cells must never have two labels from the same hierarchy level."""
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'region', 0)")
        cursor.execute("INSERT INTO hierarchy VALUES (2, 'state',  1)")
        cursor.execute("INSERT INTO cell VALUES (1, 0)")
        cursor.execute("INSERT INTO label VALUES (1, 1, 'Midwest')")
        cursor.execute("INSERT INTO cell_label VALUES (1, 1, 1, 1)")

        with self.assertRaises(sqlite3.IntegrityError):
            cursor.execute("INSERT INTO cell_label VALUES (2, 1, 1, 1)")

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

        regex = 'CHECK constraint failed: cell_label'

        # Insert label_id combination that conflicts with cell_id 1.
        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute("INSERT INTO cell VALUES (3, 0)")
            cursor.execute("INSERT INTO cell_label VALUES (5, 3, 1, 1)")
            cursor.execute("INSERT INTO cell_label VALUES (6, 3, 2, 2)")

        # Update label_id creating conflict with cell_id 1.
        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute("UPDATE cell_label SET label_id=2 WHERE cell_label_id=4")

        # Delete cell_label records to create a conflict between cell_id 1 and 2.
        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute("DELETE FROM cell_label WHERE cell_label_id=3")
            cursor.execute("DELETE FROM cell_label WHERE cell_label_id=2")

    def test_unmapped_level_constraint(self):
        """Cells with "UNMAPPED" labels must

        """
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'region', 0)")
        cursor.execute("INSERT INTO label VALUES (1, 1, 'Midwest')")

        cursor.execute("INSERT INTO hierarchy VALUES (2, 'state',  1)")
        cursor.execute("INSERT INTO label VALUES (2, 2, 'Ohio')")

        cursor.execute("INSERT INTO cell VALUES (1, 0)")
        cursor.execute("INSERT INTO cell_label VALUES (1, 1, 1, 1)")
        cursor.execute("INSERT INTO cell_label VALUES (2, 1, 2, 2)")

        cursor.execute("INSERT INTO label VALUES (4, 1, 'UNMAPPED')")
        cursor.execute("INSERT INTO label VALUES (5, 2, 'UNMAPPED')")

        # Insert valid cell: ('Midwest', 'UNMAPPED').
        cursor.execute("INSERT INTO cell VALUES (3, 0)")
        cursor.execute("INSERT INTO cell_label VALUES (5, 3, 1, 1)")
        cursor.execute("INSERT INTO cell_label VALUES (6, 3, 2, 5)")
        self.connection.commit()

        # Insert invalid cell: ('UNMAPPED', 'Ohio').
        regex = 'invalid unmapped level'
        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute("INSERT INTO cell VALUES (5, 0)")
            cursor.execute("INSERT INTO cell_label VALUES (9,  5, 1, 4)")
            cursor.execute("INSERT INTO cell_label VALUES (10, 5, 2, 2)")
        cursor.connection.rollback()

        # Update to invalid cell: ('UNMAPPED', 'Ohio').
        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute("UPDATE cell_label SET label_id=4 WHERE cell_label_id=1")

        # Update to invalid hierarchy order: (1, 'region', 2).
        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            cursor.execute("UPDATE hierarchy SET hierarchy_level=2 WHERE hierarchy_id=1")

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
