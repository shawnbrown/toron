"""Tests for toron/_data_access/schema.py module."""
import sqlite3
import unittest
from contextlib import closing

from toron._data_access.schema import (
    SQLITE_ENABLE_JSON1,
    SQLITE_ENABLE_MATH_FUNCTIONS,
    create_node_schema,
    create_sql_function,
    create_json_valid,
)


class TestCompileTimeOptions(unittest.TestCase):
    def test_sqlite_features(self):
        self.assertIsInstance(SQLITE_ENABLE_JSON1, bool)
        self.assertIsInstance(SQLITE_ENABLE_MATH_FUNCTIONS, bool)


class TestCreateNodeSchema(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(
            database=':memory:',
            detect_types=sqlite3.PARSE_DECLTYPES,
            isolation_level=None,
        )
        self.addCleanup(self.connection.close)

    @staticmethod
    def get_tables(connection):
        """Helper function to return tables present in SQLite database."""
        with closing(connection.cursor()) as cur:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return {row[0] for row in cur}

    def test_empty_schema(self):
        """Should create new schema when database is empty."""
        create_node_schema(self.connection)

        tables = self.get_tables(self.connection)
        expected = {
            'attribute',
            'edge',
            'location',
            'node_index',
            'property',
            'quantity',
            'relation',
            'structure',
            'weight',
            'weighting',
            'sqlite_sequence',  # <- Table added by SQLite.
        }
        self.assertSetEqual(tables, expected)

    def test_nonempty_schema(self):
        """Should raise an error when database already has other tables."""
        self.connection.execute("""
            CREATE TABLE dummy_table (
                dummy_id INTEGER PRIMARY KEY,
                dummy_value TEXT
            )
        """)

        regex = "database must be empty; found tables: 'dummy_table'"
        with self.assertRaisesRegex(RuntimeError, regex):
            create_node_schema(self.connection)


class TestCreateSqlFunction(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)

    def test_creation(self):
        create_sql_function(
            self.connection,           # <- positional `connection`
            'title_case',              # <- positional `name`
            1,                         # <- positional `narg`
            lambda x: str(x).title(),  # <- positional `func`
            deterministic=True,        # <- keyword only argument
        )

        cur = self.connection.execute("SELECT title_case('hello world')")
        self.assertEqual(cur.fetchall(), [('Hello World',)])

    def test_error(self):
        """Errors from function should not receive special handling."""
        def bad_func(x):
            raise Exception

        create_sql_function(self.connection, 'bad_func_name', 1, bad_func)

        with self.assertRaises(sqlite3.OperationalError):
            self.connection.execute("SELECT bad_func_name('hello world')")


class TestCreateJsonValid(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)
        create_json_valid(self.connection, alt_name='user_json_valid')

    def test_wellformed_json(self):
        values = [
            '123',
            '1.23',
            '"abc"',
            'true',
            'false',
            'null',
            '[1, 2.0, "3"]',
            '{"a": 1, "b": [2, 3]}',
        ]
        for value in values:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT user_json_valid(?)', [value])
                self.assertEqual(cur.fetchall(), [(1,)])

    def test_malformed_json(self):
        values = [
            '{"a": "one", "b": "two"',   # <- No closing curly-brace.
            '{"a": "one", "b": "two}',   # <- No closing quote.
            '[1, 2',                     # <- No closing bracket.
            "{'a': 'one', 'b': 'two'}",  # <- Requires double quotes.
            'abc',                       # <- Not quoted.
            '',                          # <- No contents.
        ]
        for value in values:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT user_json_valid(?)', [value])
                self.assertEqual(cur.fetchall(), [(0,)])

    def test_none(self):
        cur = self.connection.execute('SELECT user_json_valid(NULL)')
        self.assertEqual(cur.fetchall(), [(0,)])
