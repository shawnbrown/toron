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
    create_sql_triggers_property_value,
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


class BaseJsonValidTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.wellformed_json = [
            '123',
            '1.23',
            '"abc"',
            'true',
            'false',
            'null',
            '[1, 2.0, "3"]',
            '{"a": 1, "b": [2, 3]}',
        ]

        cls.malformed_json = [
            ('{"a": "one", "b": "two"',  'missing closing curly-brace'),
            ('{"a": "one", "b": "two}',  'missing closing quote'),
            ('[1, 2',                    'missing closing bracket'),
            ("{'a': 'one', 'b': 'two'}", 'requires double quotes'),
            ('abc',                      'not quoted'),
            ('',                         'has no contents'),
        ]


class TestCreateJsonValid(BaseJsonValidTestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)
        create_json_valid(self.connection, alt_name='user_json_valid')

    def test_wellformed_json(self):
        for value in self.wellformed_json:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT user_json_valid(?)', [value])
                msg = f'should be 1 for well-formed JSON: {value!r}'
                self.assertEqual(cur.fetchall(), [(1,)], msg=msg)

    def test_malformed_json(self):
        for value, desc in self.malformed_json:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT user_json_valid(?)', [value])
                msg = f'should be 0, JSON {value!r} {desc}'
                self.assertEqual(cur.fetchall(), [(0,)], msg=msg)

    def test_none(self):
        cur = self.connection.execute('SELECT user_json_valid(NULL)')
        self.assertEqual(cur.fetchall(), [(0,)])


class TestCreateSqlTriggersPropertyValue(BaseJsonValidTestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)

        create_node_schema(self.connection)
        if not SQLITE_ENABLE_JSON1:
            create_json_valid(self.connection)
        create_sql_triggers_property_value(self.connection)

    def test_insert_wellformed(self):
        cur = self.connection.executemany(
            'INSERT INTO property VALUES (?, ?)',
            [(str(key), val) for key, val in enumerate(self.wellformed_json)],
        )
        self.assertEqual(cur.rowcount, 8, msg='should insert all eight records')

    def test_insert_malformed(self):
        regex = 'property.value must be well-formed JSON'

        for value, desc in self.malformed_json:
            with self.subTest(value=value):
                msg = f'should raise IntegrityError, JSON {value!r} {desc}'
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex, msg=msg):
                    self.connection.execute(
                        'INSERT INTO property VALUES (?, ?)',
                        ('key1', value),
                    )
