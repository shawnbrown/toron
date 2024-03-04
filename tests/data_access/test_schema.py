"""Tests for toron/_data_access/schema.py module."""
import sqlite3
import unittest
from contextlib import closing

from toron._data_access.schema import (
    SQLITE_ENABLE_JSON1,
    SQLITE_ENABLE_MATH_FUNCTIONS,
    create_node_schema,
    create_sql_function,
    create_toron_check_property_value,
    create_triggers_property_value,
    create_user_attributes_valid,
    create_sql_triggers_attribute_value,
    create_user_userproperties_valid,
    create_sql_triggers_user_properties,
    create_user_selectors_valid,
    create_sql_triggers_selectors,
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


class TestCreateToronCheckPropertyValue(BaseJsonValidTestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)
        create_toron_check_property_value(self.connection)

    def test_wellformed_json(self):
        for value in self.wellformed_json:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT toron_check_property_value(?)', [value])
                msg = f'should be 1 for well-formed JSON: {value!r}'
                self.assertEqual(cur.fetchall(), [(1,)], msg=msg)

    def test_malformed_json(self):
        for value, desc in self.malformed_json:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT toron_check_property_value(?)', [value])
                msg = f'should be 0, JSON {value!r} {desc}'
                self.assertEqual(cur.fetchall(), [(0,)], msg=msg)

    def test_none(self):
        cur = self.connection.execute('SELECT toron_check_property_value(NULL)')
        self.assertEqual(cur.fetchall(), [(0,)])


class TestCreateTriggersPropertyValue(BaseJsonValidTestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)

        create_node_schema(self.connection)
        if not SQLITE_ENABLE_JSON1:
            create_toron_check_property_value(self.connection)
        create_triggers_property_value(self.connection)

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


class BaseAttributesValidTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Valid TEXT_ATTRIBUTES values must be JSON objects with string values."""
        cls.valid_attributes = [
            '{"a": "one", "b": "two"}',
            '{"c": "three"}',
        ]

        cls.invalid_attributes = [
            ('{"a": "one", "b": 2}',     'contains non-text values (contains integer)'),
            ('{"a": {"b": "two"}}',      'contains non-text values (contains nested object)'),
            ('["one", "two"]',           'not a JSON object (array)'),
            ('"one"',                    'not a JSON object (text)'),
            ('123',                      'not a JSON object (integer)'),
            ('3.14',                     'not a JSON object (real)'),
            ('true',                     'not a JSON object (boolean)'),
            ('{"a": "one", "b": "two"',  'malformed (no closing curly-brace)'),
            ('{"a": "one", "b": "two}',  'malformed (no closing quote)'),
            ('[1, 2',                    'malformed (no closing bracket)'),
            ("{'a': 'one', 'b': 'two'}", 'malformed (requires double quotes)'),
            ('abc',                      'malformed (not quoted)'),
            ('',                         'malformed (no contents)')
        ]


class TestCreateUserAttributeValid(BaseAttributesValidTestCase):
    """Check user-defined SQL function ``user_attributes_valid``."""
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)
        create_user_attributes_valid(self.connection)

    def test_valid_attributes(self):
        for value in self.valid_attributes:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT user_attributes_valid(?)', [value])
                msg = f'should be 1 for well-formed TEXT_ATTRIBUTES: {value!r}'
                self.assertEqual(cur.fetchall(), [(1,)], msg=msg)

    def test_invalid_attributes(self):
        for value, desc in self.invalid_attributes:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT user_attributes_valid(?)', [value])
                msg = f'should be 0, TEXT_ATTRIBUTES {value!r} {desc}'
                self.assertEqual(cur.fetchall(), [(0,)], msg=msg)


class TestCreateSqlTriggersAttributeValue(BaseAttributesValidTestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)

        create_node_schema(self.connection)
        if not SQLITE_ENABLE_JSON1:
            create_user_attributes_valid(self.connection)
        create_sql_triggers_attribute_value(self.connection)

    def test_insert_valid_attributes(self):
        cur = self.connection.executemany(
            'INSERT INTO attribute VALUES (?, ?)',
            [(i, val) for i, val in enumerate(self.valid_attributes)],
        )
        self.assertEqual(cur.rowcount, 2, msg='should insert all two records')

    def test_insert_invalid_attributes(self):
        regex = 'attribute.attribute_value must be a JSON object with text values'

        for value, desc in self.invalid_attributes:
            with self.subTest(value=value):
                msg = f'should raise IntegrityError, TEXT_ATTRIBUTES {value!r} {desc}'
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex, msg=msg):
                    self.connection.execute(
                        'INSERT INTO attribute VALUES (?, ?)',
                        (1, value),
                    )


class BaseUserpropertiesValidTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Valid TEXT_USERPROPERTIES values must be JSON objects."""
        cls.valid_userproperties = [
            '{"a": "one", "b": "two"}',          # <- object with text
            '{"a": 1, "b": 2.0}',                # <- object with integer and real
            '{"a": [1, 2], "b": {"three": 3}}',  # <- object with array and object
        ]

        cls.invalid_userproperties = [
            ('["one", "two"]',           'not an object (array)'),
            ('"one"',                    'not an object (text)'),
            ('123',                      'not an object (integer)'),
            ('3.14',                     'not an object (real)'),
            ('true',                     'not an object (boolean)'),
            ('{"a": "one", "b": "two"',  'malformed (no closing curly-brace)'),
            ('{"a": "one", "b": "two}',  'malformed (no closing quote)'),
            ('[1, 2',                    'malformed (no closing bracket)'),
            ("{'a': 'one', 'b': 'two'}", 'malformed (requires double quotes)'),
            ('abc',                      'malformed (not quoted)'),
            ('',                         'malformed (no contents)'),
        ]


class TestUserUserpropertiesValid(BaseUserpropertiesValidTestCase):
    """Check application defined SQL function for TEXT_USERPROPERTIES."""
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)
        create_user_userproperties_valid(self.connection)

    def test_valid_userproperties(self):
        for value in self.valid_userproperties:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT user_userproperties_valid(?)', [value])
                msg = f'should be 1 for well-formed TEXT_USERPROPERTIES: {value!r}'
                self.assertEqual(cur.fetchall(), [(1,)], msg=msg)

    def test_invalid_attributes(self):
        for value, desc in self.invalid_userproperties:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT user_userproperties_valid(?)', [value])
                msg = f'should be 0, TEXT_USERPROPERTIES {value!r} {desc}'
                self.assertEqual(cur.fetchall(), [(0,)], msg=msg)


class TestCreateSqlTriggersUserpropertiesValue(BaseUserpropertiesValidTestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)

        create_node_schema(self.connection)
        if not SQLITE_ENABLE_JSON1:
            create_user_userproperties_valid(self.connection)
        create_sql_triggers_user_properties(self.connection)

    def test_insert_valid_userproperties(self):
        cur = self.connection.executemany(
            'INSERT INTO edge (user_properties, name, other_unique_id) VALUES (?, ?, ?)',
            [(val, 'name', str(i)) for i, val in enumerate(self.valid_userproperties)],
        )
        self.assertEqual(cur.rowcount, 3, msg='should insert all three records')

    def test_insert_invalid_userproperties(self):
        regex = 'edge.user_properties must be well-formed JSON object type'

        for value, desc in self.invalid_userproperties:
            with self.subTest(value=value):
                msg = f'should raise IntegrityError, TEXT_USERPROPERTIES {value!r} {desc}'
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex, msg=msg):
                    self.connection.execute(
                        "INSERT INTO edge (user_properties, name, other_unique_id) VALUES (?, ?, ?)",
                        (value, 'foo', '1'),
                    )


class BaseUserSelectorsValidTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Valid TEXT_SELECTORS values must be JSON arrays with string values."""
        cls.valid_selector_json = [
            r'["[a=\"one\"]", "[b=\"two\"]"]',
            r'["[c]"]',
        ]

        cls.invalid_selector_json = [
            (r'["[a=\"one\"]", 2]',               'contains non-string value (integer)'),
            (r'["[a=\"one\"]", ["[b=\"two\"]"]]', 'contains non-string value (nested object)'),
            ('{"a": "one", "b": "two"}',          'not an array (object)'),
            ('"one"',                             'not an array (text)'),
            ('123',                               'not an array (integer)'),
            ('3.14',                              'not an array (real)'),
            ('true',                              'not an array (boolean)'),
            (r'["[a=\"one\"]", "[b=\"two\"]"',    'malformed (no closing bracket)'),
            (r'["[a=\"one\"]", "[b=\"two\"]]',    'malformed (no closing quote)'),
            (r"['[a=\"one\"]', '[b=\"two\"]']",   'malformed (requires double quotes)'),
            ('abc',                               'malformed (not quoted)'),
            ('',                                  'no contents'),
        ]


class TestUserSelectorsValid(BaseUserSelectorsValidTestCase):
    """Check application defined SQL function for TEXT_SELECTORS."""
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)
        create_user_selectors_valid(self.connection)

    def test_valid_selectors(self):
        for value in self.valid_selector_json:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT user_selectors_valid(?)', [value])
                msg = f'should be 1 for well-formed TEXT_SELECTORS: {value!r}'
                self.assertEqual(cur.fetchall(), [(1,)], msg=msg)

    def test_invalid_attributes(self):
        for value, desc in self.invalid_selector_json:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT user_selectors_valid(?)', [value])
                msg = f'should be 0, TEXT_SELECTORS {value!r} {desc}'
                self.assertEqual(cur.fetchall(), [(0,)], msg=msg)


class TestCreateSqlTriggersSelectors(BaseUserSelectorsValidTestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)

        create_node_schema(self.connection)
        if not SQLITE_ENABLE_JSON1:
            create_user_selectors_valid(self.connection)
        create_sql_triggers_selectors(self.connection)

    def test_insert_valid_edge_selectors(self):
        cur = self.connection.executemany(
            'INSERT INTO edge (user_properties, name, other_unique_id) VALUES (?, ?, ?)',
            [(val, 'name', str(i)) for i, val in enumerate(self.valid_selector_json)],
        )
        self.assertEqual(cur.rowcount, 2, msg='should insert all two records')

    def test_insert_valid_weighting_selectors(self):
        cur = self.connection.executemany(
            'INSERT INTO weighting (name, selectors) VALUES (?, ?)',
            [(str(i), sel) for i, sel in enumerate(self.valid_selector_json)],
        )
        self.assertEqual(cur.rowcount, 2, msg='should insert all two records')

    def test_insert_invalid_edge_selectors(self):
        regex = 'edge.selectors must be a JSON array with text values'

        for value, desc in self.invalid_selector_json:
            with self.subTest(value=value):
                msg = f'should raise IntegrityError, TEXT_SELECTORS {value!r} {desc}'
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex, msg=msg):
                    self.connection.execute(
                        'INSERT INTO edge (selectors, name, other_unique_id) VALUES (?, ?, ?)',
                        (value, 'foo', '1'),
                    )

    def test_insert_invalid_weighting_selectors(self):
        regex = 'weighting.selectors must be a JSON array with text values'

        for value, desc in self.invalid_selector_json:
            with self.subTest(value=value):
                msg = f'should raise IntegrityError, TEXT_SELECTORS {value!r} {desc}'
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex, msg=msg):
                    self.connection.execute(
                        'INSERT INTO weighting (name, selectors) VALUES (?, ?)',
                        ('1', value),
                    )
