"""Tests for toron/_data_access/schema.py module."""
import sqlite3
import unittest
from contextlib import closing

from toron._selectors import SimpleSelector
from toron._utils import BitFlags
from toron._data_access.schema import (
    SQLITE_ENABLE_JSON1,
    SQLITE_ENABLE_MATH_FUNCTIONS,
    create_node_schema,
    get_unique_id,
    create_sql_function,
    create_toron_check_property_value,
    create_triggers_property_value,
    create_toron_check_attribute_value,
    create_triggers_attribute_value,
    create_toron_check_user_properties,
    create_triggers_user_properties,
    create_toron_check_selectors,
    create_triggers_selectors,
    create_log2,
    create_toron_apply_bit_flag,
    create_toron_json_object_keep,
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

    def test_unique_id(self):
        """Each node should get its own 'unique_id' value."""
        make_connection = \
            lambda: sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES)

        with closing(make_connection()) as con:
            create_node_schema(con)
            unique_id1 = get_unique_id(con)

        with closing(make_connection()) as con:
            create_node_schema(con)
            unique_id2 = get_unique_id(con)

        self.assertNotEqual(unique_id1, unique_id2)


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


class BasePropertyValueTestCase(unittest.TestCase):
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


class TestCreateToronCheckPropertyValue(BasePropertyValueTestCase):
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


class TestCreateTriggersPropertyValue(BasePropertyValueTestCase):
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


class BaseAttributeValueTestCase(unittest.TestCase):
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


class TestCreateToronCheckAttributeValue(BaseAttributeValueTestCase):
    """Check user-defined SQL function ``user_attributes_valid``."""
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)
        create_toron_check_attribute_value(self.connection)

    def test_valid_attributes(self):
        for value in self.valid_attributes:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT toron_check_attribute_value(?)', [value])
                msg = f'should be 1 for well-formed TEXT_ATTRIBUTES: {value!r}'
                self.assertEqual(cur.fetchall(), [(1,)], msg=msg)

    def test_invalid_attributes(self):
        for value, desc in self.invalid_attributes:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT toron_check_attribute_value(?)', [value])
                msg = f'should be 0, TEXT_ATTRIBUTES {value!r} {desc}'
                self.assertEqual(cur.fetchall(), [(0,)], msg=msg)


class TestCreateTriggersAttributeValue(BaseAttributeValueTestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)

        create_node_schema(self.connection)
        if not SQLITE_ENABLE_JSON1:
            create_toron_check_attribute_value(self.connection)
        create_triggers_attribute_value(self.connection)

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


class BaseUserPropertiesTestCase(unittest.TestCase):
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


class TestCreateToronCheckUserProperties(BaseUserPropertiesTestCase):
    """Check application defined SQL function for TEXT_USERPROPERTIES."""
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)
        create_toron_check_user_properties(self.connection)

    def test_valid_userproperties(self):
        for value in self.valid_userproperties:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT toron_check_user_properties(?)', [value])
                msg = f'should be 1 for well-formed TEXT_USERPROPERTIES: {value!r}'
                self.assertEqual(cur.fetchall(), [(1,)], msg=msg)

    def test_invalid_attributes(self):
        for value, desc in self.invalid_userproperties:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT toron_check_user_properties(?)', [value])
                msg = f'should be 0, TEXT_USERPROPERTIES {value!r} {desc}'
                self.assertEqual(cur.fetchall(), [(0,)], msg=msg)


class TestCreateTriggersUserProperties(BaseUserPropertiesTestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)

        create_node_schema(self.connection)
        if not SQLITE_ENABLE_JSON1:
            create_toron_check_user_properties(self.connection)
        create_triggers_user_properties(self.connection)

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


class BaseSelectorsTestCase(unittest.TestCase):
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


class TestCreateToronCheckSelectors(BaseSelectorsTestCase):
    """Check application defined SQL function for TEXT_SELECTORS."""
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)
        create_toron_check_selectors(self.connection)

    def test_valid_selectors(self):
        for value in self.valid_selector_json:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT toron_check_selectors(?)', [value])
                msg = f'should be 1 for well-formed TEXT_SELECTORS: {value!r}'
                self.assertEqual(cur.fetchall(), [(1,)], msg=msg)

    def test_invalid_attributes(self):
        for value, desc in self.invalid_selector_json:
            with self.subTest(value=value):
                cur = self.connection.execute('SELECT toron_check_selectors(?)', [value])
                msg = f'should be 0, TEXT_SELECTORS {value!r} {desc}'
                self.assertEqual(cur.fetchall(), [(0,)], msg=msg)


class TestCreateTriggersSelectors(BaseSelectorsTestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)

        create_node_schema(self.connection)
        if not SQLITE_ENABLE_JSON1:
            create_toron_check_selectors(self.connection)
        create_triggers_selectors(self.connection)

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


class TestCreateLog2(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)
        create_log2(self.connection, alt_name='user_log2')

    def test_func(self):
        cur = self.connection.execute('SELECT user_log2(64)')
        self.assertEqual(cur.fetchall(), [(6.0,)])

    def test_errors(self):
        cur = self.connection.execute('SELECT user_log2(0)')  # <- ValueError
        self.assertEqual(cur.fetchall(), [(None,)])

        cur = self.connection.execute("SELECT user_log2('foo')")  # <- TypeError
        self.assertEqual(cur.fetchall(), [(None,)])


class TestCreateToronApplyBitFlag(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)
        create_toron_apply_bit_flag(self.connection)

    def test_basic_handling(self):
        bit_flags = BitFlags(1, 0, 1)

        cur = self.connection.execute(
            'SELECT toron_apply_bit_flag(?, ?, ?)',
            ('foo', bit_flags, 0),
        )
        self.assertEqual(cur.fetchall(), [('foo',)])

        cur = self.connection.execute(
            'SELECT toron_apply_bit_flag(?, ?, ?)',
            ('bar', bit_flags, 1),
        )
        self.assertEqual(cur.fetchall(), [(None,)])

        cur = self.connection.execute(
            'SELECT toron_apply_bit_flag(?, ?, ?)',
            ('baz', bit_flags, 2),
        )
        self.assertEqual(cur.fetchall(), [('baz',)])

    def test_bit_flags_is_none(self):
        cur = self.connection.execute(
            'SELECT toron_apply_bit_flag(?, ?, ?)',
            ('foo', None, 1),  # <- bit_flags is None
        )
        self.assertEqual(cur.fetchall(), [('foo',)])

    def test_index_out_of_range(self):
        bit_flags = BitFlags(1, 0, 1)

        cur = self.connection.execute(
            'SELECT toron_apply_bit_flag(?, ?, ?)',
            ('bar', bit_flags, 9),  # <- No index `9` in bit flags
        )
        self.assertEqual(cur.fetchall(), [(None,)])


class TestCreateToronJsonObjectKeep(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)
        create_toron_json_object_keep(self.connection)

    def test_matching_keys(self):
        """Should keep given keys and return a new obj in alpha order."""
        cur = self.connection.execute(
            'SELECT toron_json_object_keep(?, ?, ?)',
            ('{"b": "two", "c": "three", "a": "one"}', 'b', 'a'),
        )
        self.assertEqual(cur.fetchall(), [('{"a": "one", "b": "two"}',)])

    def test_matching_and_nonmatching_keys(self):
        """As long as at least one key matches, a result is returned."""
        cur = self.connection.execute(
            'SELECT toron_json_object_keep(?, ?, ?, ?)',
            ('{"b": "two", "c": "three", "a": "one"}', 'x', 'c', 'y'),  # <- Only "c", no "x" or "y" in JSON obj.
        )
        self.assertEqual(cur.fetchall(), [('{"c": "three"}',)])

    def test_no_matching_keys(self):
        """When keys are given but none of them match keys in the
        json_obj, then None should be returned.
        """
        cur = self.connection.execute(
            'SELECT toron_json_object_keep(?, ?, ?, ?)',
            ('{"b": "two", "c": "three", "a": "one"}', 'x', 'y', 'z'),
        )
        self.assertEqual(cur.fetchall(), [(None,)])

    def test_no_keys_given(self):
        """When no keys are given, a normalized verison of the complete
        JSON object should be returned.
        """
        cur = self.connection.execute(
            'SELECT toron_json_object_keep(?)',
            ('{"b": "two", "c": "three", "a": "one"}',),  # <- No keys given!
        )
        self.assertEqual(cur.fetchall(), [('{"a": "one", "b": "two", "c": "three"}',)])  # <- Full obj with keys in alpha order.

    def test_unsupported_json_type(self):
        """Invalid JSON type should trigger an error."""
        with self.assertRaises(sqlite3.OperationalError):
            self.connection.execute(
                'SELECT toron_json_object_keep(?, ?, ?)',
                ('"a string value"', 'a', 'b'),  # <- JSON should be object, not string.
            )

    def test_malformed_json(self):
        """JSON decode errors should trigger an error."""
        with self.assertRaises(sqlite3.OperationalError):
            self.connection.execute(
                'SELECT toron_json_object_keep(?, ?, ?)',
                ('{"a": "one}', 'a', 'b'),  # <- No closing quote.
            )


class TestRegisteredConverters(unittest.TestCase):
    """Should convert SQLite objects into appropriate Python objects."""
    def setUp(self):
        self.connection = sqlite3.connect(
            database=':memory:',
            detect_types=sqlite3.PARSE_DECLTYPES,
            isolation_level=None,
        )
        self.addCleanup(self.connection.close)
        create_node_schema(self.connection)

    def test_converter_text_json(self):
        cur = self.connection.execute(
            'INSERT INTO property (key, value) VALUES (?, ?)',
            ('mykey', '{"abc": 123}'),
        )
        cur.execute("SELECT value FROM property WHERE key='mykey'")
        self.assertEqual(cur.fetchall(), [({'abc': 123},)])

    def test_converter_text_attributes(self):
        cur = self.connection.execute(
            'INSERT INTO attribute (attribute_id, attribute_value) VALUES (?, ?)',
            (1, '{"foo": "one", "bar": "two"}'),
        )
        cur.execute('SELECT attribute_value FROM attribute')
        self.assertEqual(cur.fetchall(), [({'foo': 'one', 'bar': 'two'},)])

    def test_converter_text_userproperties(self):
        cur = self.connection.execute(
            'INSERT INTO edge (user_properties, name, other_unique_id) VALUES (?, ?, ?)',
            ('{"a": [1, 2], "b": {"three": 3}}', 'name', '1111-111-1111'),
        )
        cur.execute('SELECT user_properties FROM edge')
        self.assertEqual(cur.fetchall(), [({'a': [1, 2], 'b': {'three': 3}},)])

    def test_converter_text_selectors(self):
        cur = self.connection.execute(
            'INSERT INTO weighting (name, selectors) VALUES (?, ?)',
            ('myname', r'["[a=\"one\"]", "[b]"]'),
        )
        cur.execute('SELECT selectors FROM weighting')
        self.assertEqual(cur.fetchall(), [([SimpleSelector('a', '=', 'one'), SimpleSelector('b')],)])

    def test_converter_blob_bitflags(self):
        cur = self.connection.executescript("""
            INSERT INTO node_index (index_id) VALUES (1);
            INSERT INTO edge (edge_id, name, other_unique_id) VALUES (1, 'name', '11-1-11');
            INSERT INTO relation (edge_id, other_index_id, index_id, relation_value, mapping_level) VALUES (1, 1, 1, 25.0, X'A0');
        """)
        cur.execute('SELECT mapping_level FROM relation')
        self.assertEqual(cur.fetchall(), [(BitFlags(1, 0, 1),)])
