"""Tests for toron._schema module."""

import os
import sqlite3
import unittest
from collections import namedtuple, OrderedDict, UserString
from stat import S_IRUSR, S_IWUSR
from .common import TempDirTestCase

from toron._exceptions import ToronError
from toron._schema import (
    SQLITE_JSON1_ENABLED,
    _user_json_valid,
    _user_userproperties_valid,
    _user_attributes_valid,
    _user_selectors_valid,
    _schema_script,
    _sql_trigger_validate_attributes,
    _add_functions_and_triggers,
    _validate_permissions_get_mode,
    _path_to_sqlite_uri,
    connect,
    normalize_identifier,
    transaction,
    savepoint,
)


class TestNormalizeIdentifier(unittest.TestCase):
    values = [
        ('abc',        '"abc"'),
        ('a b c',      '"a b c"'),      # whitepsace
        ('   abc   ',  '"abc"'),        # leading/trailing whitespace
        ('a   b\tc',   '"a b c"'),      # irregular whitepsace
        ('a\n b\r\nc', '"a b c"'),      # linebreaks
        ("a 'b' c",    '"a \'b\' c"'),  # single quotes
        ('a "b" c',    '"a ""b"" c"'),  # double quotes
        ('"   abc"',   '"   abc"'),     # idempotent, leading whitespace
        ('"ab""c"',    '"ab""c"'),      # idempotent, escaped quotes
        ('"a b"c"',    '"a b""c"'),     # normalize malformed quotes
    ]

    def test_normalization(self):
        for input_value, result in self.values:
            with self.subTest(input_value=input_value, expected_output=result):
                self.assertEqual(normalize_identifier(input_value), result)

    def test_idempotence(self):
        """The function should be idempotent. I.e., if a value has
        already been normalized, running the function again on the
        result should not change it further.
        """
        values = [result for _, result in self.values]
        for result in values:
            with self.subTest(input_value=result, expected_output=result):
                self.assertEqual(normalize_identifier(result), result)

    def test_surrogate_codes(self):
        """Should only allow clean UTF-8 (no surrogate codes)."""
        column_bytes = b'tama\xf1o'  # "tamaño" is Spanish for "size"
        string_with_surrogate = column_bytes.decode('utf-8', 'surrogateescape')

        with self.assertRaises(UnicodeEncodeError):
            normalize_identifier(string_with_surrogate)

    def test_nul_byte(self):
        contains_nul = 'zip\x00 code'

        with self.assertRaises(UnicodeEncodeError):
            normalize_identifier(contains_nul)


class CheckJsonMixin(object):
    """Valid TEXT_JSON values must be wellformed JSON strings (may be
    of any data type).
    """
    valid_values = [
        '123',
        '1.23',
        '"abc"',
        'true',
        'false',
        'null',
        '[1, 2.0, "3"]',
        '{"a": 1, "b": [2, 3]}',
    ]
    malformed_json = [
        '{"a": "one", "b": "two"',   # <- No closing curly-brace.
        '{"a": "one", "b": "two}',   # <- No closing quote.
        '[1, 2',                     # <- No closing bracket.
        "{'a': 'one', 'b': 'two'}",  # <- Requires double quotes.
        'abc',                       # <- Not quoted.
        '',                          # <- No contents.
    ]


class TestUserJsonValid(unittest.TestCase, CheckJsonMixin):
    """Check application defined SQL function for TEXT_JSON."""
    def test_valid_values(self):
        for value in self.valid_values:
            with self.subTest(value=value):
                self.assertTrue(_user_json_valid(value))

    def test_malformed_json(self):
        for value in self.malformed_json:
            with self.subTest(value=value):
                self.assertFalse(_user_json_valid(value))

    def test_none(self):
        self.assertFalse(_user_json_valid(None))


class TestJsonTrigger(unittest.TestCase, CheckJsonMixin):
    """Check trigger behavior for `property.value` column (uses the
    TEXT_JSON declared type).
    """
    def setUp(self):
        self.con = connect('mynode.toron', mode='memory')
        self.cur = self.con.cursor()
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def test_valid_values(self):
        for index, value in enumerate(self.valid_values):
            with self.subTest(value=value):
                parameters = (f'key{index}', value)
                self.cur.execute("INSERT INTO property VALUES (?, ?)", parameters)

    def test_malformed_json(self):
        regex = 'must be wellformed JSON'
        for index, value in enumerate(self.malformed_json):
            with self.subTest(value=value):
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
                    parameters = (f'key{index}', value)
                    self.cur.execute("INSERT INTO property VALUES (?, ?)", parameters)

    def test_none(self):
        """The property.value column should accept None/NULL values."""
        self.cur.execute("INSERT INTO property VALUES (?, ?)", ('some_key', None))


class CheckUserPropertiesMixin(object):
    """Valid TEXT_USERPROPERTIES values must be JSON objects."""
    valid_values = [
        '{"a": "one", "b": "two"}',          # <- object of text
        '{"a": 1, "b": 2.0}',                # <- object of integer and real
        '{"a": [1, 2], "b": {"three": 3}}',  # <- object of array and object
    ]
    not_an_object = [
        '["one", "two"]',  # <- array
        '"one"',           # <- text
        '123',             # <- integer
        '3.14',            # <- real
        'true',            # <- boolean
    ]
    malformed_json = [
        '{"a": "one", "b": "two"',   # <- No closing curly-brace.
        '{"a": "one", "b": "two}',   # <- No closing quote.
        '[1, 2',                     # <- No closing bracket.
        "{'a': 'one', 'b': 'two'}",  # <- Requires double quotes.
        'abc',                       # <- Not quoted.
        '',                          # <- No contents.
    ]


class TestUserUserpropertiesValid(unittest.TestCase, CheckUserPropertiesMixin):
    """Check application defined SQL function for TEXT_USERPROPERTIES."""
    def test_valid_values(self):
        for value in self.valid_values:
            with self.subTest(value=value):
                self.assertTrue(_user_userproperties_valid(value))

    def test_not_an_object(self):
        for value in self.not_an_object:
            with self.subTest(value=value):
                self.assertFalse(_user_userproperties_valid(value))

    def test_malformed_json(self):
        for value in self.malformed_json:
            with self.subTest(value=value):
                self.assertFalse(_user_userproperties_valid(value))

    def test_none(self):
        self.assertFalse(_user_userproperties_valid(None))


class TestUserPropertiesTrigger(unittest.TestCase, CheckUserPropertiesMixin):
    """Check TRIGGER behavior for edge.user_properties column."""

    def setUp(self):
        self.con = connect('mynode.toron', mode='memory')
        self.cur = self.con.cursor()
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    @staticmethod
    def make_parameters(index, value):
        """Helper function to return formatted SQL query parameters."""
        return (
            None,                      # edge_id (INTEGER PRIMARY KEY)
            f'name{index}',            # name
            None,                      # description
            None,                      # selectors
            value,                     # user_properties
            '00000000-0000-0000-0000-000000000000',  # other_uuid
            f'other{index}.toron',     # other_filename_hint
            'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',  # other_element_hash
            0,                         # is_complete
        )

    def test_valid_values(self):
        for index, value in enumerate(self.valid_values):
            parameters = self.make_parameters(index, value)
            with self.subTest(value=value):
                self.cur.execute("INSERT INTO edge VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", parameters)

    def test_invalid_values(self):
        """Check all `not_an_object` and `malformed_json` values."""
        invalid_values = self.not_an_object + self.malformed_json  # Make a single list.

        regex = 'must be wellformed JSON object'

        for index, value in enumerate(invalid_values):
            parameters = self.make_parameters(index, value)
            with self.subTest(value=value):
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
                    self.cur.execute("INSERT INTO edge VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", parameters)

    def test_none(self):
        """Currently, edge.user_properties allows NULL values (refer to
        the associated CREATE TABLE statement).
        """
        parameters = self.make_parameters('x', None)
        self.cur.execute("INSERT INTO edge VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", parameters)


class CheckAttributesMixin(object):
    """Valid TEXT_ATTRIBUTES values must be JSON objects with string values."""
    valid_values = [
        '{"a": "one", "b": "two"}',
        '{"c": "three"}',
    ]
    non_string_values = [
        '{"a": "one", "b": 2}',  # <- contains integer
        '{"a": {"b": "two"}}',   # <- contains nested object
    ]
    not_an_object = [
        '["one", "two"]',  # <- array
        '"one"',           # <- text
        '123',             # <- integer
        '3.14',            # <- real
        'true',            # <- boolean
    ]
    malformed_json = [
        '{"a": "one", "b": "two"',   # <- No closing curly-brace.
        '{"a": "one", "b": "two}',   # <- No closing quote.
        '[1, 2',                     # <- No closing bracket.
        "{'a': 'one', 'b': 'two'}",  # <- Requires double quotes.
        'abc',                       # <- Not quoted.
        '',                          # <- No contents.
    ]


class TestUserAttributesValid(unittest.TestCase, CheckAttributesMixin):
    """Check application defined SQL function for TEXT_ATTRIBUTES."""
    def test_valid_values(self):
        for value in self.valid_values:
            with self.subTest(value=value):
                self.assertTrue(_user_attributes_valid(value))

    def test_non_string_values(self):
        for value in self.non_string_values:
            with self.subTest(value=value):
                self.assertFalse(_user_attributes_valid(value))

    def test_not_an_object(self):
        for value in self.not_an_object:
            with self.subTest(value=value):
                self.assertFalse(_user_attributes_valid(value))

    def test_malformed_json(self):
        for value in self.malformed_json:
            with self.subTest(value=value):
                self.assertFalse(_user_attributes_valid(value))

    def test_none(self):
        self.assertFalse(_user_attributes_valid(None))


class TestAttributesTrigger(unittest.TestCase, CheckAttributesMixin):
    """Check trigger behavior for `quantity.attributes` column with the
    TEXT_ATTRIBUTES declared type.
    """
    def setUp(self):
        self.con = connect('mynode.toron', mode='memory')
        self.cur = self.con.cursor()
        self.cur.execute("INSERT INTO location (_location_id) VALUES (1)")
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

        self.sql_insert = 'INSERT INTO quantity (_location_id, attributes, value) VALUES (?, ?, ?)'

    def test_valid_values(self):
        for attributes in self.valid_values:
            with self.subTest(attributes=attributes):
                parameters = (1, attributes, 100)
                self.cur.execute(self.sql_insert, parameters)

    def test_non_string_values(self):
        regex = 'must be a JSON object with text values'
        for attributes in self.non_string_values:
            with self.subTest(attributes=attributes):
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
                    parameters = (1, attributes, 100)
                    self.cur.execute(self.sql_insert, parameters)

    def test_not_an_object(self):
        regex = 'must be a JSON object with text values'
        for attributes in self.not_an_object:
            with self.subTest(attributes=attributes):
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
                    parameters = (1, attributes, 100)
                    self.cur.execute(self.sql_insert, parameters)

    def test_malformed_json(self):
        regex = 'must be a JSON object with text values'
        for attributes in self.malformed_json:
            with self.subTest(attributes=attributes):
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
                    parameters = (1, attributes, 100)
                    self.cur.execute(self.sql_insert, parameters)

    def test_none(self):
        """The `quantity.attributes` column should not accept NULL values."""
        regex = 'NOT NULL constraint failed'
        parameters = (1, None, 100)
        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            self.cur.execute(self.sql_insert, parameters)


class CheckSelectorsMixin(object):
    """Valid TEXT_SELECTORS values must be JSON arrays with string values."""
    valid_values = [
        r'["[a=\"one\"]", "[b=\"two\"]"]',
        r'["[c]"]',
    ]
    non_string_values = [
        r'["[a=\"one\"]", 2]',                # <- contains integer
        r'["[a=\"one\"]", ["[b=\"two\"]"]]',  # <- contains nested object
    ]
    not_an_array = [
        '{"a": "one", "b": "two"}',  # <- array
        '"one"',                     # <- text
        '123',                       # <- integer
        '3.14',                      # <- real
        'true',                      # <- boolean
    ]
    malformed_json = [
        r'["[a=\"one\"]", "[b=\"two\"]"',   # <- No closing bracket.
        r'["[a=\"one\"]", "[b=\"two\"]]',   # <- No closing quote.
        r"['[a=\"one\"]', '[b=\"two\"]']",  # <- Requires double quotes.
        'abc',                              # <- Not quoted.
        '',                                 # <- No contents.
    ]


class TestUserSelectorsValid(unittest.TestCase, CheckSelectorsMixin):
    """Check application defined SQL function for TEXT_SELECTORS."""
    def test_valid_values(self):
        for value in self.valid_values:
            with self.subTest(value=value):
                self.assertTrue(_user_selectors_valid(value))

    def test_non_string_values(self):
        for value in self.non_string_values:
            with self.subTest(value=value):
                self.assertFalse(_user_selectors_valid(value))

    def test_not_an_array(self):
        for value in self.not_an_array:
            with self.subTest(value=value):
                self.assertFalse(_user_selectors_valid(value))

    def test_malformed_json(self):
        for value in self.malformed_json:
            with self.subTest(value=value):
                self.assertFalse(_user_selectors_valid(value))

    def test_none(self):
        self.assertFalse(_user_selectors_valid(None))


class TestSelectorsTrigger(unittest.TestCase, CheckSelectorsMixin):
    """Check trigger behavior for columns with the TEXT_SELECTORS
    declared type.

    There are two columns that use this type:
      * edge.selectors
      * weighting.selectors.
    """
    def setUp(self):
        self.con = connect('mynode.toron', mode='memory')
        self.cur = self.con.cursor()
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

        self.sql_insert = 'INSERT INTO weighting (name, selectors) VALUES (?, ?)'

    def test_valid_values(self):
        for index, value in enumerate(self.valid_values):
            with self.subTest(value=value):
                parameters = (f'name{index}', value)
                self.cur.execute(self.sql_insert, parameters)

    def test_non_string_values(self):
        regex = 'must be a JSON array with text values'
        for index, value in enumerate(self.non_string_values):
            with self.subTest(value=value):
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
                    parameters = (f'name{index}', value)
                    self.cur.execute(self.sql_insert, parameters)

    def test_not_an_array(self):
        regex = 'must be a JSON array with text values'
        for index, value in enumerate(self.not_an_array):
            with self.subTest(value=value):
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
                    parameters = (f'name{index}', value)
                    self.cur.execute(self.sql_insert, parameters)

    def test_malformed_json(self):
        regex = 'must be a JSON array with text values'
        for index, value in enumerate(self.malformed_json):
            with self.subTest(value=value):
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
                    parameters = (f'name{index}', value)
                    self.cur.execute(self.sql_insert, parameters)

    def test_none(self):
        """The `weighting.selectors` column should accept None/NULL values."""
        parameters = ('blerg', None)
        self.cur.execute(self.sql_insert, parameters)


class TestTriggerCoverage(unittest.TestCase):
    """Check that TEXT_ATTRIBUTES columns have needed triggers."""

    # NOTE: I think it is important to address a bit of design
    # philosophy that this test case touches on. This test dynamically
    # builds a list of 'TEXT_ATTRIBUTES' type columns and checks that
    # the needed triggers exist for each column.
    #
    # One might ask, "Why not generate this list dynamically in the
    # application itself and automatically apply the triggers using
    # that list?"
    #
    # In this case, there is a tradeoff between the complexity of the
    # application and the complexity of the tests. And since test code
    # is relatively simple, I decided that it was better to push this
    # small bit of complexity into the tests rather than into the
    # application.

    def setUp(self):
        con = sqlite3.connect(':memory:')
        self.cur = con.cursor()
        self.addCleanup(con.close)
        self.addCleanup(self.cur.close)

    def get_actual_trigger_names(self):
        """Helper function to return list of actual temporary trigger names."""
        self.cur.execute("SELECT name FROM sqlite_temp_master WHERE type='trigger'")
        return [row[0] for row in self.cur]

    def get_table_names(self):
        """Helper function to return list of table names from main schema.
        Internal schema tables (beginning with 'sqlite_') are omitted).

        For information, see:
            https://www.sqlite.org/fileformat2.html#internal_schema_objects
        """
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_names = [row[0] for row in self.cur]
        table_names = [name for name in table_names if not name.startswith('sqlite_')]
        return table_names

    def get_custom_type_columns(self, table):
        """Return names of all columns whose defined type does not
        exactly match a built-in SQLite column affinity.
        """
        builtins = {'TEXT', 'NUMERIC', 'INTEGER', 'REAL', 'BLOB'}
        orig_factory = self.cur.row_factory
        try:
            self.cur.row_factory = sqlite3.Row
            self.cur.execute(f"PRAGMA main.table_info('{table}')")
            filtered_rows = [row for row in self.cur if row['type'] not in builtins]
            column_names = [row['name'] for row in filtered_rows]
        finally:
            self.cur.row_factory = orig_factory
        return column_names

    @staticmethod
    def make_trigger_name(insert_or_update, table, column):
        """Helper function to build expected trigger name."""
        return f'trigger_check_{insert_or_update}_{table}_{column}'

    def get_expected_trigger_names(self):
        """Helper function to return list of expected trigger names."""
        expected_triggers = []
        for table in self.get_table_names():
            column_names = self.get_custom_type_columns(table)
            for column in column_names:
                expected_triggers.append(self.make_trigger_name('insert', table, column))
                expected_triggers.append(self.make_trigger_name('update', table, column))

        return expected_triggers

    def test_add_functions_and_triggers(self):
        """Check that all custom 'TEXT_...' type columns have
        associated INSERT and UPDATE triggers.
        """
        self.cur.executescript(_schema_script)  # <- Create database tables.
        _add_functions_and_triggers(self.cur.connection)  # <- Create triggers.

        actual_triggers = self.get_actual_trigger_names()
        expected_triggers = self.get_expected_trigger_names()
        self.assertEqual(set(actual_triggers), set(expected_triggers))


class TestValidatePermissionsGetMode():
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        # Define path and create read-write dummy file.
        cls.rw_path = 'rw_file.toron'
        open(cls.rw_path, 'w').close()

        # Define path and create read-only dummy file.
        cls.ro_path = 'ro_file.toron'
        open(cls.ro_path, 'w').close()
        os.chmod(cls.ro_path, S_IRUSR)  # Make sure file is read-only.

        # Define path but don't create a file (file should not exist).
        cls.new_path = 'new_file.toron'

    def test_readonly_required(self):
        result = _validate_permissions_get_mode(self.ro_path, required_permissions='readonly')
        self.assertEqual(result, 'ro')

        with self.assertRaises(PermissionError):
            _validate_permissions_get_mode(self.rw_path, required_permissions='readonly')

        with self.assertRaises(ToronError):
            _validate_permissions_get_mode(self.new_path, required_permissions='readonly')

    def test_readwrite_required(self):
        result = _validate_permissions_get_mode(self.rw_path, required_permissions='readwrite')
        self.assertEqual(result, 'rw')

        with self.assertRaises(PermissionError):
            _validate_permissions_get_mode(self.ro_path, required_permissions='readwrite')

        result = _validate_permissions_get_mode(self.new_path, required_permissions='readwrite')
        self.assertEqual(result, 'rwc')

    def test_none_required(self):
        result = _validate_permissions_get_mode(self.ro_path, required_permissions=None)
        msg = ("Should return 'rw' mode even if file permissions are "
               "read-only. This will make sure that "
               "connection can "
               "write if permissions are changed during run-time.")
        self.assertEqual(result, 'rw', msg=msg)

        result = _validate_permissions_get_mode(self.rw_path, required_permissions=None)
        self.assertEqual(result, 'rw')

        result = _validate_permissions_get_mode(self.new_path, required_permissions=None)
        self.assertEqual(result, 'rwc')


class TestPathToSqliteUri(unittest.TestCase):
    def test_common_cases(self):
        self.assertEqual(
            _path_to_sqlite_uri('mynode.toron'),
            'file:mynode.toron',
        )
        self.assertEqual(
            _path_to_sqlite_uri('my?node.toron'),
            'file:my%3Fnode.toron',
        )
        self.assertEqual(
            _path_to_sqlite_uri('path///to//mynode.toron'),
            'file:path/to/mynode.toron',
        )

    def test_windows_specifics(self):
        if os.name != 'nt':
            return

        self.assertEqual(
            _path_to_sqlite_uri(r'path\to\mynode.toron'),
            'file:path/to/mynode.toron',
        )

        self.assertEqual(
            _path_to_sqlite_uri(r'C:\path\to\my node.toron'),
            'file:/C:/path/to/my%20node.toron',
        )

        self.assertEqual(
            _path_to_sqlite_uri(r'C:\path\to\myno:de.toron'),  # <- Errant ":".
            'file:/C:/path/to/myno%3Ade.toron',
        )

        self.assertEqual(
            _path_to_sqlite_uri(r'C:mynode.toron'),  # <- Relative path with drive letter.
            f'file:/{os.getcwd()}/mynode.toron'.replace("\\", "/"),
        )


class TestConnect(TempDirTestCase):
    def test_new_file(self):
        """If a node file doesn't exist it should be created."""
        path = 'mynode.node'
        connect(path).close()  # Creates Toron database at given path.

        con = sqlite3.connect(path)
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur}
        tables.discard('sqlite_sequence')  # <- Table added by SQLite.

        expected = {
            'edge',
            'element',
            'location',
            'property',
            'quantity',
            'relation',
            'structure',
            'weight',
            'weighting',
        }
        self.assertSetEqual(tables, expected)

    def test_nonfile_path(self):
        """Non-file resources should fail immediately."""
        path = 'mydirectory'
        os.mkdir(path)  # <- Create a directory with the given `path` name.

        regex = "unable to open node file 'mydirectory'"
        msg = 'should fail if path is a directory instead of a file'
        with self.assertRaisesRegex(ToronError, regex, msg=msg):
            con = connect(path)

    def test_nondatabase_file(self):
        """Non-database files should fail."""
        # Build a non-database file.
        path = 'not_a_database.txt'
        with open(path, 'w') as f:
            f.write('Hello World\n')

        with self.assertRaises(ToronError):
            con = connect(path)

    def test_unknown_schema(self):
        """Database files with unknown schemas should fail."""
        # Build a non-Toron SQLite database file.
        path = 'mydata.db'
        con = sqlite3.connect(path)
        con.executescript('''
            CREATE TABLE mytable(col1, col2);
            INSERT INTO mytable VALUES ('a', 1), ('b', 2), ('c', 3);
        ''')
        con.close()

        with self.assertRaises(ToronError):
            con = connect(path)

    def test_unsupported_schema_version(self):
        """Unsupported schema version should fail."""
        path = 'mynode.toron'

        con = connect(path)
        con.execute("INSERT OR REPLACE INTO property VALUES ('schema_version', '999')")
        con.commit()
        con.close()

        regex = 'Unsupported Toron node format: schema version 999'
        with self.assertRaisesRegex(ToronError, regex):
            con = connect(path)

    def test_read_write_mode(self):
        regex = "unable to open node file 'path1.toron'"
        with self.assertRaisesRegex(ToronError, regex):
            connect('path1.toron', mode='rw')  # Open nonexistent node (fails).

        connect('path2.toron', mode='rwc').close()  # Create node.
        connect('path2.toron', mode='rw')  # Open existing node.

    def test_read_only_mode(self):
        regex = "unable to open node file 'path1.toron'"
        with self.assertRaisesRegex(ToronError, regex):
            connect('path1.toron', mode='ro')  # Open nonexistent node (fails).

        connect('path2.toron', mode='rwc').close()  # Create node.
        con = connect('path2.toron', mode='ro')  # Open existing node.

        regex = 'attempt to write a readonly database'
        with self.assertRaisesRegex(sqlite3.OperationalError, regex):
            con.execute('INSERT INTO property VALUES (?, ?)', ('key1', '"value1"'))

    def test_invalid_access_mode(self):
        regex = 'no such access mode: badmode'
        with self.assertRaisesRegex(ToronError, regex):
            connect('path1.toron', mode='badmode')

    def test_read_only_via_filesystem(self):
        """When the filesystem status of a database file is read-only,
        the connection should behave as if it were accessed in 'ro'
        mode regardless of what mode was actually used.
        """
        file_path = 'node42.toron'

        # Create a new node and set its filesystem status to read-only.
        connect(file_path, mode='rwc').close()
        os.chmod(file_path, S_IRUSR)

        # Open the existing node in read-write-create mode.
        con = connect(file_path, mode='rwc')

        # Try to insert records into the database.
        regex = 'attempt to write a readonly database'
        msg = "despite 'rwc' mode, database should be read-only via filesystem status"
        with self.assertRaisesRegex(sqlite3.OperationalError, regex, msg=msg):
            con.execute('INSERT INTO property VALUES (?, ?)', ('key1', '123'))

        # Close the connection and change the status back to read-write.
        con.close()
        os.chmod(file_path, S_IRUSR|S_IWUSR)


class TestTransactionOnDisk(TempDirTestCase):
    """Tests for the transaction() context manager."""
    def setUp(self):
        self.addCleanup(self.cleanup_temp_files)

    def test_path_and_mode(self):
        """When given *path* and *mode* arguments, transaction()
        should establish its own connection and then close this
        connection once it is finished.
        """
        path = 'mynode.toron'
        mode = 'rwc'
        connect(path, mode=mode).close()  # Create file with Toron schema.

        with transaction(path, mode) as cursor:
            connection = cursor.connection
            self.assertTrue(connection.in_transaction)

        # Cursor should be closed after `with` block exits.
        regex = 'closed cursor'
        msg = 'Cursor should be closed after exiting context.'
        with self.assertRaisesRegex(sqlite3.ProgrammingError, regex, msg=msg):
            cursor.execute('SELECT 1')

        # Connection should be closed after `with` block exits.
        regex = 'closed database'
        msg = 'Connection should be closed after exiting context.'
        with self.assertRaisesRegex(sqlite3.ProgrammingError, regex, msg=msg):
            connection.cursor()


class TestTransactionInMemory(unittest.TestCase):
    """Tests for the transaction() context manager."""
    def test_existing_connection(self):
        """When given a existing Connection, transaction() should use
        the connection as provided and leave it open when finished.
        """
        connection = connect(':memory:')  # Create in-memory database with Toron schema.

        with transaction(connection) as cursor:
            self.assertTrue(connection.in_transaction)

        regex = 'closed cursor'
        msg = 'Cursor should be closed after exiting context.'
        with self.assertRaisesRegex(sqlite3.ProgrammingError, regex, msg=msg):
            cursor.execute('SELECT 1')

        try:
            cursor = connection.cursor()  # <- Should pass without error.
            cursor.close()
            connection.close()
        except sqlite3.ProgrammingError as err:
            if 'closed database' not in str(err):
                raise
            msg = 'existing connections must remain open after exiting context'
            self.fail(msg)

    def test_transaction_commit(self):
        connection = connect(':memory:')

        with transaction(connection) as cursor:
            cursor.execute("""INSERT INTO property VALUES ('key1', '"value1"')""")

        result = connection.execute("SELECT * FROM property WHERE key='key1'").fetchone()
        msg = 'successful transaction should commit changes to database'
        self.assertEqual(result, ('key1', 'value1'), msg=msg)

    def test_transaction_rollback(self):
        connection = connect(':memory:')

        with self.assertRaises(Exception):
            with transaction(connection) as cursor:
                cursor.execute("""INSERT INTO property VALUES ('key1', '"value1"')""")  # <- Success.
                cursor.execute("""INSERT INTO property VALUES ('key2', 'bad json')""")  # <- Failure.

        result = connection.execute("SELECT * FROM property WHERE key='key1'").fetchone()
        msg = 'a failed transaction should rollback all changes to the database'
        self.assertEqual(result, None, msg=msg)


class TestJsonConversion(unittest.TestCase):
    """Registered converters should select JSON strings as objects."""
    def setUp(self):
        self.con = connect('mynode.node', mode='memory')
        self.cur = self.con.cursor()
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def test_text_json(self):
        """Selecting TEXT_JSON should convert strings into objects."""
        self.cur.execute(
            'INSERT INTO property VALUES (?, ?)',
            ('key1', '[1, 2, 3]')
        )
        self.cur.execute("SELECT value FROM property WHERE key='key1'")
        self.assertEqual(self.cur.fetchall(), [([1, 2, 3],)])

    def test_text_attributes(self):
        """Selecting TEXT_ATTRIBUTES should convert strings into objects."""
        self.cur.execute('INSERT INTO location (_location_id) VALUES (1)')

        self.cur.execute(
            'INSERT INTO quantity VALUES (?, ?, ?, ?)',
            (1, 1, '{"foo": "bar"}', 100)
        )
        self.cur.execute("SELECT attributes FROM quantity WHERE quantity_id=1")
        self.assertEqual(self.cur.fetchall(), [({'foo': 'bar'},)])

    def test_text_selectors(self):
        """Selecting TEXT_SELECTORS should convert strings into objects."""
        self.cur.execute(
            'INSERT INTO weighting (name, selectors) VALUES (?, ?)',
            ('foo', r'["[bar=\"baz\"]"]')
        )
        self.cur.execute("SELECT selectors FROM weighting WHERE name='foo'")
        self.assertEqual(self.cur.fetchall(), [(['[bar="baz"]'],)])


class TestSavepoint(unittest.TestCase):
    def setUp(self):
        con = sqlite3.connect(':memory:')
        con.isolation_level = None
        self.cursor = con.cursor()
        self.addCleanup(con.close)
        self.addCleanup(self.cursor.close)

    def test_transaction_status(self):
        con = self.cursor.connection

        self.assertFalse(con.in_transaction)

        with savepoint(self.cursor):
            self.assertTrue(con.in_transaction)

        self.assertFalse(con.in_transaction)

    def test_release(self):
        cur = self.cursor

        with savepoint(cur):
            cur.execute('CREATE TEMPORARY TABLE test_table ("A")')
            cur.execute("INSERT INTO test_table VALUES ('one')")
            cur.execute("INSERT INTO test_table VALUES ('two')")
            cur.execute("INSERT INTO test_table VALUES ('three')")

        cur.execute('SELECT * FROM test_table')
        self.assertEqual(cur.fetchall(), [('one',), ('two',), ('three',)])

    def test_nested_releases(self):
        cur = self.cursor

        with savepoint(cur):
            cur.execute('CREATE TEMPORARY TABLE test_table ("A")')
            cur.execute("INSERT INTO test_table VALUES ('one')")
            with savepoint(cur):  # <- Nested!
                cur.execute("INSERT INTO test_table VALUES ('two')")
            cur.execute("INSERT INTO test_table VALUES ('three')")

        cur.execute('SELECT * FROM test_table')
        self.assertEqual(cur.fetchall(), [('one',), ('two',), ('three',)])

    def test_rollback(self):
        cur = self.cursor

        with savepoint(cur):  # <- Released.
            cur.execute('CREATE TEMPORARY TABLE test_table ("A")')

        try:
            with savepoint(cur):  # <- Rolled back!
                cur.execute("INSERT INTO test_table VALUES ('one')")
                cur.execute("INSERT INTO test_table VALUES ('two')")
                cur.execute("INSERT INTO missing_table VALUES ('three')")  # <- Bad table.
        except sqlite3.OperationalError:
            pass

        cur.execute('SELECT * FROM test_table')
        self.assertEqual(cur.fetchall(), [], 'Table should exist but contain no records.')

    def test_nested_rollback(self):
        cur = self.cursor

        with savepoint(cur):  # <- Released.
            cur.execute('CREATE TEMPORARY TABLE test_table ("A")')
            cur.execute("INSERT INTO test_table VALUES ('one')")
            try:
                with savepoint(cur):  # <- Nested rollback!
                    cur.execute("INSERT INTO test_table VALUES ('two')")
                    raise Exception()
            except Exception:
                pass
            cur.execute("INSERT INTO test_table VALUES ('three')")

        cur.execute('SELECT * FROM test_table')
        self.assertEqual(cur.fetchall(), [('one',), ('three',)])

    def test_bad_isolation_level(self):
        connection = sqlite3.connect(':memory:')
        connection.isolation_level = 'DEFERRED'  # <- Incompatible isolation level.
        cur = connection.cursor()

        regex = "isolation_level must be None, got: 'DEFERRED'"
        with self.assertRaisesRegex(sqlite3.OperationalError, regex):
            with savepoint(cur):  # <- Should raise error.
                pass

