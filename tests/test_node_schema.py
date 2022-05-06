"""Tests for toron._node_schema module."""

import os
import sqlite3
import unittest
from collections import namedtuple, OrderedDict, UserString
from textwrap import dedent
from .common import TempDirTestCase

from toron._node_schema import SQLITE_JSON1_ENABLED
from toron._node_schema import _is_wellformed_json
from toron._node_schema import _is_wellformed_user_properties
from toron._node_schema import _is_wellformed_attributes
from toron._node_schema import _schema_script
from toron._node_schema import _make_trigger_for_attributes
from toron._node_schema import _add_functions_and_triggers
from toron._node_schema import connect


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


class TestIsWellformedJson(unittest.TestCase, CheckJsonMixin):
    """Check application defined SQL function for TEXT_JSON."""
    def test_valid_values(self):
        for value in self.valid_values:
            with self.subTest(value=value):
                self.assertTrue(_is_wellformed_json(value))

    def test_malformed_json(self):
        for value in self.malformed_json:
            with self.subTest(value=value):
                self.assertFalse(_is_wellformed_json(value))

    def test_none(self):
        self.assertFalse(_is_wellformed_json(None))


class TestJsonTrigger(TempDirTestCase, CheckJsonMixin):
    """Check trigger behavior for `property.value` column (uses the
    TEXT_JSON declared type).
    """
    def setUp(self):
        self.con = connect('mynode.toron')
        self.cur = self.con.cursor()
        self.addCleanup(self.cleanup_temp_files)
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


class TestIsWellformedUserProperties(unittest.TestCase, CheckUserPropertiesMixin):
    """Check application defined SQL function for TEXT_USERPROPERTIES."""
    def test_valid_values(self):
        for value in self.valid_values:
            with self.subTest(value=value):
                self.assertTrue(_is_wellformed_user_properties(value))

    def test_not_an_object(self):
        for value in self.not_an_object:
            with self.subTest(value=value):
                self.assertFalse(_is_wellformed_user_properties(value))

    def test_malformed_json(self):
        for value in self.malformed_json:
            with self.subTest(value=value):
                self.assertFalse(_is_wellformed_user_properties(value))

    def test_none(self):
        self.assertFalse(_is_wellformed_user_properties(None))


class TestUserPropertiesTrigger(TempDirTestCase, CheckUserPropertiesMixin):
    """Check TRIGGER behavior for edge.user_properties column."""

    def setUp(self):
        self.con = connect('mynode.toron')
        self.cur = self.con.cursor()
        self.addCleanup(self.cleanup_temp_files)
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    @staticmethod
    def make_parameters(index, value):
        """Helper function to return formatted SQL query parameters."""
        return (
            None,                      # edge_id (INTEGER PRIMARY KEY)
            f'name{index}',            # name
            None,                      # description
            '{"category": "census"}',  # type_info
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


class TestIsWellformedAttributes(unittest.TestCase, CheckAttributesMixin):
    """Check application defined SQL function for TEXT_ATTRIBUTES."""
    def test_valid_values(self):
        for value in self.valid_values:
            with self.subTest(value=value):
                self.assertTrue(_is_wellformed_attributes(value))

    def test_non_string_values(self):
        for value in self.non_string_values:
            with self.subTest(value=value):
                self.assertFalse(_is_wellformed_attributes(value))

    def test_not_an_object(self):
        for value in self.not_an_object:
            with self.subTest(value=value):
                self.assertFalse(_is_wellformed_attributes(value))

    def test_malformed_json(self):
        for value in self.malformed_json:
            with self.subTest(value=value):
                self.assertFalse(_is_wellformed_attributes(value))

    def test_none(self):
        self.assertFalse(_is_wellformed_attributes(None))


class TestAttributesTrigger(TempDirTestCase, CheckAttributesMixin):
    """Check trigger behavior for columns with the TEXT_ATTRIBUTES
    declared type.

    There are three columns that use this type:
      * edge.type_info
      * quantity.attributes
      * weight_info.type_info.
    """
    def setUp(self):
        self.con = connect('mynode.toron')
        self.cur = self.con.cursor()
        self.addCleanup(self.cleanup_temp_files)
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def test_valid_values(self):
        for index, value in enumerate(self.valid_values):
            with self.subTest(value=value):
                parameters = (None, f'name{index}', None, value, 0)
                self.cur.execute("INSERT INTO weight_info VALUES (?, ?, ?, ?, ?)", parameters)

    def test_non_string_values(self):
        regex = 'must be a JSON object with text values'
        for value in self.non_string_values:
            with self.subTest(value=value):
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
                    parameters = (None, 'nonstring', None, value, 0)
                    self.cur.execute("INSERT INTO weight_info VALUES (?, ?, ?, ?, ?)", parameters)

    def test_not_an_object(self):
        regex = 'must be a JSON object with text values'
        for value in self.not_an_object:
            with self.subTest(value=value):
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
                    parameters = (None, 'nonobject', None, value, 0)
                    self.cur.execute("INSERT INTO weight_info VALUES (?, ?, ?, ?, ?)", parameters)

    def test_malformed_json(self):
        regex = 'must be a JSON object with text values'
        for index, value in enumerate(self.malformed_json):
            with self.subTest(value=value):
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
                    parameters = (None, 'malformed', None, value, 0)
                    self.cur.execute("INSERT INTO weight_info VALUES (?, ?, ?, ?, ?)", parameters)

    def test_none(self):
        """The property.value column should accept None/NULL values."""
        value = None

        with self.assertRaises(sqlite3.IntegrityError):
            parameters = (None, 'blerg', None, value, 0)
            self.cur.execute("INSERT INTO weight_info VALUES (?, ?, ?, ?, ?)", parameters)


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
        """Helper function to return list of actual temp trigger names."""
        self.cur.execute("SELECT name FROM sqlite_temp_master WHERE type='trigger'")
        return [row[0] for row in self.cur]

    def get_all_table_names(self):
        """Helper function to return list of table names from main schema."""
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in self.cur]

    def get_text_attributes_columns(self, table):
        """Helper function to return list of TEXT_ATTRIBUTES columns."""
        orig_factory = self.cur.row_factory
        try:
            self.cur.row_factory = sqlite3.Row
            self.cur.execute(f"PRAGMA main.table_info('{table}')")
            filtered_rows = [row for row in self.cur if row['type'] == 'TEXT_ATTRIBUTES']
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
        table_names = self.get_all_table_names()

        expected_triggers = []
        for table in table_names:
            column_names = self.get_text_attributes_columns(table)
            for column in column_names:
                expected_triggers.append(self.make_trigger_name('insert', table, column))
                expected_triggers.append(self.make_trigger_name('update', table, column))

        expected_triggers.append('trigger_check_insert_edge_user_properties')
        expected_triggers.append('trigger_check_update_edge_user_properties')

        expected_triggers.append('trigger_check_insert_property_value')
        expected_triggers.append('trigger_check_update_property_value')

        return expected_triggers

    def test_add_functions_and_triggers(self):
        """Test that all TEXT_ATTRIBUTES columns have proper INSERT and
        UPDATE triggers.
        """
        self.cur.executescript(_schema_script)  # <- Create database tables.
        _add_functions_and_triggers(self.cur.connection)  # <- Create triggers.

        actual_triggers = self.get_actual_trigger_names()
        expected_triggers = self.get_expected_trigger_names()
        self.assertEqual(set(actual_triggers), set(expected_triggers))


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
            'weight_info',
        }
        self.assertSetEqual(tables, expected)

    def test_nonfile_path(self):
        """Non-file resources should fail immediately."""
        path = 'mydirectory'
        os.mkdir(path)  # <- Create a directory with the given `path` name.

        msg = 'should fail if path is a directory instead of a file'
        with self.assertRaisesRegex(Exception, 'not a Toron Node', msg=msg):
            con = connect(path)

    def test_nondatabase_file(self):
        """Non-database files should fail."""
        # Build a non-database file.
        path = 'not_a_database.txt'
        with open(path, 'w') as f:
            f.write('Hello World\n')

        with self.assertRaises(sqlite3.DatabaseError):
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

        with self.assertRaises(sqlite3.OperationalError):
            con = connect(path)

    def test_read_write_mode(self):
        regex = 'unable to open database file'
        with self.assertRaisesRegex(sqlite3.OperationalError, regex):
            connect('path1.toron', mode='rw')  # Open nonexistent node (fails).

        connect('path2.toron', mode='rwc').close()  # Create node.
        connect('path2.toron', mode='rw')  # Open existing node.

    def test_read_only_mode(self):
        regex = 'unable to open database file'
        with self.assertRaisesRegex(sqlite3.OperationalError, regex):
            connect('path1.toron', mode='ro')  # Open nonexistent node (fails).

        connect('path2.toron', mode='rwc').close()  # Create node.
        con = connect('path2.toron', mode='ro')  # Open existing node.

        regex = 'attempt to write a readonly database'
        with self.assertRaisesRegex(sqlite3.OperationalError, regex):
            con.execute('INSERT INTO property VALUES (?, ?)', ('key1', '"value1"'))

    def test_invalid_access_mode(self):
        regex = 'no such access mode: badmode'
        with self.assertRaisesRegex(sqlite3.OperationalError, regex):
            connect('path1.toron', mode='badmode')


class TestJsonConversion(TempDirTestCase):
    """Registered converters should select JSON strings as objects."""
    def setUp(self):
        self.con = connect('mynode.node')
        self.cur = self.con.cursor()
        self.addCleanup(self.cleanup_temp_files)
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
        self.cur.execute(
            'INSERT INTO weight_info VALUES (?, ?, ?, ?, ?)',
            (None, 'foo', None, '{"bar": "baz"}', 0)
        )
        self.cur.execute("SELECT type_info FROM weight_info WHERE name='foo'")
        self.assertEqual(self.cur.fetchall(), [({'bar': 'baz'},)])

