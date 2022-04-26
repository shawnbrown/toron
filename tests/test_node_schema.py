"""Tests for toron._node_schema module."""

import os
import sqlite3
import unittest
from collections import namedtuple, OrderedDict, UserString
from textwrap import dedent
from .common import TempDirTestCase
from toron._node_schema import get_primitive_repr
from toron._node_schema import dumps, loads
from toron._node_schema import InvalidSerialization
from toron._node_schema import SQLITE_JSON1_ENABLED
from toron._node_schema import _is_flat_json_object
from toron._node_schema import _pre_execute_functions
from toron._node_schema import _schema_script
from toron._node_schema import _make_trigger_assert_flat_object
from toron._node_schema import _execute_post_schema_triggers
from toron._node_schema import connect


class TestGetPrimitiveRepr(unittest.TestCase):
    def test_supported_types(self):
        """Check that all supported instance types get expected reprs."""
        supported_instances = [
            ('abc',  "'abc'"),   # str
            (b'xyz', "b'xyz'"),  # bytes
            (123,    '123'),     # int
            (1.125,  '1.125'),   # float
            (True,   'True'),    # bool
            (None,   'None'),    # NoneType
            ((3+0j), '(3+0j)'),  # complex
        ]
        for obj, obj_repr in supported_instances:
            with self.subTest(obj=obj):
                self.assertEqual(get_primitive_repr(obj), obj_repr)

    def test_unsupported_types(self):
        """Should return None for unsupported types (containers, etc.)"""
        self.assertIsNone(get_primitive_repr(Ellipsis))
        self.assertIsNone(get_primitive_repr([1, 2]))
        self.assertIsNone(get_primitive_repr({'a': 1}))

    def test_exact_type_matching(self):
        """Values that are a subclass of supported types should get None."""
        class StrSubclass(UserString):
            pass

        instance_of_str_subclass = StrSubclass('abc')
        self.assertIsNone(get_primitive_repr(instance_of_str_subclass))

    def test_no_valid_literal_repr(self):
        """Values that don't have a literal representation must return
        a None value even if the instance is of a supported type.
        """
        self.assertIsNone(get_primitive_repr(float('nan')))
        self.assertIsNone(get_primitive_repr(float('inf')))


class TestInvalidSerialization(unittest.TestCase):
    def test_initialization(self):
        bad_string = '[1, 2,'
        invalid = InvalidSerialization(bad_string)

        self.assertIsInstance(invalid, InvalidSerialization)
        self.assertEqual(invalid.data, bad_string)

    def test_representation(self):
        invalid = InvalidSerialization('[1, 2,')
        self.assertEqual(repr(invalid), "InvalidSerialization('[1, 2,')")

    def test_equality(self):
        bad_string = '[1, 2,'
        invalid_a = InvalidSerialization(bad_string)
        invalid_b = InvalidSerialization(bad_string)

        self.assertEqual(invalid_a, invalid_b)
        self.assertNotEqual(bad_string, invalid_a)
        self.assertNotEqual(invalid_a, InvalidSerialization("'foo"))


class TestDumpS(unittest.TestCase):
    def test_primitive_types(self):
        self.assertEqual(dumps(1.125), '1.125')
        self.assertEqual(dumps(b'abc'), "b'abc'")

    def test_list_or_tuple(self):
        self.assertEqual(dumps([4, 8, 2]), "[4, 8, 2]")
        self.assertEqual(dumps((1, 'a', 2.25)), "(1, 'a', 2.25)")

        msg = 'should not serialize nested containers'
        with self.assertRaises(TypeError, msg=msg):
            dumps([1, [2, 3]])

        msg = 'should not serialize instances of subclasses'
        with self.assertRaises(TypeError, msg=msg):
            coord = namedtuple('coord', ['x', 'y'])
            dumps(coord(1, 2))

    def test_set(self):
        msg = 'serialized form should always be in sorted order'
        self.assertEqual(dumps({4, 8, 2}), "{2, 4, 8}", msg=msg)

        msg = 'mixed types should sort without problems'
        self.assertEqual(dumps({None, 2, 'a', 1.25}), "{'a', 1.25, 2, None}", msg=msg)

        msg = 'should not serialize nested containers'
        with self.assertRaises(TypeError, msg=msg):
            dumps({4, (8, 2)})

        msg = 'should not serialize instances of subclasses'
        with self.assertRaises(TypeError, msg=msg):
            dumps(frozenset([1, 2, 3]))

    def test_dict(self):
        msg = 'serialized form should always be in sorted order'
        self.assertEqual(dumps({'b': 2, 'a': 1}), "{'a': 1, 'b': 2}", msg=msg)

        msg = 'mixed types should sort without problems'
        self.assertEqual(dumps({None: 2, 'a': 1.25}), "{'a': 1.25, None: 2}", msg=msg)

        msg = 'should not serialize nested containers'
        with self.assertRaises(TypeError, msg=msg):
            dumps({4: (8, 2)})

        msg = 'should not serialize non-primitive keys'
        with self.assertRaises(TypeError, msg=msg):
            dumps({(4, 8): 2})

        msg = 'should not serialize instances of subclasses'
        with self.assertRaises(TypeError, msg=msg):
            dumps(OrderedDict([('b', 2), ('a', 1)]))

    def test_unsupported_types(self):
        with self.assertRaises(TypeError):
            dumps(frozenset([1, 2, 3]))

        with self.assertRaises(TypeError):
            dumps(Ellipsis)


class TestLoadS(unittest.TestCase):
    def test_valid_strings(self):
        self.assertEqual(loads('1.125'), 1.125)
        self.assertEqual(loads("('a', 1, 2.25)"), ('a', 1, 2.25))
        self.assertEqual(loads("{'a': 1, 'b': 2}"), {'a': 1, 'b': 2})

    def test_syntax_error(self):
        bad_value = "['a', 'b',"  # <- No closing bracket.

        with self.assertRaises(SyntaxError):
            loads(bad_value)  # Default handling is "strict".

        with self.assertWarns(RuntimeWarning):
            returned_value = loads(bad_value, errors='warn')
        self.assertEqual(returned_value, InvalidSerialization(bad_value))

        returned_value = loads(bad_value, errors='ignore')
        self.assertIsNone(returned_value)

    def test_value_error(self):
        bad_value = "float('inf')"  # <- Not a literal representation.

        with self.assertRaises(ValueError):
            loads(bad_value)  # Default handling is "strict".

        with self.assertWarns(RuntimeWarning):
            returned_value = loads(bad_value, errors='warn')
        self.assertEqual(returned_value, InvalidSerialization(bad_value))

        returned_value = loads(bad_value, errors='ignore')
        self.assertIsNone(returned_value)


class TestIsFlatJsonObject(unittest.TestCase):
    def test_is_flat_object(self):
        self.assertTrue(_is_flat_json_object('{"a": 1, "b": 2.2, "c": "three"}'))
        self.assertTrue(_is_flat_json_object('{"a": true, "b": false, "c": null}'))

    def test_not_flat(self):
        self.assertFalse(_is_flat_json_object('{"a": 1, "b": {"c": 3}}'))
        self.assertFalse(_is_flat_json_object('{"a": 1, "b": [2, [3, 4]]}'))

    def test_not_an_object(self):
        self.assertFalse(_is_flat_json_object('123'))
        self.assertFalse(_is_flat_json_object('3.14'))
        self.assertFalse(_is_flat_json_object('"abc"'))
        self.assertFalse(_is_flat_json_object('[1, 2]'))
        self.assertFalse(_is_flat_json_object('true'))

    def test_malformed_json(self):
        self.assertFalse(_is_flat_json_object('[1, 2'))  # No closing bracket.
        self.assertFalse(_is_flat_json_object('{"a": 1'))  # No closing curly-brace.
        self.assertFalse(_is_flat_json_object("{'a': 1}"))  # Requires double quotes.
        self.assertFalse(_is_flat_json_object('abc'))  # Not quoted.
        self.assertFalse(_is_flat_json_object(''))  # No contents.

    def test_none(self):
        self.assertFalse(_is_flat_json_object(None))


class TestMakeTriggerAssertFlatObject(unittest.TestCase):
    maxDiff = None

    def test_trigger_sql(self):
        actual = _make_trigger_assert_flat_object('INSERT', 'mytbl', 'mycol')

        if SQLITE_JSON1_ENABLED:
            expected = """
                CREATE TEMPORARY TRIGGER IF NOT EXISTS trg_assert_flat_mytbl_mycol_insert
                AFTER INSERT ON main.mytbl FOR EACH ROW
                WHEN
                    NEW.mycol IS NOT NULL
                    AND (json_type(NEW.mycol) != 'object'
                        OR (SELECT COUNT(*)
                            FROM json_each(NEW.mycol)
                            WHERE json_each.type IN ('object', 'array')) != 0)
                BEGIN
                    SELECT RAISE(
                        ABORT,
                        'mycol must be JSON object containing strings, numbers, true, false, or null'
                    );
                END;
            """
        else:
            expected = """
                CREATE TEMPORARY TRIGGER IF NOT EXISTS trg_assert_flat_mytbl_mycol_insert
                AFTER INSERT ON main.mytbl FOR EACH ROW
                WHEN is_flat_json_object(NEW.mycol) = 0
                BEGIN
                    SELECT RAISE(
                        ABORT,
                        'mycol must be JSON object containing strings, numbers, true, false, or null'
                    );
                END;
            """
        self.assertEqual(dedent(actual).strip(), dedent(expected).strip())

    def test_bad_action(self):
        with self.assertRaises(ValueError):
            _make_trigger_assert_flat_object('DELETE', 'mytbl', 'mycol')


class TestTriggerCoverage(unittest.TestCase):
    """Check that TEXT_JSONFLATOBJ columns have needed triggers."""

    # NOTE: I think it is important to address a bit of design
    # philosophy that this test case touches on. This test dynamically
    # builds a list of 'TEXT_JSONFLATOBJ' type columns and checks that
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

    def get_text_jsonflatobj_columns(self, table):
        """Helper function to return list of TEXT_JSONFLATOBJ columns."""
        orig_factory = self.cur.row_factory
        try:
            self.cur.row_factory = sqlite3.Row
            self.cur.execute(f"PRAGMA main.table_info('{table}')")
            filtered_rows = [row for row in self.cur if row['type'] == 'TEXT_JSONFLATOBJ']
            column_names = [row['name'] for row in filtered_rows]
        finally:
            self.cur.row_factory = orig_factory
        return column_names

    @staticmethod
    def make_trigger_name(insert_or_update, table, column):
        """Helper function to build expected trigger name."""
        return f'trg_assert_flat_{table}_{column}_{insert_or_update}'

    def get_expected_trigger_names(self):
        """Helper function to return list of expected trigger names."""
        table_names = self.get_all_table_names()

        expected_triggers = []
        for table in table_names:
            column_names = self.get_text_jsonflatobj_columns(table)
            for column in column_names:
                expected_triggers.append(self.make_trigger_name('insert', table, column))
                expected_triggers.append(self.make_trigger_name('update', table, column))

        return expected_triggers

    def test_execute_post_schema_triggers(self):
        """Test that all TEXT_JSONFLATOBJ columns have proper INSERT and
        UPDATE triggers.
        """
        _pre_execute_functions(self.cur.connection)
        self.cur.executescript(_schema_script)   # <- Create database tables.
        _execute_post_schema_triggers(self.cur)  # <- Create triggers.

        actual_triggers = self.get_actual_trigger_names()
        expected_triggers = self.get_expected_trigger_names()
        self.assertEqual(set(actual_triggers), set(expected_triggers))


class TestConnect(TempDirTestCase):
    def test_new_file(self):
        """If a node file doesn't exist it should be created."""
        path = 'mynode.node'
        node = connect(path)  # Creates node file if none exists.

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
            node = connect(path)


class TestColumnTextJson(TempDirTestCase):
    """Test the behavior of columns using the TEXT_JSON type."""
    def setUp(self):
        self.con = connect('mynode.node')
        self.cur = self.con.cursor()

        def cleanup():
            self.cur.close()
            self.con.close()
            self.cleanup_temp_files()

        self.addCleanup(cleanup)

    def test_column_type(self):
        """Make sure that the `property.value` column is TEXT_JSON."""
        orig_factory = self.cur.row_factory
        try:
            self.cur.row_factory = sqlite3.Row
            self.cur.execute("PRAGMA main.table_info('property')")
            value_column = [row for row in self.cur if row['name'] == 'value'].pop()
        finally:
            self.cur.row_factory = orig_factory

        declared_type = value_column['type']
        self.assertEqual(declared_type, 'TEXT_JSON')

    def test_insert_wellformed_json(self):
        """Valid JSON strings should be inserted without errors."""
        parameters = [
            ('key1', '123'),
            ('key2', '1.23'),
            ('key3', '"abc"'),
            ('key4', 'true'),
            ('key5', 'false'),
            ('key6', 'null'),
            ('key7', '[1, 2.0, "3"]'),
            ('key8', '{"a": 1, "b": [2, 3]}'),
            ('key9', None),  # <- The property.value column allows NULLs.
        ]
        self.cur.executemany("INSERT INTO property VALUES (?, ?)", parameters)

    def test_insert_malformed_json(self):
        """Invalid JSON strings should fail with CHECK constraint."""
        regex = '^CHECK constraint failed'

        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            self.cur.execute('INSERT INTO property VALUES (?, ?)', ('key1', 'abc'))

        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            self.cur.execute('INSERT INTO property VALUES (?, ?)', ('key2', '[1,2,3'))


class TestColumnTextJsonFlatObj(TempDirTestCase):
    """Test the behavior of columns using the TEXT_JSONFLATOBJ type."""
    def setUp(self):
        self.con = connect('mynode.node')
        self.cur = self.con.cursor()

        def cleanup():
            self.cur.close()
            self.con.close()
            self.cleanup_temp_files()

        self.addCleanup(cleanup)

    def test_column_type(self):
        """Make sure that the `weight_info.type_info` column is
        TEXT_JSONFLATOBJ.
        """
        orig_factory = self.cur.row_factory
        try:
            self.cur.row_factory = sqlite3.Row
            self.cur.execute("PRAGMA main.table_info('weight_info')")
            type_info_column = [row for row in self.cur if row['name'] == 'type_info'].pop()
            declared_type = type_info_column['type']
        finally:
            self.cur.row_factory = orig_factory

        self.assertEqual(declared_type, 'TEXT_JSONFLATOBJ')

    def test_insert_wellformed_flat_obj(self):
        """Flat JSON objects should be inserted without errors."""
        parameters = [
            (None, 'name1', None, '{"a": 1, "b": 2}', 0),
            (None, 'name2', None, '{"a": 1.1, "b": 2.2}', 0),
            (None, 'name3', None, '{"a": "x", "b": "y"}', 0),
            (None, 'name4', None, '{"a": true, "b": false, "c": null}', 0),
        ]
        self.cur.executemany("INSERT INTO weight_info VALUES (?, ?, ?, ?, ?)", parameters)

    def test_insert_malformed_json(self):
        """Invalid JSON strings should fail with CHECK constraint."""
        regex = '^CHECK constraint failed'

        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            self.cur.execute(
                'INSERT INTO weight_info VALUES (?, ?, ?, ?, ?)',
                (None, 'name1', None, '{"a": 1, "b": 2', 0),  # Invalid JSON, no closing "}".
            )

        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            self.cur.execute(
                'INSERT INTO weight_info VALUES (?, ?, ?, ?, ?)',
                (None, 'name3', None, '{"a": "x", "b": y}', 0),  # Invalid JSON, "y" must be quoted.
            )

    def test_insert_wellformed_but_not_obj(self):
        """Non-object types should fail."""
        with self.assertRaises(sqlite3.IntegrityError):
            self.cur.execute(
                'INSERT INTO weight_info VALUES (?, ?, ?, ?, ?)',
                (None, 'name1', None, '[1, 2, 3]', 0),  # JSON is wellformed array.
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.cur.execute(
                'INSERT INTO weight_info VALUES (?, ?, ?, ?, ?)',
                (None, 'name3', None, '"xyz"', 0),  # JSON is wellformed text.
            )

    def test_insert_wellformed_obj_but_not_flat(self):
        """Flat JSON objects must not contain nested object or array types."""
        with self.assertRaises(sqlite3.IntegrityError):
            self.cur.execute(
                'INSERT INTO weight_info VALUES (?, ?, ?, ?, ?)',
                (None, 'name1', None, '{"a": 1, "b": {"c": 3}}', 0),
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.cur.execute(
                'INSERT INTO weight_info VALUES (?, ?, ?, ?, ?)',
                (None, 'name3', None, '{"a": "x", "b": ["y", "z"]}', 0),
            )

