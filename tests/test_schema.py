"""Tests for toron._schema module."""

import gc
import itertools
import json
import os
import sqlite3
import sys
import unittest
import weakref
from collections import namedtuple, OrderedDict, UserString
from stat import S_IRUSR, S_IWUSR
from .common import TempDirTestCase

from toron._typing import Generator
from toron._utils import ToronError
from toron._selectors import SimpleSelector
from toron._schema import (
    SQLITE_JSON1_ENABLED,
    BitList,
    BitFlags,
    _user_json_object_keep,
    _user_json_valid,
    _user_userproperties_valid,
    _user_attributes_valid,
    _user_selectors_valid,
    _schema_script,
    _sql_trigger_validate_attributes,
    _add_functions_and_triggers,
    _validate_permissions,
    _make_sqlite_uri_filepath,
    get_connection,
    normalize_identifier,
    sql_string_literal,
    savepoint,
    begin,
    _USERFUNC_NAME_REGISTRY,
    _USERFUNC_NAME_GENERATOR,
    _sql_function_exists,
    get_userfunc,
)


class TestBitList(unittest.TestCase):
    def test_init(self):
        list_of_values = [1, 1, 1, 1, 1, 1, 1, 1]
        bits = BitList(list_of_values)
        self.assertEqual(bits.data, list_of_values)

        list_of_values = [1, 0, 0, 0, 0, 0, 0, 0]
        bits = BitList(list_of_values)
        self.assertEqual(bits.data, list_of_values)

        list_of_values = [0, 0, 0, 0, 0, 0, 0, 1]
        bits = BitList(list_of_values)
        self.assertEqual(bits.data, list_of_values)

        list_of_values = [0, 0, 0, 0, 0, 0, 0, 0]
        bits = BitList(list_of_values)
        self.assertEqual(bits.data, list_of_values)

    def test_eq(self):
        self.assertTrue(BitList([1, 1]) == BitList([1, 1]))
        self.assertTrue(BitList([1, 1]) == BitList([1, 1, 0, 0]))
        self.assertTrue(BitList([]) == BitList())
        self.assertFalse(BitList([1, 1]) == BitList([0, 1]))
        self.assertFalse(BitList([1, 1]) == 1234)
        self.assertFalse(1234 == BitList([1, 1]))

    def test_normalization(self):
        list_of_values = ['', '', '', '', 'x', 'x', 'x', 'x']
        bits = BitList(list_of_values)
        msg = 'truthy and falsy values should normalize as integers'
        self.assertEqual(bits.data, [0, 0, 0, 0, 1, 1, 1, 1], msg=msg)

    def test_from_bytes(self):
        bits = BitList.from_bytes(b'\xff')
        self.assertEqual(bits.data, [1, 1, 1, 1, 1, 1, 1, 1])

        bits = BitList.from_bytes(b'\x01')
        self.assertEqual(bits.data, [0, 0, 0, 0, 0, 0, 0, 1])

        bits = BitList.from_bytes(b'\x80\x00')
        self.assertEqual(bits.data, [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

        bits = BitList.from_bytes(b'\x00\x01')
        self.assertEqual(bits.data, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])

        bits = BitList.from_bytes(b'')
        self.assertEqual(bits.data, [])

    def test_bytes_magic_method(self):
        bits = BitList([1, 1, 1, 1, 1, 1, 1, 1])
        self.assertEqual(bytes(bits), b'\xff')

        bits = BitList([1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0])
        msg = 'excess zeros are not included in bytes representation'
        self.assertEqual(bytes(bits), b'\xff', msg=msg)

        bits = BitList([0, 0, 0, 0, 0, 0, 0, 1])
        self.assertEqual(bytes(bits), b'\x01')

        bits = BitList([0, 0, 0, 0, 0, 0, 0, 0, 1])
        self.assertEqual(bytes(bits), b'\x00\x80')

        bits = BitList([1])
        self.assertEqual(bytes(bits), b'\x80')

        bits = BitList([])
        self.assertEqual(bytes(bits), b'')

        bits = BitList([0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        self.assertEqual(bytes(bits), b'')

    def test_repr(self):
        list_of_values = [1, 1, 1, 0, 0, 0, 0, 0]
        bits = BitList(list_of_values)
        self.assertEqual(repr(bits), 'BitList([1, 1, 1, 0, 0, 0, 0, 0])')

    def test_addition_operations(self):
        list_a = [1, 1]
        list_b = [1, 1, 0, 0, 0]
        expected = [1, 1, 1, 1, 0, 0, 0]

        # Test __add__() behavior.
        bits = BitList(list_a) + list_b
        self.assertIsInstance(bits, BitList)
        self.assertEqual(bits.data, expected)

        # Test __radd__() behavior.
        bits = list_a + BitList(list_b)
        self.assertIsInstance(bits, BitList)
        self.assertEqual(bits.data, expected)

        # This also triggers BitList.__radd__().
        bits = list_a
        bits += BitList(list_b)
        self.assertIsInstance(bits, BitList)
        self.assertEqual(bits.data, expected)

        # Test __iadd__() behavior (in-place addition).
        bits = BitList(list_a)
        bits += list_b
        self.assertIsInstance(bits, BitList)
        self.assertEqual(bits.data, expected)

        # Verify that sample lists have not been changed (meta-test).
        self.assertEqual(list_a, [1, 1])
        self.assertEqual(list_b, [1, 1, 0, 0, 0])


class TestBitFlags(unittest.TestCase):
    def test_init(self):
        all_values = [
            (1, 1, 1, 1, 1, 1, 1, 1),
            (1, 0, 0, 0, 0, 0, 0, 0),
            (0, 0, 0, 0, 0, 0, 0, 1),
            (0, 0, 0, 0, 0, 0, 0, 0),
        ]
        for values in all_values:
            with self.subTest(values=values):
                bits = BitFlags(*values)
                self.assertEqual(bits.data, values)

    def test_normalize_values(self):
        values = ('x', 'x', '', 'x', '', '', '', '')
        bits = BitFlags(*values)
        msg = 'values should be normalized as 0s and 1s'
        self.assertEqual(bits.data, (1, 1, 0, 1, 0, 0, 0, 0), msg=msg)

    def test_normalize_length(self):
        self.assertEqual(
            BitFlags(1, 1, 0, 1).data,
            (1, 1, 0, 1, 0, 0, 0, 0),
            msg='bits should be padded to multiple of eight',
        )

        self.assertEqual(
            BitFlags(0, 0, 0, 0, 0, 0, 0, 0, 1).data,
            (0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0),
            msg='bits should be padded to multiple of eight',
        )

        self.assertEqual(
            BitFlags(1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0).data,
            (1, 1, 0, 1, 0, 0, 0, 0),
            msg='excess trailing 0s should be removed',
        )

    def test_repr(self):
        bits = BitFlags(1, 1, 0, 1, 0, 0, 0, 0)
        self.assertEqual(repr(bits), 'BitFlags(1, 1, 0, 1, 0, 0, 0, 0)')

    def test_eq(self):
        equal_values = [
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), BitFlags(1, 1, 0, 1, 0, 0, 0, 0)],
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), (1, 1, 0, 1, 0, 0, 0, 0)],
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), [1, 1, 0, 1, 0, 0, 0, 0]],
            [(1, 1, 0, 1, 0, 0, 0, 0), BitFlags(1, 1, 0, 1, 0, 0, 0, 0)],
            [[1, 1, 0, 1, 0, 0, 0, 0], BitFlags(1, 1, 0, 1, 0, 0, 0, 0)],
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), (1, 1, 0, 1)],
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), [1, 1, 0, 1]],
            [(1, 1, 0, 1), BitFlags(1, 1, 0, 1, 0, 0, 0, 0)],
            [[1, 1, 0, 1], BitFlags(1, 1, 0, 1, 0, 0, 0, 0)],
        ]
        for a, b in equal_values:
            with self.subTest(a=a, b=b):
                self.assertTrue(a == b)

        not_equal_values = [
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), BitFlags(1, 1, 1, 1, 1, 1, 1, 1)],
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), (1, 1, 1, 1, 1, 1, 1, 1)],
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), ('x', 'x', '', 'x', '', '', '', '')],
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), [1, 1, 1, 1, 1, 1, 1, 1]],
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), 1234],
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), 'blerg'],
        ]
        for a, b in not_equal_values:
            with self.subTest(a=a, b=b):
                self.assertFalse(a == b)

    def test_immutable(self):
        bits = BitFlags(1, 1, 0, 1, 0, 0, 0, 0)

        regex = "'BitFlags' object does not support assignment"
        with self.assertRaisesRegex(TypeError, regex):
            bits._data = (1, 1, 1, 1, 1, 1, 1, 1)

        regex = "'BitFlags' object does not support deletion"
        with self.assertRaisesRegex(TypeError, regex):
            del bits._data

    def test_hashable(self):
        bits = BitFlags(1, 1, 0, 1, 0, 0, 0, 0)
        self.assertEqual(hash(bits), hash(bits))


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


class TestSqlStringLiteral(unittest.TestCase):
    def test_normalization(self):
        """Check SQL string literal formatting."""
        # The result values given below were generated with SQLite's
        # built-in quote() function. For example:
        #
        #     >>> import sqlite3
        #     >>> con = sqlite3.connect(":memory:")
        #     >>> con.execute("SELECT quote(?)", ("abc",)).fetchone()[0]
        #     "'abc'"
        values = [
            ("abc",       "'abc'"),
            ("a b c",     "'a b c'"),       # whitepsace
            ("  abc",     "'  abc'"),       # leading/trailing whitespace
            ('"abc"',     '\'"abc"\''),     # double quotes
            (' "abc" ',   '\' "abc" \''),   # double quotes and whitespace
            ("O'Connell", "'O''Connell'"),  # single quotes
            ("abc\nabc",  "'abc\nabc'"),    # linebreaks
            ("abc\\abc",  "'abc\\abc'"),    # escaped slash
            ("abc%%%abc", "'abc%%%abc'"),   # percent signs
            ("'a b'c'",   "'''a b''c'''"),  # dubious quoting escaped as-is
        ]
        for input_value, result in values:
            with self.subTest(input_value=input_value, expected_output=result):
                self.assertEqual(sql_string_literal(input_value), result)

    def test_non_idempotence(self):
        """This function should NOT be idempotent--applying the function
        multiple times should return successively different results.
        """
        value = "abc"
        applied_once = sql_string_literal(value)
        applied_twice = sql_string_literal(applied_once)
        applied_thrice = sql_string_literal(applied_twice)

        self.assertEqual(applied_once, "'abc'")
        self.assertEqual(applied_twice, "'''abc'''")
        self.assertEqual(applied_thrice, "'''''''abc'''''''")

    def test_surrogate_codes(self):
        """Should only allow clean UTF-8 (no surrogate codes)."""
        column_bytes = b'tama\xf1o'  # "tamaño" is Spanish for "size"
        string_with_surrogate = column_bytes.decode('utf-8', 'surrogateescape')

        with self.assertRaises(UnicodeEncodeError):
            sql_string_literal(string_with_surrogate)

    def test_nul_byte(self):
        """Strings with NUL bytes should raise an error."""
        contains_nul = 'zip\x00 code'

        with self.assertRaises(UnicodeEncodeError):
            sql_string_literal(contains_nul)


class TestUserJsonObjectKeep(unittest.TestCase):
    def setUp(self):
        # Define JSON object (keys not in alpha order).
        self.json_obj = '{"b": "two", "c": "three", "a": "one"}'

    def test_matching_keys(self):
        """Should keep given keys and return a new obj in alpha order."""
        actual = _user_json_object_keep(self.json_obj, 'b', 'a')  # <- Args not in alpha order.
        expected = '{"a": "one", "b": "two"}'  # <- Keys in alpha order.
        self.assertEqual(actual, expected)

    def test_matching_and_nonmatching_keys(self):
        """As long as at least one key matches, a result is returned."""
        actual = _user_json_object_keep(self.json_obj, 'x', 'c', 'y')
        expected = '{"c": "three"}'  # <- Only "c", no "x" or "y" in json_obj.
        self.assertEqual(actual, expected)

    def test_no_matching_keys(self):
        """When keys are given but none of them match keys in the
        json_obj, then None should be returned.
        """
        actual = _user_json_object_keep(self.json_obj, 'x', 'y', 'z')
        self.assertIsNone(actual)

    def test_no_keys_given(self):
        """When no keys are given, a normalized verison of the complete
        JSON object should be returned.
        """
        actual = _user_json_object_keep(self.json_obj)  # <- No keys given!
        expected = '{"a": "one", "b": "two", "c": "three"}'  # <- Full obj with keys in alpha order.
        self.assertEqual(actual, expected)

    def test_unsupported_json_type(self):
        bad_json_type = '"a string value"'  # <- JSON should be object, not string.
        with self.assertRaises(ValueError):
            _user_json_object_keep(bad_json_type, 'a', 'b')

    def test_bad_keys(self):
        with self.assertRaises(TypeError):
            _user_json_object_keep(self.json_obj, [1, 2], 'c')  # <- Keys should be strings.

    def test_malformed_json(self):
        """JSON decode errors should be raised as normal."""
        malformed_json = '{"a": "one}'  # <- No closing quote.
        with self.assertRaises(json.JSONDecodeError):
            _user_json_object_keep(malformed_json, 'a', 'b')


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
        self.con = get_connection(':memory:', None)
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
        self.con = get_connection(':memory:', None)
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
            '00000000-0000-0000-0000-000000000000',  # other_unique_id
            f'other{index}.toron',     # other_filename_hint
            'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',  # other_index_hash
            0,                         # is_complete
            None,                      # is_default
        )

    def test_valid_values(self):
        for index, value in enumerate(self.valid_values):
            parameters = self.make_parameters(index, value)
            with self.subTest(value=value):
                self.cur.execute("INSERT INTO edge VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", parameters)

    def test_invalid_values(self):
        """Check all `not_an_object` and `malformed_json` values."""
        invalid_values = self.not_an_object + self.malformed_json  # Make a single list.

        regex = 'must be wellformed JSON object'

        for index, value in enumerate(invalid_values):
            parameters = self.make_parameters(index, value)
            with self.subTest(value=value):
                with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
                    self.cur.execute("INSERT INTO edge VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", parameters)

    def test_none(self):
        """Currently, edge.user_properties allows NULL values (refer to
        the associated CREATE TABLE statement).
        """
        parameters = self.make_parameters('x', None)
        self.cur.execute("INSERT INTO edge VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", parameters)


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
        self.con = get_connection(':memory:', None)
        self.cur = self.con.cursor()
        self.cur.execute("INSERT INTO location (_location_id) VALUES (1)")
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

        self.sql_insert = 'INSERT INTO quantity (_location_id, attributes, quantity_value) VALUES (?, ?, ?)'

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
        '{"a": "one", "b": "two"}',  # <- object
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
        self.con = get_connection(':memory:', None)
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
        custom_type_columns = []
        for table in self.get_table_names():
            column_names = self.get_custom_type_columns(table)
            for column in column_names:
                custom_type_columns.append((table, column))

        # Remove "relation.mapping_level" (a BLOB_BITLIST type) because
        # it doesn't need any associated SQL triggers.
        custom_type_columns.remove(('relation', 'mapping_level'))

        expected_triggers = []
        for table, column in custom_type_columns:
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


class TestValidatePermissions(TempDirTestCase):
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

    @classmethod
    def tearDownClass(cls):
        # Add write-permissions back to `ro_path` file so TemporaryDirectory()
        # can properly clean-up after itself on Windows when using some older
        # versions of Python.
        #
        # See related issue:
        #     https://bugs.python.org/issue29982
        os.chmod(cls.ro_path, S_IRUSR|S_IWUSR)
        super().tearDownClass()

    def test_readonly_required(self):
        try:
            _validate_permissions(self.ro_path, required_permissions='readonly')
        except Exception:
            self.fail("read-only file should work when requiring 'readonly'")

        with self.assertRaises(PermissionError):
            _validate_permissions(self.rw_path, required_permissions='readonly')

        with self.assertRaises(ToronError):
            _validate_permissions(self.new_path, required_permissions='readonly')

    def test_readwrite_required(self):
        try:
            _validate_permissions(self.rw_path, required_permissions='readwrite')
        except Exception:
            self.fail("read-write file should work when requiring 'readwrite'")

        with self.assertRaises(PermissionError):
            _validate_permissions(self.ro_path, required_permissions='readwrite')

        try:
            _validate_permissions(self.new_path, required_permissions='readwrite')
        except Exception:
            self.fail("nonexistent file should work when requiring 'readwrite'")

    def test_none_required(self):
        try:
            _validate_permissions(self.ro_path, required_permissions=None)
            _validate_permissions(self.rw_path, required_permissions=None)
            _validate_permissions(self.new_path, required_permissions=None)
        except Exception:
            self.fail("requiring `None` permissions should work in all cases")


class TestMakeSqliteUriFilepath(unittest.TestCase):
    def test_cases_without_mode(self):
        self.assertEqual(
            _make_sqlite_uri_filepath('mynode.toron', mode=None),
            'file:mynode.toron',
        )
        self.assertEqual(
            _make_sqlite_uri_filepath('my?node.toron', mode=None),
            'file:my%3Fnode.toron',
        )
        self.assertEqual(
            _make_sqlite_uri_filepath('path///to//mynode.toron', mode=None),
            'file:path/to/mynode.toron',
        )

    def test_cases_with_mode(self):
        self.assertEqual(
            _make_sqlite_uri_filepath('mynode.toron', mode='ro'),
            'file:mynode.toron?mode=ro',
        )
        self.assertEqual(
            _make_sqlite_uri_filepath('my?node.toron', mode='rw'),
            'file:my%3Fnode.toron?mode=rw',
        )
        self.assertEqual(
            _make_sqlite_uri_filepath('path///to//mynode.toron', mode='rwc'),
            'file:path/to/mynode.toron?mode=rwc',
        )

    def test_windows_specifics(self):
        if os.name != 'nt':
            return

        path = r'path\to\mynode.toron'
        expected = 'file:path/to/mynode.toron'
        self.assertEqual(_make_sqlite_uri_filepath(path, mode=None), expected)

        path = r'C:\path\to\my node.toron'
        expected = 'file:/C:/path/to/my%20node.toron'
        self.assertEqual(_make_sqlite_uri_filepath(path, mode=None), expected)

        path = r'C:\path\to\myno:de.toron'  # <- Errant ":".
        expected = 'file:/C:/path/to/myno%3Ade.toron'
        self.assertEqual(_make_sqlite_uri_filepath(path, mode=None), expected)

        path = r'C:mynode.toron'  # <- Relative path with drive letter.
        expected = f'file:/{os.getcwd()}/mynode.toron'.replace("\\", "/")
        self.assertEqual(_make_sqlite_uri_filepath(path, mode=None), expected)


class TestConnectDb(TempDirTestCase):
    def setUp(self):
        self.addCleanup(self.cleanup_temp_files)

    def test_new_file(self):
        """If a node file doesn't exist it should be created."""
        path = 'mynode.node'
        get_connection(path, required_permissions=None).close()  # Creates Toron db at given path.

        con = sqlite3.connect(path)
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur}
        tables.discard('sqlite_sequence')  # <- Table added by SQLite.

        expected = {
            'edge',
            'location',
            'node_index',
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
            get_connection(path, required_permissions=None)

    def test_nondatabase_file(self):
        """Non-database files should fail."""
        # Build a non-database file.
        path = 'not_a_database.txt'
        with open(path, 'w') as f:
            f.write('Hello World\n')

        with self.assertRaises(ToronError):
            get_connection(path, required_permissions=None)

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
            get_connection(path, required_permissions=None)

    def test_unsupported_schema_version(self):
        """Unsupported schema version should fail."""
        path = 'mynode.toron'

        con = get_connection(path, required_permissions=None)
        con.execute("INSERT OR REPLACE INTO property VALUES ('toron_schema_version', '999')")
        con.commit()
        con.close()

        regex = 'Unsupported Toron node format: schema version 999'
        with self.assertRaisesRegex(ToronError, regex):
            get_connection(path, required_permissions=None)

    def test_readwrite_permissions(self):
        path = 'mynode.toron'

        # Connect to nonexistent file (creates file).
        self.assertFalse(os.path.isfile(path))
        get_connection(path, required_permissions='readwrite').close()

        # Connect to existing file.
        self.assertTrue(os.path.isfile(path))
        get_connection(path, required_permissions='readwrite').close()  # Connects to existing.

        # Connect to existing file with read-only permissions (should fail).
        os.chmod(path, S_IRUSR)  # Set to read-only.
        self.addCleanup(lambda: os.chmod(path, S_IRUSR|S_IWUSR))  # Revert to read-write after test.
        with self.assertRaises(PermissionError):
            get_connection(path, required_permissions='readwrite')

    def test_readonly_permissions(self):
        # Open nonexistent file with "readonly" permissions (fails).
        regex = ("file 'path1.toron' does not exist, must require "
                 "'readwrite' or None permissions, got 'readonly'")
        with self.assertRaisesRegex(ToronError, regex):
            get_connection('path1.toron', required_permissions='readonly')

        path = 'path2.toron'
        get_connection(path, required_permissions='readwrite').close()  # Create node.

        # Open existing, but not-readonly file with "readonly" permissions.
        regex = f"required 'readonly' permissions but {path!r} is not read-only"
        with self.assertRaisesRegex(PermissionError, regex):
            get_connection(path, required_permissions='readonly')

        os.chmod(path, S_IRUSR)  # Set file permissions to read-only.
        self.addCleanup(lambda: os.chmod(path, S_IRUSR|S_IWUSR))

        # Open readonly connection to file with read-only permissions.
        con = get_connection(path, required_permissions='readonly')
        self.addCleanup(con.close)
        regex = 'attempt to write a readonly database'
        with self.assertRaisesRegex(sqlite3.OperationalError, regex):
            con.execute('INSERT INTO property VALUES (?, ?)', ('key1', '"value1"'))

    def test_none_permissions(self):
        path = 'mynode.toron'

        self.assertFalse(os.path.isfile(path))
        get_connection(path, required_permissions=None).close()  # Creates node.

        self.assertTrue(os.path.isfile(path))
        get_connection(path, required_permissions=None).close()  # Connects to existing.

        os.chmod(path, S_IRUSR)  # Set file permissions to read-only.
        self.addCleanup(lambda: os.chmod(path, S_IRUSR|S_IWUSR))
        get_connection(path, required_permissions=None).close()  # Connects to read-only.

    def test_invalid_permissions(self):
        path = 'mynode.toron'
        regex = (f"file {path!r} does not exist, must require 'readwrite' "
                 f"or None permissions, got 'badpermissions'")
        with self.assertRaisesRegex(ToronError, regex):
            get_connection(path, required_permissions='badpermissions')

        get_connection(path, required_permissions='readwrite').close()  # Create node.

        regex = (f"`required_permissions` must be 'readonly', 'readwrite', "
                 f"or None; got 'badpermissions'")
        with self.assertRaisesRegex(ToronError, regex):
            get_connection(path, required_permissions='badpermissions')


class TestBitListConversionAndAdaptation(unittest.TestCase):
    def setUp(self):
        self.con = get_connection(':memory:', None)
        self.cur = self.con.cursor()
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)
        self.cur.execute(
            'INSERT INTO edge (edge_id, name, other_unique_id, other_filename_hint) VALUES (?, ?, ?, ?)',
            (1, 'myedge', '00000000-0000-0000-0000-000000000000', 'mynode.toron')
        )
        self.other_index_id_generator = itertools.count(42)  # Start at 42.

    def insert_mapping_level(self, mapping_level):
        """Helper function to insert a relation and mapping_level."""
        other_index_id = next(self.other_index_id_generator)
        self.cur.execute(
            """
                INSERT INTO relation
                    (edge_id, other_index_id, index_id, relation_value, mapping_level)
                    VALUES (?, ?, ?, ?, ?)
            """,
            (1, other_index_id, None, 1.0, mapping_level)
        )

    def get_mapping_levels(self):
        """Helper function to get all mapping_level values."""
        self.cur.execute('SELECT mapping_level FROM relation WHERE edge_id=1')
        return [row[0] for row in self.cur.fetchall()]

    def test_basic_roundtrip(self):
        bit_list = BitList([0, 1, 0, 0, 0, 1, 0, 1])
        self.insert_mapping_level(bit_list)  # <- Store BitList as bytes.
        result = self.get_mapping_levels()  # <- Retrieve BitList from bytes.

        self.assertIsInstance(result[0], BitList)
        self.assertEqual(result, [bit_list])

    def test_raw_bytes(self):
        raw_bytes = b'\x45'
        self.insert_mapping_level(raw_bytes)  # <- Store raw bytes (as bytes).
        result = self.get_mapping_levels()  # <- Retrieve BitList from bytes.

        self.assertIsInstance(result[0], BitList)
        self.assertEqual(result, [BitList.from_bytes(raw_bytes)])


class TestJsonConversion(unittest.TestCase):
    """Registered converters should select JSON strings as objects."""
    def setUp(self):
        self.con = get_connection(':memory:', None)
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
        self.assertEqual(self.cur.fetchall(), [([SimpleSelector('bar', '=', 'baz')],)])


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
                    raise Exception('dummy error')
            except Exception as err:
                if str(err) != 'dummy error':
                    raise
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


class TestBegin(unittest.TestCase):
    def setUp(self):
        con = sqlite3.connect(':memory:')
        con.isolation_level = None
        self.cursor = con.cursor()
        self.addCleanup(con.close)
        self.addCleanup(self.cursor.close)

    def test_transaction_status(self):
        con = self.cursor.connection

        self.assertFalse(con.in_transaction)

        with begin(self.cursor):
            self.assertTrue(con.in_transaction)

        self.assertFalse(con.in_transaction)

    def test_release(self):
        cur = self.cursor

        with begin(cur):
            cur.execute('CREATE TEMPORARY TABLE test_table ("A")')
            cur.execute("INSERT INTO test_table VALUES ('one')")
            cur.execute("INSERT INTO test_table VALUES ('two')")
            cur.execute("INSERT INTO test_table VALUES ('three')")

        cur.execute('SELECT * FROM test_table')
        self.assertEqual(cur.fetchall(), [('one',), ('two',), ('three',)])

    def test_no_transaction_nesting(self):
        """Transactions initialized with BEGIN should fail if nested."""
        cur = self.cursor

        with self.assertRaises(sqlite3.OperationalError):
            with begin(cur):
                with begin(cur):  # <- Nested inside BEGIN!
                    pass

        with self.assertRaises(sqlite3.OperationalError):
            with savepoint(cur):
                with begin(cur):  # <- Nested inside SAVEPOINT!
                    pass

        with self.assertRaises(sqlite3.OperationalError):
            with begin(cur):
                with savepoint(cur):  # <- Cannot contain SAVEPOINT either!
                    pass

    def test_rollback(self):
        cur = self.cursor

        with begin(cur):  # <- Released.
            cur.execute('CREATE TEMPORARY TABLE test_table ("A")')

        try:
            with begin(cur):  # <- Rolled back!
                cur.execute("INSERT INTO test_table VALUES ('one')")
                cur.execute("INSERT INTO test_table VALUES ('two')")
                cur.execute("INSERT INTO missing_table VALUES ('three')")  # <- Bad table.
        except sqlite3.OperationalError:
            pass

        cur.execute('SELECT * FROM test_table')
        self.assertEqual(cur.fetchall(), [], 'Table should exist but contain no records.')

    def test_bad_isolation_level(self):
        connection = sqlite3.connect(':memory:')
        connection.isolation_level = 'DEFERRED'  # <- Incompatible isolation level.
        cur = connection.cursor()

        regex = "isolation_level must be None, got: 'DEFERRED'"
        with self.assertRaisesRegex(sqlite3.OperationalError, regex):
            with begin(cur):  # <- Should raise error.
                pass


class TestGetUserfunc(unittest.TestCase):
    def setUp(self):
        con = sqlite3.connect(':memory:')
        con.isolation_level = None
        self.cursor = con.cursor()
        self.addCleanup(con.close)
        self.addCleanup(self.cursor.close)

    def test_sql_function_exists(self):
        if sqlite3.sqlite_version_info < (3, 30, 0):
            # The _sql_function_exists() function is not supported in
            # versions of SQLite before 3.30.0.
            return

        newfunc_name = 'new_userfunc'
        self.assertFalse(
            _sql_function_exists(self.cursor, newfunc_name),
            msg=f'function {newfunc_name!r} should not exist yet',
        )

        newfunc = lambda x: x * 2
        self.cursor.connection.create_function(newfunc_name, 1, newfunc)

        self.assertTrue(
            _sql_function_exists(self.cursor, newfunc_name),
            msg=f'function {newfunc_name!r} should exist now',
        )

    def test_userfunc_name_generator(self):
        """Should generate values like 'userfunc_0', 'userfunc_1', etc."""
        self.assertIsInstance(_USERFUNC_NAME_GENERATOR, Generator)

        sample_name = next(_USERFUNC_NAME_GENERATOR)
        before, sep, after = sample_name.partition('_')

        self.assertEqual(before, 'userfunc')
        self.assertTrue(after.isdecimal())

    def test_userfunc_name_registry(self):
        names_before = list(_USERFUNC_NAME_REGISTRY.values())

        newfunc = lambda x: x * 2
        name = get_userfunc(self.cursor, newfunc)

        names_after = list(_USERFUNC_NAME_REGISTRY.values())

        self.assertNotIn(name, names_before)
        self.assertIn(name, names_after)

    def test_userfunc_in_sql_statement(self):
        """Check that user-defined function is usable from SQLite."""
        newfunc = lambda x: x * 2
        userfunc = get_userfunc(self.cursor, lambda x: x * 2)
        self.cursor.execute(f'SELECT {userfunc}(3)')  # <- Used from SQL.
        self.assertEqual(self.cursor.fetchall(), [(6,)])

    def test_persistent_function_names(self):
        """Functions should get persistent SQL function names. When a
        function is reused during the same interpreter session, it
        should get its previously established SQL function name--not
        a new name. This should hold true even when using different
        cursors backed by different connections.
        """
        first_con = sqlite3.connect(':memory:')
        self.addCleanup(first_con.close)

        second_con = sqlite3.connect(':memory:')
        self.addCleanup(second_con.close)

        newfunc = lambda x: x * 2

        name1 = get_userfunc(first_con.cursor(), newfunc)
        name2 = get_userfunc(first_con.cursor(), newfunc)
        self.assertEqual(name1, name2, msg='should get same name')

        name1 = get_userfunc(first_con.cursor(), newfunc)
        name2 = get_userfunc(second_con.cursor(), newfunc)
        self.assertEqual(name1, name2, msg='should get same name even on different connections')

