"""Tests for toron/_utils.py module."""

import csv
import io
import unittest

import pandas

from toron._utils import ToronError
from toron._utils import (
    make_readerlike,
    make_dictreaderlike,
    wide_to_narrow,
)


class TestMakeReaderLike(unittest.TestCase):
    def test_csv_reader(self):
        """CSV reader() objects should be returned unchanged."""
        reader = csv.reader(io.StringIO(
            'col1,col2\n'
            '1,a\n'
            '2,b\n'
            '3,c\n'
        ))
        result = make_readerlike(reader)
        self.assertIs(result, reader, msg='should be original object')

        expected = [
            ['col1', 'col2'],
            ['1', 'a'],
            ['2', 'b'],
            ['3', 'c'],
        ]
        self.assertEqual(list(result), expected, msg='should return all values')

    def test_csv_dictreader(self):
        dictreader = csv.DictReader(io.StringIO(
            'col1,col2\n'
            '1,a\n'
            '2,b\n'
            '3,c\n'
        ))
        result = make_readerlike(dictreader)

        expected = [
            ['col1', 'col2'],
            ['1', 'a'],
            ['2', 'b'],
            ['3', 'c'],
        ]
        self.assertEqual(list(result), expected, msg='should return all values')

    def test_sequence_unchanged(self):
        data = [
            ['a', 'b', 'c'],
            [1,   2,   3],
            [4,   5,   6],
        ]
        result = make_readerlike(data)
        self.assertEqual(list(result), data)

    def test_dict_rows(self):
        data = [
            {'a': 1, 'b': 2, 'c': 3},
            {'a': 4, 'b': 5, 'c': 6},
        ]
        result = make_readerlike(data)

        expected = [
            ['a', 'b', 'c'],
            [1,   2,   3],
            [4,   5,   6],
        ]
        self.assertEqual(list(result), expected)

    def test_empty_dataset(self):
        data = iter([])
        result = make_readerlike(data)
        self.assertEqual(list(result), [])

    def test_bad_object(self):
        data = 123
        regex = "cannot normalize object as tabular data, got 'int': 123"
        with self.assertRaisesRegex(TypeError, regex):
            result = make_readerlike(data)

    def test_bad_types(self):
        data = [
            {'a', 'b', 'c'},
            {1,   2,   3},
            {4,   5,   6},
        ]
        regex = "rows must be sequences, got 'set': {.+}"
        with self.assertRaisesRegex(TypeError, regex):
            result = make_readerlike(data)


class TestMakeReaderLikePandas(unittest.TestCase):
    def setUp(self):
        self.df = pandas.DataFrame({
            'col1': (1, 2, 3),
            'col2': ('a', 'b', 'c'),
        })

    def test_rangeindex_unnamed(self):
        normalized = make_readerlike(self.df)
        expected = [
            ['col1', 'col2'],
            [1, 'a'],
            [2, 'b'],
            [3, 'c'],
        ]
        self.assertEqual(list(normalized), expected)

    def test_rangeindex_named(self):
        self.df.index.name = 'col0'

        normalized = make_readerlike(self.df)
        expected = [
            ['col0', 'col1', 'col2'],
            [0, 1, 'a'],
            [1, 2, 'b'],
            [2, 3, 'c'],
        ]
        self.assertEqual(list(normalized), expected)

    def test_index_unnamed(self):
        self.df.index = pandas.Index(['x', 'y', 'z'])

        normalized = make_readerlike(self.df)
        expected = [
            ['col1', 'col2'],
            [1, 'a'],
            [2, 'b'],
            [3, 'c'],
        ]
        self.assertEqual(list(normalized), expected)

    def test_index_named(self):
        self.df.index = pandas.Index(['x', 'y', 'z'], name='col0')

        normalized = make_readerlike(self.df)
        expected = [
            ['col0', 'col1', 'col2'],
            ['x', 1, 'a'],
            ['y', 2, 'b'],
            ['z', 3, 'c'],
        ]
        self.assertEqual(list(normalized), expected)

    def test_multiindex_unnamed(self):
        index_values = [('x', 'one'), ('x', 'two'), ('y', 'three')]
        index = pandas.MultiIndex.from_tuples(index_values)
        self.df.index = index

        regex = r"MultiIndex names must not be None, got \[None, None\]"
        with self.assertRaisesRegex(ValueError, regex):
            normalized = make_readerlike(self.df)

    def test_multiindex_named(self):
        index_values = [('x', 'one'), ('x', 'two'), ('y', 'three')]
        index = pandas.MultiIndex.from_tuples(index_values, names=['A', 'B'])
        self.df.index = index

        normalized = make_readerlike(self.df)
        expected = [
            ['A', 'B', 'col1', 'col2'],
            ['x', 'one', 1, 'a'],
            ['x', 'two', 2, 'b'],
            ['y', 'three', 3, 'c'],
        ]
        self.assertEqual(list(normalized), expected)


class TestMakeDictReaderLike(unittest.TestCase):
    def test_csv_dictreader(self):
        dictreader = csv.DictReader(io.StringIO(
            'col1,col2\n'
            '1,a\n'
            '2,b\n'
            '3,c\n'
        ))
        result = make_dictreaderlike(dictreader)
        self.assertIs(result, dictreader, msg='should be original object')

        expected = [
            {'col1': '1', 'col2': 'a'},
            {'col1': '2', 'col2': 'b'},
            {'col1': '3', 'col2': 'c'},
        ]
        self.assertEqual(list(result), expected, msg='should return all values')

    def test_csv_reader(self):
        reader = csv.reader(io.StringIO(
            'col1,col2\n'
            '1,a\n'
            '2,b\n'
            '3,c\n'
        ))
        result = make_dictreaderlike(reader)

        expected = [
            {'col1': '1', 'col2': 'a'},
            {'col1': '2', 'col2': 'b'},
            {'col1': '3', 'col2': 'c'},
        ]
        self.assertEqual(list(result), expected)

    def test_dictrows_unchanged(self):
        data = [
            {'a': '1', 'b': '2', 'c': '3'},
            {'a': '4', 'b': '5', 'c': '6'},
        ]
        result = make_dictreaderlike(data)
        self.assertEqual(list(result), data)

    def test_sequence_rows(self):
        data = [
            ['a', 'b', 'c'],
            [1,   2,   3],
            [4,   5,   6],
        ]
        result = make_dictreaderlike(data)

        expected = [
            {'a': 1, 'b': 2, 'c': 3},
            {'a': 4, 'b': 5, 'c': 6},
        ]
        self.assertEqual(list(result), expected)

    def test_pandas_dataframe(self):
        df = pandas.DataFrame({
            'col1': (1, 2, 3),
            'col2': ('a', 'b', 'c'),
        })
        result = make_dictreaderlike(df)

        expected = [
            {'col1': 1, 'col2': 'a'},
            {'col1': 2, 'col2': 'b'},
            {'col1': 3, 'col2': 'c'},
        ]
        self.assertEqual(list(result), expected)

    def test_empty_dataset(self):
        data = iter([])
        result = make_dictreaderlike(data)
        self.assertEqual(list(result), [])

    def test_bad_object(self):
        data = 123
        regex = "cannot make iterator of dictrows, got 'int': 123"
        with self.assertRaisesRegex(TypeError, regex):
            result = make_dictreaderlike(data)

    def test_bad_types(self):
        data = [
            {'a', 'b', 'c'},
            {1,   2,   3},
            {4,   5,   6},
        ]
        regex = "rows must be sequences, got 'set': {.+}"
        with self.assertRaisesRegex(TypeError, regex):
            result = make_dictreaderlike(data)


class TestWideToNarrow(unittest.TestCase):
    maxDiff = None

    def test_multiple_value_vars(self):
        data = [
            ('state', 'county',   'TOT_MALE', 'TOT_FEMALE'),
            ('OH',    'BUTLER',   180140,     187990),
            ('OH',    'FRANKLIN', 566499,     596915),
        ]
        result = wide_to_narrow(data, ['TOT_MALE', 'TOT_FEMALE'])

        expected = [
            {'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_MALE',   'value': 180140},
            {'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_FEMALE', 'value': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_MALE',   'value': 566499},
            {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_FEMALE', 'value': 596915},
        ]
        self.assertEqual(list(result), expected)

    def test_single_value_var(self):
        data = [
            ('state', 'county',   'TOT_ALL'),
            ('OH',    'BUTLER',   368130),
            ('OH',    'FRANKLIN', 1163414),
        ]
        result = wide_to_narrow(data, ['TOT_ALL'])

        expected = [
            {'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_ALL', 'value': 368130},
            {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_ALL', 'value': 1163414},
        ]
        self.assertEqual(list(result), expected)

    def test_explicit_var_name(self):
        data = [
            ('state', 'county',   'TOT_MALE', 'TOT_FEMALE'),
            ('OH',    'BUTLER',   180140,     187990),
            ('OH',    'FRANKLIN', 566499,     596915),
        ]
        result = wide_to_narrow(data, ['TOT_MALE', 'TOT_FEMALE'], var_name='census')

        # Uses "census" as attr key.
        expected = [
            {'state': 'OH', 'county': 'BUTLER',   'census': 'TOT_MALE',   'value': 180140},
            {'state': 'OH', 'county': 'BUTLER',   'census': 'TOT_FEMALE', 'value': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'census': 'TOT_MALE',   'value': 566499},
            {'state': 'OH', 'county': 'FRANKLIN', 'census': 'TOT_FEMALE', 'value': 596915},
        ]
        self.assertEqual(list(result), expected)

    def test_explicit_value_name(self):
        data = [
            ('state', 'county',   'TOT_MALE', 'TOT_FEMALE'),
            ('OH',    'BUTLER',   180140,     187990),
            ('OH',    'FRANKLIN', 566499,     596915),
        ]
        result = wide_to_narrow(data, ['TOT_MALE', 'TOT_FEMALE'], value_name='count')

        # Uses "census" as attr key.
        expected = [
            {'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_MALE',   'count': 180140},
            {'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_FEMALE', 'count': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_MALE',   'count': 566499},
            {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_FEMALE', 'count': 596915},
        ]
        self.assertEqual(list(result), expected)

    def test_dict_rows(self):
        data = [
            {'state': 'OH', 'county': 'BUTLER',   'TOT_MALE': 180140, 'TOT_FEMALE': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'TOT_MALE': 566499, 'TOT_FEMALE': 596915},
        ]
        result = wide_to_narrow(data, ['TOT_MALE', 'TOT_FEMALE'])

        expected = [
            {'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_MALE',   'value': 180140},
            {'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_FEMALE', 'value': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_MALE',   'value': 566499},
            {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_FEMALE', 'value': 596915},
        ]
        self.assertEqual(list(result), expected)

    def test_non_mapping_non_sequence(self):
        """Given *data* must contain dict-rows or sequence-rows."""
        data = [
            {'state', 'county',   'TOT_MALE', 'TOT_FEMALE'},
            {'OH',    'BUTLER',   180140,     187990},
            {'OH',    'FRANKLIN', 566499,     596915},
        ]
        with self.assertRaises(TypeError):
            generator = wide_to_narrow(data, ['TOT_MALE', 'TOT_FEMALE'])
            list(generator)  # <- Must consume generator (it's not primed).

    def test_empty_values(self):
        data = [
            ('state', 'county',   'TOT_MALE', 'TOT_FEMALE', 'OTHER'),
            ('OH',    'BUTLER',   180140,     187990,        None),  # <- Omits None.
            ('OH',    'FRANKLIN', 566499,     596915,        ''),    # <- Omits empty string.
            ('OH',    '-',        None,       '',            0),     # <- Retains zero.
        ]
        result = wide_to_narrow(data, ['TOT_MALE', 'TOT_FEMALE', 'OTHER'])

        expected = [
            {'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_MALE',   'value': 180140},
            {'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_FEMALE', 'value': 187990},
            {'state': 'OH', 'county': 'BUTLER',   'variable': 'OTHER',      'value': None},
            {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_MALE',   'value': 566499},
            {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_FEMALE', 'value': 596915},
            {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'OTHER',      'value': ''},
            {'state': 'OH', 'county': '-',        'variable': 'TOT_MALE',   'value': None},
            {'state': 'OH', 'county': '-',        'variable': 'TOT_FEMALE', 'value': ''},
            {'state': 'OH', 'county': '-',        'variable': 'OTHER',      'value': 0},
        ]
        self.assertEqual(list(result), expected)

    def test_missing_value_vars(self):
        data = [
            ('state', 'county',   'TOT_MALE', 'TOT_FEMALE'),
            ('OH',    'BUTLER',   180140,     187990),
            ('OH',    'FRANKLIN', 566499,     596915),
        ]
        regex = (
            r"wide_to_narrow column not found: "
            r"'BAD_VAR' not in \['state', 'county', 'TOT_MALE', 'TOT_FEMALE'\]"
        )
        with self.assertRaisesRegex(ToronError, regex):
            generator = wide_to_narrow(data, ['TOT_MALE', 'TOT_FEMALE', 'BAD_VAR'])
            list(generator)  # <- Must consume generator (it's not primed).
