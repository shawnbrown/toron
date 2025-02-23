"""Tests for toron/_utils.py module."""

import csv
import io
import sqlite3
import unittest
from collections.abc import Iterator

from toron._utils import (
    ToronError,
    normalize_tabular,
    verify_columns_set,
    make_readerlike,
    make_dictreaderlike,
    wide_to_long,
    wide_to_narrow,
    make_hash,
    SequenceHash,
    splitmix64,
    quantize_values,
    eagerly_initialize,
    BitFlags,
    XQuantityIterator,
)


class TestNormalizeTabular(unittest.TestCase):
    def test_sequences(self):
        input_data = [('col1', 'col2'), (1, 'a'), (2, 'b'), (3, 'c')]

        data, columns = normalize_tabular(input_data)

        self.assertEqual(list(data), [(1, 'a'), (2, 'b'), (3, 'c')])
        self.assertEqual(columns, ('col1', 'col2'))

    def test_sequences_explicit_columns(self):
        input_data = [(1, 'a'), (2, 'b'), (3, 'c')]
        input_clumns = ('col1', 'col2')

        data, columns = normalize_tabular(input_data, input_clumns)

        self.assertEqual(list(data), input_data)
        self.assertEqual(columns, input_clumns)

    def test_csv_reader(self):
        input_data = csv.reader(io.StringIO('col1,col2\n1,a\n2,b\n3,c\n'))

        data, columns = normalize_tabular(input_data)

        self.assertEqual(list(data), [['1', 'a'], ['2', 'b'], ['3', 'c']])
        self.assertEqual(columns, ['col1', 'col2'])

    def test_mappings(self):
        input_data = [{'col1': 1, 'col2': 'a'},
                      {'col1': 2, 'col2': 'b'},
                      {'col1': 3, 'col2': 'c'}]

        data, columns = normalize_tabular(input_data)

        self.assertEqual(list(data), [(1, 'a'), (2, 'b'), (3, 'c')])
        self.assertEqual(columns, ('col1', 'col2'))

    def test_mappings_explicit_columns(self):
        """Normalized mapping should use order of *columns* if given."""
        input_data = [{'col1': 1, 'col2': 'a'},
                      {'col1': 2, 'col2': 'b'},
                      {'col1': 3, 'col2': 'c'}]
        input_columns = ('col2', 'col1')  # <- Changed order.

        data, columns = normalize_tabular(input_data, input_columns)

        self.assertEqual(list(data), [('a', 1), ('b', 2), ('c', 3)])
        self.assertEqual(columns, input_columns)

    def test_empty_dataset(self):
        input_data = iter([])

        data, columns = normalize_tabular(input_data)

        self.assertEqual(list(data), [])
        self.assertEqual(columns, tuple())

    def test_bad_data_type(self):
        input_data = 123

        regex = r"data must be iterable, got 'int': 123"
        with self.assertRaisesRegex(TypeError, regex):
            data, columns = normalize_tabular(input_data)

    def test_bad_row_type(self):
        input_data = [{'col1', 'col2'}, {1, 'a'}, {2, 'b'}, {3, 'c'}]  # <- Not sequences!

        regex = r"rows must be sequence or mapping, got 'set': \{'col[12]', 'col[12]'\}"
        with self.assertRaisesRegex(TypeError, regex):
            data, columns = normalize_tabular(input_data)


class TestVerifyColumnsSet(unittest.TestCase):
    def test_passing(self):
        try:
            verify_columns_set(iter(['A', 'B', 'C']), iter(['A', 'B', 'C']))
        except Exception:
            self.fail('columns are the same, should pass without error')

    def test_missing(self):
        regex = r"^invalid column names\s+missing required columns: 'B', 'C'$"
        with self.assertRaisesRegex(ValueError, regex):
            verify_columns_set(iter(['A']), iter(['A', 'B', 'C']))

    def test_extra(self):
        regex = r"^invalid column names\s+extra columns found: 'D', 'E'$"
        with self.assertRaisesRegex(ValueError, regex):
            verify_columns_set(
                iter(['A', 'B', 'C', 'D', 'E']), iter(['A', 'B', 'C'])
            )

    def test_missing_and_extra(self):
        regex = r"^invalid column names\s+missing required columns: 'C'\s+extra columns found: 'E'$"
        with self.assertRaisesRegex(ValueError, regex):
            verify_columns_set(iter(['A', 'B', 'E']), iter(['A', 'B', 'C']))

    def test_allow_extras(self):
        verify_columns_set(
            iter(['A', 'B', 'C', 'D', 'E']),
            iter(['A', 'B', 'C']),
            allow_extras=True,
        )

        regex = r"^invalid column names\s+missing required columns: 'C'$"
        with self.assertRaisesRegex(ValueError, regex):
            verify_columns_set(
                iter(['A', 'B', 'E']),
                iter(['A', 'B', 'C']),
                allow_extras=True,
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


class TestWideToLong(unittest.TestCase):
    maxDiff = None

    def test_multiple_value_vars(self):
        result = wide_to_long(
            data=[('state', 'county',   'TOT_MALE', 'TOT_FEMALE'),
                  ('OH',    'BUTLER',   180140,     187990),
                  ('OH',    'FRANKLIN', 566499,     596915)],
            cols_to_stack=['TOT_MALE', 'TOT_FEMALE'],
        )

        self.assertEqual(
            list(result),
            [('state', 'county',   'variable',   'value'),
             ('OH',    'BUTLER',   'TOT_MALE',   180140),
             ('OH',    'BUTLER',   'TOT_FEMALE', 187990),
             ('OH',    'FRANKLIN', 'TOT_MALE',   566499),
             ('OH',    'FRANKLIN', 'TOT_FEMALE', 596915)],
        )

    def test_single_value_var(self):
        result = wide_to_long(
            data=[('state', 'county',   'TOT_ALL'),
                  ('OH',    'BUTLER',   368130),
                  ('OH',    'FRANKLIN', 1163414)],
            cols_to_stack=['TOT_ALL'],
        )

        self.assertEqual(
            list(result),
            [('state', 'county',   'variable', 'value'),
             ('OH',    'BUTLER',   'TOT_ALL',  368130),
             ('OH',    'FRANKLIN', 'TOT_ALL',  1163414)],
        )

    def test_explicit_var_name(self):
        result = wide_to_long(
            data=[('state', 'county',   'TOT_MALE', 'TOT_FEMALE'),
                  ('OH',    'BUTLER',   180140,     187990),
                  ('OH',    'FRANKLIN', 566499,     596915)],
            cols_to_stack=['TOT_MALE', 'TOT_FEMALE'],
            var_name='census',
        )

        self.assertEqual(
            list(result),
            [('state', 'county',   'census',     'value'),  # <- Uses 'census' as variable name.
             ('OH',    'BUTLER',   'TOT_MALE',   180140),
             ('OH',    'BUTLER',   'TOT_FEMALE', 187990),
             ('OH',    'FRANKLIN', 'TOT_MALE',   566499),
             ('OH',    'FRANKLIN', 'TOT_FEMALE', 596915)],
        )

    def test_explicit_value_name(self):
        result = wide_to_long(
            data=[('state', 'county',   'TOT_MALE', 'TOT_FEMALE'),
                  ('OH',    'BUTLER',   180140,     187990),
                  ('OH',    'FRANKLIN', 566499,     596915)],
            cols_to_stack=['TOT_MALE', 'TOT_FEMALE'],
            val_name='count',
        )

        self.assertEqual(
            list(result),
            [('state', 'county',   'variable',   'count'),  # <- Uses 'count' as value name.
             ('OH',    'BUTLER',   'TOT_MALE',   180140),
             ('OH',    'BUTLER',   'TOT_FEMALE', 187990),
             ('OH',    'FRANKLIN', 'TOT_MALE',   566499),
             ('OH',    'FRANKLIN', 'TOT_FEMALE', 596915)],
        )

    def test_row_dicts(self):
        result = wide_to_long(
            data=[{'state': 'OH', 'county': 'BUTLER',   'TOT_MALE': 180140, 'TOT_FEMALE': 187990},
                  {'state': 'OH', 'county': 'FRANKLIN', 'TOT_MALE': 566499, 'TOT_FEMALE': 596915}],
            cols_to_stack=['TOT_MALE', 'TOT_FEMALE'],
        )

        self.assertEqual(
            list(result),
            [{'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_MALE',   'value': 180140},
             {'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_FEMALE', 'value': 187990},
             {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_MALE',   'value': 566499},
             {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_FEMALE', 'value': 596915}],
        )

    def test_non_mapping_non_sequence(self):
        """Given *data* must contain dict-rows or sequence-rows."""
        list_of_sets = [
            {'state', 'county',   'TOT_MALE', 'TOT_FEMALE'},
            {'OH',    'BUTLER',   180140,     187990},
            {'OH',    'FRANKLIN', 566499,     596915},
        ]

        with self.assertRaises(TypeError):
            generator = wide_to_long(
                data=list_of_sets,
                cols_to_stack=['TOT_MALE', 'TOT_FEMALE'],
            )

    def test_empty_values(self):
        result = wide_to_long(
            data=[('state', 'county',   'TOT_MALE', 'TOT_FEMALE', 'OTHER'),
                  ('OH',    'BUTLER',   180140,     187990,        None),  # <- Omits None.
                  ('OH',    'FRANKLIN', 566499,     596915,        ''),    # <- Omits empty string.
                  ('OH',    '-',        None,       '',            0),     # <- Retains zero.
                  ('OH',    None,       123,        123,           123)],  # <- Retains None if in kept column.
            cols_to_stack=['TOT_MALE', 'TOT_FEMALE', 'OTHER'],
        )

        self.assertEqual(
            list(result),
            [('state', 'county',   'variable',   'value'),
             ('OH',    'BUTLER',   'TOT_MALE',   180140),
             ('OH',    'BUTLER',   'TOT_FEMALE', 187990),
             ('OH',    'BUTLER',   'OTHER',      None),  # <- Omitted.
             ('OH',    'FRANKLIN', 'TOT_MALE',   566499),
             ('OH',    'FRANKLIN', 'TOT_FEMALE', 596915),
             ('OH',    'FRANKLIN', 'OTHER',      ''),    # <- Omitted.
             ('OH',    '-',        'TOT_MALE',   None),  # <- Omitted.
             ('OH',    '-',        'TOT_FEMALE', ''),    # <- Omitted.
             ('OH',    '-',        'OTHER',      0),
             ('OH',    None,       'TOT_MALE',   123),
             ('OH',    None,       'TOT_FEMALE', 123),
             ('OH',    None,       'OTHER',      123)],
        )

    def test_missing_cols_to_stack(self):
        data = [
            ('state', 'county',   'TOT_MALE', 'TOT_FEMALE'),
            ('OH',    'BUTLER',   180140,     187990),
            ('OH',    'FRANKLIN', 566499,     596915),
        ]
        regex = ("columns missing from data: 'BAD_VAR'")
        with self.assertRaisesRegex(ValueError, regex):
            generator = wide_to_long(data, ['TOT_MALE', 'TOT_FEMALE', 'BAD_VAR'])

    def test_missing_cols_to_stack_row_dicts(self):
        data = [
            {'state': 'OH', 'county': 'BUTLER', 'TOT_MALE': 180140, 'TOT_FEMALE': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'TOT_MALE': 566499, 'TOT_FEMALE': 596915},
        ]
        regex = ("columns missing from data: 'BAD_VAR'")
        with self.assertRaisesRegex(ValueError, regex):
            generator = wide_to_long(data, ['TOT_MALE', 'TOT_FEMALE', 'BAD_VAR'])


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


class TestMakeHash(unittest.TestCase):
    # NOTE: The expected hash digests for each test case have been
    # independently verified.

    def test_sequence_of_strings(self):
        self.assertEqual(
            make_hash(['a', 'b', 'c', 'd']),  # Hash of message "a|b|c|d".
            'b54856b7a8705958e13238b3d67eac1cf256afefd4ad405d644ac956b1164870',
        )
        self.assertEqual(
            make_hash(['a', 'bc', 'd']),  # Hash of message "a|bc|d".
            '845645d1f0491e0bee5a2bf69bf76a9bec2abf157eb2716255fdb708166f5c1e',
        )

    def test_sequence_of_integers(self):
        self.assertEqual(
            make_hash([1, 2, 3, 4]),  # Hash of message "1|2|3|4".
            '8e96dc5e83d405a518a3a93fcbaa8f6a21fd909fa989f73635fe74a093615f39',
        )
        self.assertEqual(
            make_hash([1, 23, 4]),  # Hash of message "1|23|4".
            'daf1e2d7d8c08a1a9d194df37cfb030311a1d8cca3908bbc502c6c82fe3a0739',
        )

    def test_non_default_separator(self):
        self.assertEqual(
            make_hash([1, 23, 4], sep='=>'),  # Hash of message "1=>23=>4".
            'f3f6f772362b79f9205294d905f0ea4e21fb0e0801fca3d64b8a2a2dbc756465',
        )

    def test_empty_iterable(self):
        digest = make_hash([])
        self.assertIsNone(digest)


class TestSequenceHash(unittest.TestCase):
    def test_sequence(self):
        sequence_hash = SequenceHash()
        for n in [0, 1, 2]:
            sequence_hash.add_value(n)

        self.assertEqual(
            sequence_hash.get_hexdigest(),
            '5dfadd0e50910f561636c47335ecf8316251cbd85964eadb5c00103502edf177',
        )

    def test_initialize_with_sequence(self):
        sequence_hash = SequenceHash(iter([0, 1, 2]))
        self.assertEqual(
            sequence_hash.get_hexdigest(),
            '5dfadd0e50910f561636c47335ecf8316251cbd85964eadb5c00103502edf177',
        )

    def test_distinct_sequences(self):
        """Make sure [1, 2, 3, 4] and [12, 34] have distinct hashes."""
        sequence_hash = SequenceHash()
        sequence_hash.add_value(1)
        sequence_hash.add_value(2)
        sequence_hash.add_value(3)
        sequence_hash.add_value(4)
        self.assertEqual(
            sequence_hash.get_hexdigest(),
            '7236c00c170036c6de133a878210ddd58567aa1d0619a0f70f69e38ae6f916e9',
        )

        sequence_hash = SequenceHash()
        sequence_hash.add_value(12)
        sequence_hash.add_value(34)
        self.assertEqual(
            sequence_hash.get_hexdigest(),
            'efe163b58edfb9cfd3e3fd8c39ab3a134c4dc30f4768fe5f02b0581ae6d63adb',
        )

    def test_error_conditions(self):
        regex = (
            r'illegal value - values must be a strictly increasing '
            r'sequence starting at 0 or greater'
        )

        # Test not strictly increasing.
        sequence_hash = SequenceHash()
        sequence_hash.add_value(1)
        sequence_hash.add_value(2)
        with self.assertRaisesRegex(ValueError, regex):
            sequence_hash.add_value(2)  # <- Same as previous, not increasing!

        # Test sequence not starting at 0 or greater.
        sequence_hash = SequenceHash()
        with self.assertRaisesRegex(ValueError, regex):
            sequence_hash.add_value(-5)  # <- Not 0 or greater!

        # Test integer is too big to fit in 8 bytes.
        sequence_hash = SequenceHash()
        with self.assertRaisesRegex(ValueError, 'int too big'):
            sequence_hash.add_value(18446744073709551616)


class TestQuantizeValues(unittest.TestCase):
    def test_splitmix64(self):
        """Test SplitMix64 pseudo-random number generation."""
        input_and_output = [
            (0, 16294208416658607535),
            (1, 10451216379200822465),
            (9, 12587370737594032228),
            (10, 614480483733483466),
            (99, 4824385676517010403),
            (100, 2532601429470541124),
            ((2 ** 64 - 1), 16490336266968443936),  # <- Largest 64-bit int.
        ]
        for input_value, expected_output in input_and_output:
            with self.subTest(input=input_value):
                hash_digest = splitmix64(input_value)
                self.assertEqual(hash_digest, expected_output)

    def test_whole_remainder(self):
        """Test when fractional parts sum to whole number (2)."""
        input_items = {
            (1, 3.250),
            (2, 3.625),  # <- Second highest fractional part.
            (3, 3.125),
            (4, 2.125),
            (5, 1.875),  # <- First highest fractional part.
        }
        expected_output = {
            (1, 3.0),
            (2, 4.0),  # <- Gets 1 remainder.
            (3, 3.0),
            (4, 2.0),
            (5, 2.0),  # <- Gets 1 remainder.
        }
        self.assertEqual(set(quantize_values(input_items, 14.0)), expected_output)

    def test_fractional_remainder(self):
        """Test when fractional parts sum to a non-whole number (2.125)."""
        input_items = {
            (1, 3.250),  # <- Third highest fractional part.
            (2, 3.625),  # <- Second highest fractional part.
            (3, 3.1875),
            (4, 2.1875),
            (5, 1.875),  # <- First highest fractional part.
        }
        expected_output = {
            (1, 3.125),  # <- Gets fractional remainder.
            (2, 4.0),    # <- Gets whole remainder.
            (3, 3.0),
            (4, 2.0),
            (5, 2.0),    # <- Gets whole remainder.
        }
        self.assertEqual(set(quantize_values(input_items, 14.125)), expected_output)

    def test_negative_values(self):
        """Test when values are negative."""
        input_items = {
            (1, -3.250),  # <- Third largest magnitude fractional part.
            (2, -3.625),  # <- Second largest magnitude fractional part.
            (3, -3.1875),
            (4, -2.1875),
            (5, -1.875),  # <- First largest magnitude fractional part.
        }
        expected_output = {
            (1, -3.125),  # <- Gets fractional remainder.
            (2, -4.0),    # <- Gets whole remainder.
            (3, -3.0),
            (4, -2.0),
            (5, -2.0),    # <- Gets whole remainder.
        }
        self.assertEqual(set(quantize_values(input_items, -14.125)), expected_output)

    def test_tie_breaking(self):
        """Test when many fractional parts have the same magnitude.

        Ties are resolved using a repeatable, pseudo-random shuffle.
        """
        input_items = [
            (1, 3.25),  # <- Fractional parts have equal magnitude.
            (2, 3.25),  # <- Fractional parts have equal magnitude.
            (3, 3.25),  # <- Fractional parts have equal magnitude.
            (4, 2.25),  # <- Fractional parts have equal magnitude.
            (5, 1.25),  # <- Fractional parts have equal magnitude.
            (6, 2.25),  # <- Fractional parts have equal magnitude.
            (7, 3.25),  # <- Fractional parts have equal magnitude.
            (8, 4.25),  # <- Fractional parts have equal magnitude.
            (9, 4.00),
        ]
        expected_output = {
            (1, 3.0),
            (2, 3.0),
            (3, 4.0),  # <- Gets 1 remainder.
            (4, 2.0),
            (5, 2.0),  # <- Gets 1 remainder.
            (6, 2.0),
            (7, 3.0),
            (8, 4.0),
            (9, 4.0),
        }
        self.assertEqual(set(quantize_values(input_items, 27.0)), expected_output)


class TestEagerlyInitialize(unittest.TestCase):
    @staticmethod
    def dummy_generator(status_good):
        if not status_good:
            raise AssertionError
        yield 1
        yield 2
        yield 3

    def test_undecorated_behavior(self):
        gen = self.dummy_generator(False)  # <- No error on instantiation.
        with self.assertRaises(AssertionError):
            list(gen)  # <- Raises error when consumed.

    def test_decorator(self):
        decorated = eagerly_initialize(self.dummy_generator)  # <- Apply decorator.
        with self.assertRaises(AssertionError):
            gen = decorated(False)  # <- Raises error on instantiation.


class TestBitFlags(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bits_to_bytestring = [
            ((0, 0, 0, 0), b''),
            (tuple(),      b''),
            ((0, 0, 0, 1), b'\x10'),
            ((1, 0, 0, 0), b'\x80'),
            ((1, 1, 1, 1), b'\xf0'),
            ((0, 0, 0, 0, 0, 0, 0, 0), b''),
            ((0, 0, 0, 0, 0, 0, 0, 1), b'\x01'),
            ((1, 0, 0, 0, 0, 0, 0, 0), b'\x80'),
            ((0, 0, 0, 0, 1, 1, 1, 1), b'\x0f'),
            ((1, 1, 1, 1, 1, 1, 1, 1), b'\xff'),
            ((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), b''),
            ((0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0), b'\x01'),
            ((1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), b'\x80'),
            ((0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0), b'\x00\x80'),
            ((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), b''),
            ((0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0), b'\x01'),
            ((0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0), b'\x00\x80'),
        ]

    def test_init_from_bytes(self):
        all_values = [
            (b'\xff',     b'\xff'),  # (1, 1, 1, 1, 1, 1, 1, 1)
            (b'\x80',     b'\x80'),  # (1, 0, 0, 0, 0, 0, 0, 0)
            (b'\x01',     b'\x01'),  # (0, 0, 0, 0, 0, 0, 0, 1)
            (b'\x01\x00', b'\x01'),  # (0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0)
            (b'',         b''),      # (0, 0, 0, 0, 0, 0, 0, 0)
            (b'\x00',     b''),      # (0, 0, 0, 0, 0, 0, 0, 0)
            (b'\x00\x00', b''),      # (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        ]
        for original, normalized in all_values:
            with self.subTest(byte_string=original):
                bits = BitFlags(original)
                self.assertEqual(bits._bytes, normalized)

    def test_convert_to_bytes(self):
        bits = BitFlags(1, 1, 0, 1, 0, 0, 0, 0)
        self.assertEqual(bytes(bits), b'\xd0')

    def test_init_from_iterable_of_bits(self):
        for bitstream, expected in self.bits_to_bytestring:
            with self.subTest(input=bitstream):
                # Using bitstream as single argument.
                bit_flags = BitFlags(bitstream)
                self.assertEqual(bit_flags._bytes, expected)

                # Using unpacked bitstream as multiple arguments.
                bit_flags = BitFlags(*bitstream)
                self.assertEqual(bit_flags._bytes, expected)

    def test_init_from_single_nonbyte_noniterable(self):
        self.assertEqual(BitFlags(0)._bytes, b'')
        self.assertEqual(BitFlags(1)._bytes, b'\x80')
        self.assertEqual(BitFlags(None)._bytes, b'')
        self.assertEqual(BitFlags(object())._bytes,  b'\x80')

    def test_bitstream_to_bytes(self):
        for bitstream, expected in self.bits_to_bytestring:
            with self.subTest(input=bitstream):
                result = BitFlags._bitstream_to_bytes(bitstream)
                self.assertEqual(result, expected)

    def test_bytes_to_bitstream(self):
        all_values = [
            (b'',     tuple()),
            (b'\x00', (0, 0, 0, 0, 0, 0, 0, 0)),
            (b'\x01', (0, 0, 0, 0, 0, 0, 0, 1)),
            (b'\x0f', (0, 0, 0, 0, 1, 1, 1, 1)),
            (b'\x10', (0, 0, 0, 1, 0, 0, 0, 0)),
            (b'\x80', (1, 0, 0, 0, 0, 0, 0, 0)),
            (b'\xf0', (1, 1, 1, 1, 0, 0, 0, 0)),
            (b'\xff', (1, 1, 1, 1, 1, 1, 1, 1)),
            (b'\x00\x00', (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)),
            (b'\x00\x80', (0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0)),
        ]
        for byte_string, expected in all_values:
            with self.subTest(input=byte_string):
                result = BitFlags._bytes_to_bitstream(byte_string)
                self.assertEqual(tuple(result), expected)

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
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), ('x', 'x', '', 'x', '', '', '', '')],
            [BitFlags(), (0, 0, 0, 0, 0, 0, 0, 0, 0, 0)],
            [[], BitFlags(0, 0, 0, 0, 0, 0, 0, 0)],
        ]
        for a, b in equal_values:
            with self.subTest(a=a, b=b):
                self.assertTrue(a == b)

        not_equal_values = [
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), BitFlags(1, 1, 1, 1, 1, 1, 1, 1)],
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), (1, 1, 1, 1, 1, 1, 1, 1)],
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), [1, 1, 1, 1, 1, 1, 1, 1]],
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), 1234],
            [BitFlags(1, 1, 0, 1, 0, 0, 0, 0), 'blerg'],
        ]
        for a, b in not_equal_values:
            with self.subTest(a=a, b=b):
                self.assertFalse(a == b)

    def test_hashable(self):
        bits = BitFlags(1, 1, 0, 1, 0, 0, 0, 0)
        self.assertEqual(hash(bits), hash(bits))

    def test_len(self):
        self.assertEqual(len(BitFlags(1, 1, 0, 1)), 8)
        self.assertEqual(len(BitFlags(1, 1, 0, 1, 0, 0, 0, 0)), 8)
        self.assertEqual(len(BitFlags(1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0)), 8)
        self.assertEqual(len(BitFlags(1, 1, 0, 1, 0, 0, 0, 0, 1)), 16)
        self.assertEqual(len(BitFlags(0, 0, 0, 0, 0, 0, 0, 0)), 0)

    def test_truth_value(self):
        self.assertTrue(BitFlags(0, 0, 0, 1))
        self.assertFalse(BitFlags(0, 0, 0, 0), msg='Empty should test as False.')

    def test_getitem(self):
        bits = BitFlags(1, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0)

        self.assertEqual(bits[0], 1)
        self.assertEqual(bits[1], 1)
        self.assertEqual(bits[2], 0)
        self.assertEqual(bits[3], 1)
        self.assertEqual(bits[4], 0)
        self.assertEqual(bits[5], 0)
        self.assertEqual(bits[6], 0)
        self.assertEqual(bits[7], 0)
        self.assertEqual(bits[8], 1)

        # Reverse index.
        self.assertEqual(bits[-8],  1)
        self.assertEqual(bits[-9],  0)
        self.assertEqual(bits[-10], 0)
        self.assertEqual(bits[-11], 0)
        self.assertEqual(bits[-12], 0)
        self.assertEqual(bits[-13], 1)
        self.assertEqual(bits[-14], 0)
        self.assertEqual(bits[-15], 1)
        self.assertEqual(bits[-16], 1)

        # Check for index out of range.
        with self.assertRaises(IndexError):
            bits[16]

        with self.assertRaises(IndexError):
            bits[-17]

        # Check slice behavior.
        sliced = bits[2:]
        self.assertEqual(sliced, BitFlags(0, 1, 0, 0, 0, 0, 1, 0))

        # Bad index type.
        regex = 'must be integers or slices, not str'
        with self.assertRaisesRegex(TypeError, regex):
            bits['foo']

    def test_iter(self):
        bit_list = [1, 1, 0, 1, 0, 0, 0, 0]
        bits = BitFlags(bit_list)

        self.assertIsInstance(iter(bits), Iterator)

        self.assertEqual(list(bits), bit_list)

    def test_bitwise_or(self):
        bits_a = BitFlags(1, 1, 1, 1, 1, 1, 1, 1)
        bits_b = BitFlags(1, 1, 1, 1, 0, 0, 0, 0)
        bits_c = BitFlags(0, 0, 0, 0, 1, 1, 1, 1)
        bits_d = BitFlags(1, 0, 1, 0, 1, 0, 1, 0)
        bits_e = BitFlags(0, 1, 0, 1, 0, 1, 0, 1)
        bits_f = BitFlags(0, 0, 0, 0, 0, 0, 0, 0, 1, 0)
        bits_g = BitFlags(0, 0, 0, 0, 0, 0, 0, 0, 0, 1)

        self.assertEqual(bits_a | bits_b, bits_a)
        self.assertEqual(bits_b | bits_c, bits_a)
        self.assertEqual(bits_d | bits_e, bits_a)
        self.assertEqual(bits_b | bits_d, BitFlags(1, 1, 1, 1, 1, 0, 1, 0))
        self.assertEqual(bits_f | bits_g, BitFlags(0, 0, 0, 0, 0, 0, 0, 0, 1, 1))
        self.assertEqual(bits_e | bits_g, BitFlags(0, 1, 0, 1, 0, 1, 0, 1, 0, 1))


class TestQuantityIterator(unittest.TestCase):
    def test_iterator_protocol(self):
        iterator = XQuantityIterator('0000-00-00-00-000000', [
            (1, {'a': 'foo'}, 4.5),
            (2, {'a': 'foo'}, 2.5),
            (3, {'a': 'foo'}, 3.0),
            (4, {'a': 'foo'}, 9.0),
        ])
        self.assertIs(iter(iterator), iter(iterator))
        self.assertIsInstance(iterator, Iterator)

        list(iterator)  # Consume iterator.
        with self.assertRaises(StopIteration):
            next(iterator)

        iterator.close()  # Close internal database connection.
        with self.assertRaises(StopIteration, msg='Should raise StopIteration, not sqlite3 error.'):
            next(iterator)

    def test_unchanged_data(self):
        data = [
            (1, {'a': 'foo'}, 4.5),
            (2, {'a': 'foo'}, 2.5),
            (3, {'a': 'foo'}, 3.0),
            (4, {'a': 'foo'}, 9.0),
        ]
        iterator = XQuantityIterator('0000-00-00-00-000000', data)
        self.assertEqual(list(iterator), data)

    def test_aggregated_output(self):
        iterator = XQuantityIterator('0000-00-00-00-000000', [
            (1, {'a': 'foo'}, 4.5),
            (2, {'a': 'foo'}, 2.5),
            (3, {'a': 'foo'}, 3.0),
            (4, {'a': 'foo'}, 3.0),  # <- Gets aggregated.
            (4, {'a': 'foo'}, 2.0),  # <- Gets aggregated.
            (4, {'a': 'foo'}, 4.0),  # <- Gets aggregated.
        ])

        expected = [
            (1, {'a': 'foo'}, 4.5),
            (2, {'a': 'foo'}, 2.5),
            (3, {'a': 'foo'}, 3.0),
            (4, {'a': 'foo'}, 9.0),  # <- Aggregated from 3.0 + 2.0 + 4.0
        ]
        self.assertEqual(list(iterator), expected)

    def test_attribute_keys(self):
        data = [
            (1, {'a': 'foo'}, 4.5),
            (2, {'a': 'bar'}, 2.5),
            (3, {'b': 'baz'}, 3.0),
            (4, {'c': 'qux'}, 9.0),
        ]

        iterator1 = XQuantityIterator(
            '0000-00-00-00-000000',
            data,
        )
        self.assertEqual(iterator1.attribute_keys, {'a', 'b', 'c'})

        iterator2 = XQuantityIterator(
            '0000-00-00-00-000000',
            data,
            _attribute_keys={'a', 'b', 'c'},
        )
        self.assertEqual(iterator2.attribute_keys, {'a', 'b', 'c'})

    def test_failure_to_load(self):
        bogus_data = [
            (1, {'a': 'foo'}, 4.5),
            (2, {'a': 'bar'}, 2.5),
            (3, {'b': 'baz'}, None),  # <- Will violate NOT NULL constraint.
            (4, {'c': 'qux'}, 9.0),
        ]

        msg = 'failure to load should only raise a sqlite3.IntegrityError'
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            XQuantityIterator('0000-00-00-00-000000', bogus_data)
