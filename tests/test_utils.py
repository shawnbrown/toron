"""Tests for toron/_utils.py module."""

import csv
import io
import unittest
from collections.abc import Iterator

try:
    import pandas
except ModuleNotFoundError:
    pandas = None

from toron._utils import (
    ToronError,
    make_readerlike,
    make_dictreaderlike,
    wide_to_narrow,
    make_hash,
    eagerly_initialize,
    BitFlags,
    QuantityIterator,
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


@unittest.skipUnless(pandas, 'requires pandas')
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

    @unittest.skipUnless(pandas, 'requires pandas')
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
    def test_init(self):
        all_values = [
            (1, 1, 1, 1, 1, 1, 1, 1),
            (1, 0, 0, 0, 0, 0, 0, 0),
            (0, 0, 0, 0, 0, 0, 0, 1),
            (0, 0, 0, 0, 0, 0, 0, 0),
        ]
        for values in all_values:
            with self.subTest(values=values):
                # Test multiple single-bit arguments.
                bits = BitFlags(*values)
                self.assertEqual(bits.data, values)

                # Test one iterable argument containing multiple bits.
                bits = BitFlags(values)
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

    def test_from_bytes(self):
        bits = BitFlags.from_bytes(b'\xff')
        self.assertEqual(bits._data, (1, 1, 1, 1, 1, 1, 1, 1))

        bits = BitFlags.from_bytes(b'\x01')
        self.assertEqual(bits._data, (0, 0, 0, 0, 0, 0, 0, 1))

        bits = BitFlags.from_bytes(b'\x80\x00')
        self.assertEqual(bits._data, (1, 0, 0, 0, 0, 0, 0, 0))

        bits = BitFlags.from_bytes(b'\x00\x01')
        self.assertEqual(bits._data, (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1))

        bits = BitFlags.from_bytes(b'\x00')
        self.assertEqual(bits._data, (0, 0, 0, 0, 0, 0, 0, 0))

        bits = BitFlags.from_bytes(b'')
        self.assertEqual(bits._data, (0, 0, 0, 0, 0, 0, 0, 0))

        regex = r"expected bytes object, got list: \['a', '', 'c'\]"
        with self.assertRaisesRegex(TypeError, regex):
            bits = BitFlags.from_bytes(['a', '', 'c'])

    def test_convert_to_bytes(self):
        bits = BitFlags(1, 1, 1, 1, 1, 1, 1, 1)
        self.assertEqual(bytes(bits), b'\xff')

        bits = BitFlags(1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0)
        msg = 'excess zeros are not included in bytes representation'
        self.assertEqual(bytes(bits), b'\xff', msg=msg)

        bits = BitFlags(0, 0, 0, 0, 0, 0, 0, 1)
        self.assertEqual(bytes(bits), b'\x01')

        bits = BitFlags(0, 0, 0, 0, 0, 0, 0, 0, 1)
        self.assertEqual(bytes(bits), b'\x00\x80')

        bits = BitFlags(1)
        self.assertEqual(bytes(bits), b'\x80')

        bits = BitFlags()
        self.assertEqual(bytes(bits), b'\x00')

        bits = BitFlags(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        self.assertEqual(bytes(bits), b'\x00')


class TestQuantityIterator(unittest.TestCase):
    def test_iterator_protocol(self):
        iterator = QuantityIterator('0000-00-00-00-000000', [
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
        iterator = QuantityIterator('0000-00-00-00-000000', data)
        self.assertEqual(list(iterator), data)

    def test_aggregated_output(self):
        iterator = QuantityIterator('0000-00-00-00-000000', [
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

        iterator1 = QuantityIterator(
            '0000-00-00-00-000000',
            data,
        )
        self.assertEqual(iterator1.attribute_keys, {'a', 'b', 'c'})

        iterator2 = QuantityIterator(
            '0000-00-00-00-000000',
            data,
            _attribute_keys={'a', 'b', 'c'},
        )
        self.assertEqual(iterator2.attribute_keys, {'a', 'b', 'c'})
