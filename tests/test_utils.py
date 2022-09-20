"""Tests for toron/_utils.py module."""

import unittest

from toron._utils import wide_to_long


class TestWideToLong(unittest.TestCase):
    maxDiff = None

    def test_multiple_value_vars(self):
        data = [
            ('state', 'county',   'TOT_MALE', 'TOT_FEMALE'),
            ('OH',    'BUTLER',   180140,     187990),
            ('OH',    'FRANKLIN', 566499,     596915),
        ]
        result = wide_to_long(data, ['TOT_MALE', 'TOT_FEMALE'])

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
        result = wide_to_long(data, ['TOT_ALL'])

        expected = [
            {'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_ALL', 'value': 368130},
            {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_ALL', 'value': 1163414},
        ]
        self.assertEqual(list(result), expected)

    def test_make_attrs_string(self):
        """When `make_attrs` is str, it should be used as the attr key."""
        data = [
            ('state', 'county',   'TOT_MALE', 'TOT_FEMALE'),
            ('OH',    'BUTLER',   180140,     187990),
            ('OH',    'FRANKLIN', 566499,     596915),
        ]
        result = wide_to_long(data, ['TOT_MALE', 'TOT_FEMALE'], 'census')

        # Uses "census" as attr key.
        expected = [
            {'state': 'OH', 'county': 'BUTLER',   'census': 'TOT_MALE',   'value': 180140},
            {'state': 'OH', 'county': 'BUTLER',   'census': 'TOT_FEMALE', 'value': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'census': 'TOT_MALE',   'value': 566499},
            {'state': 'OH', 'county': 'FRANKLIN', 'census': 'TOT_FEMALE', 'value': 596915},
        ]
        self.assertEqual(list(result), expected)

    def test_make_attrs_callable(self):
        """When `make_attrs` is callable, it should return an attribute dict."""
        data = [
            ('state', 'county',   'TOT_MALE', 'TOT_FEMALE'),
            ('OH',    'BUTLER',   180140,     187990),
            ('OH',    'FRANKLIN', 566499,     596915),
        ]
        result = wide_to_long(
            data,
            ['TOT_MALE', 'TOT_FEMALE'],
            lambda x: {'census': x.lower()},  # <- Returns attr dict.
        )

        expected = [
            {'state': 'OH', 'county': 'BUTLER',   'census': 'tot_male',   'value': 180140},
            {'state': 'OH', 'county': 'BUTLER',   'census': 'tot_female', 'value': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'census': 'tot_male',   'value': 566499},
            {'state': 'OH', 'county': 'FRANKLIN', 'census': 'tot_female', 'value': 596915},
        ]
        self.assertEqual(list(result), expected)

    def test_make_attrs_callable_multiple_attributes(self):
        data = [
            ('state', 'county',   'TOT_MALE', 'TOT_FEMALE'),
            ('OH',    'BUTLER',   180140,     187990),
            ('OH',    'FRANKLIN', 566499,     596915),
        ]
        result = wide_to_long(
            data,
            ['TOT_MALE', 'TOT_FEMALE'],
            lambda x: zip(['catalog', 'sex'], x.split('_'))  # <- make_attrs
        )

        expected = [
            {'state': 'OH', 'county': 'BUTLER',   'catalog': 'TOT', 'sex': 'MALE',   'value': 180140},
            {'state': 'OH', 'county': 'BUTLER',   'catalog': 'TOT', 'sex': 'FEMALE', 'value': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'catalog': 'TOT', 'sex': 'MALE',   'value': 566499},
            {'state': 'OH', 'county': 'FRANKLIN', 'catalog': 'TOT', 'sex': 'FEMALE', 'value': 596915},
        ]
        self.assertEqual(list(result), expected)

    def test_no_header(self):
        """Passing columns as an argument instead of a header row."""
        data = [
            ('OH',    'BUTLER',   180140,     187990),
            ('OH',    'FRANKLIN', 566499,     596915),
        ]
        columns = ('state', 'county', 'TOT_MALE', 'TOT_FEMALE')
        result = wide_to_long(data, ['TOT_MALE', 'TOT_FEMALE'], columns=columns)

        expected = [
            {'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_MALE',   'value': 180140},
            {'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_FEMALE', 'value': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_MALE',   'value': 566499},
            {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_FEMALE', 'value': 596915},
        ]
        self.assertEqual(list(result), expected)

    def test_dict_rows(self):
        data = [
            {'state': 'OH', 'county': 'BUTLER',   'TOT_MALE': 180140, 'TOT_FEMALE': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'TOT_MALE': 566499, 'TOT_FEMALE': 596915},
        ]
        result = wide_to_long(data, ['TOT_MALE', 'TOT_FEMALE'])

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
            generator = wide_to_long(data, ['TOT_MALE', 'TOT_FEMALE'])
            list(generator)  # <- Must consume generator (it's not primed).

    def test_omit_empty_values_retain_zeros(self):
        """Omit None and empty string, retain zeros."""
        data = [
            ('state', 'county',   'TOT_MALE', 'TOT_FEMALE', 'OTHER'),
            ('OH',    'BUTLER',   180140,     187990,        None),  # <- Omits None.
            ('OH',    'FRANKLIN', 566499,     596915,        ''),    # <- Omits empty string.
            ('OH',    '-',        None,       '',            0),     # <- Retains zero.
        ]
        result = wide_to_long(data, ['TOT_MALE', 'TOT_FEMALE', 'OTHER'])

        expected = [
            {'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_MALE',   'value': 180140},
            {'state': 'OH', 'county': 'BUTLER',   'variable': 'TOT_FEMALE', 'value': 187990},
            {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_MALE',   'value': 566499},
            {'state': 'OH', 'county': 'FRANKLIN', 'variable': 'TOT_FEMALE', 'value': 596915},
            {'state': 'OH', 'county': '-',        'variable': 'OTHER',      'value': 0},
        ]
        self.assertEqual(list(result), expected)

    def test_missing_value_vars(self):
        data = [
            ('state', 'county',   'TOT_MALE', 'TOT_FEMALE'),
            ('OH',    'BUTLER',   180140,     187990),
            ('OH',    'FRANKLIN', 566499,     596915),
        ]
        regex = r"'BAD_VAR' not in \('state', 'county', 'TOT_MALE', 'TOT_FEMALE'\)"
        with self.assertRaisesRegex(KeyError, regex):
            generator = wide_to_long(data, ['TOT_MALE', 'TOT_FEMALE', 'BAD_VAR'])
            list(generator)  # <- Must consume generator (it's not primed).

