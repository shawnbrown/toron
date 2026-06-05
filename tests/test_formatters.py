"""Tests for toron.formatters module."""
import unittest
from toron.formatters import (
    sort_categories,
    format_granularity,
)


class TestSortCategories(unittest.TestCase):
    def test_basic_sorting(self):
        """Should sort categories and items within categories."""
        discrete_categories=[
            {'a', 'b'},
            {'a', 'c'},
            {'a', 'b', 'c'},
        ]

        self.assertEqual(
            sort_categories(discrete_categories, labels=['a', 'b', 'c']),
            [['a', 'b', 'c'], ['a', 'b'], ['a', 'c']],
        )

        self.assertEqual(
            sort_categories(discrete_categories, labels=['c', 'b', 'a']),
            [['c', 'b', 'a'], ['c', 'a'], ['b', 'a']],
        )

    def test_adding_whole_space(self):
        """Shold add "whole space" when not included in given categories."""
        discrete_categories=[{'a', 'c'}, {'b', 'a'}]  # <- No whole space, ['a', 'b', 'c'].

        self.assertEqual(
            sort_categories(discrete_categories, labels=['a', 'b', 'c']),
            [['a', 'b', 'c'], ['a', 'b'], ['a', 'c']],
        )

    def test_label_mismatch(self):
        """When value is missing from labels, error message should give context."""
        discrete_categories=[{'a', 'b', 'c'}, {'a', 'b'}, {'a', 'c'}]

        regex = r"category label 'c' missing from given labels \['a', 'b', 'd'\]"
        with self.assertRaisesRegex(ValueError, regex):
            sort_categories(discrete_categories, labels=['a', 'b', 'd']),


class TestFormatGranularity(unittest.TestCase):
    def test_minimum_precision(self):
        """Should always round to at least two decimal places."""
        self.assertEqual(
            format_granularity([12.650378635397704, 8.297246124988996]),
            ['12.65', ' 8.30'],
        )

        self.assertEqual(
            format_granularity([12, 8]),
            ['12.00', ' 8.00'],
            msg='should use two digits of signifigance regardless of rounding',
        )

    def test_additional_precision(self):
        """Should use more decimal places to assure uniqueness."""
        self.assertEqual(
            format_granularity([12.650378635397704, 12.647267731680174]),
            ['12.650', '12.647'],
        )

    def test_non_zero_rounding(self):
        """Non-zero values should not get rounded to zero."""
        self.assertEqual(
            format_granularity([0.00028, 5.303016085958896]),
            ['0.0003', '5.3030'],
        )

    def test_duplicate_values(self):
        """Duplicate values round to the same representation."""
        self.assertEqual(
            format_granularity([5.303016085958896, 5.303016085958896]),
            ['5.30', '5.30'],
        )

    def test_none_value(self):
        """Should handle ``None`` values, too."""
        self.assertEqual(
            format_granularity([12.650378635397704, None, 8.297246124988996]),
            ['12.65', ' None', ' 8.30'],
        )

    def test_multiple_cases(self):
        """Should handle multiple cases at the same time."""
        self.assertEqual(
            format_granularity(
                [12.650378635397704,
                 12.647267731680174,
                 8.297246124988996,
                 5.303016085958896,
                 5.303016085958896,
                 0.04080055546045003]),
                ['12.650',
                 '12.647',
                 ' 8.297',
                 ' 5.303',
                 ' 5.303',
                 ' 0.041'],
        )

    def test_empty_input(self):
        """Empty input should result in empty output."""
        self.assertEqual(format_granularity([]), [])
