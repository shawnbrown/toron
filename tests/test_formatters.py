"""Tests for toron.formatters module."""
import unittest
from decimal import Decimal
from toron.formatters import (
    sort_partition_definitions,
    format_granularity,
)


class TestSortPartitionDefinitions(unittest.TestCase):
    def test_basic_sorting(self):
        """Should sort definitions and items within definitions."""
        partition_definitions=[
            {'a', 'b'},
            {'a', 'c'},
            {'a', 'b', 'c'},
        ]

        self.assertEqual(
            sort_partition_definitions(partition_definitions, labels=['a', 'b', 'c']),
            [['a', 'b', 'c'], ['a', 'b'], ['a', 'c']],
        )

        self.assertEqual(
            sort_partition_definitions(partition_definitions, labels=['c', 'b', 'a']),
            [['c', 'b', 'a'], ['c', 'a'], ['b', 'a']],
        )

    def test_adding_whole_space(self):
        """Shold add "whole space" when not included in given categories."""
        partition_definitions=[{'a', 'c'}, {'b', 'a'}]  # <- No whole space, ['a', 'b', 'c'].

        self.assertEqual(
            sort_partition_definitions(partition_definitions, labels=['a', 'b', 'c']),
            [['a', 'b', 'c'], ['a', 'b'], ['a', 'c']],
        )

    def test_label_mismatch(self):
        """When value is missing from labels, error message should give context."""
        partition_definitions=[{'a', 'b', 'c'}, {'a', 'b'}, {'a', 'c'}]

        regex = r"partition label 'c' missing from given labels \['a', 'b', 'd'\]"
        with self.assertRaisesRegex(ValueError, regex):
            sort_partition_definitions(partition_definitions, labels=['a', 'b', 'd']),


class TestFormatGranularity(unittest.TestCase):
    def test_minimum_precision(self):
        """Should always round to at least three decimal places."""
        self.assertEqual(
            format_granularity([12.650378635397704, 8.297246124988996]),
            ['12.650', ' 8.297'],
        )

        self.assertEqual(
            format_granularity([12, 8]),
            ['12.000', ' 8.000'],
            msg='should use three digits of signifigance regardless of rounding',
        )

    def test_additional_precision(self):
        """Should use more decimal places to assure uniqueness."""
        self.assertEqual(
            format_granularity([12.650378635397704, 12.650325958503286]),
            ['12.6504', '12.6503'],
        )

    def test_non_zero_rounding(self):
        """Non-zero values should not get rounded to zero."""
        self.assertEqual(
            format_granularity([0.000028, 5.303016085958896]),
            ['0.00003', '5.30302'],
        )

    def test_duplicate_values(self):
        """Duplicate values round to the same representation."""
        self.assertEqual(
            format_granularity([5.303016085958896, 5.303016085958896]),
            ['5.303', '5.303'],
        )

    def test_none_value(self):
        """Should handle ``None`` values, too."""
        self.assertEqual(
            format_granularity([12.650378635397704, None, 8.297246124988996]),
            ['12.650', '  None', ' 8.297'],
        )

    def test_multiple_cases(self):
        """Should handle multiple cases at the same time."""
        self.assertEqual(
            format_granularity(
                [12.650378635397704,
                 12.650325958503286,
                 8.297246124988996,
                 5.303016085958896,
                 5.303016085958896,
                 0.04080055546045003]),
                ['12.6504',
                 '12.6503',
                 ' 8.2972',
                 ' 5.3030',
                 ' 5.3030',
                 ' 0.0408'],
        )

    def test_empty_input(self):
        """Empty input should result in empty output."""
        self.assertEqual(format_granularity([]), [])

    def test_no_distinct_repr(self):
        """Should raise error if unable to create a distinct repr."""
        regex = r'cannot find a unique representation'
        msg = 'difference should be too small to create distinct output'
        with self.assertRaisesRegex(ValueError, regex, msg=msg):
            format_granularity([
                Decimal('12.650378635397704001'),
                Decimal('12.650378635397704'),
            ])
