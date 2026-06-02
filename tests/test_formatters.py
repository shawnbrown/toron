"""Tests for toron.formatters module."""
import unittest
from toron.formatters import (
    format_granularity,
)

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
