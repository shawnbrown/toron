"""Tests for toron/cli/command_crosswalk.py module."""
from .. import _unittest as unittest

from toron.cli import command_crosswalk


class TestGetLocationFactory(unittest.TestCase):
    def setUp(self):
        self.header = ['foo', 'bar', 'baz', 'qux', 'foo', 'bar']
        self.data = [
            ['A-1', 'X-1', '1-1', 100.0, 'A-2', 'X-2'],
            ['B-1', 'Y-1', '2-1', 200.0, 'B-2', 'Y-2'],
            ['C-1', 'Z-1', '3-1', 300.0, 'C-2', 'Z-2'],
        ]

    def test_for_slice_0_to_3(self):
        """Check the left-side of the source data, slice(0, 3)."""
        get_location = command_crosswalk.get_location_factory(
            self.header,
            label_columns=['foo', 'bar', 'baz'],
            start=0,
            stop=3,
        )

        actual = [get_location(row) for row in self.data]
        expected = [
            ['A-1', 'X-1', '1-1'],
            ['B-1', 'Y-1', '2-1'],
            ['C-1', 'Z-1', '3-1'],
        ]
        self.assertEqual(actual, expected)

    def test_for_slice_0_to_3_different_order(self):
        """Values should be output in `label_columns` order."""
        get_location = command_crosswalk.get_location_factory(
            self.header,
            label_columns=['baz', 'foo', 'bar'],
            start=0,
            stop=3,
        )

        actual = [get_location(row) for row in self.data]
        expected = [
            ['1-1', 'A-1', 'X-1'],  # <- values in `label_columns` order
            ['2-1', 'B-1', 'Y-1'],  # <- values in `label_columns` order
            ['3-1', 'C-1', 'Z-1'],  # <- values in `label_columns` order

        ]
        self.assertEqual(actual, expected)

    def test_for_slice_3_to_6(self):
        """Check the right-side of the source data, slice(3, 6)."""
        get_location = command_crosswalk.get_location_factory(
            self.header,
            label_columns=['foo', 'bar', 'baz'],
            start=3,
            stop=6,
        )

        actual = [get_location(row) for row in self.data]
        expected = [
            ['A-2', 'X-2', ''],  # <- empty string for 'baz' (not found in slice)
            ['B-2', 'Y-2', ''],  # <- empty string for 'baz' (not found in slice)
            ['C-2', 'Z-2', ''],  # <- empty string for 'baz' (not found in slice)
        ]
        self.assertEqual(actual, expected)

    def test_duplicate_header_column(self):
        """The values of 'foo' and 'bar' appear twice in slice(0, 6)."""
        regex = r'found duplicate values in header'
        with self.assertRaisesRegex(ValueError, regex):
            get_location = command_crosswalk.get_location_factory(
                self.header,
                label_columns=['foo', 'bar', 'baz'],
                start=0,
                stop=6,
            )
