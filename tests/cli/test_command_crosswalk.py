"""Tests for toron/cli/command_crosswalk.py module."""
from .. import _unittest as unittest
from toron import TopoNode
from toron._utils import ToronError

from toron.cli import command_crosswalk


class TwoNodeFixtures(object):
    def setUp(self):
        self.maxDiff = None

        self.node_a = TopoNode()
        self.node_a._connector._unique_id = '11111111-1111-1111-1111-111111111111'
        self.node_a.add_index_columns('foo', 'bar', 'baz')
        self.node_a.add_discrete_categories({'foo', 'bar', 'baz'})
        self.node_a.add_weight_group('qux', make_default=True)
        self.node_a.insert_index([
            ['foo', 'bar', 'baz', 'qux'],
            ['A-1', 'X-1', '1-1', 100.0],
            ['B-1', 'Y-1', '2-1', 200.0],
            ['C-1', 'Z-1', '3-1', 300.0],
        ])

        self.node_b = TopoNode()
        self.node_b._connector._unique_id = '22222222-2222-2222-2222-222222222222'
        self.node_b.add_index_columns('foo', 'bar')
        self.node_b.add_discrete_categories({'foo', 'bar'})
        self.node_b.add_weight_group('quux', make_default=True)
        self.node_b.insert_index([
            ['foo', 'bar', 'quux'],
            ['A-2', 'X-2', 100.0],
            ['B-2', 'Y-2', 200.0],
            ['C-2', 'Z-2', 300.0],
        ])


class TestGetColumnPositions(TwoNodeFixtures, unittest.TestCase):
    def test_simple_case(self):
        header = ['index_code', 'foo', 'bar', 'baz', 'corge', 'index_code', 'foo', 'bar']
        data_list = [
            ['1XA0157D6E', 'A-1', 'X-1', '1-1', 100.0, '1XF7F2FF38', 'A-2', 'X-2'],
            ['2XF38F26EA', 'B-1', 'Y-1', '2-1', 200.0, '2XA468A4BC', 'B-2', 'Y-2'],
            ['3X7429EDA9', 'C-1', 'Z-1', '3-1', 300.0, '3X23CE6FFF', 'C-2', 'Z-2'],
        ]

        result = command_crosswalk.get_column_positions(
            node1=self.node_a,
            node2=self.node_b,
            crosswalk_name='corge',
            data=data_list,
            columns=header,
        )

        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        positions, data_iter = result

        self.assertEqual(
            positions,
            {'node1_index_pos': 0,
             'node1_start': 0,
             'node1_stop': 4,
             'node2_index_pos': 5,
             'node2_start': 5,
             'node2_stop': 8,
             'value_position': 4},
        )
        self.assertEqual(list(data_iter), data_list)

    def test_one_missing_index(self):
        """When only one index is found, check other side for header match."""
        positions, _ = command_crosswalk.get_column_positions(
            node1=self.node_a,
            node2=self.node_b,
            crosswalk_name='corge',
            data=[
                ['1XA0157D6E', 'A-1', 'X-1', '1-1', 100.0, 'A-2', 'X-2'],
                ['2XF38F26EA', 'B-1', 'Y-1', '2-1', 200.0, 'B-2', 'Y-2'],
                ['3X7429EDA9', 'C-1', 'Z-1', '3-1', 300.0, 'C-2', 'Z-2'],
            ],
            columns=['index_code', 'foo', 'bar', 'baz', 'corge', 'foo', 'bar'],
        )
        self.assertEqual(
            positions,
            {'node1_index_pos': 0,
             'node1_start': 0,
             'node1_stop': 4,
             'node2_index_pos': None,  # <- No node2 index.
             'node2_start': 5,
             'node2_stop': 7,
             'value_position': 4},
        )

        positions, _ = command_crosswalk.get_column_positions(
            node1=self.node_a,
            node2=self.node_b,
            crosswalk_name='corge',
            data=[
                ['A-1', 'X-1', '1-1', 100.0, '1XF7F2FF38', 'A-2', 'X-2'],
                ['B-1', 'Y-1', '2-1', 200.0, '2XA468A4BC', 'B-2', 'Y-2'],
                ['C-1', 'Z-1', '3-1', 300.0, '3X23CE6FFF', 'C-2', 'Z-2'],
            ],
            columns=['foo', 'bar', 'baz', 'corge', 'index_code', 'foo', 'bar'],
        )
        self.assertEqual(
            positions,
            {'node1_index_pos': None,  # <- No node1 index.
             'node1_start': 0,
             'node1_stop': 3,
             'node2_index_pos': 4,
             'node2_start': 4,
             'node2_stop': 7,
             'value_position': 3},
        )

    def test_one_missing_index_no_header_match(self):
        """Raise an error if index is missing and header does not match."""
        regex = r"unable to find FILE2 columns;\s+Expected: 'foo', 'bar'\s+Found: 'XXX', 'YYY'"
        with self.assertRaisesRegex(ToronError, regex):
            positions, _ = command_crosswalk.get_column_positions(
                node1=self.node_a,
                node2=self.node_b,
                crosswalk_name='corge',
                data=[
                    ['1XA0157D6E', 'A-1', 'X-1', '1-1', 100.0, 'A-2', 'X-2'],
                    ['2XF38F26EA', 'B-1', 'Y-1', '2-1', 200.0, 'B-2', 'Y-2'],
                    ['3X7429EDA9', 'C-1', 'Z-1', '3-1', 300.0, 'C-2', 'Z-2'],
                ],
                columns=['index_code', 'foo', 'bar', 'baz', 'corge', 'XXX', 'YYY'],
            )

        regex = r"unable to find FILE1 columns;\s+Expected: 'foo', 'bar', 'baz'\s+Found: 'XXX', 'YYY', 'ZZZ'"
        with self.assertRaisesRegex(ToronError, regex):
            positions, _ = command_crosswalk.get_column_positions(
                node1=self.node_a,
                node2=self.node_b,
                crosswalk_name='corge',
                data=[
                    ['A-1', 'X-1', '1-1', 100.0, '1XF7F2FF38', 'A-2', 'X-2'],
                    ['B-1', 'Y-1', '2-1', 200.0, '2XA468A4BC', 'B-2', 'Y-2'],
                    ['C-1', 'Z-1', '3-1', 300.0, '3X23CE6FFF', 'C-2', 'Z-2'],
                ],
                columns=['XXX', 'YYY', 'ZZZ', 'corge', 'index_code', 'foo', 'bar'],
            )

    def test_bad_column_order(self):
        regex = r'Invalid column order in mapping data.'
        with self.assertRaisesRegex(RuntimeError, regex):
            command_crosswalk.get_column_positions(
                node1=self.node_a,
                node2=self.node_b,
                crosswalk_name='corge',
                data=[
                    ['1XA0157D6E', '1XF7F2FF38', 100.0],
                    ['2XF38F26EA', '2XA468A4BC', 200.0],
                    ['3X7429EDA9', '3X23CE6FFF', 300.0],
                ],
                columns=['index_code1', 'index_code2', 'corge'],
            )


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
