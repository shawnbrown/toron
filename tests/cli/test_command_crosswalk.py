"""Tests for toron/cli/command_crosswalk.py module."""
import argparse
from dataclasses import astuple
from .. import _unittest as unittest
from ..common import DummyRedirection, TopoNodeFixtures
from toron._utils import ToronError, BitFlags

from toron.cli import command_crosswalk
from toron.cli.common import ExitCode


class TestGetColumnPositions(TopoNodeFixtures, unittest.TestCase):
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

    def test_index_codes_only(self):
        positions, _ = command_crosswalk.get_column_positions(
            node1=self.node_a,
            node2=self.node_b,
            crosswalk_name='corge',
            data=[
                ['1XA0157D6E', 100.0, '1XF7F2FF38'],
                ['2XF38F26EA', 200.0, '2XA468A4BC'],
                ['3X7429EDA9', 300.0, '3X23CE6FFF'],
            ],
            columns=['index_code', 'corge', 'index_code'],
        )

        self.assertEqual(
            positions,
            {'node1_index_pos': 0,
             'node1_start': 0,
             'node1_stop': 1,
             'node2_index_pos': 2,
             'node2_start': 2,
             'node2_stop': 3,
             'value_position': 1},
        )

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

    def test_no_indexes_only_label_columns(self):
        """If no indexes are given, label columns must match exactly
        (with node1 on the left and node2 on the right).
        """
        positions, _ = command_crosswalk.get_column_positions(
            node1=self.node_a,
            node2=self.node_b,
            crosswalk_name='corge',
            data=[
                ['A-1', 'X-1', '1-1', 100.0, 'A-2', 'X-2'],
                ['B-1', 'Y-1', '2-1', 200.0, 'B-2', 'Y-2'],
                ['C-1', 'Z-1', '3-1', 300.0, 'C-2', 'Z-2'],
            ],
            columns=['foo', 'bar', 'baz', 'corge', 'foo', 'bar'],
        )

        self.assertEqual(
            positions,
            {'node1_index_pos': None,
             'node1_start': 0,
             'node1_stop': 3,
             'node2_index_pos': None,
             'node2_start': 4,
             'node2_stop': 6,
             'value_position': 3},
        )

    def test_no_indexes_no_label_column_match(self):
        """Should raise an error if no indexes and headers don't match."""
        regex = (
            r"no index codes found, unable to match by label columns;\s+"
            r"unable to find FILE1 columns;\s+"
            r"Expected: 'foo', 'bar', 'baz'\s+"
            r"Found: 'foo', 'bar'\s+"
            r"unable to find FILE2 columns;\s+"
            r"Expected: 'foo', 'bar'\s+"
            r"Found: 'foo', 'bar', 'baz'"
        )

        with self.assertRaisesRegex(ToronError, regex):
            command_crosswalk.get_column_positions(
                node1=self.node_a,
                node2=self.node_b,
                crosswalk_name='corge',
                data=[
                    ['A-2', 'X-2', 100.0, 'A-1', 'X-1', '1-1'],
                    ['B-2', 'Y-2', 200.0, 'B-1', 'Y-1', '2-1'],
                    ['C-2', 'Z-2', 300.0, 'C-1', 'Z-1', '3-1'],
                ],
                columns=['foo', 'bar', 'corge', 'foo', 'bar', 'baz'],
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

    def test_missing_crosswalk_column(self):
        regex = r"crosswalk 'blerg' not found in columns: 'index_code', 'corge', 'index_code'"
        with self.assertRaisesRegex(ToronError, regex):
            command_crosswalk.get_column_positions(
                node1=self.node_a,
                node2=self.node_b,
                crosswalk_name='blerg',
                data=[
                    ['1XA0157D6E', 100.0, '1XF7F2FF38'],
                    ['2XF38F26EA', 200.0, '2XA468A4BC'],
                    ['3X7429EDA9', 300.0, '3X23CE6FFF'],
                ],
                columns=['index_code', 'corge', 'index_code'],
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


class TestMakeGetterFunctions(TopoNodeFixtures, unittest.TestCase):
    def test_return_types(self):
        result = command_crosswalk.make_getter_functions(
            node=self.node_a,
            index_code_pos=0,
            sample_header=['index_code', 'foo', 'bar', 'baz', 'corge', 'index_code', 'foo', 'bar'],
            start=0,
            stop=4,
        )
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 3)
        self.assertTrue(callable(result[0]))
        self.assertTrue(callable(result[1]))
        self.assertTrue(callable(result[2]))

    def test_node_get_index_id(self):
        node_get_index_id, _, _ = command_crosswalk.make_getter_functions(
            node=self.node_a,
            index_code_pos=0,
            sample_header=['index_code', 'foo', 'bar', 'baz', 'corge', 'index_code', 'foo', 'bar'],
            start=0,
            stop=4,
        )
        data_list = [
            ['1XA0157D6E', 'A-1', 'X-1', '1-1', 100.0, '1XF7F2FF38', 'A-2', 'X-2'],
            ['2XF38F26EA', 'B-1', 'Y-1', '2-1', 200.0, '2XA468A4BC', 'B-2', 'Y-2'],
            ['3X7429EDA9', 'C-1', 'Z-1', '3-1', 300.0, '3X23CE6FFF', 'C-2', 'Z-2'],
        ]
        index_ids = [node_get_index_id(row) for row in data_list]
        self.assertEqual(index_ids, [1, 2, 3])

        # Missing index_code position.
        node_get_index_id, _, _ = command_crosswalk.make_getter_functions(
            node=self.node_a,
            index_code_pos=None,  # <- Position is None!
            sample_header=['foo', 'bar', 'baz', 'corge', 'index_code', 'foo', 'bar'],
            start=0,
            stop=3,
        )
        data_list = [
            ['A-1', 'X-1', '1-1', 100.0, '1XF7F2FF38', 'A-2', 'X-2'],
            ['B-1', 'Y-1', '2-1', 200.0, '2XA468A4BC', 'B-2', 'Y-2'],
            ['C-1', 'Z-1', '3-1', 300.0, '3X23CE6FFF', 'C-2', 'Z-2'],
        ]
        actual = [node_get_index_id(row) for row in data_list]
        self.assertEqual(actual, [None, None, None])

    def test_node_get_location(self):
        _, node_get_location, _ = command_crosswalk.make_getter_functions(
            node=self.node_a,
            index_code_pos=0,
            sample_header=['index_code', 'foo', 'bar', 'baz', 'corge', 'index_code'],
            start=0,
            stop=4,
        )
        data_list = [
            ['1XA0157D6E', 'A-1', 'X-1', '1-1', 100.0, '1XF7F2FF38'],
            ['2XF38F26EA', 'B-1', 'Y-1', '2-1', 200.0, '2XA468A4BC'],
            ['3X7429EDA9', 'C-1', 'Z-1', '3-1', 300.0, '3X23CE6FFF'],
        ]
        actual = [node_get_location(row) for row in data_list]
        expected = [
            ['A-1', 'X-1', '1-1'],
            ['B-1', 'Y-1', '2-1'],
            ['C-1', 'Z-1', '3-1'],
        ]
        self.assertEqual(actual, expected)

        # No label columns.
        _, node_get_location, _ = command_crosswalk.make_getter_functions(
            node=self.node_a,
            index_code_pos=0,
            sample_header=['index_code', 'corge', 'index_code'],
            start=0,
            stop=1,
        )
        data_list = [
            ['1XA0157D6E', 100.0, '1XF7F2FF38'],
            ['2XF38F26EA', 200.0, '2XA468A4BC'],
            ['3X7429EDA9', 300.0, '3X23CE6FFF'],
        ]
        actual = [node_get_location(row) for row in data_list]
        expected = [
            ['', '', ''],
            ['', '', ''],
            ['', '', ''],
        ]
        self.assertEqual(actual, expected)

    def test_node_get_level(self):
        _, _, node_get_level = command_crosswalk.make_getter_functions(
            node=self.node_a,
            index_code_pos=0,
            sample_header=['index_code', 'foo', 'bar', 'baz', 'corge', 'index_code'],
            start=0,
            stop=4,
        )

        self.assertEqual(
            node_get_level(1, ['A-1', 'X-1', '1-1']),
            BitFlags(1, 1, 1),
        )
        self.assertEqual(
            node_get_level(1, ['', '', '']),
            BitFlags(1, 1, 1),
            msg='when index is given, bitflags should be all ones even if labels are omitted',
        )
        self.assertEqual(
            node_get_level(None, ['A-1', 'X-1', '1-1']),
            BitFlags(1, 1, 1),
        )
        self.assertEqual(
            node_get_level(None, ['A-1', 'X-1', '']),
            BitFlags(1, 1, 0),
        )
        self.assertEqual(
            node_get_level(None, ['A-1', '', '']),
            BitFlags(1, 0, 0),
        )


class TestNormalizeMappingData(TopoNodeFixtures, unittest.TestCase):
    def test_index_codes_and_labels(self):
        actual = command_crosswalk.normalize_mapping_data(
            node1=self.node_a,
            node2=self.node_b,
            crosswalk_name='corge',
            data=[
                ['index_code', 'foo', 'bar', 'baz', 'corge', 'index_code', 'foo', 'bar'],
                ['1XA0157D6E', 'A-1', 'X-1', '1-1',   100.0, '1XF7F2FF38', 'A-2', 'X-2'],
                ['2XF38F26EA', 'B-1', 'Y-1', '2-1',   200.0, '2XA468A4BC', 'B-2', 'Y-2'],
                ['3X7429EDA9', 'C-1', 'Z-1', '3-1',   300.0, '3X23CE6FFF', 'C-2', 'Z-2'],
            ],
        )

        expected = [
            [1, ['A-1', 'X-1', '1-1'], BitFlags(1, 1, 1), 1, ['A-2', 'X-2'], BitFlags(1, 1), 100.0],
            [2, ['B-1', 'Y-1', '2-1'], BitFlags(1, 1, 1), 2, ['B-2', 'Y-2'], BitFlags(1, 1), 200.0],
            [3, ['C-1', 'Z-1', '3-1'], BitFlags(1, 1, 1), 3, ['C-2', 'Z-2'], BitFlags(1, 1), 300.0],
        ]
        self.assertEqual(list(actual), expected)

    def test_input_flipped(self):
        """Regardless of input order, should output node1 (left) node2 (right)."""
        flipped_input_data = [
            ['index_code', 'foo', 'bar', 'corge', 'index_code', 'foo', 'bar', 'baz'],
            ['1XF7F2FF38', 'A-2', 'X-2',   100.0, '1XA0157D6E', 'A-1', 'X-1', '1-1'],
            ['2XA468A4BC', 'B-2', 'Y-2',   200.0, '2XF38F26EA', 'B-1', 'Y-1', '2-1'],
            ['3X23CE6FFF', 'C-2', 'Z-2',   300.0, '3X7429EDA9', 'C-1', 'Z-1', '3-1'],
        ]
        actual = command_crosswalk.normalize_mapping_data(
            node1=self.node_a,
            node2=self.node_b,
            crosswalk_name='corge',
            data=flipped_input_data,  # <- Flipped left-to-right.
        )

        expected = [
            [1, ['A-1', 'X-1', '1-1'], BitFlags(1, 1, 1), 1, ['A-2', 'X-2'], BitFlags(1, 1), 100.0],
            [2, ['B-1', 'Y-1', '2-1'], BitFlags(1, 1, 1), 2, ['B-2', 'Y-2'], BitFlags(1, 1), 200.0],
            [3, ['C-1', 'Z-1', '3-1'], BitFlags(1, 1, 1), 3, ['C-2', 'Z-2'], BitFlags(1, 1), 300.0],
        ]
        self.assertEqual(list(actual), expected, msg='order should be: <node1> <node2> <crosswalk>')

    def test_index_codes_only(self):
        actual = command_crosswalk.normalize_mapping_data(
            node1=self.node_a,
            node2=self.node_b,
            crosswalk_name='corge',
            data=[
                ['index_code', 'corge', 'index_code'],
                ['1XA0157D6E',   100.0, '1XF7F2FF38'],
                ['2XF38F26EA',   200.0, '2XA468A4BC'],
                ['3X7429EDA9',   300.0, '3X23CE6FFF'],
            ],
        )

        expected = [
            [1, ['', '', ''], BitFlags(1, 1, 1), 1, ['', ''], BitFlags(1, 1), 100.0],
            [2, ['', '', ''], BitFlags(1, 1, 1), 2, ['', ''], BitFlags(1, 1), 200.0],
            [3, ['', '', ''], BitFlags(1, 1, 1), 3, ['', ''], BitFlags(1, 1), 300.0],
        ]
        self.assertEqual(list(actual), expected)

    def test_partial_index_codes_and_partial_labels(self):
        actual = command_crosswalk.normalize_mapping_data(
            node1=self.node_a,
            node2=self.node_b,
            crosswalk_name='corge',
            data=[
                ['index_code', 'foo', 'bar', 'baz', 'corge', 'index_code', 'foo', 'bar'],
                [        None, 'A-1',    '',    '',   100.0, '1XF7F2FF38',    '',    ''],
                ['2XF38F26EA', 'B-1', 'Y-1', '2-1',   200.0,         None, 'B-2', 'Y-2'],
                ['3X7429EDA9', 'C-1', 'Z-1', '3-1',   300.0,         None, 'C-2',    ''],
            ],
        )

        expected = [
            [None, ['A-1',    '',    ''], BitFlags(1, 0, 0),    1, [   '',    ''], BitFlags(1, 1), 100.0],
            [   2, ['B-1', 'Y-1', '2-1'], BitFlags(1, 1, 1), None, ['B-2', 'Y-2'], BitFlags(1, 1), 200.0],
            [   3, ['C-1', 'Z-1', '3-1'], BitFlags(1, 1, 1), None, ['C-2',    ''], BitFlags(1, 0), 300.0],
        ]
        self.assertEqual(list(actual), expected)

    def test_immediate_error(self):
        """Should raise errors immediately, rather than waiting for iteration."""
        regex = r"crosswalk 'blerg' not found"
        with self.assertRaisesRegex(ToronError, regex):
            command_crosswalk.normalize_mapping_data(
                node1=self.node_a,
                node2=self.node_b,
                crosswalk_name='blerg',
                data=[
                    ['index_code', 'corge', 'index_code'],
                    ['1XA0157D6E',   100.0, '1XF7F2FF38'],
                    ['2XF38F26EA',   200.0, '2XA468A4BC'],
                    ['3X7429EDA9',   300.0, '3X23CE6FFF'],
                ],
            )


class TestReadFromStdin(TopoNodeFixtures, unittest.TestCase):
    @staticmethod
    def get_relations(source_node, target_node, crosswalk_name):
        with target_node._managed_cursor() as cur:
            relation_repository = target_node._dal.RelationRepository(cur)
            crosswalk = target_node._get_crosswalk(
                source_node,
                crosswalk_name,
                target_node._dal.CrosswalkRepository(cur),
            )
            if not crosswalk:
                raise Exception
            relations = relation_repository.find(crosswalk_id=crosswalk.id)
            return set(astuple(rel) for rel in relations)

    def test_insert_both_directions(self):
        self.node_c.add_crosswalk(node=self.node_d,
                                  crosswalk_name='population',
                                  other_filename_hint='node_d',
                                  is_default=True)

        self.node_d.add_crosswalk(node=self.node_c,
                                  crosswalk_name='population',
                                  other_filename_hint='node_c',
                                  is_default=True)

        args = argparse.Namespace(
            command='crosswalk',
            node1=self.node_c,
            node2=self.node_d,
            crosswalk='population',
            direction='both',
            match_limit=1,
            allow_overlapping=False,
            stdin=DummyRedirection(
                'index_c,population,index_d\n'
                '1X73808335,10,1X583DFB94\n'
                '1X73808335,70,2X0BA7A010\n'
                '2X201AD8B1,20,3X8C016B53\n'
                '2X201AD8B1,60,4XAC931718\n'
                '3XA7BC13F2,30,5X2B35DC5B\n'
                '3XA7BC13F2,50,6X78AF87DF\n'
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_crosswalk.read_from_stdin(args)  # <- Function under test.

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            cm.output,
            ['INFO:app-toron:matching FILE1 index records',
             'INFO:app-toron:matching FILE2 index records',
             'INFO:app-toron:loading relations: FILE1 -> FILE2',
             'INFO:app-toron.node:loaded 6 relations',
             'INFO:app-toron:crosswalk is complete',
             'INFO:app-toron:loading relations: FILE1 <- FILE2',
             'INFO:app-toron.node:loaded 6 relations',
             'INFO:app-toron:crosswalk is complete'],
        )

        self.assertEqual(
            self.get_relations(self.node_c, self.node_d, 'population'),
            {(1, 1, 1, 1, b'\xc0', 10.0, 0.125),
             (2, 1, 1, 2, b'\xc0', 70.0, 0.875),
             (3, 1, 2, 3, b'\xc0', 20.0, 0.25),
             (4, 1, 2, 4, b'\xc0', 60.0, 0.75),
             (5, 1, 3, 5, b'\xc0', 30.0, 0.375),
             (6, 1, 3, 6, b'\xc0', 50.0, 0.625),
             (7, 1, 0, 0,    None,  0.0, 1.0)},
        )

        self.assertEqual(
            self.get_relations(self.node_d, self.node_c, 'population'),
            {(1, 1, 1, 1, b'\x80', 10.0, 1.0),
             (2, 1, 2, 1, b'\x80', 70.0, 1.0),
             (3, 1, 3, 2, b'\x80', 20.0, 1.0),
             (4, 1, 4, 2, b'\x80', 60.0, 1.0),
             (5, 1, 5, 3, b'\x80', 30.0, 1.0),
             (6, 1, 6, 3, b'\x80', 50.0, 1.0),
             (7, 1, 0, 0,    None,  0.0, 1.0)},
        )

    def test_missing_one_side(self):
        self.node_d.add_crosswalk(node=self.node_c,
                                  crosswalk_name='population',
                                  other_filename_hint='node_c',
                                  is_default=True)

        args = argparse.Namespace(
            command='crosswalk',
            node1=self.node_c,
            node2=self.node_d,
            crosswalk='population',
            direction='both',  # <- Direction indicates both, but left-side is missing.
            match_limit=1,
            allow_overlapping=False,
            stdin=DummyRedirection(
                'index_c,population,index_d\n'
                '1X73808335,10,1X583DFB94\n'
                '1X73808335,70,2X0BA7A010\n'
                '2X201AD8B1,20,3X8C016B53\n'
                '2X201AD8B1,60,4XAC931718\n'
                '3XA7BC13F2,30,5X2B35DC5B\n'
                '3XA7BC13F2,50,6X78AF87DF\n'
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_crosswalk.read_from_stdin(args)  # <- Function under test.

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            cm.output,
            ["WARNING:app-toron:no 'population' crosswalk in FILE1",
             'INFO:app-toron:matching FILE1 index records',
             'INFO:app-toron:matching FILE2 index records',
             'INFO:app-toron:loading relations: FILE1 -> FILE2',
             'INFO:app-toron.node:loaded 6 relations',
             'INFO:app-toron:crosswalk is complete'],
        )

        self.assertEqual(
            self.get_relations(self.node_c, self.node_d, 'population'),
            {(1, 1, 1, 1, b'\xc0', 10.0, 0.125),
             (2, 1, 1, 2, b'\xc0', 70.0, 0.875),
             (3, 1, 2, 3, b'\xc0', 20.0, 0.25),
             (4, 1, 2, 4, b'\xc0', 60.0, 0.75),
             (5, 1, 3, 5, b'\xc0', 30.0, 0.375),
             (6, 1, 3, 6, b'\xc0', 50.0, 0.625),
             (7, 1, 0, 0,    None,  0.0, 1.0)},
        )

    def test_missing_both_sides(self):
        args = argparse.Namespace(
            command='crosswalk',
            node1=self.node_c,
            node2=self.node_d,
            crosswalk='population',
            direction='both',
            stdin=DummyRedirection(
                'index_c,population,index_d\n'
                '1X73808335,10,1X583DFB94\n'
                '1X73808335,70,2X0BA7A010\n'
                '2X201AD8B1,20,3X8C016B53\n'
                '2X201AD8B1,60,4XAC931718\n'
                '3XA7BC13F2,30,5X2B35DC5B\n'
                '3XA7BC13F2,50,6X78AF87DF\n'
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_crosswalk.read_from_stdin(args)  # <- Function under test.

        self.assertEqual(exit_code, ExitCode.ERR)

        self.assertEqual(
            cm.output,
            ["ERROR:app-toron:no 'population' crosswalk in FILE1 or FILE2"],
        )

    def test_match_limit_without_overlapping(self):
        self.node_d.add_crosswalk(node=self.node_c,
                                  crosswalk_name='population',
                                  other_filename_hint='node_c',
                                  is_default=True)

        args = argparse.Namespace(
            command='crosswalk',
            node1=self.node_c,
            node2=self.node_d,
            crosswalk='population',
            direction='right',
            match_limit=2,  # <- Allow up to one-to-two matches.
            allow_overlapping=False,  # <- Default (no overlapping allowed).
            stdin=DummyRedirection(
                'index_c,population,index_d,lbl1,lbl2\n'
                '1X73808335,90,,A,\n'             # <- Matched to 2 right-side records.
                '2X201AD8B1,20,3X8C016B53,B,x\n'  # <- Exact match (by index code).
                '2X201AD8B1,60,,B,y\n'            # <- Exact match (by index labels).
                '3XA7BC13F2,28,,C,\n'             # <- Matched to 2 right-side records (2-ambiguous, minus 1-exact overlap).
                '3XA7BC13F2,7,6X78AF87DF,C,y\n'   # <- Exact match (overlaps the records matched on "C" alone).
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_crosswalk.read_from_stdin(args)  # <- Function under test.

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            self.get_relations(self.node_c, self.node_d, 'population'),
            {(1, 1, 1, 1, b'\x80', 22.5, 0.25),  # <- Gets proportion of weight.
             (2, 1, 1, 2, b'\x80', 67.5, 0.75),  # <- Gets proportion of weight.
             (3, 1, 2, 3, b'\xc0', 20.0, 0.25),
             (4, 1, 2, 4, b'\xc0', 60.0, 0.75),
             (5, 1, 3, 5, b'\x80', 28.0, 0.8),   # <- Gets full weight after excluding split created by overlap.
             (6, 1, 3, 6, b'\xc0',  7.0, 0.2),   # <- Exact match that was overlapped.
             (7, 1, 0, 0,    None,  0.0, 1.0)},
        )

        self.assertEqual(
            cm.output,
            ['INFO:app-toron:matching FILE1 index records',
             'INFO:app-toron:matching FILE2 index records',
             'WARNING:app-toron.mapper:omitted 1 ambiguous matches that ' \
                'overlap with records that were already matched at a finer ' \
                'level of granularity',
             'INFO:app-toron:loading relations: FILE1 -> FILE2',
             'INFO:app-toron.node:loaded 6 relations',
             'INFO:app-toron:crosswalk is complete'],
        )


class TestWriteToStdout(TopoNodeFixtures, unittest.TestCase):
    def test_full_mapping(self):
        self.node_d.add_crosswalk(node=self.node_c,
                                  crosswalk_name='population',
                                  other_filename_hint='node_c',
                                  is_default=True)

        self.node_d.insert_relations2(
            self.node_c,
            'population',
            data=[(1, 1, b'\xc0', 10.0),
                  (1, 2, b'\xc0', 70.0),
                  (2, 3, b'\xc0', 20.0),
                  (2, 4, b'\xc0', 60.0),
                  (3, 5, b'\xc0', 30.0),
                  (3, 6, b'\xc0', 50.0),
                  (0, 0,    None,  0.0)],
            columns=['other_index_id', 'index_id', 'mapping_level', 'relation_value'],
        )

        dummy_stdout = DummyRedirection()
        args = argparse.Namespace(
            command='crosswalk',
            node1=self.node_c,
            node2=self.node_d,
            crosswalk='population',
            direction='both',
            stdout=dummy_stdout,
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_crosswalk.write_to_stdout(args)  # <- Function under test.

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            dummy_stdout.getvalue(),
            ('index_code,lbl1,population,index_code,lbl1,lbl2\n'
             '0XF4264876,-,0.0,0XDF9B30D7,-,-\n'
             '1X73808335,A,10.0,1X583DFB94,A,x\n'
             '1X73808335,A,70.0,2X0BA7A010,A,y\n'
             '2X201AD8B1,B,20.0,3X8C016B53,B,x\n'
             '2X201AD8B1,B,60.0,4XAC931718,B,y\n'
             '3XA7BC13F2,C,30.0,5X2B35DC5B,C,x\n'
             '3XA7BC13F2,C,50.0,6X78AF87DF,C,y\n'),
        )

    def test_some_ambiguous_some_disjoint(self):
        self.node_d.add_crosswalk(node=self.node_c,
                                  crosswalk_name='population',
                                  other_filename_hint='node_c',
                                  is_default=True)

        self.node_d.insert_relations2(
            self.node_c,
            'population',
            data=[(1, 1, b'\xc0', 10.0),
                  (1, 2, b'\xc0', 70.0),
                  (2, 3, b'\x80', 20.0),
                  (2, 4, b'\x80', 60.0),
                  # Omitting 3 -> 5
                  # Omitting 3 -> 6
                  (0, 0,    None,  0.0)],
            columns=['other_index_id', 'index_id', 'mapping_level', 'relation_value'],
        )

        dummy_stdout = DummyRedirection()
        args = argparse.Namespace(
            command='crosswalk',
            node1=self.node_c,
            node2=self.node_d,
            crosswalk='population',
            direction='both',
            stdout=dummy_stdout,
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_crosswalk.write_to_stdout(args)  # <- Function under test.

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            dummy_stdout.getvalue(),
            ('index_code,lbl1,population,index_code,lbl1,lbl2,ambiguous_fields\n'
             '0XF4264876,-,0.0,0XDF9B30D7,-,-,\n'
             '1X73808335,A,10.0,1X583DFB94,A,x,\n'
             '1X73808335,A,70.0,2X0BA7A010,A,y,\n'
             '2X201AD8B1,B,20.0,3X8C016B53,B,x,lbl2\n'  # <- 'lbl2' is ambiguous
             '2X201AD8B1,B,60.0,4XAC931718,B,y,lbl2\n'  # <- 'lbl2' is ambiguous
             ',,,5X2B35DC5B,C,x,\n'  # <- Target index_id 5 is disjoint.
             ',,,6X78AF87DF,C,y,\n'  # <- Target index_id 6 is disjoint.
             '3XA7BC13F2,C,,,,,\n'),  # <- Source index_id 3 is disjoint.
        )

    def test_full_disjoint(self):
        self.node_d.add_crosswalk(node=self.node_c,
                                  crosswalk_name='population',
                                  other_filename_hint='node_c',
                                  is_default=True)

        dummy_stdout = DummyRedirection()
        args = argparse.Namespace(
            command='crosswalk',
            node1=self.node_c,
            node2=self.node_d,
            crosswalk='population',
            direction='both',
            stdout=dummy_stdout,
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_crosswalk.write_to_stdout(args)  # <- Function under test.

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            dummy_stdout.getvalue(),
            ('index_code,lbl1,population,index_code,lbl1,lbl2\n'
             '0XF4264876,-,0,0XDF9B30D7,-,-\n'  # <- Undefined records always match to each other.
             ',,,1X583DFB94,A,x\n'
             ',,,2X0BA7A010,A,y\n'
             ',,,3X8C016B53,B,x\n'
             ',,,4XAC931718,B,y\n'
             ',,,5X2B35DC5B,C,x\n'
             ',,,6X78AF87DF,C,y\n'
             '1X73808335,A,,,,\n'
             '2X201AD8B1,B,,,,\n'
             '3XA7BC13F2,C,,,,\n'),
        )
