"""Tests for toron/cli/command_relation.py module."""
import argparse
import os
import tempfile
from dataclasses import astuple
from .. import _unittest as unittest

from ..common import DummyRedirection, DataSpaceFixturesMixin
from toron import DataSpace, read_file, bind_file
from toron._utils import ToronError, BitFlags
from toron.cli.common import ExitCode
from toron.data_models import Link
from toron.cli import command_relation


class TestAddLink(unittest.TestCase):
    def setUp(self):
        with tempfile.NamedTemporaryFile(delete=False) as tmp1:
            self.filepath1 = tmp1.name
        self.addCleanup(os.remove, self.filepath1)
        ds1 = DataSpace()
        with ds1._managed_transaction() as cur:
            property_repo = ds1._dal.PropertyRepository(cur)
            property_repo.add_or_update(
                'unique_id', '11111111-1111-1111-1111-111111111111'
            )
        ds1.to_file(self.filepath1)

        with tempfile.NamedTemporaryFile(delete=False) as tmp2:
            self.filepath2 = tmp2.name
        self.addCleanup(os.remove, self.filepath2)
        ds2 = DataSpace()
        with ds2._managed_transaction() as cur:
            property_repo = ds2._dal.PropertyRepository(cur)
            property_repo.add_or_update(
                'unique_id', '22222222-2222-2222-2222-222222222222'
            )
        ds2.to_file(self.filepath2)

    def test_add_link(self):
        """Add link link in both directions (default behavior)."""
        args = argparse.Namespace(
            filepath=self.filepath1,
            command=':',
            subcommand='create',
            filepath2=self.filepath2,
            link_name='population',
            direction='both',
            description=None,
            selectors=None,
            make_default=True,
        )
        command_relation.add_link(args)  # <- Method under test.

        # Check right-side link (ds1 -> ds2).
        self.assertEqual(
            read_file(self.filepath2).get_link(self.filepath1, 'population'),
            Link(
                id=1,
                other_unique_id='11111111-1111-1111-1111-111111111111',
                other_filename_hint=self.filepath1,
                name='population',
                is_default=True,
            ),
        )

        # Check left-side link (ds1 <- ds2).
        self.assertEqual(
            read_file(self.filepath1).get_link(self.filepath2, 'population'),
            Link(
                id=1,
                other_unique_id='22222222-2222-2222-2222-222222222222',
                other_filename_hint=self.filepath2,
                name='population',
                is_default=True,
            ),
        )

    def test_with_direction(self):
        args = argparse.Namespace(
            filepath=self.filepath1,
            command=':',
            subcommand='create',
            filepath2=self.filepath2,
            link_name='population',
            direction='right',  # <- Right-side link only.
            description=None,
            selectors=None,
            make_default=True,
        )
        command_relation.add_link(args)  # <- Method under test.

        # Check right-side link (ds1 -> ds2).
        self.assertEqual(
            read_file(self.filepath2).get_link(self.filepath1, 'population'),
            Link(
                id=1,
                other_unique_id='11111111-1111-1111-1111-111111111111',
                other_filename_hint=self.filepath1,
                name='population',
                is_default=True,
            ),
        )

        # Check that left-side link (ds1 <- ds2) does not exist.
        regex = r"no links match reference .+'"
        with self.assertRaisesRegex(ToronError, regex):
            read_file(self.filepath1).get_link(self.filepath2, 'population')

    def test_link_already_exists(self):
        ds1 = bind_file(self.filepath1, mode='rw')
        ds2 = bind_file(self.filepath2, mode='rw')
        ds1.add_link(
            space=ds2,
            link_name='population',
            other_filename_hint=ds2.path_hint,
            description=None,
            selectors=None,
            is_default=True,
        )

        args = argparse.Namespace(
            filepath=self.filepath1,
            command=':',
            subcommand='create',
            filepath2=self.filepath2,
            link_name='population',
            direction='both',
            description=None,
            selectors=None,
            make_default=True,
        )

        regex = r"a link named 'population' already exists"
        with self.assertRaisesRegex(ToronError, regex):
            command_relation.add_link(args)  # <- Method under test.


class TestRemoveLink(unittest.TestCase):
    def setUp(self):
        # Create node objects and set `unique_id` values.
        ds1 = DataSpace()
        self.change_unique_id(ds1, '11111111-1111-1111-1111-111111111111')

        ds2 = DataSpace()
        self.change_unique_id(ds2, '22222222-2222-2222-2222-222222222222')

        # Create temporary file locations.
        with tempfile.NamedTemporaryFile(delete=False) as tmp1:
            self.filepath1 = tmp1.name
        self.addCleanup(os.remove, self.filepath1)

        with tempfile.NamedTemporaryFile(delete=False) as tmp2:
            self.filepath2 = tmp2.name
        self.addCleanup(os.remove, self.filepath2)

        # Save nodes to temporary file locations.
        ds1.to_file(self.filepath1)
        ds2.to_file(self.filepath2)

    @staticmethod
    def change_unique_id(node, unique_id):
        """Helper function to specify a `unique_id` for testing."""
        node._connector._unique_id = unique_id
        with node._managed_transaction() as cur:
            property_repo = node._dal.PropertyRepository(cur)
            property_repo.add_or_update('unique_id', unique_id)

    @staticmethod
    def add_link(tail_file, head_file, link_name, is_default=None):
        """Helper function to add links between files for testing."""
        tail = bind_file(tail_file, mode='rw')
        head = bind_file(head_file, mode='rw')
        head.add_link(
            space=tail,
            link_name=link_name,
            other_filename_hint=tail.path_hint,
            is_default=is_default,
        )

    def assertLinkExists(self, tail_file, head_file, link_name, msg=None):
        try:
            read_file(head_file).get_link(read_file(tail_file), link_name)
        except ToronError as e:
            self.fail(msg or str(e))

    def assertLinkNotExists(self, tail_file, head_file, link_name, msg=None):
        try:
            read_file(head_file).get_link(read_file(tail_file), link_name)
            self.fail(msg or f'found unexpected link {link_name!r}')
        except ToronError:
            pass

    def test_remove_both_directions(self):
        self.add_link(self.filepath1, self.filepath2, 'population', is_default=True)
        self.add_link(self.filepath2, self.filepath1, 'population', is_default=True)

        args = argparse.Namespace(
            filepath=self.filepath1,
            command=':',
            filepath2=self.filepath2,
            subcommand='remove',
            link_name='population',
            direction='both',
        )

        exit_code = command_relation.remove_link(args)  # <- Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertLinkNotExists(self.filepath1, self.filepath2, 'population')
        self.assertLinkNotExists(self.filepath2, self.filepath1, 'population')

    def test_remove_left_direction(self):
        self.add_link(self.filepath1, self.filepath2, 'population', is_default=True)
        self.add_link(self.filepath2, self.filepath1, 'population', is_default=True)

        args = argparse.Namespace(
            filepath=self.filepath1,
            command=':',
            filepath2=self.filepath2,
            subcommand='remove',
            link_name='population',
            direction='left',
        )

        exit_code = command_relation.remove_link(args)  # <- Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertLinkExists(self.filepath1, self.filepath2, 'population')
        self.assertLinkNotExists(self.filepath2, self.filepath1, 'population')

    def test_remove_right_direction(self):
        self.add_link(self.filepath1, self.filepath2, 'population', is_default=True)
        self.add_link(self.filepath2, self.filepath1, 'population', is_default=True)

        args = argparse.Namespace(
            filepath=self.filepath1,
            command=':',
            filepath2=self.filepath2,
            subcommand='remove',
            link_name='population',
            direction='right',
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            exit_code = command_relation.remove_link(args)  # <- Function under test.

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            logs_cm.output,
            ["INFO:app-toron:removed 'population' link from FILE2"],
        )

        self.assertLinkNotExists(self.filepath1, self.filepath2, 'population')
        self.assertLinkExists(self.filepath2, self.filepath1, 'population')

    def test_remove_both_directions_one_missing(self):
        self.add_link(self.filepath1, self.filepath2, 'population', is_default=True)

        args = argparse.Namespace(
            filepath=self.filepath1,
            command=':',
            filepath2=self.filepath2,
            subcommand='remove',
            link_name='population',
            direction='both',
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_relation.remove_link(args)  # <- Function under test.

        self.assertEqual(
            logs_cm.output,
            ["INFO:app-toron:no 'population' link found in FILE1", # <- Message about missing link.
             "INFO:app-toron:removed 'population' link from FILE2"],
        )

        self.assertLinkNotExists(self.filepath1, self.filepath2, 'population')
        self.assertLinkNotExists(self.filepath2, self.filepath1, 'population')

    def test_remove_both_directions_both_missing(self):
        args = argparse.Namespace(
            filepath=self.filepath1,
            command=':',
            filepath2=self.filepath2,
            subcommand='remove',
            link_name='population',
            direction='both',
        )

        regex = r"no 'population' link in FILE1 or FILE2"
        with self.assertRaisesRegex(ToronError, regex):
            command_relation.remove_link(args)  # <- Function under test.

    def test_remove_single_directions_missing(self):
        args = argparse.Namespace(
            filepath=self.filepath1,
            command=':',
            filepath2=self.filepath2,
            subcommand='remove',
            link_name='population',
            direction='right',
        )

        regex = r"no 'population' link found in FILE2"
        with self.assertRaisesRegex(ToronError, regex):
            command_relation.remove_link(args)  # <- Function under test.


class TestGetColumnPositions(DataSpaceFixturesMixin, unittest.TestCase):
    def test_simple_case(self):
        header = ['index_code', 'foo', 'bar', 'baz', 'corge', 'index_code', 'foo', 'bar']
        data_list = [
            ['1XA0157D6E', 'A-1', 'X-1', '1-1', 100.0, '1XF7F2FF38', 'A-2', 'X-2'],
            ['2XF38F26EA', 'B-1', 'Y-1', '2-1', 200.0, '2XA468A4BC', 'B-2', 'Y-2'],
            ['3X7429EDA9', 'C-1', 'Z-1', '3-1', 300.0, '3X23CE6FFF', 'C-2', 'Z-2'],
        ]

        result = command_relation.get_column_positions(
            node1=self.node_a,
            node2=self.node_b,
            link_name='corge',
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
        positions, _ = command_relation.get_column_positions(
            node1=self.node_a,
            node2=self.node_b,
            link_name='corge',
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
        positions, _ = command_relation.get_column_positions(
            node1=self.node_a,
            node2=self.node_b,
            link_name='corge',
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

        positions, _ = command_relation.get_column_positions(
            node1=self.node_a,
            node2=self.node_b,
            link_name='corge',
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
            positions, _ = command_relation.get_column_positions(
                node1=self.node_a,
                node2=self.node_b,
                link_name='corge',
                data=[
                    ['1XA0157D6E', 'A-1', 'X-1', '1-1', 100.0, 'A-2', 'X-2'],
                    ['2XF38F26EA', 'B-1', 'Y-1', '2-1', 200.0, 'B-2', 'Y-2'],
                    ['3X7429EDA9', 'C-1', 'Z-1', '3-1', 300.0, 'C-2', 'Z-2'],
                ],
                columns=['index_code', 'foo', 'bar', 'baz', 'corge', 'XXX', 'YYY'],
            )

        regex = r"unable to find FILE1 columns;\s+Expected: 'foo', 'bar', 'baz'\s+Found: 'XXX', 'YYY', 'ZZZ'"
        with self.assertRaisesRegex(ToronError, regex):
            positions, _ = command_relation.get_column_positions(
                node1=self.node_a,
                node2=self.node_b,
                link_name='corge',
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
        positions, _ = command_relation.get_column_positions(
            node1=self.node_a,
            node2=self.node_b,
            link_name='corge',
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
            command_relation.get_column_positions(
                node1=self.node_a,
                node2=self.node_b,
                link_name='corge',
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
            command_relation.get_column_positions(
                node1=self.node_a,
                node2=self.node_b,
                link_name='corge',
                data=[
                    ['1XA0157D6E', '1XF7F2FF38', 100.0],
                    ['2XF38F26EA', '2XA468A4BC', 200.0],
                    ['3X7429EDA9', '3X23CE6FFF', 300.0],
                ],
                columns=['index_code1', 'index_code2', 'corge'],
            )

    def test_missing_link_column(self):
        regex = r"required column 'blerg' not found in: 'index_code', 'corge', 'index_code'"
        with self.assertRaisesRegex(ToronError, regex):
            command_relation.get_column_positions(
                node1=self.node_a,
                node2=self.node_b,
                link_name='blerg',
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
        get_location = command_relation.get_location_factory(
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
        get_location = command_relation.get_location_factory(
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
        get_location = command_relation.get_location_factory(
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
            get_location = command_relation.get_location_factory(
                self.header,
                label_columns=['foo', 'bar', 'baz'],
                start=0,
                stop=6,
            )


class TestMakeGetterFunctions(DataSpaceFixturesMixin, unittest.TestCase):
    def test_return_types(self):
        result = command_relation.make_getter_functions(
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
        node_get_index_id, _, _ = command_relation.make_getter_functions(
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

        # Should raise ToronError if index code is malformed or fails checksum.
        with self.assertRaises(ToronError):
            node_get_index_id(['1XBADVALUE', '', '', '', 100.0, '2XA468A4BC', '', ''])

        # Missing index_code position.
        node_get_index_id, _, _ = command_relation.make_getter_functions(
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
        _, node_get_location, _ = command_relation.make_getter_functions(
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
        _, node_get_location, _ = command_relation.make_getter_functions(
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
        _, _, node_get_level = command_relation.make_getter_functions(
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


class TestNormalizeMappingData(DataSpaceFixturesMixin, unittest.TestCase):
    def test_index_codes_and_labels(self):
        actual = command_relation.normalize_mapping_data(
            node1=self.node_a,
            node2=self.node_b,
            link_name='corge',
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
        actual = command_relation.normalize_mapping_data(
            node1=self.node_a,
            node2=self.node_b,
            link_name='corge',
            data=flipped_input_data,  # <- Flipped left-to-right.
        )

        expected = [
            [1, ['A-1', 'X-1', '1-1'], BitFlags(1, 1, 1), 1, ['A-2', 'X-2'], BitFlags(1, 1), 100.0],
            [2, ['B-1', 'Y-1', '2-1'], BitFlags(1, 1, 1), 2, ['B-2', 'Y-2'], BitFlags(1, 1), 200.0],
            [3, ['C-1', 'Z-1', '3-1'], BitFlags(1, 1, 1), 3, ['C-2', 'Z-2'], BitFlags(1, 1), 300.0],
        ]
        self.assertEqual(list(actual), expected, msg='order should be: <node1> <node2> <link>')

    def test_index_codes_only(self):
        actual = command_relation.normalize_mapping_data(
            node1=self.node_a,
            node2=self.node_b,
            link_name='corge',
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
        actual = command_relation.normalize_mapping_data(
            node1=self.node_a,
            node2=self.node_b,
            link_name='corge',
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
        regex = r"required column 'blerg' not found"
        with self.assertRaisesRegex(ToronError, regex):
            command_relation.normalize_mapping_data(
                node1=self.node_a,
                node2=self.node_b,
                link_name='blerg',
                data=[
                    ['index_code', 'corge', 'index_code'],
                    ['1XA0157D6E',   100.0, '1XF7F2FF38'],
                    ['2XF38F26EA',   200.0, '2XA468A4BC'],
                    ['3X7429EDA9',   300.0, '3X23CE6FFF'],
                ],
            )


class TestRelationImportRecords(DataSpaceFixturesMixin, unittest.TestCase):
    @staticmethod
    def get_mappings(source_node, target_node, link_name):
        with target_node._managed_cursor() as cur:
            mapping_repo = target_node._dal.MappingRepository(cur)
            link = target_node._get_link(
                source_node,
                link_name,
                target_node._dal.LinkRepository(cur),
            )
            if not link:
                raise Exception
            mappings = mapping_repo.find(link_id=link.id)
            return set(astuple(rel) for rel in mappings)

    def test_insert_both_directions(self):
        self.node_c.add_link(space=self.node_d,
                             link_name='population',
                             other_filename_hint='node_d',
                             is_default=True)

        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_relation._import_records(  # <- Function under test.
                ds1=self.node_c,
                ds2=self.node_d,
                link_name='population',
                reader=[
                    ['index_c', 'population', 'index_d'],
                    ['0XF4264876',  '0', '0XDF9B30D7'],
                    ['1X73808335', '18', '1X583DFB94'],
                    ['1X73808335', '46', '2X0BA7A010'],
                    ['0XF4264876', '34', '2X0BA7A010'],
                    ['2X201AD8B1', '20', '3X8C016B53'],
                    ['2X201AD8B1', '10', '0XDF9B30D7'],
                    ['2X201AD8B1', '50', '4XAC931718'],
                    ['3XA7BC13F2', '30', '5X2B35DC5B'],
                    ['3XA7BC13F2', '50', '6X78AF87DF'],
                ],
                direction='both',
                match_limit=1,
                allow_overlapping=False,
                allow_incomplete=False,
            )

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            cm.output,
            ['INFO:app-toron:matching FILE1 index records',
             'INFO:app-toron:matching FILE2 index records',
             'INFO:app-toron:loading mappings: FILE1 -> FILE2',
             'INFO:app-toron.space:loaded 8 mappings',
             'INFO:app-toron:mapping is complete',
             'INFO:app-toron:loading mappings: FILE1 <- FILE2',
             'INFO:app-toron.space:loaded 8 mappings',
             'INFO:app-toron:mapping is complete'],
        )

        self.assertEqual(
            self.get_mappings(self.node_c, self.node_d, 'population'),
            {(1, 1, 0, 2, b'\xc0', 34.0, 0.00000),
             (2, 1, 1, 1, b'\xc0', 18.0, 0.28125),
             (3, 1, 1, 2, b'\xc0', 46.0, 0.71875),
             (4, 1, 2, 0, b'\xc0', 10.0, 0.12500),
             (5, 1, 2, 3, b'\xc0', 20.0, 0.25000),
             (6, 1, 2, 4, b'\xc0', 50.0, 0.62500),
             (7, 1, 3, 5, b'\xc0', 30.0, 0.37500),
             (8, 1, 3, 6, b'\xc0', 50.0, 0.62500)},
        )

        self.assertEqual(
            self.get_mappings(self.node_d, self.node_c, 'population'),
            {(1, 1, 0, 2, b'\x80', 10.0, 0.000),
             (2, 1, 1, 1, b'\x80', 18.0, 1.000),
             (3, 1, 2, 0, b'\x80', 34.0, 0.425),
             (4, 1, 2, 1, b'\x80', 46.0, 0.575),
             (5, 1, 3, 2, b'\x80', 20.0, 1.000),
             (6, 1, 4, 2, b'\x80', 50.0, 1.000),
             (7, 1, 5, 3, b'\x80', 30.0, 1.000),
             (8, 1, 6, 3, b'\x80', 50.0, 1.000)},
        )

    def test_insert_both_directions_with_undefined_cases(self):
        self.node_c.add_link(space=self.node_d,
                             link_name='population',
                             other_filename_hint='node_d',
                             is_default=True)

        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_relation._import_records(  # <- Function under test.
                ds1=self.node_c,
                ds2=self.node_d,
                link_name='population',
                reader=[
                    ['index_c', 'population', 'index_d'],
                    ['0XF4264876', '0', '0XDF9B30D7'],   # <- From undefined, to undefined.
                    ['0XF4264876', '18', '1X583DFB94'],  # <- From undefined, to defined (exlusive)
                    ['1X73808335', '18', '0XDF9B30D7'],  # <- From defined, to undefined (exlusive)
                    ['2X201AD8B1', '10', '2X0BA7A010'],
                    ['0XF4264876', '10', '2X0BA7A010'],  # <- From undefined, to defined (non-exclusive)
                    ['3XA7BC13F2', '20', '3X8C016B53'],
                    ['3XA7BC13F2', '12', '0XDF9B30D7'],  # <- From defined, to undefined (non-exclusive)
                    ['0XF4264876', '45', '4XAC931718'],  # <- From undefined, to defined (exlusive)
                    ['0XF4264876', '29', '5X2B35DC5B'],  # <- From undefined, to defined (exlusive)
                    ['0XF4264876', '50', '6X78AF87DF'],  # <- From undefined, to defined (exlusive)
                ],
                direction='both',
                match_limit=1,
                allow_overlapping=False,
                allow_incomplete=False,
            )

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            cm.output,
            ['INFO:app-toron:matching FILE1 index records',
             'INFO:app-toron:matching FILE2 index records',
             'INFO:app-toron:loading mappings: FILE1 -> FILE2',
             'INFO:app-toron.space:loaded 9 mappings',
             'INFO:app-toron:mapping is complete',
             'INFO:app-toron:loading mappings: FILE1 <- FILE2',
             'INFO:app-toron.space:loaded 9 mappings',
             'INFO:app-toron:mapping is complete'],
        )

        self.assertEqual(
            self.get_mappings(self.node_c, self.node_d, 'population'),
            {(1, 1, 0, 1, b'\xc0', 18.0, 0.000),   # <- From undefined, to defined.
             (2, 1, 0, 2, b'\xc0', 10.0, 0.000),   # <- From undefined, to defined.
             (3, 1, 0, 4, b'\xc0', 45.0, 0.000),   # <- From undefined, to defined.
             (4, 1, 0, 5, b'\xc0', 29.0, 0.000),   # <- From undefined, to defined.
             (5, 1, 0, 6, b'\xc0', 50.0, 0.000),   # <- From undefined, to defined.
             (6, 1, 1, 0, b'\xc0', 18.0, 1.000),   # <- From defined, to undefined.
             (7, 1, 2, 2, b'\xc0', 10.0, 1.000),
             (8, 1, 3, 0, b'\xc0', 12.0, 0.375),   # <- From defined, to undefined.
             (9, 1, 3, 3, b'\xc0', 20.0, 0.625)},
        )

        self.assertEqual(
            self.get_mappings(self.node_d, self.node_c, 'population'),
            {(1, 1, 0, 1, b'\x80', 18.0, 0.000),   # <- From undefined, to defined.
             (2, 1, 0, 3, b'\x80', 12.0, 0.000),   # <- From undefined, to defined.
             (3, 1, 1, 0, b'\x80', 18.0, 1.000),   # <- From defined, to undefined.
             (4, 1, 2, 0, b'\x80', 10.0, 0.500),   # <- From defined, to undefined.
             (5, 1, 2, 2, b'\x80', 10.0, 0.500),
             (6, 1, 3, 3, b'\x80', 20.0, 1.000),
             (7, 1, 4, 0, b'\x80', 45.0, 1.000),   # <- From defined, to undefined.
             (8, 1, 5, 0, b'\x80', 29.0, 1.000),   # <- From defined, to undefined.
             (9, 1, 6, 0, b'\x80', 50.0, 1.000)},  # <- From defined, to undefined.
        )

    def test_missing_one_side(self):
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_relation._import_records(  # <- Function under test.
                ds1=self.node_c,
                ds2=self.node_d,
                link_name='population',
                reader=[
                    ['index_c', 'population', 'index_d'],
                    ['1X73808335', '10', '1X583DFB94'],
                    ['1X73808335', '70', '2X0BA7A010'],
                    ['2X201AD8B1', '20', '3X8C016B53'],
                    ['2X201AD8B1', '60', '4XAC931718'],
                    ['3XA7BC13F2', '30', '5X2B35DC5B'],
                    ['3XA7BC13F2', '50', '6X78AF87DF'],
                ],
                direction='both',  # <- Direction indicates both, but left-side is missing.
                match_limit=1,
                allow_overlapping=False,
                allow_incomplete=False,
            )

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            cm.output,
            ["WARNING:app-toron:no 'population' link from FILE2 to FILE1",
             "INFO:app-toron:matching FILE1 index records",
             "INFO:app-toron:matching FILE2 index records",
             "INFO:app-toron:loading mappings: FILE1 -> FILE2",
             "INFO:app-toron.space:loaded 6 mappings",
             "INFO:app-toron:mapping is complete"],
        )

        self.assertEqual(
            self.get_mappings(self.node_c, self.node_d, 'population'),
            {(1, 1, 1, 1, b'\xc0', 10.0, 0.125),
             (2, 1, 1, 2, b'\xc0', 70.0, 0.875),
             (3, 1, 2, 3, b'\xc0', 20.0, 0.25),
             (4, 1, 2, 4, b'\xc0', 60.0, 0.75),
             (5, 1, 3, 5, b'\xc0', 30.0, 0.375),
             (6, 1, 3, 6, b'\xc0', 50.0, 0.625)},
        )

    def test_missing_both_sides(self):
        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_relation._import_records(  # <- Function under test.
                ds1=self.node_c,
                ds2=self.node_d,
                link_name='population',
                reader=[
                    ['index_c', 'population', 'index_d'],
                    ['1X73808335', '10', '1X583DFB94'],
                    ['1X73808335', '70', '2X0BA7A010'],
                    ['2X201AD8B1', '20', '3X8C016B53'],
                    ['2X201AD8B1', '60', '4XAC931718'],
                    ['3XA7BC13F2', '30', '5X2B35DC5B'],
                    ['3XA7BC13F2', '50', '6X78AF87DF'],
                ],
                direction='both',
                match_limit=1,
                allow_overlapping=False,
                allow_incomplete=False,
            )

        self.assertEqual(exit_code, ExitCode.ERR)

        self.assertEqual(
            cm.output,
            ["ERROR:app-toron:no 'population' link exists between FILE1 "
                 "and FILE2 in either direction"],
        )

    def test_match_limit_without_overlapping(self):
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_relation._import_records(  # <- Function under test.
                ds1=self.node_c,
                ds2=self.node_d,
                link_name='population',
                reader=[
                    ['index_c', 'population', 'index_d', 'lbl1', 'lbl2'],
                    ['1X73808335', '90', '', 'A', ''],             # <- Matched to 2 right-side records.
                    ['2X201AD8B1', '20', '3X8C016B53', 'B', 'x'],  # <- Exact match (by index code).
                    ['2X201AD8B1', '60', '', 'B', 'y'],            # <- Exact match (by index labels).
                    ['3XA7BC13F2', '28', '', 'C', ''],             # <- Matched to 2 right-side records (2-ambiguous, minus 1-exact overlap).
                    ['3XA7BC13F2', '7', '6X78AF87DF', 'C', 'y'],   # <- Exact match (overlaps the records matched on "C" alone).
                ],
                direction='right',
                match_limit=2,  # <- Allow up to one-to-two matches.
                allow_overlapping=False,  # <- Default (no overlapping allowed).
                allow_incomplete=False,
            )

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            self.get_mappings(self.node_c, self.node_d, 'population'),
            {(1, 1, 1, 1, b'\x80', 22.5, 0.25),  # <- Gets proportion of weight.
             (2, 1, 1, 2, b'\x80', 67.5, 0.75),  # <- Gets proportion of weight.
             (3, 1, 2, 3, b'\xc0', 20.0, 0.25),
             (4, 1, 2, 4, b'\xc0', 60.0, 0.75),
             (5, 1, 3, 5, b'\x80', 28.0, 0.8),   # <- Gets full weight after excluding split created by overlap.
             (6, 1, 3, 6, b'\xc0',  7.0, 0.2)},  # <- Exact match that was overlapped.
        )

        self.assertEqual(
            cm.output,
            ['INFO:app-toron:matching FILE1 index records',
             'INFO:app-toron:matching FILE2 index records',
             'WARNING:app-toron.mapper:omitted 1 ambiguous matches that ' \
                'overlap with records that were already matched at a finer ' \
                'level of granularity',
             'INFO:app-toron:loading mappings: FILE1 -> FILE2',
             'INFO:app-toron.space:loaded 6 mappings',
             'INFO:app-toron:mapping is complete'],
        )

    def test_match_limit_with_allow_overlapping(self):
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_relation._import_records(  # <- Function under test.
                ds1=self.node_c,
                ds2=self.node_d,
                link_name='population',
                reader=[
                    ['index_c', 'population', 'index_d', 'lbl1', 'lbl2'],
                    ['1X73808335', '90', '', 'A', ''],             # <- Matched to 2 right-side records.
                    ['2X201AD8B1', '20', '3X8C016B53', 'B', 'x'],  # <- Exact match (by index code).
                    ['2X201AD8B1', '60', '', 'B', 'y'],            # <- Exact match (by index labels).
                    ['3XA7BC13F2', '28', '', 'C', ''],             # <- Matched to 2 right-side records (2-ambiguous, minus 1-exact overlap).
                    ['3XA7BC13F2', '7', '6X78AF87DF', 'C', 'y'],   # <- Exact match (overlaps the records matched on "C" alone).
                ],
                direction='right',
                match_limit=2,  # <- Allow up to one-to-two matches.
                allow_overlapping=True,  # <- Allowing overlaps.
                allow_incomplete=False,
            )

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            self.get_mappings(self.node_c, self.node_d, 'population'),
            {(1, 1, 1, 1, b'\x80', 22.5,   0.25),   # <- Gets proportion of weight.
             (2, 1, 1, 2, b'\x80', 67.5,   0.75),   # <- Gets proportion of weight.
             (3, 1, 2, 3, b'\xc0', 20.0,   0.25),
             (4, 1, 2, 4, b'\xc0', 60.0,   0.75),
             (5, 1, 3, 5, b'\x80', 11.375, 0.325),  # <- Gets proportion of weight.
             (6, 1, 3, 6, b'\x80', 16.625, 0.475),  # <- Gets proportion of weight, overlaps with exact match `3, 6`.
             (7, 1, 3, 6, b'\xc0',  7.0,   0.2)},   # <- Exact match overlapped by ambiguous match.
        )

        self.assertEqual(
            cm.output,
            ['INFO:app-toron:matching FILE1 index records',
             'INFO:app-toron:matching FILE2 index records',
             'INFO:app-toron.mapper:included 1 ambiguous matches that ' \
                'overlap with records that were also matched at a finer ' \
                'level of granularity',
             'INFO:app-toron:loading mappings: FILE1 -> FILE2',
             'INFO:app-toron.space:loaded 7 mappings',
             'INFO:app-toron:mapping is complete'],
        )

    def test_incomplete_match_error(self):
        """Default behavior is for incomplete matches to trigger an error."""
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        regex = r'mapping is incomplete, no records loaded'
        with self.assertRaisesRegex(ToronError, regex):
            exit_code = command_relation._import_records(  # <- Function under test.
                ds1=self.node_c,
                ds2=self.node_d,
                link_name='population',
                reader=[
                    ['index_c', 'population', 'index_d'],
                    ['0XF4264876',  '0', '0XDF9B30D7'],
                    ['1X73808335', '50', '1X583DFB94'],
                    ['1X73808335', '50', '2X0BA7A010'],
                    ['3XA7BC13F2', '50', '6X78AF87DF'],
                ],
                direction='right',
                match_limit=1,
                allow_overlapping=False,
                allow_incomplete=False,  # <- Default (incomplete not allowed).
            )

    def test_incomplete_match_allowed(self):
        """Incomplete matches can be loaded with `--allow-incomplete`."""
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_relation._import_records(  # <- Function under test.
                ds1=self.node_c,
                ds2=self.node_d,
                link_name='population',
                reader=[
                    ['index_c', 'population', 'index_d'],
                    ['0XF4264876',  '0', '0XDF9B30D7'],
                    ['1X73808335', '50', '1X583DFB94'],
                    ['1X73808335', '50', '2X0BA7A010'],
                    ['3XA7BC13F2', '50', '6X78AF87DF'],
                ],
                direction='right',
                match_limit=1,
                allow_overlapping=False,
                allow_incomplete=True,  # <- Allowing incomplete matches.
            )

        msg = 'when using --allow-incomplete, process should load partial matches'
        self.assertEqual(exit_code, ExitCode.OK, msg=msg)

        self.assertEqual(
            cm.output,
            ['INFO:app-toron:matching FILE1 index records',
             'INFO:app-toron:matching FILE2 index records',
             'INFO:app-toron:loading mappings: FILE1 -> FILE2',
             'INFO:app-toron.space:loaded 3 mappings',
             'WARNING:app-toron:mapping is incomplete'],
        )

        self.assertEqual(
            self.get_mappings(self.node_c, self.node_d, 'population'),
            {(1, 1, 1, 1, b'\xc0', 50.0, 0.5),
             (2, 1, 1, 2, b'\xc0', 50.0, 0.5),
             (3, 1, 3, 6, b'\xc0', 50.0, 1.0)},
        )


class TestReadFromStdin(DataSpaceFixturesMixin, unittest.TestCase):
    @staticmethod
    def get_mappings(source_node, target_node, link_name):
        with target_node._managed_cursor() as cur:
            mapping_repo = target_node._dal.MappingRepository(cur)
            link = target_node._get_link(
                source_node,
                link_name,
                target_node._dal.LinkRepository(cur),
            )
            if not link:
                raise Exception
            mappings = mapping_repo.find(link_id=link.id)
            return set(astuple(rel) for rel in mappings)

    def test_insert_both_directions(self):
        self.node_c.add_link(space=self.node_d,
                             link_name='population',
                             other_filename_hint='node_d',
                             is_default=True)

        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        args = argparse.Namespace(
            link_name='population',
            direction='both',
            match_limit=1,
            allow_overlapping=False,
            allow_incomplete=False,
            stdin=DummyRedirection(
                'index_c,population,index_d\n'
                '0XF4264876,0,0XDF9B30D7\n'
                '1X73808335,18,1X583DFB94\n'
                '1X73808335,46,2X0BA7A010\n'
                '0XF4264876,34,2X0BA7A010\n'
                '2X201AD8B1,20,3X8C016B53\n'
                '2X201AD8B1,10,0XDF9B30D7\n'
                '2X201AD8B1,50,4XAC931718\n'
                '3XA7BC13F2,30,5X2B35DC5B\n'
                '3XA7BC13F2,50,6X78AF87DF\n'
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_relation.read_from_stdin(  # <- Function under test.
                args,
                self.node_c,
                self.node_d
            )

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            cm.output,
            ['INFO:app-toron:matching FILE1 index records',
             'INFO:app-toron:matching FILE2 index records',
             'INFO:app-toron:loading mappings: FILE1 -> FILE2',
             'INFO:app-toron.space:loaded 8 mappings',
             'INFO:app-toron:mapping is complete',
             'INFO:app-toron:loading mappings: FILE1 <- FILE2',
             'INFO:app-toron.space:loaded 8 mappings',
             'INFO:app-toron:mapping is complete'],
        )

        self.assertEqual(
            self.get_mappings(self.node_c, self.node_d, 'population'),
            {(1, 1, 0, 2, b'\xc0', 34.0, 0.00000),
             (2, 1, 1, 1, b'\xc0', 18.0, 0.28125),
             (3, 1, 1, 2, b'\xc0', 46.0, 0.71875),
             (4, 1, 2, 0, b'\xc0', 10.0, 0.12500),
             (5, 1, 2, 3, b'\xc0', 20.0, 0.25000),
             (6, 1, 2, 4, b'\xc0', 50.0, 0.62500),
             (7, 1, 3, 5, b'\xc0', 30.0, 0.37500),
             (8, 1, 3, 6, b'\xc0', 50.0, 0.62500)},
        )

        self.assertEqual(
            self.get_mappings(self.node_d, self.node_c, 'population'),
            {(1, 1, 0, 2, b'\x80', 10.0, 0.000),
             (2, 1, 1, 1, b'\x80', 18.0, 1.000),
             (3, 1, 2, 0, b'\x80', 34.0, 0.425),
             (4, 1, 2, 1, b'\x80', 46.0, 0.575),
             (5, 1, 3, 2, b'\x80', 20.0, 1.000),
             (6, 1, 4, 2, b'\x80', 50.0, 1.000),
             (7, 1, 5, 3, b'\x80', 30.0, 1.000),
             (8, 1, 6, 3, b'\x80', 50.0, 1.000)},
        )

    def test_insert_both_directions_with_undefined_cases(self):
        self.node_c.add_link(space=self.node_d,
                             link_name='population',
                             other_filename_hint='node_d',
                             is_default=True)

        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        args = argparse.Namespace(
            link_name='population',
            direction='both',
            match_limit=1,
            allow_overlapping=False,
            allow_incomplete=False,
            stdin=DummyRedirection(
                'index_c,population,index_d\n'
                '0XF4264876,0,0XDF9B30D7\n'   # <- From undefined, to undefined.
                '0XF4264876,18,1X583DFB94\n'  # <- From undefined, to defined (exlusive)
                '1X73808335,18,0XDF9B30D7\n'  # <- From defined, to undefined (exlusive)
                '2X201AD8B1,10,2X0BA7A010\n'
                '0XF4264876,10,2X0BA7A010\n'  # <- From undefined, to defined (non-exclusive)
                '3XA7BC13F2,20,3X8C016B53\n'
                '3XA7BC13F2,12,0XDF9B30D7\n'  # <- From defined, to undefined (non-exclusive)
                '0XF4264876,45,4XAC931718\n'  # <- From undefined, to defined (exlusive)
                '0XF4264876,29,5X2B35DC5B\n'  # <- From undefined, to defined (exlusive)
                '0XF4264876,50,6X78AF87DF\n'  # <- From undefined, to defined (exlusive)
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_relation.read_from_stdin(  # <- Function under test.
                args,
                self.node_c,
                self.node_d,
            )

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            cm.output,
            ['INFO:app-toron:matching FILE1 index records',
             'INFO:app-toron:matching FILE2 index records',
             'INFO:app-toron:loading mappings: FILE1 -> FILE2',
             'INFO:app-toron.space:loaded 9 mappings',
             'INFO:app-toron:mapping is complete',
             'INFO:app-toron:loading mappings: FILE1 <- FILE2',
             'INFO:app-toron.space:loaded 9 mappings',
             'INFO:app-toron:mapping is complete'],
        )

        self.assertEqual(
            self.get_mappings(self.node_c, self.node_d, 'population'),
            {(1, 1, 0, 1, b'\xc0', 18.0, 0.000),   # <- From undefined, to defined.
             (2, 1, 0, 2, b'\xc0', 10.0, 0.000),   # <- From undefined, to defined.
             (3, 1, 0, 4, b'\xc0', 45.0, 0.000),   # <- From undefined, to defined.
             (4, 1, 0, 5, b'\xc0', 29.0, 0.000),   # <- From undefined, to defined.
             (5, 1, 0, 6, b'\xc0', 50.0, 0.000),   # <- From undefined, to defined.
             (6, 1, 1, 0, b'\xc0', 18.0, 1.000),   # <- From defined, to undefined.
             (7, 1, 2, 2, b'\xc0', 10.0, 1.000),
             (8, 1, 3, 0, b'\xc0', 12.0, 0.375),   # <- From defined, to undefined.
             (9, 1, 3, 3, b'\xc0', 20.0, 0.625)},
        )

        self.assertEqual(
            self.get_mappings(self.node_d, self.node_c, 'population'),
            {(1, 1, 0, 1, b'\x80', 18.0, 0.000),   # <- From undefined, to defined.
             (2, 1, 0, 3, b'\x80', 12.0, 0.000),   # <- From undefined, to defined.
             (3, 1, 1, 0, b'\x80', 18.0, 1.000),   # <- From defined, to undefined.
             (4, 1, 2, 0, b'\x80', 10.0, 0.500),   # <- From defined, to undefined.
             (5, 1, 2, 2, b'\x80', 10.0, 0.500),
             (6, 1, 3, 3, b'\x80', 20.0, 1.000),
             (7, 1, 4, 0, b'\x80', 45.0, 1.000),   # <- From defined, to undefined.
             (8, 1, 5, 0, b'\x80', 29.0, 1.000),   # <- From defined, to undefined.
             (9, 1, 6, 0, b'\x80', 50.0, 1.000)},  # <- From defined, to undefined.
        )

    def test_missing_one_side(self):
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        args = argparse.Namespace(
            link_name='population',
            direction='both',  # <- Direction indicates both, but left-side is missing.
            match_limit=1,
            allow_overlapping=False,
            allow_incomplete=False,
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
            exit_code = command_relation.read_from_stdin(  # <- Function under test.
                args,
                self.node_c,
                self.node_d,
            )

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            cm.output,
            ["WARNING:app-toron:no 'population' link from FILE2 to FILE1",
             "INFO:app-toron:matching FILE1 index records",
             "INFO:app-toron:matching FILE2 index records",
             "INFO:app-toron:loading mappings: FILE1 -> FILE2",
             "INFO:app-toron.space:loaded 6 mappings",
             "INFO:app-toron:mapping is complete"],
        )

        self.assertEqual(
            self.get_mappings(self.node_c, self.node_d, 'population'),
            {(1, 1, 1, 1, b'\xc0', 10.0, 0.125),
             (2, 1, 1, 2, b'\xc0', 70.0, 0.875),
             (3, 1, 2, 3, b'\xc0', 20.0, 0.25),
             (4, 1, 2, 4, b'\xc0', 60.0, 0.75),
             (5, 1, 3, 5, b'\xc0', 30.0, 0.375),
             (6, 1, 3, 6, b'\xc0', 50.0, 0.625)},
        )

    def test_missing_both_sides(self):
        args = argparse.Namespace(
            link_name='population',
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
            exit_code = command_relation.read_from_stdin(  # <- Function under test.
                args,
                self.node_c,
                self.node_d,
            )

        self.assertEqual(exit_code, ExitCode.ERR)

        self.assertEqual(
            cm.output,
            ["ERROR:app-toron:no 'population' link exists between FILE1 "
                 "and FILE2 in either direction"],
        )

    def test_match_limit_without_overlapping(self):
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        args = argparse.Namespace(
            link_name='population',
            direction='right',
            match_limit=2,  # <- Allow up to one-to-two matches.
            allow_overlapping=False,  # <- Default (no overlapping allowed).
            allow_incomplete=False,
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
            exit_code = command_relation.read_from_stdin(  # <- Function under test.
                args,
                self.node_c,
                self.node_d,
            )

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            self.get_mappings(self.node_c, self.node_d, 'population'),
            {(1, 1, 1, 1, b'\x80', 22.5, 0.25),  # <- Gets proportion of weight.
             (2, 1, 1, 2, b'\x80', 67.5, 0.75),  # <- Gets proportion of weight.
             (3, 1, 2, 3, b'\xc0', 20.0, 0.25),
             (4, 1, 2, 4, b'\xc0', 60.0, 0.75),
             (5, 1, 3, 5, b'\x80', 28.0, 0.8),   # <- Gets full weight after excluding split created by overlap.
             (6, 1, 3, 6, b'\xc0',  7.0, 0.2)},  # <- Exact match that was overlapped.
        )

        self.assertEqual(
            cm.output,
            ['INFO:app-toron:matching FILE1 index records',
             'INFO:app-toron:matching FILE2 index records',
             'WARNING:app-toron.mapper:omitted 1 ambiguous matches that ' \
                'overlap with records that were already matched at a finer ' \
                'level of granularity',
             'INFO:app-toron:loading mappings: FILE1 -> FILE2',
             'INFO:app-toron.space:loaded 6 mappings',
             'INFO:app-toron:mapping is complete'],
        )

    def test_match_limit_with_allow_overlapping(self):
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        args = argparse.Namespace(
            link_name='population',
            direction='right',
            match_limit=2,  # <- Allow up to one-to-two matches.
            allow_overlapping=True,  # <- Allowing overlaps.
            allow_incomplete=False,
            stdin=DummyRedirection(
                'index_c,population,index_d,lbl1,lbl2\n'
                '1X73808335,90,,A,\n'             # <- Matched to 2 right-side records.
                '2X201AD8B1,20,3X8C016B53,B,x\n'  # <- Exact match (by index code).
                '2X201AD8B1,60,,B,y\n'            # <- Exact match (by index labels).
                '3XA7BC13F2,28,,C,\n'             # <- Matched to 2 right-side records (one of which is an overlap).
                '3XA7BC13F2,7,6X78AF87DF,C,y\n'   # <- Exact match (overlaps the records matched on "C" alone).
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_relation.read_from_stdin(  # <- Function under test.
                args,
                self.node_c,
                self.node_d,
            )

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            self.get_mappings(self.node_c, self.node_d, 'population'),
            {(1, 1, 1, 1, b'\x80', 22.5,   0.25),   # <- Gets proportion of weight.
             (2, 1, 1, 2, b'\x80', 67.5,   0.75),   # <- Gets proportion of weight.
             (3, 1, 2, 3, b'\xc0', 20.0,   0.25),
             (4, 1, 2, 4, b'\xc0', 60.0,   0.75),
             (5, 1, 3, 5, b'\x80', 11.375, 0.325),  # <- Gets proportion of weight.
             (6, 1, 3, 6, b'\x80', 16.625, 0.475),  # <- Gets proportion of weight, overlaps with exact match `3, 6`.
             (7, 1, 3, 6, b'\xc0',  7.0,   0.2)},   # <- Exact match overlapped by ambiguous match.
        )

        self.assertEqual(
            cm.output,
            ['INFO:app-toron:matching FILE1 index records',
             'INFO:app-toron:matching FILE2 index records',
             'INFO:app-toron.mapper:included 1 ambiguous matches that ' \
                'overlap with records that were also matched at a finer ' \
                'level of granularity',
             'INFO:app-toron:loading mappings: FILE1 -> FILE2',
             'INFO:app-toron.space:loaded 7 mappings',
             'INFO:app-toron:mapping is complete'],
        )

    def test_incomplete_match_error(self):
        """Default behavior is for incomplete matches to trigger an error."""
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        args = argparse.Namespace(
            link_name='population',
            direction='right',
            match_limit=1,
            allow_overlapping=False,
            allow_incomplete=False,  # <- Default (incomplete not allowed).
            stdin=DummyRedirection(
                'index_c,population,index_d\n'
                '0XF4264876,0,0XDF9B30D7\n'
                '1X73808335,50,1X583DFB94\n'
                '1X73808335,50,2X0BA7A010\n'
                '3XA7BC13F2,50,6X78AF87DF\n'
            ),
        )

        regex = r'mapping is incomplete, no records loaded'
        with self.assertRaisesRegex(ToronError, regex):
            command_relation.read_from_stdin(  # <- Function under test.
                args,
                self.node_c,
                self.node_d,
            )

    def test_incomplete_match_allowed(self):
        """Incomplete matches can be loaded with ``--allow-incomplete``."""
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        args = argparse.Namespace(
            link_name='population',
            direction='right',
            match_limit=1,
            allow_overlapping=False,
            allow_incomplete=True,  # <- Allowing incomplete matches.
            stdin=DummyRedirection(
                'index_c,population,index_d\n'
                '0XF4264876,0,0XDF9B30D7\n'
                '1X73808335,50,1X583DFB94\n'
                '1X73808335,50,2X0BA7A010\n'
                '3XA7BC13F2,50,6X78AF87DF\n'
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_relation.read_from_stdin(  # <- Function under test.
                args,
                self.node_c,
                self.node_d,
            )

        msg = 'when using --allow-incomplete, process should load partial matches'
        self.assertEqual(exit_code, ExitCode.OK, msg=msg)

        self.assertEqual(
            cm.output,
            ['INFO:app-toron:matching FILE1 index records',
             'INFO:app-toron:matching FILE2 index records',
             'INFO:app-toron:loading mappings: FILE1 -> FILE2',
             'INFO:app-toron.space:loaded 3 mappings',
             'WARNING:app-toron:mapping is incomplete'],
        )

        self.assertEqual(
            self.get_mappings(self.node_c, self.node_d, 'population'),
            {(1, 1, 1, 1, b'\xc0', 50.0, 0.5),
             (2, 1, 1, 2, b'\xc0', 50.0, 0.5),
             (3, 1, 3, 6, b'\xc0', 50.0, 1.0)},
        )


class TestRelationExportRecords(DataSpaceFixturesMixin, unittest.TestCase):
    def test_full_mapping(self):
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        self.node_d.insert_mappings2(
            self.node_c,
            'population',
            data=[(1, 1, b'\xc0', 10.0),
                  (1, 2, b'\xc0', 70.0),
                  (2, 3, b'\xc0', 20.0),
                  (2, 4, b'\xc0', 60.0),
                  (3, 5, b'\xc0', 30.0),
                  (3, 6, b'\xc0', 50.0)],
            columns=['other_index_id', 'index_id', 'mapping_level', 'mapping_value'],
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            generator = command_relation._export_records(  # <- Function under test.
                self.node_c, self.node_d, 'population'
            )

            self.assertEqual(
                list(generator),
                [['index_code', 'lbl1', 'population', 'index_code', 'lbl1', 'lbl2'],
                 ['0XF4264876', '-',  0.0, '0XDF9B30D7', '-', '-'],
                 ['1X73808335', 'A', 10.0, '1X583DFB94', 'A', 'x'],
                 ['1X73808335', 'A', 70.0, '2X0BA7A010', 'A', 'y'],
                 ['2X201AD8B1', 'B', 20.0, '3X8C016B53', 'B', 'x'],
                 ['2X201AD8B1', 'B', 60.0, '4XAC931718', 'B', 'y'],
                 ['3XA7BC13F2', 'C', 30.0, '5X2B35DC5B', 'C', 'x'],
                 ['3XA7BC13F2', 'C', 50.0, '6X78AF87DF', 'C', 'y']],
            )

        self.assertEqual(
            cm.output,
            ['INFO:app-toron:written 7 records'],
        )

    def test_some_ambiguous_some_disjoint(self):
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        self.node_d.insert_mappings2(
            self.node_c,
            'population',
            data=[(1, 1, b'\xc0', 10.0),
                  (1, 2, b'\xc0', 70.0),
                  (2, 3, b'\x80', 20.0),
                  (2, 4, b'\x80', 60.0)],
                  # Omitting 3 -> 5
                  # Omitting 3 -> 6
            columns=['other_index_id', 'index_id', 'mapping_level', 'mapping_value'],
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            generator = command_relation._export_records(  # <- Function under test.
                self.node_c, self.node_d, 'population'
            )

            self.assertEqual(
                list(generator),
                [['index_code', 'lbl1', 'population', 'index_code', 'lbl1', 'lbl2', 'ambiguous_fields'],
                 ['0XF4264876', '-',  0.0, '0XDF9B30D7', '-', '-', None],
                 ['1X73808335', 'A', 10.0, '1X583DFB94', 'A', 'x', None],
                 ['1X73808335', 'A', 70.0, '2X0BA7A010', 'A', 'y', None],
                 ['2X201AD8B1', 'B', 20.0, '3X8C016B53', 'B', 'x', 'lbl2'],  # <- 'lbl2' is ambiguous
                 ['2X201AD8B1', 'B', 60.0, '4XAC931718', 'B', 'y', 'lbl2'],  # <- 'lbl2' is ambiguous
                 [None, None, None, '5X2B35DC5B', 'C', 'x', None],  # <- Target index_id 5 is disjoint.
                 [None, None, None, '6X78AF87DF', 'C', 'y', None],  # <- Target index_id 6 is disjoint.
                 ['3XA7BC13F2', 'C', None, None, None, None, None]],  # <- Source index_id 3 is disjoint.
            )

    def test_full_disjoint(self):
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        with self.assertLogs('app-toron', level='INFO') as cm:
            generator = command_relation._export_records(  # <- Function under test.
                self.node_c, self.node_d, 'population'
            )

            self.assertEqual(
                list(generator),
                [['index_code', 'lbl1', 'population', 'index_code', 'lbl1', 'lbl2'],
                 ['0XF4264876', '-',  0.0 , '0XDF9B30D7', '-', '-'],  # <- Undefined records always match to each other.
                 [None, None, None, '1X583DFB94', 'A', 'x'],
                 [None, None, None, '2X0BA7A010', 'A', 'y'],
                 [None, None, None, '3X8C016B53', 'B', 'x'],
                 [None, None, None, '4XAC931718', 'B', 'y'],
                 [None, None, None, '5X2B35DC5B', 'C', 'x'],
                 [None, None, None, '6X78AF87DF', 'C', 'y'],
                 ['1X73808335', 'A', None, None, None, None],
                 ['2X201AD8B1', 'B', None, None, None, None],
                 ['3XA7BC13F2', 'C', None, None, None, None]],
            )


class TestWriteToStdout(DataSpaceFixturesMixin, unittest.TestCase):
    def test_full_mapping(self):
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        self.node_d.insert_mappings2(
            self.node_c,
            'population',
            data=[(1, 1, b'\xc0', 10.0),
                  (1, 2, b'\xc0', 70.0),
                  (2, 3, b'\xc0', 20.0),
                  (2, 4, b'\xc0', 60.0),
                  (3, 5, b'\xc0', 30.0),
                  (3, 6, b'\xc0', 50.0)],
            columns=['other_index_id', 'index_id', 'mapping_level', 'mapping_value'],
        )

        dummy_stdout = DummyRedirection()
        args = argparse.Namespace(
            link_name='population',
            direction='both',
            stdout=dummy_stdout,
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_relation.write_to_stdout(  # <- Function under test.
                args,
                self.node_c,
                self.node_d,
            )

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
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        self.node_d.insert_mappings2(
            self.node_c,
            'population',
            data=[(1, 1, b'\xc0', 10.0),
                  (1, 2, b'\xc0', 70.0),
                  (2, 3, b'\x80', 20.0),
                  (2, 4, b'\x80', 60.0)],
                  # Omitting 3 -> 5
                  # Omitting 3 -> 6
            columns=['other_index_id', 'index_id', 'mapping_level', 'mapping_value'],
        )

        dummy_stdout = DummyRedirection()
        args = argparse.Namespace(
            link_name='population',
            direction='both',
            stdout=dummy_stdout,
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_relation.write_to_stdout(  # <- Function under test.
                args,
                self.node_c,
                self.node_d,
            )

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
        self.node_d.add_link(space=self.node_c,
                             link_name='population',
                             other_filename_hint='node_c',
                             is_default=True)

        dummy_stdout = DummyRedirection()
        args = argparse.Namespace(
            link_name='population',
            direction='both',
            stdout=dummy_stdout,
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_relation.write_to_stdout(  # <- Function under test.
                args,
                self.node_c,
                self.node_d,
            )

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            dummy_stdout.getvalue(),
            ('index_code,lbl1,population,index_code,lbl1,lbl2\n'
             '0XF4264876,-,0.0,0XDF9B30D7,-,-\n'  # <- Undefined records always match to each other.
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
