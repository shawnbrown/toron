"""Tests for toron/cli/command_remove.py module."""
import argparse
import os
import tempfile
from .. import _unittest as unittest
from toron import TopoNode, ToronError, read_file, bind_node

from toron.cli import command_remove
from toron.cli.common import ExitCode


class TestRemoveLink(unittest.TestCase):
    def setUp(self):
        # Create node objects and set `unique_id` values.
        node1 = TopoNode()
        self.change_unique_id(node1, '11111111-1111-1111-1111-111111111111')

        node2 = TopoNode()
        self.change_unique_id(node2, '22222222-2222-2222-2222-222222222222')

        # Create temporary file locations.
        with tempfile.NamedTemporaryFile(delete=False) as tmp1:
            self.filepath1 = tmp1.name
        self.addCleanup(os.remove, self.filepath1)

        with tempfile.NamedTemporaryFile(delete=False) as tmp2:
            self.filepath2 = tmp2.name
        self.addCleanup(os.remove, self.filepath2)

        # Save nodes to temporary file locations.
        node1.to_file(self.filepath1)
        node2.to_file(self.filepath2)

    @staticmethod
    def change_unique_id(node, unique_id):
        """Helper function to specify a ``unique_id`` for testing."""
        node._connector._unique_id = unique_id
        with node._managed_transaction() as cur:
            property_repo = node._dal.PropertyRepository(cur)
            property_repo.add_or_update('unique_id', unique_id)

    @staticmethod
    def add_link(tail_file, head_file, link_name, is_default=None):
        """Helper function to add links between files for testing."""
        tail = bind_node(tail_file, mode='rw')
        head = bind_node(head_file, mode='rw')
        head.add_link(
            node=tail,
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
            command='remove',
            element='link',
            filepath2=self.filepath2,
            link='population',
            direction='both',
        )

        exit_code = command_remove.remove_link(args)  # <- Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertLinkNotExists(self.filepath1, self.filepath2, 'population')
        self.assertLinkNotExists(self.filepath2, self.filepath1, 'population')

    def test_remove_left_direction(self):
        self.add_link(self.filepath1, self.filepath2, 'population', is_default=True)
        self.add_link(self.filepath2, self.filepath1, 'population', is_default=True)

        args = argparse.Namespace(
            filepath=self.filepath1,
            command='remove',
            element='link',
            filepath2=self.filepath2,
            link='population',
            direction='left',
        )

        exit_code = command_remove.remove_link(args)  # <- Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertLinkExists(self.filepath1, self.filepath2, 'population')
        self.assertLinkNotExists(self.filepath2, self.filepath1, 'population')

    def test_remove_right_direction(self):
        self.add_link(self.filepath1, self.filepath2, 'population', is_default=True)
        self.add_link(self.filepath2, self.filepath1, 'population', is_default=True)

        args = argparse.Namespace(
            filepath=self.filepath1,
            command='remove',
            element='link',
            filepath2=self.filepath2,
            link='population',
            direction='right',
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            exit_code = command_remove.remove_link(args)  # <- Function under test.

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
            command='remove',
            element='link',
            filepath2=self.filepath2,
            link='population',
            direction='both',
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_remove.remove_link(args)  # <- Function under test.

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
            command='remove',
            element='link',
            filepath2=self.filepath2,
            link='population',
            direction='both',
        )

        regex = r"no 'population' link in FILE1 or FILE2"
        with self.assertRaisesRegex(ToronError, regex):
            command_remove.remove_link(args)  # <- Function under test.

    def test_remove_single_directions_missing(self):
        args = argparse.Namespace(
            filepath=self.filepath1,
            command='remove',
            element='link',
            filepath2=self.filepath2,
            link='population',
            direction='right',
        )

        regex = r"no 'population' link found in FILE2"
        with self.assertRaisesRegex(ToronError, regex):
            command_remove.remove_link(args)  # <- Function under test.
