"""Tests for toron/cli/command_update.py module."""
import argparse
from .. import _unittest as unittest
from toron import TopoNode, ToronError

from toron.cli import command_update
from toron.cli.common import ExitCode


class TestUpdateLabel(unittest.TestCase):
    def test_update_label(self):
        node = TopoNode()
        node.add_index_columns('A', 'C', 'B', 'D')

        args = argparse.Namespace(
            command='update',
            element='label',
            node=node,
            label='B',
            move_left=1,
            move_right=0,
        )
        exit_code = command_update.update_label(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(node.get_label_columns(), ['A', 'B', 'C', 'D'])

    def test_bad_label(self):
        node = TopoNode()
        node.add_index_columns('A', 'C', 'B', 'D')

        args = argparse.Namespace(
            command='update',
            element='label',
            node=node,
            label='X',  # <- No label named "X".
            move_left=1,
            move_right=0,
        )

        with self.assertRaises(ToronError):
            command_update.update_label(args)  # Function under test.

    def test_invalid_direction(self):
        node = TopoNode()
        node.add_index_columns('A', 'C', 'B', 'D')

        args = argparse.Namespace(
            command='update',
            element='label',
            node=node,
            label='B',
            move_left=2,   # <- Should not have both left and right counts.
            move_right=2,  # <- Should not have both left and right counts.
        )

        with self.assertRaises(Exception) as cm:
            command_update.update_label(args)  # Function under test.

        self.assertNotIsInstance(
            cm.exception,
            ToronError,
            msg=(
                'this should not raise a ToronError, we want this to fail '
                'with a full traceback even in the CLI because this '
                'condition should not normally occur and would represent '
                'a bug that needs fixed, rather than an invalid user input'
            ),
        )
