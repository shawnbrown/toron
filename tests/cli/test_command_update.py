"""Tests for toron/cli/command_update.py module."""
import argparse
import os
import tempfile
from .. import _unittest as unittest
from toron import TopoNode, ToronError, bind_node

from toron.cli import command_update
from toron.cli.common import ExitCode


class TestUpdateLabel(unittest.TestCase):
    def setUp(self):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            self.filepath = tmp.name
        self.addCleanup(os.remove, self.filepath)

        node = TopoNode()
        node.to_file(self.filepath)

    def test_update_label(self):
        bind_node(self.filepath, mode='rw').add_index_columns('A', 'C', 'B', 'D')

        args = argparse.Namespace(
            filepath=self.filepath,
            command='update',
            element='label',
            label='B',
            move_left=1,
            move_right=0,
        )
        exit_code = command_update.update_label(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_node(self.filepath, mode='ro').get_label_columns(),
            ['A', 'B', 'C', 'D'],
        )

    def test_bad_label(self):
        bind_node(self.filepath, mode='rw').add_index_columns('A', 'C', 'B', 'D')

        args = argparse.Namespace(
            filepath=self.filepath,
            command='update',
            element='label',
            label='X',  # <- No label named "X".
            move_left=1,
            move_right=0,
        )

        regex = r"no label named 'X'"
        with self.assertRaisesRegex(ToronError, regex):
            command_update.update_label(args)  # Function under test.

    def test_invalid_direction(self):
        bind_node(self.filepath, mode='rw').add_index_columns('A', 'C', 'B', 'D')

        args = argparse.Namespace(
            filepath=self.filepath,
            command='update',
            element='label',
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
