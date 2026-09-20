"""Tests for toron/cli/command_label.py module."""
import argparse
from .. import _unittest as unittest
from ..common import TempDataSpaceMixin
from toron import ToronError, read_file, bind_file

from toron.cli import command_label
from toron.cli.common import ExitCode


class TestLabelAdd(TempDataSpaceMixin, unittest.TestCase):
    def test_add_labels(self):
        command_label.add(argparse.Namespace(
            filepath=self.filepath,
            command='add',
            element='label',
            names=['A', 'B', 'C'],
            backup=False,
        ))

        self.assertEqual(
            read_file(self.filepath).index_columns,
            ['A', 'B', 'C'],
        )

    def test_label_already_exists(self):
        command_label.add(argparse.Namespace(
            filepath=self.filepath,
            command='add',
            element='label',
            names=['A', 'B', 'C'],
            backup=False,
        ))

        regex = r"index label column 'B' already exists"
        with self.assertRaisesRegex(ToronError, regex):
            command_label.add(argparse.Namespace(
                filepath=self.filepath,
                command='add',
                element='label',
                names=['B'],
                backup=False,
            ))

    def test_add_label_comma_separated_value(self):
        command_label.add(argparse.Namespace(
            filepath=self.filepath,
            command='add',
            element='label',
            names=['A,B,C'],  # <- Comma-separated value.
            backup=False,
        ))

        self.assertEqual(
            read_file(self.filepath).index_columns,
            ['A', 'B', 'C'],
        )


class TestLabelUpdate(TempDataSpaceMixin, unittest.TestCase):
    def test_update_label(self):
        bind_file(self.filepath, mode='rw').add_index_columns('A', 'C', 'B', 'D')

        args = argparse.Namespace(
            filepath=self.filepath,
            command='update',
            element='label',
            name='B',
            move_left=1,
            move_right=0,
        )
        exit_code = command_label.update(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_file(self.filepath, mode='ro').get_label_columns(),
            ['A', 'B', 'C', 'D'],
        )

    def test_bad_label(self):
        bind_file(self.filepath, mode='rw').add_index_columns('A', 'C', 'B', 'D')

        args = argparse.Namespace(
            filepath=self.filepath,
            command='update',
            element='label',
            name='X',  # <- No label named "X".
            move_left=1,
            move_right=0,
        )

        regex = r"'X' not found"
        with self.assertRaisesRegex(ToronError, regex):
            command_label.update(args)  # Function under test.

    def test_invalid_direction(self):
        bind_file(self.filepath, mode='rw').add_index_columns('A', 'C', 'B', 'D')

        args = argparse.Namespace(
            filepath=self.filepath,
            command='update',
            element='label',
            name='B',
            move_left=2,   # <- Should not have both left and right counts.
            move_right=2,  # <- Should not have both left and right counts.
        )

        with self.assertRaises(Exception) as cm:
            command_label.update(args)  # Function under test.

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
