"""Tests for toron/cli/command_attribute.py module."""
import argparse
from .. import _unittest as unittest
from ..common import TempDataSpaceMixin
from toron import ToronError, read_file, bind_file

from toron.cli import command_attribute
from toron.cli.common import ExitCode


class TestAddAttributes(TempDataSpaceMixin, unittest.TestCase):
    def test_add_attributes(self):
        args = argparse.Namespace(
            filepath=self.filepath,
            command='attribute',
            subcommand='attribute',
            names=['foo', 'bar', 'baz'],
            backup=False,
            func=command_attribute.add,
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_attribute.add(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            cm.output,
            ["INFO:app-toron:added attribute columns: 'foo', 'bar', 'baz'"],
        )
        self.assertEqual(
            read_file(self.filepath).get_registered_attributes(),
            ['foo', 'bar', 'baz'],
        )

    def test_attribute_already_exists(self):
        command_attribute.add(argparse.Namespace(
            filepath=self.filepath,
            command='attribute',
            subcommand='attribute',
            names=['baz'],
            backup=False,
            func=command_attribute.add,
        ))

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_attribute.add(argparse.Namespace(
                filepath=self.filepath,
                command='attribute',
                subcommand='attribute',
                names=['foo', 'bar', 'baz'],
                backup=False,
                func=command_attribute.add,
            ))

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            cm.output,
            ["WARNING:app-toron:skipping 'baz' (already registered)",
             "INFO:app-toron:added attribute columns: 'foo', 'bar'"],
        )
        self.assertEqual(
            read_file(self.filepath).get_registered_attributes(),
            ['baz', 'foo', 'bar'],  # <- First item is 'baz'.
            msg="since 'baz' already existed, it retains its original position",
        )

    def test_bad_attribute_name(self):
        regex = r"'domain' is a reserved name"
        with self.assertRaisesRegex(ToronError, regex):
            command_attribute.add(argparse.Namespace(
                filepath=self.filepath,
                command='attribute',
                subcommand='attribute',
                names=['foo', 'bar', 'domain'],
                backup=False,
                func=command_attribute.add,
            ))

    def test_add_attributes_comma_separated_value(self):
        command_attribute.add(argparse.Namespace(
            filepath=self.filepath,
            command='attribute',
            subcommand='attribute',
            names=['foo,bar,baz'],  # <- Comma-separated value.
            backup=False,
            func=command_attribute.add,
        ))

        self.assertEqual(
            read_file(self.filepath).get_registered_attributes(),
            ['foo', 'bar', 'baz'],
        )


class TestUpdate(TempDataSpaceMixin, unittest.TestCase):
    def test_update_attribute(self):
        bind_file(self.filepath, mode='rw').set_registered_attributes(['A', 'C', 'B', 'D'])

        args = argparse.Namespace(
            filepath=self.filepath,
            command='attribute',
            subcommand='update',
            name='B',
            move_left=1,
            move_right=0,
        )
        exit_code = command_attribute.update(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_file(self.filepath, mode='ro').get_registered_attributes(),
            ['A', 'B', 'C', 'D'],
        )

    def test_bad_label(self):
        bind_file(self.filepath, mode='rw').set_registered_attributes(['A', 'C', 'B', 'D'])

        args = argparse.Namespace(
            filepath=self.filepath,
            command='attribute',
            subcommand='update',
            name='X',  # <- No attribute named "X".
            move_left=1,
            move_right=0,
        )

        regex = r"'X' not found"
        with self.assertRaisesRegex(ToronError, regex):
            command_attribute.update(args)  # Function under test.

    def test_invalid_direction(self):
        bind_file(self.filepath, mode='rw').set_registered_attributes(['A', 'C', 'B', 'D'])

        args = argparse.Namespace(
            filepath=self.filepath,
            command='attribute',
            subcommand='update',
            name='B',
            move_left=2,   # <- Should not have both left and right counts.
            move_right=2,  # <- Should not have both left and right counts.
        )

        with self.assertRaises(Exception) as cm:
            command_attribute.update(args)  # Function under test.

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
