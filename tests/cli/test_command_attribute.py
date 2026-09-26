"""Tests for toron/cli/command_attribute.py module."""
import argparse
from .. import _unittest as unittest
from ..common import TempDataSpaceMixin
from toron import ToronError, read_file

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
