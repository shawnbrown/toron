"""Tests for toron/cli/command_partition.py module."""
import argparse
from .. import _unittest as unittest
from ..common import TempDataSpaceMixin
from toron import ToronError, read_file, bind_file

from toron.cli import command_partition
from toron.cli.common import ExitCode


class TestPartitionAdd(TempDataSpaceMixin, unittest.TestCase):
    def test_new_partition(self):
        bind_file(self.filepath, mode='rw').add_index_columns('A', 'B', 'C')

        exit_code = command_partition.add(argparse.Namespace(  # <- Function under test.
            filepath=self.filepath,
            command='partition',
            subcommand='add',
            names=['A', 'B'],
            backup=False,
        ))

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            read_file(self.filepath).partition_definitions,
            [{'A', 'B'}, {'A', 'B', 'C'}]
        )

    def test_existing_partition(self):
        bind_file(self.filepath, mode='rw').add_index_columns('A', 'B', 'C')

        args = argparse.Namespace(
            filepath=self.filepath,
            command='partition',
            subcommand='add',
            names=['A', 'B', 'C'],
            backup=False,
        )

        with self.assertRaises(ToronError):
            command_partition.add(args)  # <- Function under test.
