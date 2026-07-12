"""Tests for toron/cli/command_rename.py module."""
import argparse
import os
import tempfile
from .. import _unittest as unittest
from ..common import TempTopoNodeMixin
from toron import TopoNode, ToronError, bind_node

from toron.cli import command_rename
from toron.cli.common import ExitCode


class TestRenameLabel(TempTopoNodeMixin, unittest.TestCase):
    def test_rename_label(self):
        bind_node(self.filepath, mode='rw').add_index_columns('A', 'B', 'C', 'X')

        args = argparse.Namespace(
            filepath=self.filepath,
            command='update',
            element='rename',
            old_label='X',
            new_label='D',
        )

        exit_code = command_rename.rename_label(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_node(self.filepath, mode='ro').get_label_columns(),
            ['A', 'B', 'C', 'D'],
        )

    def test_bad_new_label(self):
        bind_node(self.filepath, mode='rw').add_index_columns('A', 'B', 'C', 'X')

        args = argparse.Namespace(
            filepath=self.filepath,
            command='update',
            element='rename',
            old_label='X',
            new_label='index_id',  # <- Not allowed.
        )

        regex = r"'index_id' is a reserved name"
        with self.assertRaisesRegex(ToronError, regex):
            command_rename.rename_label(args)  # Function under test.

    def test_missing_old_label(self):
        bind_node(self.filepath, mode='rw').add_index_columns('A', 'B', 'C', 'X')

        args = argparse.Namespace(
            filepath=self.filepath,
            command='update',
            element='rename',
            old_label='S',  # <- Missing from current labels.
            new_label='D',
        )

        regex = r"no label 'S'"
        with self.assertRaisesRegex(ToronError, regex):
            command_rename.rename_label(args)  # Function under test.


class TestRenameDomain(TempTopoNodeMixin, unittest.TestCase):
    def test_rename_domain(self):
        bind_node(self.filepath, mode='rw').set_domain('orig_value')

        args = argparse.Namespace(
            filepath=self.filepath,
            command='rename',
            element='domain',
            new_domain='new_value',
        )
        exit_code = command_rename.rename_domain(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_node(self.filepath, mode='ro').domain,
            'new_value',
        )
