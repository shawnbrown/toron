"""Tests for toron/cli/command_label.py module."""
import argparse
from .. import _unittest as unittest
from ..common import TempDataSpaceMixin
from toron import ToronError, read_file

from toron.cli import command_label


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
