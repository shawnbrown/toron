"""Tests for toron/cli/command_new.py module."""
import argparse
from .. import _unittest as unittest
from toron import TopoNode, ToronError

from toron.cli import command_add


class TestAddLabel(unittest.TestCase):
    def test_add_labels(self):
        node = TopoNode()

        self.assertEqual(node.index_columns, [])

        args = argparse.Namespace(
            command='add', element='label', node=node, labels=['A', 'B', 'C']
        )
        command_add.add_label(args)  # Function under test.

        self.assertEqual(node.index_columns, ['A', 'B', 'C'])

    def test_label_already_exists(self):
        node = TopoNode()
        command_add.add_label(argparse.Namespace(
            command='add', element='label', node=node, labels=['A', 'B', 'C']
        ))

        regex = r"index label column 'B' already exists"
        with self.assertRaisesRegex(ToronError, regex):
            command_add.add_label(argparse.Namespace(
                command='add', element='label', node=node, labels=['B']
            ))
