"""Tests for toron/cli/command_index.py module."""
import argparse
from .. import _unittest as unittest
from ..common import DummyRedirection
from toron import TopoNode

from toron.cli.command_index import (
    read_from_stdin,
)


class TestIndexReadFromStdin(unittest.TestCase):
    def test_input_labels(self):
        node = TopoNode()
        node.add_index_columns('state', 'county')

        args = argparse.Namespace(command='index', node=node)
        stdin = DummyRedirection(
            'state,county\n'
            'Illinois,Cook\n'
            'Indiana,Porter\n'
            'Michigan,Cass\n'
        )

        read_from_stdin(args, stdin=stdin)  # <- Function under test.

        index_values = list(node.select_index(header=True))
        expected_values = [
            ('index_id', 'state', 'county'),
            (0, '-', '-'),
            (1, 'Illinois', 'Cook'),
            (2, 'Indiana', 'Porter'),
            (3, 'Michigan', 'Cass'),
        ]
        self.assertEqual(index_values, expected_values)
