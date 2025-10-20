"""Tests for toron/cli/command_index.py module."""
import io
from .. import _unittest as unittest
from ..common import StreamWrapperTestCase
from toron import TopoNode, bind_node
import toron
from toron.cli.main import get_parser

from toron.cli.command_index import (
    read_from_stdin,
)


class TestIndexReadFromStdin(StreamWrapperTestCase):
    def test_input_labels(self):
        file_path = self.get_tempfile_path()
        node = TopoNode()
        node.add_index_columns('state', 'county')
        node.to_file(file_path)

        with self.patched_stdin(
            'state,county\n'
            'Illinois,Cook\n'
            'Indiana,Porter\n'
            'Michigan,Cass\n'
        ):
            parser = get_parser()
            args = parser.parse_args(['index', file_path])

            read_from_stdin(args)  # <- Function under test.

            node = toron.read_file(file_path)
            index_values = list(node.select_index(header=True))
            expected_values = [
                ('index_id', 'state', 'county'),
                (0, '-', '-'),
                (1, 'Illinois', 'Cook'),
                (2, 'Indiana', 'Porter'),
                (3, 'Michigan', 'Cass'),
            ]
            self.assertEqual(index_values, expected_values)
