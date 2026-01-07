"""Tests for toron/cli/command_index.py module."""
import argparse
from .. import _unittest as unittest
from ..common import DummyRedirection
from toron import TopoNode

from toron.cli import command_index


class TestIndexWriteToStdout(unittest.TestCase):
    def test_basic_behavior(self):
        node = TopoNode()
        node.add_index_columns('state', 'county')
        node.insert_index([
            ['state', 'county'],
            ['Illinois', 'Cook'],
            ['Indiana', 'Porter'],
            ['Michigan', 'Cass'],
        ])
        dummy_stdout = DummyRedirection()
        args = argparse.Namespace(command='index', node=node, stdout=dummy_stdout)

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index.write_to_stdout(args)  # <- Function under test.

        expected_values = (
            'index_id,state,county\n'
            '0,-,-\n'
            '1,Illinois,Cook\n'
            '2,Indiana,Porter\n'
            '3,Michigan,Cass\n'
        )
        self.assertEqual(dummy_stdout.getvalue(), expected_values)
        self.assertEqual(logs_cm.output, ['INFO:app-toron:written 4 records'])


class TestIndexReadFromStdin(unittest.TestCase):
    def test_input_labels_and_weights(self):
        node = TopoNode()
        node.add_index_columns('state', 'county')
        node.add_weight_group('population', make_default=True)

        dummy_stdin = DummyRedirection(
            'state,county,population\n'
            'Illinois,Cook,5275541\n'
            'Indiana,Porter,175860\n'
            'Michigan,Cass,51589\n'
        )
        args = argparse.Namespace(command='index', node=node, stdin=dummy_stdin)

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index.read_from_stdin(args)  # <- Function under test.

        index_values = list(node.select_index(header=True))
        expected_values = [
            ('index_id', 'state', 'county'),
            (0, '-', '-'),
            (1, 'Illinois', 'Cook'),
            (2, 'Indiana', 'Porter'),
            (3, 'Michigan', 'Cass'),
        ]
        self.assertEqual(index_values, expected_values)
        self.assertEqual(
            logs_cm.output,
            ['INFO:app-toron.node:loaded 3 index labels',
             'INFO:app-toron.node:loaded 3 index weights'],
        )
