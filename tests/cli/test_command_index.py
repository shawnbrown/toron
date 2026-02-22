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

        args = argparse.Namespace(
            command='index',
            node=node,
            on_label_conflict='abort',
            on_weight_conflict='abort',
            stdin=DummyRedirection(
                'state,county,population\n'
                'Illinois,Cook,5275541\n'
                'Indiana,Porter,175860\n'
                'Michigan,Cass,51589\n'
            ),
        )

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

    def test_abort_on_label_conflict(self):
        node = TopoNode()
        node.add_index_columns('state', 'county')
        node.add_weight_group('population', make_default=True)
        node.insert_index([['state', 'county'],
                           ['Illinois', 'Cook'],
                           ['Indiana', 'Porter'],
                           ['Michigan', 'Cass']])

        args = argparse.Namespace(
            command='index',
            node=node,
            on_label_conflict='abort',
            on_weight_conflict='abort',
            stdin=DummyRedirection(
                'index_id,state,county,population\n'
                '1,Illinois,Cook,5275541\n'
                '2,Indiana,Porter,175860\n'
                '3,Michigan,OTHERVALUE,51589\n'  # <- Will abort operation.
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index.read_from_stdin(args)  # <- Function under test.

        self.assertEqual(
            logs_cm.output,
            ["ERROR:app-toron:index_id 3 and labels ('Michigan', 'OTHERVALUE') "
               "do not match Index(id=3, labels=('Michigan', 'Cass'))\n"
               "  load behavior can be changed using --on-label-conflict "
               "and --on-weight-conflict"]
        )

    def test_ignore_on_label_conflict(self):
        node = TopoNode()
        node.add_index_columns('state', 'county')
        node.add_weight_group('population', make_default=True)
        node.insert_index([['state', 'county'],
                           ['Illinois', 'Cook'],
                           ['Indiana', 'Porter'],
                           ['Michigan', 'Cass']])

        args = argparse.Namespace(
            command='index',
            node=node,
            on_label_conflict='ignore',
            on_weight_conflict='abort',
            stdin=DummyRedirection(
                'index_id,state,county,population\n'
                '1,Illinois,Cook,5275541\n'
                '2,Indiana,Porter,175860\n'
                '3,Michigan,OTHERVALUE,51589\n'  # <- Will abort operation.
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index.read_from_stdin(args)  # <- Function under test.

        self.assertEqual(
            logs_cm.output,
            ['INFO:app-toron.node:ignored 1 non-matching index labels',
             'INFO:app-toron.node:loaded 3 index weights']
        )

    def test_replace_on_label_conflict(self):
        node = TopoNode()
        node.add_index_columns('state', 'county')
        node.add_weight_group('population', make_default=True)
        node.insert_index([['state', 'county'],
                           ['Illinois', 'Cook'],
                           ['Indiana', 'Porter'],
                           ['Michigan', 'Cass']])

        args = argparse.Namespace(
            command='index',
            node=node,
            on_label_conflict='replace',
            on_weight_conflict='abort',
            stdin=DummyRedirection(
                'index_id,state,county,population\n'
                '1,Illinois,Cook,5275541\n'
                '2,Indiana,Porter,175860\n'
                '3,Michigan,OTHERVALUE,51589\n'  # <- Will abort operation.
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index.read_from_stdin(args)  # <- Function under test.

        self.assertEqual(
            logs_cm.output,
            ['INFO:app-toron.node:replaced 1 index labels',
             'INFO:app-toron.node:loaded 3 index weights']
        )

    def test_abort_on_weight_conflict(self):
        node = TopoNode()
        node.add_index_columns('state', 'county')
        node.add_weight_group('population', make_default=True)

        args = argparse.Namespace(
            command='index',
            node=node,
            on_label_conflict='abort',
            on_weight_conflict='abort',
            stdin=DummyRedirection(
                'state,county,population\n'
                'Illinois,Cook,5275541\n'
                'Indiana,Porter,175860\n'
                'Michigan,Cass,51589\n'
                'Michigan,Cass,50000\n'  # <- Will abort operation.
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index.read_from_stdin(args)  # <- Function under test.

        index_values = list(node.select_index(header=True))
        expected_values = [('index_id', 'state', 'county'), (0, '-', '-')]
        self.assertEqual(index_values, expected_values)
        self.assertEqual(
            logs_cm.output,
            ["ERROR:app-toron:weight group 'population' already has "
               "a value for Index(id=3, labels=('Michigan', 'Cass'))\n"
               "  load behavior can be changed using --on-label-conflict "
               "and --on-weight-conflict"],
        )

    def test_replace_on_weight_conflict(self):
        node = TopoNode()
        node.add_index_columns('state', 'county')
        node.add_weight_group('population', make_default=True)

        args = argparse.Namespace(
            command='index',
            node=node,
            on_label_conflict='abort',
            on_weight_conflict='replace',
            stdin=DummyRedirection(
                'state,county,population\n'
                'Illinois,Cook,5275541\n'
                'Indiana,Porter,175860\n'
                'Michigan,Cass,0\n'  # <- Will get replaced by later record.
                'Michigan,Cass,51589\n'
            ),
        )

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
             'INFO:app-toron.node:loaded 3 index weights',
             'INFO:app-toron.node:replaced 1 index weights'],
        )
