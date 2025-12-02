"""Tests for toron/cli/command_new.py module."""
import argparse
from .. import _unittest as unittest
from toron import TopoNode, ToronError
from toron.data_models import WeightGroup

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


class TestAddWeight(unittest.TestCase):
    def test_add_weight(self):
        node = TopoNode()

        args = argparse.Namespace(
            command='add',
            element='weight',
            node=node,
            weight='population',
            description='Population count.',
            selectors=['[foo]', '[bar="baz"]'],
            make_default=True,
        )
        command_add.add_weight(args)  # Function under test.

        actual = node.get_weight_group('population')
        expected = WeightGroup(
            id=1,
            name='population',
            description='Population count.',
            selectors=['[foo]', '[bar="baz"]'],
            is_complete=0,
        )
        self.assertEqual(actual, expected)

    def test_weight_already_exists(self):
        node = TopoNode()
        args = argparse.Namespace(
            command='add',
            element='weight',
            node=node,
            weight='population',
            description=None,
            selectors=None,
            make_default=True,
        )
        command_add.add_weight(args)

        regex = r"index weight group 'population' already exists"
        with self.assertRaisesRegex(ToronError, regex):
            command_add.add_weight(args)
