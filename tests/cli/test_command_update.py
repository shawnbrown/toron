"""Tests for toron/cli/command_update.py module."""
import argparse
import os
import tempfile
from .. import _unittest as unittest
from ..common import ClassTempFileMixin, TempTopoNodeMixin
from toron import TopoNode, ToronError, bind_node

from toron.cli import command_update
from toron.cli.common import ExitCode
from toron.data_models import WeightGroup


class TestUpdateLabel(TempTopoNodeMixin, unittest.TestCase):
    def test_update_label(self):
        bind_node(self.filepath, mode='rw').add_index_columns('A', 'C', 'B', 'D')

        args = argparse.Namespace(
            filepath=self.filepath,
            command='update',
            element='label',
            label='B',
            move_left=1,
            move_right=0,
        )
        exit_code = command_update.update_label(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_node(self.filepath, mode='ro').get_label_columns(),
            ['A', 'B', 'C', 'D'],
        )

    def test_bad_label(self):
        bind_node(self.filepath, mode='rw').add_index_columns('A', 'C', 'B', 'D')

        args = argparse.Namespace(
            filepath=self.filepath,
            command='update',
            element='label',
            label='X',  # <- No label named "X".
            move_left=1,
            move_right=0,
        )

        regex = r"'X' not found"
        with self.assertRaisesRegex(ToronError, regex):
            command_update.update_label(args)  # Function under test.

    def test_invalid_direction(self):
        bind_node(self.filepath, mode='rw').add_index_columns('A', 'C', 'B', 'D')

        args = argparse.Namespace(
            filepath=self.filepath,
            command='update',
            element='label',
            label='B',
            move_left=2,   # <- Should not have both left and right counts.
            move_right=2,  # <- Should not have both left and right counts.
        )

        with self.assertRaises(Exception) as cm:
            command_update.update_label(args)  # Function under test.

        self.assertNotIsInstance(
            cm.exception,
            ToronError,
            msg=(
                'this should not raise a ToronError, we want this to fail '
                'with a full traceback even in the CLI because this '
                'condition should not normally occur and would represent '
                'a bug that needs fixed, rather than an invalid user input'
            ),
        )


class TestUpdateWeight(ClassTempFileMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        node = TopoNode()
        node.add_weight_group(
            name='myweight',
            description='Original description.',
            selectors=None,
            is_complete=True,
            make_default=False,
        )
        node.to_file(self.filepath)

    def get_namespace(self, **kwds):  # <- Helper function.
        """Get parsed arguments as an `argparse.Namespace` instance."""
        # Start with basic args for "update weight" subcommand.
        args_dict = {
            'filepath': self.filepath,
            'command': 'update',
            'element': 'weight',
            'backup': False,
            'weight': 'myweight',
            'description': None,
            'add_selector': None,
            'remove_selector': None,
            'make_default': False,
            'func': command_update.update_weight,
        }
        args_dict.update(kwds)
        return argparse.Namespace(**args_dict)

    def test_description_option(self):
        args = self.get_namespace(description='New description.')

        exit_code = command_update.update_weight(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_node(self.filepath, mode='ro').weight_groups,
             [
                WeightGroup(
                    id=1,
                    name='myweight',
                    description='New description.',
                    selectors=None,
                    is_complete=1,
                ),
            ],
        )

    def test_add_and_remove_selector_options(self):
        # Add selectors when none exist.
        args = self.get_namespace(add_selector=['[A]', '[C="ccc"]'])
        exit_code = command_update.update_weight(args)  # Function under test.
        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_node(self.filepath, mode='ro').weight_groups,
             [
                WeightGroup(
                    id=1,
                    name='myweight',
                    description='Original description.',
                    selectors=['[A]', '[C="ccc"]'],
                    is_complete=1,
                ),
            ],
        )

        # Add one more selector to existing selectors.
        args = self.get_namespace(add_selector=['[B="bbb"][D]'])
        exit_code = command_update.update_weight(args)  # Function under test.
        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_node(self.filepath, mode='ro').weight_groups,
             [
                WeightGroup(
                    id=1,
                    name='myweight',
                    description='Original description.',
                    selectors=['[A]', '[B="bbb"][D]', '[C="ccc"]'],
                    is_complete=1,
                ),
            ],
        )

        # Remove selector.
        args = self.get_namespace(remove_selector=['[A]'])
        exit_code = command_update.update_weight(args)  # Function under test.
        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_node(self.filepath, mode='ro').weight_groups,
             [
                WeightGroup(
                    id=1,
                    name='myweight',
                    description='Original description.',
                    selectors=['[B="bbb"][D]', '[C="ccc"]'],
                    is_complete=1,
                ),
            ],
        )

        # Remove remaining selectors.
        args = self.get_namespace(remove_selector=['[B="bbb"][D]', '[C="ccc"]'])
        exit_code = command_update.update_weight(args)  # Function under test.
        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_node(self.filepath, mode='ro').weight_groups,
             [
                WeightGroup(
                    id=1,
                    name='myweight',
                    description='Original description.',
                    selectors=None,
                    is_complete=1,
                ),
            ],
        )

        # Remove selectors when then are none to remove.
        args = self.get_namespace(remove_selector=['[A]'])
        exit_code = command_update.update_weight(args)  # Function under test.
        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_node(self.filepath, mode='ro').weight_groups,
            [
                WeightGroup(
                    id=1,
                    name='myweight',
                    description='Original description.',
                    selectors=None,
                    is_complete=1,
                ),
            ],
        )

    def test_make_default_option(self):
        node = bind_node(self.filepath, mode='ro')

        self.assertIsNone(
            node.get_default_weight_group(),
            msg='should start with no default weight group',
        )

        args = self.get_namespace(make_default=True)
        exit_code = command_update.update_weight(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            node.get_default_weight_group(),
            WeightGroup(
                id=1,
                name='myweight',
                description='Original description.',
                selectors=None,
                is_complete=1,
            ),
        )


class TestUpdateAttribute(TempTopoNodeMixin, unittest.TestCase):
    def test_update_attribute(self):
        bind_node(self.filepath, mode='rw').set_registered_attributes(['A', 'C', 'B', 'D'])

        args = argparse.Namespace(
            filepath=self.filepath,
            command='update',
            element='attribute',
            attribute='B',
            move_left=1,
            move_right=0,
        )
        exit_code = command_update.update_attribute(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_node(self.filepath, mode='ro').get_registered_attributes(),
            ['A', 'B', 'C', 'D'],
        )

    def test_bad_label(self):
        bind_node(self.filepath, mode='rw').set_registered_attributes(['A', 'C', 'B', 'D'])

        args = argparse.Namespace(
            filepath=self.filepath,
            command='update',
            element='attribute',
            attribute='X',  # <- No attribute named "X".
            move_left=1,
            move_right=0,
        )

        regex = r"'X' not found"
        with self.assertRaisesRegex(ToronError, regex):
            command_update.update_attribute(args)  # Function under test.

    def test_invalid_direction(self):
        bind_node(self.filepath, mode='rw').set_registered_attributes(['A', 'C', 'B', 'D'])

        args = argparse.Namespace(
            filepath=self.filepath,
            command='update',
            element='attribute',
            attribute='B',
            move_left=2,   # <- Should not have both left and right counts.
            move_right=2,  # <- Should not have both left and right counts.
        )

        with self.assertRaises(Exception) as cm:
            command_update.update_attribute(args)  # Function under test.

        self.assertNotIsInstance(
            cm.exception,
            ToronError,
            msg=(
                'this should not raise a ToronError, we want this to fail '
                'with a full traceback even in the CLI because this '
                'condition should not normally occur and would represent '
                'a bug that needs fixed, rather than an invalid user input'
            ),
        )
