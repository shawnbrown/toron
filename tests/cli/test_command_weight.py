"""Tests for toron/cli/command_weight.py module."""
import argparse
from .. import _unittest as unittest

from ..common import TempDataSpaceMixin
from toron import ToronError, read_file, bind_file
from toron.data_models import WeightGroup

from toron.cli import command_weight
from toron.cli.common import ExitCode


class TestAdd(TempDataSpaceMixin, unittest.TestCase):
    def test_add_weight(self):
        command_weight.add(argparse.Namespace(
            filepath=self.filepath,
            command='weight',
            subcommand='add',
            name='population',
            description='Census 2020 Population',
            selectors=['[foo]', '[bar="baz"]'],
            make_default=True,
            backup=False,
        ))

        self.assertEqual(
            read_file(self.filepath).get_weight_group('population'),
            WeightGroup(
                id=1,
                name='population',
                description='Census 2020 Population',
                selectors=['[foo]', '[bar="baz"]'],
                is_complete=0,
            )
        )

    def test_weight_already_exists(self):
        command_weight.add(argparse.Namespace(
            filepath=self.filepath,
            command='weight',
            subcommand='add',
            name='population',
            description=None,
            selectors=None,
            make_default=True,
            backup=False,
        ))

        regex = r"index weight group 'population' already exists"
        with self.assertRaisesRegex(ToronError, regex):
            command_weight.add(argparse.Namespace(
                filepath=self.filepath,
                command='weight',
                subcommand='add',
                name='population',
                description=None,
                selectors=None,
                make_default=True,
                backup=False,
            ))


class TestUpdate(TempDataSpaceMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        ds = bind_file(self.filepath, mode='rw')
        ds.add_weight_group(
            name='myweight',
            description='Original description.',
            selectors=None,
            is_complete=True,
            make_default=False,
        )

    def get_namespace(self, **kwds):  # <- Helper function.
        """Get parsed arguments as an `argparse.Namespace` instance."""
        # Start with basic args for "weight update" subcommand.
        args_dict = {
            'filepath': self.filepath,
            'command': 'weight',
            'subcommand': 'update',
            'backup': False,
            'name': 'myweight',
            'description': None,
            'add_selector': None,
            'remove_selector': None,
            'make_default': False,
            'func': command_weight.update,
        }
        args_dict.update(kwds)
        return argparse.Namespace(**args_dict)

    def test_description_option(self):
        args = self.get_namespace(description='New description.')

        exit_code = command_weight.update(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_file(self.filepath, mode='ro').weight_groups,
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
        exit_code = command_weight.update(args)  # Function under test.
        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_file(self.filepath, mode='ro').weight_groups,
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
        exit_code = command_weight.update(args)  # Function under test.
        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_file(self.filepath, mode='ro').weight_groups,
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
        exit_code = command_weight.update(args)  # Function under test.
        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_file(self.filepath, mode='ro').weight_groups,
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
        exit_code = command_weight.update(args)  # Function under test.
        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_file(self.filepath, mode='ro').weight_groups,
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
        exit_code = command_weight.update(args)  # Function under test.
        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            bind_file(self.filepath, mode='ro').weight_groups,
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
        ds = bind_file(self.filepath, mode='ro')

        self.assertIsNone(
            ds.get_default_weight_group(),
            msg='should start with no default weight group',
        )

        args = self.get_namespace(make_default=True)
        exit_code = command_weight.update(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            ds.get_default_weight_group(),
            WeightGroup(
                id=1,
                name='myweight',
                description='Original description.',
                selectors=None,
                is_complete=1,
            ),
        )
