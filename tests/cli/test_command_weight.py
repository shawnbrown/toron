"""Tests for toron/cli/command_weight.py module."""
import argparse
from .. import _unittest as unittest

from ..common import TempDataSpaceMixin
from toron import ToronError, read_file
from toron.data_models import WeightGroup

from toron.cli import command_weight


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
