"""Tests for toron/cli/command_relation.py module."""
import argparse
import os
import tempfile
from .. import _unittest as unittest


from toron import DataSpace, ToronError, read_file, bind_file
from toron.cli.common import ExitCode
from toron.data_models import Link
from toron.cli import command_relation


class TestCreateLink(unittest.TestCase):
    def setUp(self):
        with tempfile.NamedTemporaryFile(delete=False) as tmp1:
            self.filepath1 = tmp1.name
        self.addCleanup(os.remove, self.filepath1)
        ds1 = DataSpace()
        with ds1._managed_transaction() as cur:
            property_repo = ds1._dal.PropertyRepository(cur)
            property_repo.add_or_update(
                'unique_id', '11111111-1111-1111-1111-111111111111'
            )
        ds1.to_file(self.filepath1)

        with tempfile.NamedTemporaryFile(delete=False) as tmp2:
            self.filepath2 = tmp2.name
        self.addCleanup(os.remove, self.filepath2)
        ds2 = DataSpace()
        with ds2._managed_transaction() as cur:
            property_repo = ds2._dal.PropertyRepository(cur)
            property_repo.add_or_update(
                'unique_id', '22222222-2222-2222-2222-222222222222'
            )
        ds2.to_file(self.filepath2)

    def test_add_link(self):
        """Add link link in both directions (default behavior)."""
        args = argparse.Namespace(
            filepath=self.filepath1,
            command=':',
            subcommand='create',
            filepath2=self.filepath2,
            link='population',
            direction='both',
            description=None,
            selectors=None,
            make_default=True,
        )
        command_relation.create_link(args)  # <- Method under test.

        # Check right-side link (ds1 -> ds2).
        self.assertEqual(
            read_file(self.filepath2).get_link(self.filepath1, 'population'),
            Link(
                id=1,
                other_unique_id='11111111-1111-1111-1111-111111111111',
                other_filename_hint=self.filepath1,
                name='population',
                is_default=True,
            ),
        )

        # Check left-side link (ds1 <- ds2).
        self.assertEqual(
            read_file(self.filepath1).get_link(self.filepath2, 'population'),
            Link(
                id=1,
                other_unique_id='22222222-2222-2222-2222-222222222222',
                other_filename_hint=self.filepath2,
                name='population',
                is_default=True,
            ),
        )

    def test_with_direction(self):
        args = argparse.Namespace(
            filepath=self.filepath1,
            command=':',
            subcommand='create',
            filepath2=self.filepath2,
            link='population',
            direction='right',  # <- Right-side link only.
            description=None,
            selectors=None,
            make_default=True,
        )
        command_relation.create_link(args)  # <- Method under test.

        # Check right-side link (ds1 -> ds2).
        self.assertEqual(
            read_file(self.filepath2).get_link(self.filepath1, 'population'),
            Link(
                id=1,
                other_unique_id='11111111-1111-1111-1111-111111111111',
                other_filename_hint=self.filepath1,
                name='population',
                is_default=True,
            ),
        )

        # Check that left-side link (ds1 <- ds2) does not exist.
        regex = r"no links match reference .+'"
        with self.assertRaisesRegex(ToronError, regex):
            read_file(self.filepath1).get_link(self.filepath2, 'population')

    def test_link_already_exists(self):
        ds1 = bind_file(self.filepath1, mode='rw')
        ds2 = bind_file(self.filepath2, mode='rw')
        ds1.add_link(
            space=ds2,
            link_name='population',
            other_filename_hint=ds2.path_hint,
            description=None,
            selectors=None,
            is_default=True,
        )

        args = argparse.Namespace(
            filepath=self.filepath1,
            command=':',
            subcommand='create',
            filepath2=self.filepath2,
            link='population',
            direction='both',
            description=None,
            selectors=None,
            make_default=True,
        )

        regex = r"a link named 'population' already exists"
        with self.assertRaisesRegex(ToronError, regex):
            command_relation.create_link(args)  # <- Method under test.
