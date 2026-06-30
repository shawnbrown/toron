"""Tests for toron/cli/command_add.py module."""
import argparse
import os
import tempfile
from .. import _unittest as unittest
from toron import TopoNode, ToronError, read_file, bind_node
from toron.data_models import Crosswalk, WeightGroup

from toron.cli import command_add
from toron.cli.common import ExitCode

from ..common import StreamWrapperTestCase


class TempNodeMixin(object):
    """A mixin helper with setUp() that creats a tempoary node file."""
    def setUp(self):
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            self.filepath = tmp.name
        self.addCleanup(lambda: os.remove(self.filepath))

        node = TopoNode()
        node.to_file(self.filepath)


class TestAddLabels(TempNodeMixin, unittest.TestCase):
    def test_add_label(self):
        command_add.add_label(argparse.Namespace(
            filepath=self.filepath,
            command='add',
            element='label',
            labels=['A', 'B', 'C'],
            backup=False,
        ))

        self.assertEqual(
            read_file(self.filepath).index_columns,
            ['A', 'B', 'C'],
        )

    def test_label_already_exists(self):
        command_add.add_label(argparse.Namespace(
            filepath=self.filepath,
            command='add',
            element='label',
            labels=['A', 'B', 'C'],
            backup=False,
        ))

        regex = r"index label column 'B' already exists"
        with self.assertRaisesRegex(ToronError, regex):
            command_add.add_label(argparse.Namespace(
                filepath=self.filepath,
                command='add',
                element='label',
                labels=['B'],
                backup=False,
            ))

    def test_add_label_comma_separated_value(self):
        command_add.add_label(argparse.Namespace(
            filepath=self.filepath,
            command='add',
            element='label',
            labels=['A,B,C'],  # <- Comma-separated value.
            backup=False,
        ))

        self.assertEqual(
            read_file(self.filepath).index_columns,
            ['A', 'B', 'C'],
        )


class TestAddWeight(TempNodeMixin, unittest.TestCase):
    def test_add_weight(self):
        command_add.add_weight(argparse.Namespace(
            filepath=self.filepath,
            command='add',
            element='weight',
            weight='population',
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
        command_add.add_weight(argparse.Namespace(
            filepath=self.filepath,
            command='add',
            element='weight',
            weight='population',
            description=None,
            selectors=None,
            make_default=True,
            backup=False,
        ))

        regex = r"index weight group 'population' already exists"
        with self.assertRaisesRegex(ToronError, regex):
            command_add.add_weight(argparse.Namespace(
                filepath=self.filepath,
                command='add',
                element='weight',
                weight='population',
                description=None,
                selectors=None,
                make_default=True,
                backup=False,
            ))


class TestAddCategory(TempNodeMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        node = bind_node(self.filepath, mode='rw')
        node.add_index_columns('A', 'B', 'C')

    def test_add_category(self):
        command_add.add_category(argparse.Namespace(
            filepath=self.filepath,
            command='add',
            element='category',
            labels=['A', 'B'],
        ))

        self.assertEqual(
            read_file(self.filepath).discrete_categories,
            [{'A', 'B'}, {'A', 'B', 'C'}],
        )

    def test_error_case(self):
        """Failures should raise a ``ToronError``."""
        regex = r"invalid category, no index labels 'D', 'E'"
        with self.assertRaisesRegex(ToronError, regex):
            command_add.add_category(argparse.Namespace(
                filepath=self.filepath,
                command='add',
                element='category',
                labels=['C', 'D', 'E'],
            ))

    def test_add_category_comma_separated_value(self):
        command_add.add_category(argparse.Namespace(
            filepath=self.filepath,
            command='add',
            element='category',
            labels=['A,B'],  # <- Comma-separated value.
        ))

        self.assertEqual(
            read_file(self.filepath).discrete_categories,
            [{'A', 'B'}, {'A', 'B', 'C'}],
        )


class TestAddAttributes(TempNodeMixin, unittest.TestCase):
    def test_add_attributes(self):
        args = argparse.Namespace(
            filepath=self.filepath,
            command='add',
            element='attribute',
            attributes=['foo', 'bar', 'baz'],
            backup=False,
            func=command_add.add_attribute,
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_add.add_attribute(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            cm.output,
            ["INFO:app-toron:added attribute columns: 'foo', 'bar', 'baz'"],
        )
        self.assertEqual(
            read_file(self.filepath).get_registered_attributes(),
            ['foo', 'bar', 'baz'],
        )

    def test_attribute_already_exists(self):
        command_add.add_attribute(argparse.Namespace(
            filepath=self.filepath,
            command='add',
            element='attribute',
            attributes=['baz'],
            backup=False,
            func=command_add.add_attribute,
        ))

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_add.add_attribute(argparse.Namespace(
                filepath=self.filepath,
                command='add',
                element='attribute',
                attributes=['foo', 'bar', 'baz'],
                backup=False,
                func=command_add.add_attribute,
            ))

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            cm.output,
            ["WARNING:app-toron:skipping 'baz' (already registered)",
             "INFO:app-toron:added attribute columns: 'foo', 'bar'"],
        )
        self.assertEqual(
            read_file(self.filepath).get_registered_attributes(),
            ['baz', 'foo', 'bar'],  # <- First item is 'baz'.
            msg="since 'baz' already existed, it retains its original position",
        )

    def test_bad_attribute_name(self):
        regex = r"'domain' is a reserved name"
        with self.assertRaisesRegex(ToronError, regex):
            command_add.add_attribute(argparse.Namespace(
                filepath=self.filepath,
                command='add',
                element='attribute',
                attributes=['foo', 'bar', 'domain'],
                backup=False,
                func=command_add.add_attribute,
            ))

    def test_add_attributes_comma_separated_value(self):
        command_add.add_attribute(argparse.Namespace(
            filepath=self.filepath,
            command='add',
            element='attribute',
            attributes=['foo,bar,baz'],  # <- Comma-separated value.
            backup=False,
            func=command_add.add_attribute,
        ))

        self.assertEqual(
            read_file(self.filepath).get_registered_attributes(),
            ['foo', 'bar', 'baz'],
        )


class TestAddLink(unittest.TestCase):
    def setUp(self):
        with tempfile.NamedTemporaryFile(delete=False) as tmp1:
            self.filepath1 = tmp1.name
        self.addCleanup(lambda: os.remove(self.filepath1))
        node1 = TopoNode()
        with node1._managed_transaction() as cur:
            property_repo = node1._dal.PropertyRepository(cur)
            property_repo.add_or_update(
                'unique_id', '11111111-1111-1111-1111-111111111111'
            )
        node1.to_file(self.filepath1)

        with tempfile.NamedTemporaryFile(delete=False) as tmp2:
            self.filepath2 = tmp2.name
        self.addCleanup(lambda: os.remove(self.filepath2))
        node2 = TopoNode()
        with node2._managed_transaction() as cur:
            property_repo = node2._dal.PropertyRepository(cur)
            property_repo.add_or_update(
                'unique_id', '22222222-2222-2222-2222-222222222222'
            )
        node2.to_file(self.filepath2)

    def test_add_link(self):
        """Add crosswalk link in both directions (default behavior)."""
        args = argparse.Namespace(
            filepath=self.filepath1,
            command='add',
            element='link',
            filepath2=self.filepath2,
            link='population',
            direction='both',
            description=None,
            selectors=None,
            make_default=True,
        )
        command_add.add_link(args)

        # Check right-side crosswalk (node1 -> node2).
        self.assertEqual(
            read_file(self.filepath2).get_crosswalk(self.filepath1, 'population'),
            Crosswalk(
                id=1,
                other_unique_id='11111111-1111-1111-1111-111111111111',
                other_filename_hint=self.filepath1,
                name='population',
                is_default=True,
            ),
        )

        # Check left-side crosswalk (node1 <- node2).
        self.assertEqual(
            read_file(self.filepath1).get_crosswalk(self.filepath2, 'population'),
            Crosswalk(
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
            command='add',
            element='crosswalk',
            filepath2=self.filepath2,
            link='population',
            direction='right',  # <- Right-side link only.
            description=None,
            selectors=None,
            make_default=True,
        )
        command_add.add_link(args)

        # Check right-side crosswalk (node1 -> node2).
        self.assertEqual(
            read_file(self.filepath2).get_crosswalk(self.filepath1, 'population'),
            Crosswalk(
                id=1,
                other_unique_id='11111111-1111-1111-1111-111111111111',
                other_filename_hint=self.filepath1,
                name='population',
                is_default=True,
            ),
        )

        # Check that left-side crosswalk (node1 <- node2) does not exist.
        self.assertIsNone(
            read_file(self.filepath1).get_crosswalk(self.filepath2, 'population')
        )

    def test_crosswalk_already_exists(self):
        node1 = bind_node(self.filepath1, mode='rw')
        node2 = bind_node(self.filepath2, mode='rw')
        node1.add_crosswalk(
            node=node2,
            crosswalk_name='population',
            other_filename_hint=node2.path_hint,
            description=None,
            selectors=None,
            is_default=True,
        )

        args = argparse.Namespace(
            filepath=self.filepath1,
            command='add',
            element='link',
            filepath2=self.filepath2,
            link='population',
            direction='both',
            description=None,
            selectors=None,
            make_default=True,
        )

        regex = r"a link named 'population' already exists"
        with self.assertRaisesRegex(ToronError, regex):
            command_add.add_link(args)
