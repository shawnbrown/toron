"""Tests for toron/cli/command_new.py module."""
import argparse
from .. import _unittest as unittest
from toron import TopoNode, ToronError
from toron.data_models import Crosswalk, WeightGroup

from toron.cli import command_add
from toron.cli.common import ExitCode


class TestAddLabels(unittest.TestCase):
    def test_add_labels(self):
        node = TopoNode()

        self.assertEqual(node.index_columns, [])

        args = argparse.Namespace(
            command='add', element='label', node=node, labels=['A', 'B', 'C']
        )
        command_add.add_labels(args)  # Function under test.

        self.assertEqual(node.index_columns, ['A', 'B', 'C'])

    def test_label_already_exists(self):
        node = TopoNode()
        command_add.add_labels(argparse.Namespace(
            command='add', element='label', node=node, labels=['A', 'B', 'C']
        ))

        regex = r"index label column 'B' already exists"
        with self.assertRaisesRegex(ToronError, regex):
            command_add.add_labels(argparse.Namespace(
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


class TestAddAttributes(unittest.TestCase):
    def test_add_attributes(self):
        node = TopoNode()

        self.assertEqual(node.get_registered_attributes(), [])

        args = argparse.Namespace(
            command='add',
            element='attributes',
            node=node,
            attributes=['foo', 'bar', 'baz'],
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_add.add_attribute(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)
        self.assertEqual(
            cm.output,
            ["INFO:app-toron:added attribute columns: 'foo', 'bar', 'baz'"],
        )
        self.assertEqual(node.get_registered_attributes(), ['foo', 'bar', 'baz'])

    def test_attribute_already_exists(self):
        node = TopoNode()
        node.set_registered_attributes(['baz'])

        args = argparse.Namespace(
            command='add',
            element='attributes',
            node=node,
            attributes=['foo', 'bar', 'baz'],
        )

        with self.assertLogs('app-toron', level='INFO') as cm:
            exit_code = command_add.add_attribute(args)  # Function under test.

        self.assertEqual(exit_code, ExitCode.OK)

        self.assertEqual(
            cm.output,
            ["WARNING:app-toron:skipping 'baz' (already registered)",
             "INFO:app-toron:added attribute columns: 'foo', 'bar'"],
        )

        self.assertEqual(
            node.get_registered_attributes(),
            ['baz', 'foo', 'bar'],  # <- First item is 'baz'.
            msg="since 'baz' already existed, it retains its original position",
        )

    def test_bad_attribute_name(self):
        node = TopoNode()

        args = argparse.Namespace(
            command='add',
            element='attributes',
            node=node,
            attributes=['foo', 'bar', 'domain'],
        )

        regex = r"'domain' is a reserved name"
        with self.assertRaisesRegex(ToronError, regex):
            command_add.add_attribute(args)  # Function under test.


class TestAddCrosswalk(unittest.TestCase):
    def setUp(self):
        self.node1 = TopoNode()
        self.node1._connector._unique_id = '11111111-1111-1111-1111-111111111111'
        self.node1.path_hint = 'node1.toron'

        self.node2 = TopoNode()
        self.node2._connector._unique_id = '22222222-2222-2222-2222-222222222222'
        self.node2.path_hint = 'node2.toron'

    def test_add_crosswalk(self):
        """Add crosswalk in both directions (default behavior)."""
        args = argparse.Namespace(
            command='add',
            element='crosswalk',
            node1=self.node1,
            node2=self.node2,
            crosswalk='population',
            direction='both',
            description=None,
            selectors=None,
            make_default=True,
        )
        command_add.add_crosswalk(args)

        # Check right-side crosswalk (node1 -> node2).
        actual = self.node2.get_crosswalk(self.node1, 'population')
        expected = Crosswalk(
            id=1,
            other_unique_id='11111111-1111-1111-1111-111111111111',
            other_filename_hint='node1.toron',
            name='population',
            is_default=True,
        )
        self.assertEqual(actual, expected)

        # Check left-side crosswalk (node1 <- node2).
        actual = self.node1.get_crosswalk(self.node2, 'population')
        expected = Crosswalk(
            id=1,
            other_unique_id='22222222-2222-2222-2222-222222222222',
            other_filename_hint='node2.toron',
            name='population',
            is_default=True,
        )
        self.assertEqual(actual, expected)

    def test_with_direction(self):
        args = argparse.Namespace(
            command='add',
            element='crosswalk',
            node1=self.node1,
            node2=self.node2,
            crosswalk='population',
            direction='right',  # <- Right-side crosswalk only.
            description=None,
            selectors=None,
            make_default=True,
        )
        command_add.add_crosswalk(args)

        # Check right-side crosswalk (node1 -> node2).
        actual = self.node2.get_crosswalk(self.node1, 'population')
        expected = Crosswalk(
            id=1,
            other_unique_id='11111111-1111-1111-1111-111111111111',
            other_filename_hint='node1.toron',
            name='population',
            is_default=True,
        )
        self.assertEqual(actual, expected)

        # Check that left-side crosswalk (node1 <- node2) does not exist.
        actual = self.node1.get_crosswalk(self.node2, 'population')
        self.assertIsNone(actual)

    def test_crosswalk_already_exists(self):
        self.node1.add_crosswalk(
            node=self.node2,
            crosswalk_name='population',
            other_filename_hint=self.node2.path_hint,
            description=None,
            selectors=None,
            is_default=True,
        )

        args = argparse.Namespace(
            command='add',
            element='crosswalk',
            node1=self.node1,
            node2=self.node2,
            crosswalk='population',
            direction='both',
            description=None,
            selectors=None,
            make_default=True,
        )

        regex = r"a crosswalk named 'population' already exists"
        with self.assertRaisesRegex(ToronError, regex):
            command_add.add_crosswalk(args)
