"""Tests for toron/cli/main.py module."""
import argparse
import io
import os
from .. import _unittest as unittest
from ..common import StreamWrapperMixin, DummyTTY
from toron import DataSpace

from toron.cli.common import ExitCode
from toron.cli.main import (
    get_parser,
    command_init,
    command_label,
    command_weight,
    command_index,
    command_partition,
    command_attribute,
    command_quantity,
    command_info,
    command_relation,
    main,
)


class TestMainHelp(StreamWrapperMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.parser = get_parser()  # Get ToronArgumentParser instance.

    def test_help_explicit(self):
        """When calling help explicitly, should write to stdout and exit with OK."""
        with self.assertRaises(SystemExit) as cm:
            self.parser.parse_args(['-h'])

        self.assertEqual(cm.exception.code, ExitCode.OK)
        self.assertStdout(self.parser.format_help())
        self.assertStderr('', msg='should not write to stderr')

    def test_help_no_args(self):
        """Using no args, should print full help to stderr and exit with USAGE error."""
        with self.assertRaises(SystemExit) as cm:
            self.parser.parse_args([])

        self.assertEqual(cm.exception.code, ExitCode.USAGE)
        self.assertStdout('', msg='should not write to stdout')
        self.assertStderr(self.parser.format_help())

    def test_help_with_invalid_choice(self):
        """Using '-h' with unknown command should give "invalid choice" error."""
        with self.assertRaises(SystemExit) as cm:
            self.parser.parse_args(['blerg', '--help'])

        self.assertEqual(cm.exception.code, ExitCode.USAGE)
        self.assertStdout('', msg='should not write to stdout')
        self.assertInStderr("invalid choice: 'blerg'")

    def test_help_with_filename(self):
        """Using '-h' with a filename should give the main help message."""
        file_path = self.get_tempfile_path()
        DataSpace().to_file(file_path)

        with self.assertRaises(SystemExit) as cm:
            self.parser.parse_args([file_path, '-h'])

        self.assertEqual(cm.exception.code, ExitCode.OK)
        self.assertStdout(self.parser.format_help())
        self.assertStderr('', msg='should not write to stderr')

    def test_help_for_relation_commands(self):
        """Using `: COMMAND -h` should give relation subcommand help."""
        file_path = self.get_tempfile_path()
        DataSpace().to_file(file_path)

        with self.assertRaises(SystemExit) as cm:
            self.parser.parse_args([':', 'add', '-h'])

        self.assertInStdout('usage: toron FILE1 : FILE2 add')
        self.assertStderr('', msg='should not write to stderr')
        self.assertEqual(cm.exception.code, ExitCode.OK)


class TestToronArgumentParser(StreamWrapperMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.parser = get_parser()  # Get ToronArgumentParser instance.

    def assertNamespaceEqual(self, first, second, msg=None):
        """Check that `argparse.Namespace` instances are equal."""
        # Check instances types.
        self.assertIsInstance(first, argparse.Namespace, msg)
        self.assertIsInstance(second, argparse.Namespace, msg)

        # Check Namespace contents.
        self.assertDictEqual(vars(first), vars(second), msg)

    def test_subcommand_init(self):
        """Check "init" subparser."""
        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile.toron',
                'init',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='init',
                domain=None,
                func=command_init.create_file,
            ),
        )

        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile.toron',
                'init',
                '--domain', 'mydomain',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='init',
                domain='mydomain',
                func=command_init.create_file,
            ),
        )

    def test_subcommand_add_weight(self):
        """Check "add weight" subparser."""
        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile.toron',
                'weight',
                'add',
                '--description', 'Census 2020 Population',
                '--selectors', '[foo="bar"]', '[baz]',
                '--make-default',
                'population',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='weight',
                subcommand='add',
                name='population',
                description='Census 2020 Population',
                selectors=['[foo="bar"]', '[baz]'],
                make_default=True,
                backup=True,
                func=command_weight.add,
            ),
        )

        # Check minimal invocation (no description or selectors).
        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile.toron',
                'weight',
                'add',
                'population',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='weight',
                subcommand='add',
                name='population',
                description=None,
                selectors=None,
                make_default=False,
                backup=True,
                func=command_weight.add,
            ),
        )

    def test_subcommand_add_partition(self):
        """Check "add partition" subparser."""
        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile.toron',
                'partition',
                'add',
                'foo', 'bar', 'baz',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='partition',
                subcommand='add',
                names=['foo', 'bar', 'baz'],
                backup=True,
                func=command_partition.add,
            ),
        )

    def test_subcommand_add_attribute(self):
        """Check "add attribute" subparser."""
        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile.toron',
                'attribute',
                'add',
                'foo', 'bar', 'baz',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='attribute',
                subcommand='add',
                names=['foo', 'bar', 'baz'],
                backup=True,
                func=command_attribute.add,
            ),
        )

    def test_subcommand_add_link(self):
        """Check "add link" subparser."""
        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile1.toron',
                ':',
                'myfile2.toron',
                'add',
                'mylink',
                '--description', 'Description of this link.',
                '--selectors', '[foo="bar"]', '[baz]',
                '--default',
                '--no-backup',
            ]),
            argparse.Namespace(
                filepath='myfile1.toron',
                command=':',
                filepath2='myfile2.toron',
                subcommand='add',
                link_name='mylink',
                direction='both',
                description='Description of this link.',
                selectors=['[foo="bar"]', '[baz]'],
                make_default=True,
                backup=False,
                func=command_relation.add_link,
            )
        )

    def test_subcommand_update_label(self):
        """Check "update label" subparser."""
        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile.toron',
                'label',
                'update',
                'foo',
                '--move-left',  # <- Option without int arg.
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='label',
                subcommand='update',
                name='foo',
                move_left=1,  # <- Gets 1 when no int is given.
                move_right=0,
                backup=True,
                func=command_label.update,
            ),
        )

        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile.toron',
                'label',
                'update',
                'foo',
                '--move-right', '3',  # <- Option with an int arg.
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='label',
                subcommand='update',
                name='foo',
                move_left=0,
                move_right=3,  # <- Gets int as given.
                backup=True,
                func=command_label.update,
            ),
        )

        msg = 'parser should forbid negative integers here'
        with self.assertRaises(SystemExit):
            self.parser.parse_args([
                'myfile.toron',
                'update',
                'label',
                'foo',
                '--move-left',
                '-3',  # <- Negative value should raise error.
            ])

        msg = 'parser should forbid left and right at the same time'
        with self.assertRaises(SystemExit, msg=msg):
            self.parser.parse_args([
                'myfile.toron',
                'update',
                'label',
                'foo',
                '--move-left',   # <- Should only allow one direction.
                '--move-right',  # <- Should only allow one direction.
            ])

    def test_subcommand_update_weight(self):
        """Check "update weight" subparser."""
        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile.toron',
                'weight',
                'update',
                'myweight',
                '--description',
                'New description.',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='weight',
                subcommand='update',
                backup=True,
                name='myweight',
                description='New description.',
                add_selector=None,
                remove_selector=None,
                make_default=False,
                func=command_weight.update,
            ),
        )

    def test_subcommand_update_attribute(self):
        """Check "update attribute" subparser."""
        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile.toron',
                'attribute',
                'update',
                'foo',
                '--move-left',  # <- Option without int arg.
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='attribute',
                subcommand='update',
                name='foo',
                move_left=1,  # <- Gets 1 when no int is given.
                move_right=0,
                backup=True,
                func=command_attribute.update,
            ),
        )

        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile.toron',
                'attribute',
                'update',
                'foo',
                '--move-right', '3',  # <- Option with an int arg.
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='attribute',
                subcommand='update',
                name='foo',
                move_left=0,
                move_right=3,  # <- Gets int as given.
                backup=True,
                func=command_attribute.update,
            ),
        )

        msg = 'parser should forbid negative integers here'
        with self.assertRaises(SystemExit):
            self.parser.parse_args([
                'myfile.toron',
                'update',
                'attribute',
                'foo',
                '--move-left',
                '-3',  # <- Negative value should raise error.
            ])

        msg = 'parser should forbid left and right at the same time'
        with self.assertRaises(SystemExit, msg=msg):
            self.parser.parse_args([
                'myfile.toron',
                'update',
                'attribute',
                'foo',
                '--move-left',   # <- Should only allow one direction.
                '--move-right',  # <- Should only allow one direction.
            ])

    def test_subcommand_rename_label(self):
        """Check "rename label" subparser."""
        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile.toron',
                'label',
                'rename',
                'foo',
                'bar',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='label',
                subcommand='rename',
                old_name='foo',
                new_name='bar',
                backup=True,
                func=command_label.rename,
            ),
        )

    def test_subcommand_rename_domain(self):
        """Check "rename domain" subparser."""
        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile.toron',
                'info',
                '--set-domain', 'new_domain_name',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='info',
                set_domain='new_domain_name',
                func=command_info.write_to_stdout,
            ),
        )

    def test_subcommand_remove_link(self):
        """Check "remove link" subparser."""
        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile1.toron',
                ':',
                'myfile2.toron',
                'remove',
                'my_link_name',
            ]),
            argparse.Namespace(
                filepath='myfile1.toron',
                command=':',
                filepath2='myfile2.toron',
                subcommand='remove',
                link_name='my_link_name',
                direction='both',
                backup=True,
                func=command_relation.remove_link,
            ),
        )

    # def test_subcommand_index(self):
    #     """Check "index" subparser."""

    #def test_subcommand_quantity(self):
    #    """Check "quantity" subparser."""

    def test_subcommand_relation(self):
        """Check "mapping" subparser."""
        self.assertNamespaceEqual(
            self.parser.parse_args([
                'file1.toron',
                ':',
                'file2.toron',
                'export',
                '.'
            ]),
            argparse.Namespace(
                filepath='file1.toron',
                command=':',
                filepath2='file2.toron',
                subcommand='export',
                target='.',
                link_name=None,
                force=False,
                func=command_relation.export_records,
            ),
        )

    def test_subcommand_info(self):
        """Check "info" subparser."""
        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile.toron',
                'info',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='info',
                set_domain=None,
                func=command_info.write_to_stdout,
            ),
        )

    def test_subcommand_default(self):
        """When no COMMAND is given, should default to 'info'."""
        self.assertNamespaceEqual(
            self.parser.parse_args([
                'myfile.toron',  # <- FILE only (no COMMAND).
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='info',
                set_domain=None,
                func=command_info.write_to_stdout,
            ),
        )

        # Check when invoking help with a single argument.
        with self.assertRaises(SystemExit):
            args = self.parser.parse_args(['-h'])

        self.assertEqual(
            self.stdout_capture.getvalue(),
            self.parser.format_help(),
        )

    def test_help_for_subcommand(self):
        """Should support subcommand help without FILE argument."""
        # Check "init" subcommand.
        with self.assertRaises(SystemExit):
            self.parser.parse_args(['init', '--help'])

        self.assertRegex(
            self.stdout_capture.getvalue(),
            (r'^usage: toron FILE init \[-h\] \[--domain DOMAIN\]\n'
             r'\n'
             r'Create a new Toron file.'),
        )

        # Clear stdout buffer for next check.
        self.stdout_capture.seek(0)
        self.stdout_capture.truncate()

        # Check "info" subcommand.
        with self.assertRaises(SystemExit):
            self.parser.parse_args(['info', '--help'])

        self.assertRegex(
            self.stdout_capture.getvalue(),
            (r'^usage: toron FILE info \[-h\][^\n]*\n'
             r'\n'
             r'Show file information.'),
        )


class TestMainNewCommand(StreamWrapperMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()

        # Patch the `command_init` module with a mock object.
        self.mock = self.enterContext(unittest.mock.patch(target='toron.cli.main.command_init'))

    def test_create_file(self):
        """Check call to command_init.create_file()."""
        filepath = self.get_tempfile_path()

        main([filepath, 'init'])  # Function under test.

        self.mock.create_file.assert_called()

        args, kwds = self.mock.create_file.call_args
        self.assertIsInstance(args[0], argparse.Namespace)
        self.assertEqual(args[0].command, 'init')
        self.assertEqual(args[0].filepath, filepath)
