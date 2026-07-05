"""Tests for toron/cli/main.py module."""
import argparse
import io
import os
from .. import _unittest as unittest
from ..common import StreamWrapperTestCase, DummyTTY
from toron import TopoNode

from toron.cli.common import ExitCode
from toron.cli.main import (
    get_parser,
    command_init,
    command_add,
    command_update,
    command_rename,
    command_index,
    command_quantity,
    command_mapping,
    command_info,
    main,
)


class TestToronArgumentParser(StreamWrapperTestCase):
    def setUp(self):
        self.parser = get_parser()  # Get ToronArgumentParser instance.
        super().setUp()

    def test_help_explicit(self):
        """When calling help explicitly, should write to stdout and exit with OK."""
        with self.assertRaises(SystemExit) as cm:
            self.parser.parse_args(['-h'])

        self.assertEqual(cm.exception.code, ExitCode.OK)
        self.assertEqual(self.stdout_capture.getvalue(), self.parser.format_help())
        self.assertFalse(self.stderr_capture.getvalue(), msg='should not write to stderr')

    def test_help_no_args(self):
        """Using no args, should print full help to stderr and exit with USAGE error."""
        with self.assertRaises(SystemExit) as cm:
            self.parser.parse_args([])

        self.assertEqual(cm.exception.code, ExitCode.USAGE)
        self.assertFalse(self.stdout_capture.getvalue(), msg='should not write to stdout')
        self.assertEqual(self.stderr_capture.getvalue(), self.parser.format_help())

    def test_help_with_invalid_choice(self):
        """Using '-h' with unknown command should give "invalid choice" error."""
        with self.assertRaises(SystemExit) as cm:
            self.parser.parse_args(['blerg', '--help'])

        self.assertEqual(cm.exception.code, ExitCode.USAGE)
        self.assertFalse(self.stdout_capture.getvalue(), msg='should not write to stdout')
        self.assertIn("invalid choice: 'blerg'", self.stderr_capture.getvalue())

    def test_help_with_filename(self):
        """Using '-h' with a filename should give the main help message."""
        file_path = self.get_tempfile_path()
        TopoNode().to_file(file_path)

        with self.assertRaises(SystemExit) as cm:
            self.parser.parse_args([file_path, '-h'])

        self.assertEqual(cm.exception.code, ExitCode.OK)
        self.assertEqual(self.stdout_capture.getvalue(), self.parser.format_help())
        self.assertFalse(self.stderr_capture.getvalue(), msg='should not write to stderr')

    def test_subcommand_init(self):
        """Check "init" subparser."""
        self.assertEqual(
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

        self.assertEqual(
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

    def test_subcommand_add_label(self):
        """Check "add label" subparser."""
        self.assertEqual(
            self.parser.parse_args([
                'myfile.toron',
                'add',
                'label',
                'foo', 'bar', 'baz',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='add',
                element='label',
                labels=['foo', 'bar', 'baz'],
                backup=True,
                func=command_add.add_label,
            ),
        )

    def test_subcommand_add_weight(self):
        """Check "add weight" subparser."""
        self.assertEqual(
            self.parser.parse_args([
                'myfile.toron',
                'add',
                'weight',
                '--description', 'Census 2020 Population',
                '--selectors', '[foo="bar"]', '[baz]',
                '--default',
                'population',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='add',
                element='weight',
                weight='population',
                description='Census 2020 Population',
                selectors=['[foo="bar"]', '[baz]'],
                make_default=True,
                backup=True,
                func=command_add.add_weight,
            ),
        )

        # Check minimal invocation (no description or selectors).
        self.assertEqual(
            self.parser.parse_args([
                'myfile.toron',
                'add',
                'weight',
                'population',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='add',
                element='weight',
                weight='population',
                description=None,
                selectors=None,
                make_default=False,
                backup=True,
                func=command_add.add_weight,
            ),
        )

    def test_subcommand_add_category(self):
        """Check "add category" subparser."""
        self.assertEqual(
            self.parser.parse_args([
                'myfile.toron',
                'add',
                'category',
                'foo', 'bar', 'baz',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='add',
                element='category',
                labels=['foo', 'bar', 'baz'],
                backup=True,
                func=command_add.add_category,
            ),
        )

    def test_subcommand_add_attribute(self):
        """Check "add attribute" subparser."""
        self.assertEqual(
            self.parser.parse_args([
                'myfile.toron',
                'add',
                'attribute',
                'foo', 'bar', 'baz',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='add',
                element='attribute',
                attributes=['foo', 'bar', 'baz'],
                backup=True,
                func=command_add.add_attribute,
            ),
        )

    def test_subcommand_add_link(self):
        """Check "add link" subparser."""
        self.assertEqual(
            self.parser.parse_args([
                'myfile1.toron',
                'add',
                'link',
                'myfile2.toron',
                'mylink',
                '--description', 'Description of this link.',
                '--selectors', '[foo="bar"]', '[baz]',
                '--default',
                '--no-backup',
            ]),
            argparse.Namespace(
                filepath='myfile1.toron',
                command='add',
                element='link',
                filepath2='myfile2.toron',
                link='mylink',
                direction='both',
                description='Description of this link.',
                selectors=['[foo="bar"]', '[baz]'],
                make_default=True,
                backup=False,
                func=command_add.add_link,
            )
        )

    def test_subcommand_update_label(self):
        """Check "update label" subparser."""
        self.assertEqual(
            self.parser.parse_args([
                'myfile.toron',
                'update',
                'label',
                'foo',
                '--move-left',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='update',
                element='label',
                label='foo',
                move_left=1,
                move_right=0,
                backup=True,
                func=command_update.update_label,
            ),
        )

        msg = 'argparse should forbid left and right at the same time'
        with self.assertRaises(SystemExit, msg=msg):
            self.parser.parse_args([
                'myfile.toron',
                'update',
                'label',
                'foo',
                '--move-left',   # <- Should only allow one direction.
                '--move-right',  # <- Should only allow one direction.
            ]),

    def test_subcommand_rename_label(self):
        """Check "rename label" subparser."""
        self.assertEqual(
            self.parser.parse_args([
                'myfile.toron',
                'rename',
                'label',
                'foo',
                'bar',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='rename',
                element='label',
                old_label='foo',
                new_label='bar',
                backup=True,
                func=command_rename.rename_label,
            ),
        )

    def test_subcommand_index(self):
        """Check "index" subparser."""
        self.assertEqual(
            self.parser.parse_args([
                'myfile.toron',
                'index',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='index',
                on_label_conflict='abort',
                on_weight_conflict='abort',
                backup=True,
                func=command_index.process_index_action,
            ),
        )

    def test_subcommand_quantity(self):
        """Check "quantity" subparser."""
        self.assertEqual(
            self.parser.parse_args([
                'myfile.toron',
                'quantity',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='quantity',
                value_column='quantity',
                allow_invalid_label=False,
                allow_invalid_category=False,
                on_existing='abort',
                backup=True,
                func=command_quantity.process_quantity_action,
            ),
        )

    def test_subcommand_mapping(self):
        """Check "mapping" subparser."""
        self.assertEqual(
            self.parser.parse_args([
                'file1.toron',
                'mapping',
                'file2.toron',
                'linkname',
            ]),
            argparse.Namespace(
                filepath='file1.toron',
                command='mapping',
                filepath2='file2.toron',
                link='linkname',
                direction='both',
                match_limit=1,
                allow_overlapping=False,
                allow_incomplete=False,
                backup=True,
                func=command_mapping.process_mapping_action,
            ),
        )

    def test_subcommand_info(self):
        """Check "info" subparser."""
        self.assertEqual(
            self.parser.parse_args([
                'myfile.toron',
                'info',
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='info',
                func=command_info.write_to_stdout,
            ),
        )

    def test_subcommand_default(self):
        """When no COMMAND is given, should default to 'info'."""
        self.assertEqual(
            self.parser.parse_args([
                'myfile.toron',  # <- FILE only (no COMMAND).
            ]),
            argparse.Namespace(
                filepath='myfile.toron',
                command='info',
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
             r'Create a new node file.'),
        )

        # Clear stdout buffer for next check.
        self.stdout_capture.seek(0)
        self.stdout_capture.truncate()

        # Check "info" subcommand.
        with self.assertRaises(SystemExit):
            self.parser.parse_args(['info', '--help'])

        self.assertRegex(
            self.stdout_capture.getvalue(),
            (r'^usage: toron FILE info \[-h\]\n'
             r'\n'
             r'Show file information.'),
        )


class TestMainNewCommand(StreamWrapperTestCase):
    def setUp(self):
        super().setUp()

        # Patch the `command_init` module with a mock object.
        mock_cm = unittest.mock.patch(target='toron.cli.main.command_init')
        self.mock = mock_cm.__enter__()
        self.addCleanup(lambda: mock_cm.__exit__(None, None, None))

    def test_create_file(self):
        """Check call to command_init.create_file()."""
        filepath = self.get_tempfile_path()

        main([filepath, 'init'])  # Function under test.

        self.mock.create_file.assert_called()

        args, kwds = self.mock.create_file.call_args
        self.assertIsInstance(args[0], argparse.Namespace)
        self.assertEqual(args[0].command, 'init')
        self.assertEqual(args[0].filepath, filepath)


class TestMainIndexCommand(StreamWrapperTestCase):
    def setUp(self):
        super().setUp()

        # Patch `command_index.read_from_stdin()` function with mock object.
        mock_read_from_stdin_cm = unittest.mock.patch(target='toron.cli.main.command_index.read_from_stdin')
        self.mock_read_from_stdin = mock_read_from_stdin_cm.__enter__()
        self.addCleanup(lambda: mock_read_from_stdin_cm.__exit__(None, None, None))

        # Patch `command_index.write_to_stdout()` function with mock object.
        mock_write_to_stdout_cm = unittest.mock.patch(target='toron.cli.main.command_index.write_to_stdout')
        self.mock_write_to_stdout = mock_write_to_stdout_cm.__enter__()
        self.addCleanup(lambda: mock_write_to_stdout_cm.__exit__(None, None, None))

    def test_write_to_stdout(self):
        """Check call to command_index.write_to_stdout()."""
        file_path = self.get_tempfile_path()
        TopoNode().to_file(file_path)

        main([file_path, 'index'], stdin=DummyTTY())  # Function under test.

        self.mock_write_to_stdout.assert_called()

        args, kwds = self.mock_write_to_stdout.call_args
        self.assertIsInstance(args[0], argparse.Namespace)
        self.assertEqual(args[0].command, 'index')
        self.assertIsInstance(args[0].filepath, str)

        self.assertFalse(self.stdout_capture.getvalue())
        self.assertFalse(self.stderr_capture.getvalue())

        dir_name, base_name = os.path.split(file_path)
        backup_file = os.path.join(dir_name, f'backup-{base_name}')
        if os.path.isfile(backup_file):
            self.addCleanup(lambda: os.remove(backup_file))
            self.fail('backup file was created unintentionally')

    def test_read_from_stdin(self):
        """Check call to command_index.read_from_stdin()."""
        file_path = self.get_tempfile_path()
        TopoNode().to_file(file_path)

        with self.patched_stdin('A,B\nfoo,bar\n'):  # Dummy input not ingested,
            main([file_path, 'index'])              # only used for redirection.

        self.mock_read_from_stdin.assert_called()

        args, kwds = self.mock_read_from_stdin.call_args
        self.assertIsInstance(args[0], argparse.Namespace)
        self.assertEqual(args[0].command, 'index')
        self.assertIsInstance(args[0].filepath, str)

        self.assertFalse(self.stdout_capture.getvalue())
        self.assertFalse(self.stderr_capture.getvalue())

        dir_name, base_name = os.path.split(file_path)
        backup_file = os.path.join(dir_name, f'backup-{base_name}')
        self.assertTrue(os.path.isfile(backup_file))
        self.addCleanup(lambda: os.remove(backup_file))

    def test_read_from_stdin_no_backup(self):
        """Should not write a '.bak' file when passing `--no-backup`."""
        file_path = self.get_tempfile_path()
        TopoNode().to_file(file_path)

        with self.patched_stdin('A,B\nfoo,bar\n'):     # Dummy input not ingested,
            main([file_path, 'index', '--no-backup'])  # only used for redirection.

        self.mock_read_from_stdin.assert_called()

        args, kwds = self.mock_read_from_stdin.call_args
        self.assertIsInstance(args[0], argparse.Namespace)
        self.assertEqual(args[0].command, 'index')
        self.assertIsInstance(args[0].filepath, str)

        self.assertFalse(self.stdout_capture.getvalue())
        self.assertFalse(self.stderr_capture.getvalue())

        dir_name, base_name = os.path.split(file_path)
        backup_file = os.path.join(dir_name, f'backup-{base_name}')
        if os.path.isfile(backup_file):
            self.addCleanup(lambda: os.remove(backup_file))
            self.fail('backup file was created unintentionally')
