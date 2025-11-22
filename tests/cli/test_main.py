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
    main,
)


class TestToronArgumentParser(StreamWrapperTestCase):
    def setUp(self):
        self.parser = get_parser()  # Get ToronArgumentParser instance.
        super().setUp()

    def test_help_flag(self):
        """Using '-h', should print help to stdout and exit with OK."""
        with self.assertRaises(SystemExit) as cm:
            self.parser.parse_args(['-h'])

        self.assertEqual(cm.exception.code, ExitCode.OK)
        self.assertEqual(self.stdout_capture.getvalue(), self.parser.format_help())
        self.assertFalse(self.stderr_capture.getvalue(), msg='should not write to stderr')

    def test_no_args(self):
        """Using no args should print help to stderr and exit with USAGE error."""
        with self.assertRaises(SystemExit) as cm:
            self.parser.parse_args([])

        self.assertEqual(cm.exception.code, ExitCode.USAGE)
        self.assertFalse(self.stdout_capture.getvalue(), msg='should not write to stdout')
        self.assertEqual(self.stderr_capture.getvalue(), self.parser.format_help())

    def test_help_with_invalid_choice(self):
        """Using '-h' with unknown command should give "invalid choice" error."""
        with self.assertRaises(SystemExit) as cm:
            self.parser.parse_args(['foo', '-h'])  # <- Unknown command "foo".

        self.assertEqual(cm.exception.code, ExitCode.USAGE)
        self.assertFalse(self.stdout_capture.getvalue())
        self.assertIn("invalid choice: 'foo'", self.stderr_capture.getvalue())

    def test_help_with_filename(self):
        """Using '-h' with a filename should give the main help message."""
        file_path = self.get_tempfile_path()
        TopoNode().to_file(file_path)

        with self.assertRaises(SystemExit) as cm:
            self.parser.parse_args([file_path, '-h'])

        self.assertEqual(cm.exception.code, ExitCode.OK)
        self.assertEqual(self.stdout_capture.getvalue(), self.parser.format_help())
        self.assertFalse(self.stderr_capture.getvalue(), msg='should not write to stderr')

    def test_default_command(self):
        """When a filename is given, should invoke "info" by default."""
        file_path = self.get_tempfile_path()
        TopoNode().to_file(file_path)

        args1 = self.parser.parse_args([file_path])  # <- Filename is only arg.
        args2 = self.parser.parse_args(['info', file_path])

        # Contents of `args1` and `args2` should be the same.
        self.assertEqual(list(vars(args1)), list(vars(args2)))
        self.assertEqual(args1.command, args2.command)
        self.assertEqual(args1.node.path_hint, args2.node.path_hint)


class TestMainNewCommand(StreamWrapperTestCase):
    def setUp(self):
        super().setUp()

        # Patch the `command_new` module with a mock object.
        mock_cm = unittest.mock.patch(target='toron.cli.main.command_new')
        self.mock = mock_cm.__enter__()
        self.addCleanup(lambda: mock_cm.__exit__(None, None, None))

    def test_create_file(self):
        """Check call to command_new.create_file()."""
        file_path = self.get_tempfile_path()

        main(['new', file_path])  # Function under test.

        self.mock.create_file.assert_called()

        args, kwds = self.mock.create_file.call_args
        self.assertIsInstance(args[0], argparse.Namespace)
        self.assertEqual(args[0].command, 'new')
        self.assertEqual(args[0].node_path, file_path)


class TestMainIndexCommand(StreamWrapperTestCase):
    def setUp(self):
        super().setUp()

        # Patch the `command_index` module with a mock object.
        mock_cm = unittest.mock.patch(target='toron.cli.command_index')
        self.mock = mock_cm.__enter__()
        self.addCleanup(lambda: mock_cm.__exit__(None, None, None))

    def test_write_to_stdout(self):
        """Check call to command_index.write_to_stdout()."""
        file_path = self.get_tempfile_path()
        TopoNode().to_file(file_path)

        main(['index', file_path], stdin=DummyTTY())  # Function under test.

        self.mock.write_to_stdout.assert_called()

        args, kwds = self.mock.write_to_stdout.call_args
        self.assertIsInstance(args[0], argparse.Namespace)
        self.assertEqual(args[0].command, 'index')
        self.assertIsInstance(args[0].node, TopoNode)

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
            main(['index', file_path])              # only used for redirection.

        self.mock.read_from_stdin.assert_called()

        args, kwds = self.mock.read_from_stdin.call_args
        self.assertIsInstance(args[0], argparse.Namespace)
        self.assertEqual(args[0].command, 'index')
        self.assertIsInstance(args[0].node, TopoNode)

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
            main(['index', file_path, '--no-backup'])  # only used for redirection.

        self.mock.read_from_stdin.assert_called()

        args, kwds = self.mock.read_from_stdin.call_args
        self.assertIsInstance(args[0], argparse.Namespace)
        self.assertEqual(args[0].command, 'index')
        self.assertIsInstance(args[0].node, TopoNode)

        self.assertFalse(self.stdout_capture.getvalue())
        self.assertFalse(self.stderr_capture.getvalue())

        dir_name, base_name = os.path.split(file_path)
        backup_file = os.path.join(dir_name, f'backup-{base_name}')
        if os.path.isfile(backup_file):
            self.addCleanup(lambda: os.remove(backup_file))
            self.fail('backup file was created unintentionally')
