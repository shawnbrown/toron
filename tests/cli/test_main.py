"""Tests for toron/cli/main.py module."""
import io
import os
from contextlib import (
    closing,
    redirect_stdout,
    redirect_stderr,
)
from tempfile import NamedTemporaryFile
from .. import _unittest as unittest
from toron import TopoNode

from toron.cli.common import ExitCode
from toron.cli.main import (
    get_parser,
)


class TestGetParser(unittest.TestCase):
    def setUp(self):
        stdout_cm = redirect_stdout(io.StringIO())
        self.stdout_capture = stdout_cm.__enter__()
        self.addCleanup(lambda: stdout_cm.__exit__(None, None, None))

        stderr_cm = redirect_stderr(io.StringIO())
        self.stderr_capture = stderr_cm.__enter__()
        self.addCleanup(lambda: stderr_cm.__exit__(None, None, None))

    def get_tempfile_path(self):
        """Helper function to get a path to a temporary file."""
        with closing(NamedTemporaryFile(delete=False)) as tmp:
            self.addCleanup(lambda: os.remove(tmp.name))
        return tmp.name

    def test_main_help_explicit(self):
        """Calling with '-h', should print help to stdout and exit with OK."""
        parser = get_parser()

        with self.assertRaises(SystemExit) as cm:
            parser.parse_args(['-h'])

        self.assertEqual(cm.exception.code, ExitCode.OK)
        self.assertEqual(self.stdout_capture.getvalue(), parser.format_help())
        self.assertFalse(self.stderr_capture.getvalue(), msg='should not write to stderr')

    def test_main_no_args(self):
        """Calling without args should print help to stderr and exit with USAGE error."""
        parser = get_parser()

        with self.assertRaises(SystemExit) as cm:
            parser.parse_args([])

        self.assertEqual(cm.exception.code, ExitCode.USAGE)
        self.assertFalse(self.stdout_capture.getvalue(), msg='should not write to stdout')
        self.assertEqual(self.stderr_capture.getvalue(), parser.format_help())

    def test_help_with_invalid_choice(self):
        """Using '-h' with unknown command should give "invalid choice" error."""
        parser = get_parser()

        with self.assertRaises(SystemExit) as cm:
            parser.parse_args(['foo', '-h'])  # <- Unknown command "foo".

        self.assertEqual(cm.exception.code, ExitCode.USAGE)
        self.assertFalse(self.stdout_capture.getvalue())
        self.assertIn("invalid choice: 'foo'", self.stderr_capture.getvalue())

    def test_help_with_filename(self):
        """Using '-h' with a filename should give the main help message."""
        file_path = self.get_tempfile_path()
        TopoNode().to_file(file_path)

        parser = get_parser()

        with self.assertRaises(SystemExit) as cm:
            parser.parse_args([file_path, '-h'])

        self.assertEqual(cm.exception.code, ExitCode.OK)
        self.assertEqual(self.stdout_capture.getvalue(), parser.format_help())
        self.assertFalse(self.stderr_capture.getvalue(), msg='should not write to stderr')

    def test_info_explicit(self):
        """Check explicit use of the "info" command."""
        file_path = self.get_tempfile_path()
        TopoNode().to_file(file_path)

        parser = get_parser()
        args = parser.parse_args(['info', file_path])

        self.assertEqual(list(vars(args)), ['command', 'file'])
        self.assertEqual(args.command, 'info')
        self.assertEqual(args.file.path_hint, file_path)

    def test_info_implicit(self):
        """When a filename is given, "info" should be invoked by default."""
        file_path = self.get_tempfile_path()
        TopoNode().to_file(file_path)

        parser = get_parser()
        args = parser.parse_args([file_path])  # <- Filename is only arg.

        self.assertEqual(list(vars(args)), ['command', 'file'])
        self.assertEqual(args.command, 'info')
        self.assertEqual(args.file.path_hint, file_path)
