"""Tests for toron/cli/main.py module."""
import io
import os
from .. import _unittest as unittest
from ..common import StreamWrapperTestCase
from toron import TopoNode

from toron.cli.common import ExitCode
from toron.cli.main import (
    get_parser,
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
        self.assertEqual(args1.file.path_hint, args2.file.path_hint)
