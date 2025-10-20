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


class TestMainParser(StreamWrapperTestCase):
    def test_help_flag(self):
        """Using '-h', should print help to stdout and exit with OK."""
        parser = get_parser()

        with self.assertRaises(SystemExit) as cm:
            parser.parse_args(['-h'])

        self.assertEqual(cm.exception.code, ExitCode.OK)
        self.assertEqual(self.stdout_capture.getvalue(), parser.format_help())
        self.assertFalse(self.stderr_capture.getvalue(), msg='should not write to stderr')

    def test_no_args(self):
        """Using no args should print help to stderr and exit with USAGE error."""
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

    def test_filename(self):
        """Using a filename should invoke the "info" command by default."""
        file_path = self.get_tempfile_path()
        TopoNode().to_file(file_path)

        parser = get_parser()
        args = parser.parse_args([file_path])  # <- Filename is only arg.

        self.assertEqual(list(vars(args)), ['command', 'file'])
        self.assertEqual(args.command, 'info', msg=('when filename is given, '
                                                    'should default to "info"'))
        self.assertEqual(args.file.path_hint, file_path)


class TestInfoParser(StreamWrapperTestCase):
    def test_basic_use(self):
        """Check basic use of the "info" command."""
        file_path = self.get_tempfile_path()
        TopoNode().to_file(file_path)

        parser = get_parser()
        args = parser.parse_args(['info', file_path])

        self.assertEqual(list(vars(args)), ['command', 'file'])
        self.assertEqual(args.command, 'info')
        self.assertEqual(args.file.path_hint, file_path)
