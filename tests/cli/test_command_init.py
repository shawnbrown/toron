"""Tests for toron/cli/command_init.py module."""
import argparse
import os
import sys
import tempfile
from .. import _unittest as unittest

from toron.cli import command_init


class TestInit(unittest.TestCase):
    def test_create_new_file_implicit_domain(self):
        """Should create new file at given path location."""
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            filepath = os.path.join(tmpdir, 'blerg.toron')
            args = argparse.Namespace(command='init', filepath=filepath, domain=None)

            self.assertFalse(os.path.exists(filepath))
            with self.assertLogs('app-toron', level='INFO') as logs_cm:
                command_init.create_file(args)  # <- Function under test.
            self.assertTrue(os.path.exists(filepath))

        self.assertEqual(len(logs_cm.output), 2)
        self.assertRegex(
            logs_cm.output[0],
            r"INFO:app-toron:created file '.*blerg.toron'",
        )
        self.assertRegex(
            logs_cm.output[1],
            r"INFO:app-toron:domain set to 'blerg'",
            msg='domain not given by user, defaults to file stem',
        )

    def test_create_new_file_explicit_domain(self):
        """Create a new file with an explicitly given domain."""
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            filepath = os.path.join(tmpdir, 'blerg.toron')
            args = argparse.Namespace(command='new', filepath=filepath, domain='dorp')

            self.assertFalse(os.path.exists(filepath))

            with self.assertLogs('app-toron', level='INFO') as logs_cm:
                command_init.create_file(args)  # <- Function under test.

            self.assertTrue(os.path.exists(filepath))

        self.assertEqual(len(logs_cm.output), 1)
        self.assertRegex(
            logs_cm.output[0],
            r"INFO:app-toron:created file '.*blerg.toron'",
        )

    def test_missing_directory(self):
        """Should give "cancelled" error if target dir doesn't exist."""
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            filepath = os.path.join(tmpdir, 'does_not_exist', 'blerg.toron')
            args = argparse.Namespace(command='new', filepath=filepath, domain=None)

            with self.assertLogs('app-toron', level='INFO') as logs_cm:
                command_init.create_file(args)  # <- Function under test.

        self.assertEqual(len(logs_cm.output), 1)
        self.assertRegex(
            logs_cm.output[0],
            r'cancelled: cannot write to directory .+',
        )

    def test_basename_is_whitespace(self):
        """Should cancel if filename is whitespace."""
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            filepath = os.path.join(tmpdir, '    ')  # <- Path basename is whitespace.
            args = argparse.Namespace(command='new', filepath=filepath)

            with self.assertLogs('app-toron', level='INFO') as logs_cm:
                command_init.create_file(args)  # <- Function under test.

            if sys.platform != 'win32':
                self.assertFalse(os.path.exists(filepath), f'path: {filepath!r}')
            else:
                # On win32, `exists()` ignores whitepsace and matches the directory.
                self.assertFalse(os.path.isfile(filepath), f'path: {filepath!r}')

        self.assertEqual(len(logs_cm.output), 1)
        self.assertRegex(
            logs_cm.output[0],
            r'filename cannot be whitespace',
        )

    def test_file_already_exists(self):
        """Should give "cancelled" error if file already exists."""
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            filepath = os.path.join(tmpdir, 'blerg.toron')
            args = argparse.Namespace(command='new', filepath=filepath)
            open(filepath, 'w').close()  # Create a file at the given path.

            with self.assertLogs('app-toron', level='INFO') as logs_cm:
                command_init.create_file(args)  # <- Function under test.

        self.assertEqual(len(logs_cm.output), 1)
        self.assertRegex(
            logs_cm.output[0],
            r'cancelled: .+ already exists',
        )
