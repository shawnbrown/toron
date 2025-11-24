"""Tests for toron/cli/command_new.py module."""
import argparse
import os
import sys
import tempfile
from .. import _unittest as unittest

from toron.cli import command_new


class TestNew(unittest.TestCase):
    def test_create_new_file(self):
        """Should create new file at given path location."""
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            node_path = os.path.join(tmpdir, 'blerg.toron')
            args = argparse.Namespace(command='new', node_path=node_path)

            self.assertFalse(os.path.exists(node_path))
            command_new.create_file(args)
            self.assertTrue(os.path.exists(node_path))

    def test_missing_directory(self):
        """Should give "cancelled" error if target dir doesn't exist."""
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            node_path = os.path.join(tmpdir, 'does_not_exist', 'blerg.toron')
            args = argparse.Namespace(command='new', node_path=node_path)

            with self.assertLogs('app-toron', level='INFO') as logs_cm:
                command_new.create_file(args)  # <- Function under test.

            self.assertRegex(logs_cm.output[0], r'cancelled: cannot write to directory .+')

    def test_basename_is_whitespace(self):
        """Should cancel if filename is whitespace."""
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            node_path = os.path.join(tmpdir, '    ')  # <- Path basename is whitespace.
            args = argparse.Namespace(command='new', node_path=node_path)

            with self.assertLogs('app-toron', level='INFO') as logs_cm:
                command_new.create_file(args)  # <- Function under test.

            self.assertRegex(logs_cm.output[0], r'filename cannot be whitespace')

            if sys.platform != 'win32':
                self.assertFalse(os.path.exists(node_path), f'path: {node_path!r}')
            else:
                # On win32, `exists()` ignores whitepsace and matches the directory.
                self.assertFalse(os.path.isfile(node_path), f'path: {node_path!r}')

    def test_file_already_exists(self):
        """Should give "cancelled" error if file already exists."""
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            node_path = os.path.join(tmpdir, 'blerg.toron')
            args = argparse.Namespace(command='new', node_path=node_path)
            open(node_path, 'w').close()  # Create a file at the given path.

            with self.assertLogs('app-toron', level='INFO') as logs_cm:
                command_new.create_file(args)  # <- Function under test.

            self.assertRegex(logs_cm.output[0], r'cancelled: .+ already exists')
