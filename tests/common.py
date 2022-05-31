# -*- coding: utf-8 -*-
import glob
import os
import shutil
import tempfile

from . import _unittest as unittest


def get_column_names(connection_or_cursor, table):
    """Return list of column names from given database table."""
    cur = connection_or_cursor.execute(f'PRAGMA table_info({table})')
    return [row[1] for row in cur.fetchall()]


class TempDirTestCase(unittest.TestCase):
    # A TestCase to create a temporary directory, then chdir() into
    # it for testing. After testing, the original working directory
    # is restored and the temporary directory is removed.

    if hasattr(unittest.TestCase, 'addClassCleanup'):
        # The addClassCleanup() method is new in Python 3.8.
        @classmethod
        def setUpClass(cls):
            original_working_dir = os.getcwd()

            cls._tempdir = tempfile.TemporaryDirectory()
            os.chdir(cls._tempdir.name)

            def cleanup_func():
                os.chdir(original_working_dir)
                cls._tempdir.cleanup()

            cls.addClassCleanup(cleanup_func)

    else:
        # Use tearDownClass() method on older versions.
        @classmethod
        def setUpClass(cls):
            cls._original_working_dir = os.getcwd()
            cls._tempdir = tempfile.TemporaryDirectory()
            os.chdir(cls._tempdir.name)

        @classmethod
        def tearDownClass(cls):
            os.chdir(cls._original_working_dir)
            cls._tempdir.cleanup()

    def cleanup_temp_files(self):
        """Remove all files from the current temporary directory."""
        for path in glob.glob(os.path.join(self._tempdir.name, '*')):
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)

