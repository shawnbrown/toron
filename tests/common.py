"""Common functions and classes for test cases."""
import glob
import os
import shutil
import sqlite3
import sys
import tempfile

from typing import Iterable, List

from toron.data_models import Structure
from . import _unittest as unittest


if sys.platform == 'darwin' and sqlite3.sqlite_version_info == (3, 35, 5):
    # On macOS with SQLite version 3.35.5, tests reveal floating point
    # precision differences (getting 2.9999999999999996 when expecting
    # 3.0). This is only an issue when testing behavior that uses the
    # DAL1 optimized version of `calculate_granularity()`.
    sys.stderr.write('[macOS: using approximate values for `calculate_granularity()` tests]\n')

    def normalize_structures(structures: Iterable[Structure]) -> List[Structure]:
        """Return list of structures with granularity rounded to 7 places."""
        normalized = []
        for structure in structures:
            if structure.granularity:
                structure.granularity = round(structure.granularity, ndigits=7)
            normalized.append(structure)
        return normalized
else:
    def normalize_structures(structures: Iterable[Structure]) -> List[Structure]:
        """Return list of structures."""
        return list(structures)


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

