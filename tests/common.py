"""Common functions and classes for test cases."""
__unittest = True

import glob
import io
import os
import shutil
import sqlite3
import sys
import tempfile
from contextlib import (
    closing,
    contextmanager,
    redirect_stdout,
    redirect_stderr,
)
from typing import Iterable, List

from toron.data_models import Structure
from . import _unittest as unittest


if sys.platform == 'darwin' and sqlite3.sqlite_version_info in {(3, 35, 5), (3, 37, 2)}:
    # On macOS with certain versions of SQLite, tests reveal floating point
    # precision differences (e.g., getting 2.9999999999999996 when expecting
    # 3.0). This is only an issue when testing behavior that uses the DAL1
    # optimized version of `calculate_granularity()`.
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


class StreamWrapperTestCase(unittest.TestCase):
    def setUp(self):
        stdout_cm = redirect_stdout(io.StringIO())
        self.stdout_capture = stdout_cm.__enter__()
        self.addCleanup(lambda: stdout_cm.__exit__(None, None, None))

        stderr_cm = redirect_stderr(io.StringIO())
        self.stderr_capture = stderr_cm.__enter__()
        self.addCleanup(lambda: stderr_cm.__exit__(None, None, None))

    @contextmanager
    def patched_stdin(self, input_str):
        """Context manager to patch stdin with ``input_str``."""
        try:
            stdin_cm = unittest.mock.patch(target='sys.stdin',
                                           new_callable=io.StringIO)
            mock_stdin = stdin_cm.__enter__()
            mock_stdin.write(input_str)
            mock_stdin.seek(0)
            yield mock_stdin
        finally:
            stdin_cm.__exit__(None, None, None)

    def get_tempfile_path(self):
        """Helper function to get a path to a temporary file."""
        with closing(tempfile.NamedTemporaryFile(delete=False)) as tmp:
            self.addCleanup(lambda: os.remove(tmp.name))
        return tmp.name

    def assertStream(self, stream, expected, *, encoding='utf-8', msg=None):
        """Fail if ``stream`` value does not equal ``expected`` value.
        Value is decoded using UTF-8.
        """
        try:
            stream.flush()
            stream_value = stream.buffer.getvalue().decode(encoding)

        except Exception as e:
            note = 'The argument `stream` should be a TextIOWrapper'

            if hasattr(e, 'add_note'):
                if isinstance(e, (NameError, AttributeError)):
                    e.name = None  # Disable name suggestion in message.
                e.add_note(note)
            else:
                msg = f'{e}\n{note}'
                e = Exception(msg)

            raise e

        self.assertEqual(stream_value, expected, msg)


class DummyStream(io.TextIOWrapper):
    """TextIOWrapper that mimics an interactive stream (a TTY)."""
    def __init__(self):
        super().__init__(io.BytesIO())

    def isatty(self):
        return True


class DummyRedirectedStream(io.TextIOWrapper):
    """TextIOWrapper to mimic a stream being redirected or piped."""
    def __init__(self):
        super().__init__(io.BytesIO())
