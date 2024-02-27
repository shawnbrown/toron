"""DataConnector and related objects using SQLite."""

import atexit
import os
import re
import sqlite3
import tempfile
import urllib

from toron._typing import (
    Callable,
    List,
    Literal,
    Optional,
    Set,
)

from .base_classes import BaseDataConnector
from .._utils import ToronError


_tempfiles_to_remove_at_exit: Set[str] = set()


@atexit.register  # <- Register with `atexit` module.
def _cleanup_leftover_temp_files():
    """Remove temporary files left-over from `cache_to_drive` usage.

    The DataConnector class cleans-up files when __del__() is called
    but the Python documentation states:

        It is not guaranteed that __del__() methods are called
        for objects that still exist when the interpreter exits.

    For more details see:

        https://docs.python.org/3/reference/datamodel.html#object.__del__

    This function is intended to be registered with the `atexit` module
    and executed only once when the interpreter exits.
    """
    while _tempfiles_to_remove_at_exit:
        path = _tempfiles_to_remove_at_exit.pop()
        try:
            os.unlink(path)
        except Exception as e:
            import warnings
            msg = f'cannot remove temporary file {path!r}, {e.__class__.__name__}'
            warnings.warn(msg, RuntimeWarning)


def make_sqlite_uri_filepath(
        path: str, mode: Literal['ro', 'rw', 'rwc', None]
    ) -> str:
    """Return a SQLite compatible URI file path.

    Unlike pathlib's URI handling, SQLite accepts relative URI paths.
    For details, see:

        https://www.sqlite.org/uri.html#the_uri_path
    """
    if os.name == 'nt':  # Windows
        if re.match(r'^[a-zA-Z]:', path):
            path = os.path.abspath(path)  # Paths with drive-letter must be absolute.
            drive_prefix = f'/{path[:2]}'  # Must not url-quote colon after drive-letter.
            path = path[2:]
        else:
            drive_prefix = ''
        path = path.replace('\\', '/')
        path = urllib.parse.quote(path)
        path = f'{drive_prefix}{path}'
    else:
        path = urllib.parse.quote(path)

    path = re.sub('/+', '/', path)
    if mode:
        return f'file:{path}?mode={mode}'
    return f'file:{path}'


def get_sqlite_connection(
    path: str,
    access_mode: Literal['ro', 'rw', 'rwc', None] = None,
) -> sqlite3.Connection:
    """Get a SQLite connection to *path* with appropriate config.

    The returned connection will be configured with ``isolation_level``
    set to None (never implicitly open transactions) and
    ``detect_types`` set to PARSE_DECLTYPES (parse declared column
    type for query results).

    If *path* is a file, it is opened using the *access_mode* if
    specified:

    * ``'ro'``: read-only
    * ``'rw'``: read-write
    * ``'rwc'``: read-write and create if it doesn't exist

    If *path* is ``':memory:'`` or ``''``, then *access_mode* is
    ignored.

    .. important::

        This method should only establish a connection, it should
        not execute queries of any kind.
    """
    if path == ':memory:' or path == '':  # In-memory or on-drive temp db.
        normalized_path = path
        is_uri_path = False
    else:
        normalized_path = make_sqlite_uri_filepath(path, access_mode)
        is_uri_path = True

    try:
        return sqlite3.connect(
            database=normalized_path,
            detect_types=sqlite3.PARSE_DECLTYPES,
            isolation_level=None,
            uri=is_uri_path,
        )
    except sqlite3.OperationalError as err:
        error_text = str(err)
        matches = ['unable to open database', 'Could not open database']
        if any(x in error_text for x in matches):
            msg = f'unable to open node file {path!r}'
            raise ToronError(msg)
        else:
            raise


class DataConnector(BaseDataConnector):
    # Absolute path of class instance's database (None if file in memory).
    _current_working_path: Optional[str] = None
    _cleanup_funcs: List[Callable]

    def __init__(self, cache_to_drive: bool = False) -> None:
        """Initialize a new node instance."""
        self._cleanup_funcs = []

        if cache_to_drive:
            temp_f = tempfile.NamedTemporaryFile(suffix='.toron', delete=False)
            temp_f.close()
            database_path = os.path.abspath(temp_f.name)
            self._current_working_path = database_path

            _tempfiles_to_remove_at_exit.add(database_path)
            self._cleanup_funcs.extend([
                lambda: _tempfiles_to_remove_at_exit.discard(database_path),
                lambda: os.unlink(database_path),
            ])
        else:
            database_path = ':memory:'
            self._current_working_path = None

    def __del__(self):
        while self._cleanup_funcs:
            func = self._cleanup_funcs.pop()
            func()
