"""DataConnector and related objects using SQLite."""

import atexit
import os
import re
import sqlite3
import urllib
from contextlib import closing
from tempfile import NamedTemporaryFile

from toron._typing import (
    Callable,
    List,
    Literal,
    Optional,
    Set,
)

from . import schema
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
    try:
        if path == ':memory:' or path == '':  # In-memory or on-drive temp db.
            return sqlite3.connect(
                database=path,
                detect_types=sqlite3.PARSE_DECLTYPES,
                isolation_level=None,
            )
        else:
            return sqlite3.connect(
                database=make_sqlite_uri_filepath(path, access_mode),
                detect_types=sqlite3.PARSE_DECLTYPES,
                isolation_level=None,
                uri=True,
            )
    except sqlite3.OperationalError as err:
        error_text = str(err)
        matches = ['unable to open database', 'Could not open database']
        if any(x in error_text for x in matches):
            msg = f'unable to open node file {path!r}'
            raise ToronError(msg)
        else:
            raise


class DataConnector(BaseDataConnector[sqlite3.Connection]):
    def __init__(self, cache_to_drive: bool = False) -> None:
        """Initialize a new node instance."""
        self._cleanup_funcs: List[Callable]
        self._current_working_path: Optional[str]
        self._in_memory_connection: Optional[sqlite3.Connection]

        self._cleanup_funcs = []

        if cache_to_drive:
            # Create temp file and set current working path.
            with closing(NamedTemporaryFile(suffix='.toron', delete=False)) as f:
                database_path = os.path.abspath(f.name)
            self._current_working_path = database_path

            # Connect to database and create Toron node schema.
            with closing(get_sqlite_connection(database_path)) as con:
                schema.create_node_schema(con)

            # For on-drive database, in-memory connection is None.
            self._in_memory_connection = None

            # Define clean-up actions (called by garbage collection).
            _tempfiles_to_remove_at_exit.add(database_path)
            self._cleanup_funcs.extend([
                lambda: _tempfiles_to_remove_at_exit.discard(database_path),
                lambda: os.unlink(database_path),
            ])

        else:
            # For in-memory database, current working path is None.
            database_path = ':memory:'
            self._current_working_path = None

            # Connect to database and create Toron node schema.
            con = get_sqlite_connection(database_path)
            schema.create_node_schema(con)
            schema.create_functions_and_temporary_triggers(con)

            # Keep in-memory connection open.
            self._in_memory_connection = con

            # Close connection at clean-up (called by garbage collection).
            self._cleanup_funcs.append(con.close)

    def __del__(self):
        while self._cleanup_funcs:
            func = self._cleanup_funcs.pop()
            func()

    def acquire_resource(self) -> sqlite3.Connection:
        """Return a connection to the node's SQLite database."""
        if self._in_memory_connection:
            return self._in_memory_connection

        if self._current_working_path:
            connection = get_sqlite_connection(self._current_working_path)
            schema.create_functions_and_temporary_triggers(connection)
            return connection

        raise RuntimeError('unable to acquire data resource')

    def release_resource(self, resource: sqlite3.Connection) -> None:
        """Close the database connection if node is stored on drive."""
        if self._current_working_path:
            resource.close()
