"""DataConnector and related objects using SQLite."""

import os
import re
import sqlite3
import sys
import urllib
import weakref
from contextlib import closing
from tempfile import NamedTemporaryFile

from toron._typing import (
    List,
    Literal,
    Optional,
    Type,
    overload,
)

from . import schema
from .base_classes import BaseDataConnector
from .._utils import ToronError


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


class ToronSqlite3Connection(sqlite3.Connection):
    """SQLite connection wrapper to prevent accidental closing."""
    def close(self):
        raise RuntimeError(
            "cannot close directly. Did you mean: 'release_resource(...)'?"
        )


@overload
def get_sqlite_connection(
    path: str,
    access_mode: Literal['ro', 'rw', 'rwc', None] = None,
    factory: Type[ToronSqlite3Connection] = ToronSqlite3Connection,
) -> ToronSqlite3Connection:
    ...
@overload
def get_sqlite_connection(
    path: str,
    access_mode: Literal['ro', 'rw', 'rwc', None] = None,
    factory: Optional[Type[sqlite3.Connection]] = None,
) -> sqlite3.Connection:
    ...
def get_sqlite_connection(path, access_mode=None, factory=None):
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

    If given, *factory* must be a subclass of :py:class:`sqlite3.Connection`
    and will be used to create the database connection instance.

    .. important::

        This method should only establish a connection, it should
        not execute queries of any kind.
    """
    if factory and not issubclass(factory, sqlite3.Connection):
        raise TypeError(
            f'requires subclass of sqlite3.Connection, got {factory.__name__}'
        )

    try:
        if path == ':memory:' or path == '':  # In-memory or on-drive temp db.
            return sqlite3.connect(
                database=path,
                detect_types=sqlite3.PARSE_DECLTYPES,
                isolation_level=None,
                factory=factory or sqlite3.Connection,
            )
        else:
            return sqlite3.connect(
                database=make_sqlite_uri_filepath(path, access_mode),
                detect_types=sqlite3.PARSE_DECLTYPES,
                isolation_level=None,
                factory=factory or sqlite3.Connection,
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


if sys.platform == 'darwin':
    # If running on macOS, try to fsync using F_FULLFSYNC. From the
    # macOS man page for FSYNC(2):
    #
    #   For applications that require tighter guarantees about the
    #   integrity of their data, Mac OS X provides the F_FULLFSYNC
    #   fcntl.  The F_FULLFSYNC fcntl asks the drive to flush all
    #   buffered data to permanent storage.
    #
    # Also see:
    # - https://github.com/libuv/libuv/pull/2135
    # - https://github.com/python/cpython/issues/47767 (patch accepted)
    # - https://github.com/python/cpython/issues/56086 (patch rejected)

    import fcntl

    def best_effort_fsync(path: str, isdir: bool = False) -> None:
        fd = os.open(path, flags=(os.O_RDONLY if isdir else os.O_RDWR))
        try:
            r = fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
            if r != 0:  # If F_FULLFSYNC is not working or failed.
                os.fsync(fd)  # Fall back to os.fsync().
        except AttributeError:
            os.fsync(fd)  # Fall back to os.fsync().
        finally:
            os.close(fd)

else:
    def best_effort_fsync(path: str, isdir: bool = False) -> None:
        fd = os.open(path, flags=(os.O_RDONLY if isdir else os.O_RDWR))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


class DataConnector(BaseDataConnector[ToronSqlite3Connection]):
    def __init__(self, cache_to_drive: bool = False) -> None:
        """Initialize a new node instance."""
        self._current_working_path: Optional[str]
        self._in_memory_connection: Optional[ToronSqlite3Connection]
        self._unique_id: str

        if cache_to_drive:
            # Create temporary file and get path.
            with closing(NamedTemporaryFile(suffix='.toron', delete=False)) as f:
                database_path = os.path.abspath(f.name)
            weakref.finalize(self, os.unlink, database_path)

            # Create Toron node schema and close connection.
            with closing(get_sqlite_connection(database_path)) as con:
                schema.create_node_schema(con)
                self._unique_id = schema.get_unique_id(con)

            # Keep file path, no in-memory connection.
            self._current_working_path = database_path
            self._in_memory_connection = None

        else:
            # Connect to in-memory database.
            con = get_sqlite_connection(':memory:', factory=ToronSqlite3Connection)
            weakref.finalize(self, super(ToronSqlite3Connection, con).close)

            # Create Toron node schema, functions, and temp triggers.
            schema.create_node_schema(con)
            schema.create_functions_and_temporary_triggers(con)
            self._unique_id = schema.get_unique_id(con)

            # No working file path, keep in-memory connection open.
            self._current_working_path = None
            self._in_memory_connection = con

    @property
    def unique_id(self) -> str:
        """Unique identifier for the node object."""
        return self._unique_id

    def acquire_resource(self) -> ToronSqlite3Connection:
        """Return a connection to the node's SQLite database."""
        if self._in_memory_connection:
            return self._in_memory_connection

        if self._current_working_path:
            connection = get_sqlite_connection(
                self._current_working_path,
                factory=ToronSqlite3Connection,
            )
            schema.create_functions_and_temporary_triggers(connection)
            return connection

        raise RuntimeError('unable to acquire data resource')

    def release_resource(self, resource: ToronSqlite3Connection) -> None:
        """Close the database connection if node is stored on drive."""
        if self._current_working_path:
            super(ToronSqlite3Connection, resource).close()
