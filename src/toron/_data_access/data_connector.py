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
    Self,
    Type,
    Union,
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


def verify_permissions(
    path: str,
    required_permissions: Literal['ro', 'rw', None],
) -> None:
    """Raise error if file does not have required permissions.

    Toron Nodes are often opened in memory or from a temporary file
    cache, which can condition users to treat nodes as ephemeral
    objects that are always safe to modify. But when Toron files are
    opened directly from drive, changes are applied immediately and
    cannot be undone. To prevent users from accidentally altering
    on-drive files, they should be opened using read-only mode
    (``'ro'``) by default.

    .. important::

        While SQLite can open files using the read-only access mode,
        doing so does not ensure that the database file on the drive
        will always remain safe to copy. At this time, SQLite makes no
        guarantees that use of the "ro" URI access mode is equivalent
        to using a database with read-only permissions enforced by the
        filesystem.

        In a high availability computing environment, it's possible
        that an automated backup system could copy a database file
        while a transaction is in progress. For related information,
        see section 1.2 of "How To Corrupt An SQLite Database File":

            https://www.sqlite.org/howtocorrupt.html

        Out of an abundance of caution, when opening an existing
        database directly on drive, Toron defaults to read-only file
        permissions to mitigate the chance that a backup process makes
        a corrupted copy.
    """
    if os.path.exists(path):
        # Check for read permissions.
        if not os.access(path, os.R_OK):
            raise PermissionError(f'insufficient permissions to read {path!r}')

        # Check for required write permissions.
        if required_permissions == 'ro':
            if os.access(path, os.W_OK):
                raise PermissionError(
                    f'{path!r} should be read-only but has read-write permissions'
                )
        elif required_permissions == 'rw':
            if not os.access(path, os.W_OK):
                raise PermissionError(
                    f'{path!r} should be read-write but has read-only permissions'
                )
        elif required_permissions is None:
            pass  # Accepts read-only or read-write.
        else:
            raise ValueError(
                f"required_permissions must be 'ro', 'rw', or None; "
                f"got {required_permissions!r}"
            )
    else:
        # When file doesn't exist, must have permissions to write new file.
        if required_permissions == 'ro':
            msg = f"no file named {path!r}, open in 'rw' mode to create a new file"
            raise FileNotFoundError(msg)
        elif required_permissions == 'rw' or required_permissions is None:
            dir_name = os.path.dirname(path) + os.sep
            if not os.access(dir_name, os.R_OK):
                raise PermissionError(
                    f'insufficient permissions to read from directory {dir_name!r}'
                )
            if not os.access(dir_name, os.W_OK):
                raise PermissionError(
                    f'insufficient permissions to write to directory {dir_name!r}'
                )
        else:
            raise ValueError(
                f"required_permissions must be 'ro', 'rw', or None; "
                f"got {required_permissions!r}"
            )


class DataConnector(BaseDataConnector[ToronSqlite3Connection, sqlite3.Cursor]):
    def __init__(self, cache_to_drive: bool = False) -> None:
        """Initialize a new node instance."""
        self._unique_id: str
        self._access_mode: Literal['ro', 'rw', None]
        self._current_working_path: Optional[str]
        self._in_memory_connection: Optional[ToronSqlite3Connection]

        if cache_to_drive:
            # Create temporary file and get path.
            with closing(NamedTemporaryFile(suffix='.toron', delete=False)) as f:
                database_path = os.path.abspath(f.name)
            weakref.finalize(self, os.unlink, database_path)

            # Create Toron node schema and close connection.
            with closing(get_sqlite_connection(database_path)) as con:
                with closing(con.cursor()) as cur:
                    schema.create_node_schema(cur)
                    unique_id = schema.get_unique_id(cur)

            # Keep file path, no in-memory connection.
            self._unique_id = unique_id
            self._access_mode = None
            self._current_working_path = database_path
            self._in_memory_connection = None

        else:
            # Connect to in-memory database.
            con = get_sqlite_connection(':memory:', factory=ToronSqlite3Connection)
            weakref.finalize(self, super(ToronSqlite3Connection, con).close)

            # Create Toron node schema, functions, and temp triggers.
            with closing(con.cursor()) as cur:
                schema.create_node_schema(cur)
                unique_id = schema.get_unique_id(cur)

            schema.create_functions_and_temporary_triggers(con)

            # No working file path, keep in-memory connection open.
            self._unique_id = unique_id
            self._access_mode = None
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
                access_mode=self._access_mode,
                factory=ToronSqlite3Connection,
            )
            schema.create_functions_and_temporary_triggers(connection)
            return connection

        raise RuntimeError('unable to acquire data resource')

    def release_resource(self, resource: ToronSqlite3Connection) -> None:
        """Close the database connection if node is stored on drive."""
        if self._current_working_path:
            super(ToronSqlite3Connection, resource).close()

    def acquire_data_reader(
        self, resource: ToronSqlite3Connection
    ) -> sqlite3.Cursor:
        """Return a cursor from the given connection."""
        return resource.cursor()

    def release_data_reader(self, data_reader: sqlite3.Cursor) -> None:
        """Close the database cursor."""
        return data_reader.close()

    def to_file(
        self, path: Union[str, bytes, os.PathLike], *, fsync: bool = True
    ) -> None:
        """Write node data to a file.

        Parameters
        ----------
        path : :py:term:`path-like-object`
            File path where the node data should be saved.
        fsync : bool, default True
            Immediately flush any cached data to drive storage.
        """
        dst_path = os.path.abspath(os.fsdecode(path))
        dst_dirname = os.path.normpath(os.path.dirname(dst_path))

        # Check if process has write-permissions to the destination.
        if not os.access(dst_dirname, os.W_OK):
            raise PermissionError(f'cannot write to directory {dst_dirname!r}')
        if os.path.isfile(dst_path) and not os.access(dst_path, os.W_OK):
            raise PermissionError(f'file {dst_path!r} is read-only')

        # Get temporary file path.
        with closing(NamedTemporaryFile(
            suffix='.temp',
            prefix=f'{os.path.splitext(os.path.basename(dst_path))[0]}-',
            dir=dst_dirname,  # <- Use same dir as dst_path to ensure that
            delete=False,     #    tmp and dst are on the same filesystem
        )) as tmp_f:          #    (guarantees `os.replace()` is atomic).
            tmp_path = os.path.realpath(tmp_f.name)  # Use realpath() in case of symlink.

        # Write data to temp file, then perform atomic `os.replace()`.
        try:
            if fsync and sqlite3.sqlite_version_info >= (3, 40, 0):
                # Save data to file and use SQLite's "synchronous" flag to
                # flush buffered data to permanent storage. For details, see:
                #
                # - https://www.sqlite.org/changes.html#version_3_40_0
                # - https://sqlite.org/src/info/86cb21ca12581cae
                # - https://sqlite.org/forum/info/8c83764a7355f6cc8208cdf96e533dd2f91f939770c193d20980fa45140e8908
                con = self.acquire_resource()
                try:
                    with closing(con.cursor()) as cur:
                        cur.execute('PRAGMA main.synchronous')
                        original_sync = cur.fetchone()[0]

                        cur.execute('PRAGMA fullfsync')
                        original_fullfsync = cur.fetchone()[0]

                        cur.execute('PRAGMA main.synchronous=3')  # EXTRA (3)
                        cur.execute('PRAGMA fullfsync=1')
                        try:
                            cur.execute('VACUUM main INTO ?', (tmp_path,))
                        finally:
                            cur.execute(f'PRAGMA main.synchronous={original_sync}')
                            cur.execute(f'PRAGMA fullfsync={original_fullfsync}')
                finally:
                    self.release_resource(con)

                os.replace(tmp_path, dst_path)

            elif fsync:
                # Save data to file and use best_effort_fsync() to flush
                # buffered data to permanent storage. For more info, see
                # "Ensuring data reaches disk" by Jeff Moyer:
                #  - https://lwn.net/Articles/457667/
                con = self.acquire_resource()
                try:
                    with closing(con.cursor()) as cur:
                        cur.execute('VACUUM main INTO ?', (tmp_path,))
                except sqlite3.OperationalError:  # No `VACUUM INTO` before SQLite 3.27.0.
                    with closing(get_sqlite_connection(tmp_path)) as tmp_con:
                        con.backup(tmp_con)
                finally:
                    self.release_resource(con)

                best_effort_fsync(tmp_path)
                os.replace(tmp_path, dst_path)
                if sys.platform != 'win32':  # Windows cannot fsync a directory.
                    best_effort_fsync(dst_dirname, isdir=True)

            else:
                # When fsync is False, no extra steps are taken to ensure
                # that the data is flushed to drive. Although SQLite could
                # still call fsync in versions 3.40.0 and newer, depending
                # on the database's initial synchronous setting.
                con = self.acquire_resource()
                try:
                    with closing(con.cursor()) as cur:
                        cur.execute('VACUUM main INTO ?', (tmp_path,))
                except sqlite3.OperationalError:  # No `VACUUM INTO` before SQLite 3.27.0.
                    with closing(get_sqlite_connection(tmp_path)) as tmp_con:
                        con.backup(tmp_con)
                finally:
                    self.release_resource(con)

                os.replace(tmp_path, dst_path)

        except Exception:
            os.unlink(tmp_path)  # Remove temporary file.
            raise  # Re-raise error.

    @classmethod
    def from_file(
        cls,
        path: Union[str, bytes, os.PathLike],
        cache_to_drive: bool = False,
    ) -> Self:
        """Read a node file into a new data connector object.

        Parameters
        ----------
        path : :py:term:`path-like-object`
            File path containing the node data.
        """
        src_path = os.path.abspath(os.fsdecode(path))
        if not os.path.isfile(path):
            raise FileNotFoundError(src_path)

        instance = cls.__new__(cls)

        if cache_to_drive:
            # Create temporary file and get path.
            with closing(NamedTemporaryFile(suffix='.toron', delete=False)) as f:
                database_path = os.path.abspath(f.name)
            weakref.finalize(instance, os.unlink, database_path)

            # Read data from file into node database, then close all connections.
            with closing(get_sqlite_connection(database_path)) as con:
                with closing(get_sqlite_connection(src_path)) as src_con:
                    with closing(src_con.cursor()) as src_cur:
                        schema.verify_node_schema(src_cur)
                        unique_id = schema.get_unique_id(src_cur)
                    src_con.backup(con)

            # Keep file path, no in-memory connection.
            instance._unique_id = unique_id
            instance._access_mode = None
            instance._current_working_path = database_path
            instance._in_memory_connection = None

        else:
            # Connect to in-memory database.
            con = get_sqlite_connection(':memory:', factory=ToronSqlite3Connection)
            weakref.finalize(instance, super(ToronSqlite3Connection, con).close)

            # Read data from file into node database and close source connection.
            with closing(get_sqlite_connection(src_path)) as src_con:
                with closing(src_con.cursor()) as src_cur:
                    schema.verify_node_schema(src_cur)
                    unique_id = schema.get_unique_id(src_cur)
                src_con.backup(con)

            schema.create_functions_and_temporary_triggers(con)

            # No working file path, keep in-memory connection open.
            instance._unique_id = unique_id
            instance._access_mode = None
            instance._current_working_path = None
            instance._in_memory_connection = con

        return instance

    @classmethod
    def from_live_data(
        cls,
        path: Union[str, bytes, os.PathLike],
        required_permissions: Literal['ro', 'rw', None] = 'ro',
    ) -> Self:
        """Open a node directly from drive (does not load into memory).

        Parameters
        ----------
        path : :py:term:`path-like-object`
            File path containing the node data.
        required_permissions : 'ro' | 'rw' | None, default 'ro'
            Required file permissions on drive.

        .. warning::

            Use caution when changing a node that has been opened
            directly in read-write mode. Changes are applied
            **immediately** to the file on drive and cannot be undone.
        """
        database_path = os.path.abspath(os.fsdecode(path))
        verify_permissions(database_path, required_permissions)

        path_exists = os.path.exists(database_path)
        with closing(get_sqlite_connection(database_path)) as con:
            with closing(con.cursor()) as cur:
                if path_exists:
                    schema.verify_node_schema(cur)
                else:
                    schema.create_node_schema(cur)
                unique_id = schema.get_unique_id(cur)

        instance = cls.__new__(cls)
        instance._unique_id = unique_id
        instance._access_mode = required_permissions
        instance._current_working_path = database_path
        instance._in_memory_connection = None

        return instance
