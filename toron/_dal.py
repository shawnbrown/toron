"""Data access layer to interact with Toron node files."""

import atexit
import os
import sqlite3
import sys
import tempfile
from collections import Counter
from contextlib import contextmanager, nullcontext
from itertools import (
    chain,
    compress,
    groupby,
    zip_longest,
)
from json import dumps as _dumps
from json import loads as _loads
from ._selectors import (
    CompoundSelector,
    SimpleSelector,
    accepts_json_input,
    GetMatchingKey,
)
from ._typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeAlias,
    Union,
)
try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore

from . import _schema
from ._categories import make_structure
from ._categories import minimize_discrete_categories
from ._utils import (
    ToronError,
    ToronWarning,
    TabularData,
    make_readerlike,
    make_dictreaderlike,
)


if sys.platform != 'win32' and hasattr(fcntl, 'F_FULLFSYNC'):
    # If running on macOS, try to fsync using F_FULLFSYNC.
    #
    # From the macOS man page for FSYNC(2):
    #   For applications that require tighter guarantees about the integrity of
    #   their data, Mac OS X provides the F_FULLFSYNC fcntl.  The F_FULLFSYNC
    #   fcntl asks the drive to flush all buffered data to permanent storage.
    #
    # Also see:
    #   https://github.com/libuv/libuv/pull/2135
    #   https://github.com/python/cpython/issues/47767 (patch accepted)
    #   https://github.com/python/cpython/issues/56086 (patch rejected)
    def _best_effort_fsync(fd):
        """Flush buffered data to drive for dir/file descriptor *fd*."""
        r = fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
        if r != 0:  # If F_FULLFSYNC is not working or failed.
            os.fsync(fd)  # <- fall back to os.fsync().
else:
    # Else if running on Linux or other OS, use standard fsync.
    _best_effort_fsync = os.fsync


_SQLITE_VERSION_INFO = sqlite3.sqlite_version_info
_temp_files_to_delete_atexit: Set[str] = set()


@atexit.register  # <- Register with `atexit` module.
def _delete_leftover_temp_files():
    """Remove temporary files left-over from `cache_to_drive` usage.

    This function is intended to be registered with the `atexit` module
    and executed only once when the interpreter exits.

    While Node objects contain a __del__() method, it should not be
    relied upon to finalize resources. This function will clean-up
    any left-over temporary files that were not removed by __del__().

    The Python documentation states:

        It is not guaranteed that __del__() methods are called
        for objects that still exist when the interpreter exits.

    For more details see:

        https://docs.python.org/3/reference/datamodel.html#object.__del__
    """
    while _temp_files_to_delete_atexit:
        path = _temp_files_to_delete_atexit.pop()
        try:
            os.unlink(path)
        except Exception as e:
            import warnings
            msg = f'cannot remove temporary file {path!r}, {e.__class__.__name__}'
            warnings.warn(msg, RuntimeWarning)


Strategy: TypeAlias = Literal['preserve', 'restructure', 'coarsen', 'coarsenrestructure']
PathType: TypeAlias = Union[str, bytes, os.PathLike]


class DataAccessLayer(object):
    """A data access layer to interface with the underlying SQLite
    database. This class is not part of Toron's public interface--it
    is intended to be wrapped inside a toron.Node instance.

    Make a new node-backend/DAL as an in-memory database::

        >>> from toron._dal import dal_class
        >>> dal = dal_class()

    Make a new node-backend/DAL as an on-drive database (instead of
    in-memory)::

        >>> from toron._dal import dal_class
        >>> dal = dal_class(cache_to_drive=True)
    """
    _filename: Optional[str]
    _required_permissions: _schema.RequiredPermissions
    _cleanup_item: Optional[Union[str, sqlite3.Connection]]

    def __init__(self, cache_to_drive: bool = False):
        """Initialize a new node instance."""
        # Get `target_path` for temporary file or in-memory database.
        if cache_to_drive:
            temp_f = tempfile.NamedTemporaryFile(suffix='.toron', delete=False)
            temp_f.close()
            target_path = os.path.abspath(temp_f.name)
            _temp_files_to_delete_atexit.add(target_path)
        else:
            target_path = ':memory:'  # <- In-memory only (no file on-drive).

        # Create Node schema, add functions, and add triggers.
        con = _schema.get_raw_connection(target_path)  # Connect to empty db.
        con.executescript(_schema._schema_script)
        _schema._add_functions_and_triggers(con)

        # Assign object attributes.
        if cache_to_drive:
            con.close()  # Close on-drive connection (only open when accessed).
            self._filename = target_path
            self._required_permissions = 'readwrite'
            self._cleanup_item = target_path
        else:
            self._connection = con  # Keep connection open (in-memory database
                                    # is discarded once closed).
            self._filename = None
            self._required_permissions = None
            self._cleanup_item = con

    @classmethod
    def from_file(
        cls, path: PathType, cache_to_drive: bool = False
    ) -> 'DataAccessLayer':
        """Create a node from a file on drive.

        By default, nodes are loaded into memory::

            >>> from toron import Node
            >>> node = Node.from_file('mynode.toron')

        If you want to load a node into on-drive cache (instead of into
        memory), you can use ``cache_to_drive=True`` which stores the
        working node data in a temporary location::

            >>> from toron import Node
            >>> node = Node.from_file('mynode.toron', cache_to_drive=True)
        """
        path = os.fsdecode(path)
        source_con = _schema.get_raw_connection(path, access_mode='ro')

        if cache_to_drive:
            fh = tempfile.NamedTemporaryFile(suffix='.toron', delete=False)
            fh.close()
            target_path = os.path.abspath(fh.name)
            _temp_files_to_delete_atexit.add(target_path)
        else:
            target_path = ':memory:'

        try:
            target_con = _schema.get_raw_connection(target_path)
            source_con.backup(target_con)
            _schema._add_functions_and_triggers(target_con)
        finally:
            source_con.close()

        obj = cls.__new__(cls)
        if cache_to_drive:
            target_con.close()
            obj._filename = target_path
            obj._required_permissions = 'readwrite'
            obj._cleanup_item = target_path
        else:
            obj._connection = target_con
            obj._filename = None
            obj._required_permissions = None
            obj._cleanup_item = target_con

        return obj

    def to_file(self, path: PathType, fsync: bool = True) -> None:
        """Write node data to a file.

        .. code-block::

            >>> from toron._dal import dal_class
            >>> dal = dal_class()
            >>> ...
            >>> dal.to_file('mynode.toron')

        On Unix-like systems (e.g., Linux, macOS), calling with
        ``fsync=True`` (the default) tells the filesystem to
        immediately flush buffered data to permanent storage. This
        could cause a delay while data is being synchronized. If you
        prefer faster (but slightly less-safe) file handling or if
        you plan to explicitly synchronize at a later time, you can
        use ``fsync=False`` to skip this step.

        On Windows systems, the *fsync* argument is ignored and
        behavior is left entirely to the OS. This is because Windows
        provides no good way to obtain a directory descriptor--which
        is necessary for the fsync behavior implemented here.
        """
        dst_path = os.path.abspath(os.fsdecode(path))
        dst_dirname = os.path.normpath(os.path.dirname(dst_path))

        # Check if destination is read-only.
        if os.path.isfile(dst_path) and not os.access(dst_path, os.W_OK):
            msg = f'The file {dst_path!r} is read-only.'
            raise PermissionError(msg)

        # Get temporary file path.
        tmp_f = tempfile.NamedTemporaryFile(
            suffix='.temp',
            prefix=f'{os.path.splitext(os.path.basename(dst_path))[0]}-',
            dir=dst_dirname,  # <- Use same dir as dst_path to assure that
            delete=False,     #    tmp and dst are on the same filesystem.
        )
        tmp_f.close()
        tmp_path = tmp_f.name

        # Copy node data from source to destination.
        dst_con = _schema.get_raw_connection(tmp_path)
        src_con = self._get_connection()
        try:
            src_con.backup(dst_con)
        finally:
            dst_con.close()
            if src_con is not getattr(self, '_connection', None):
                src_con.close()

        # Again, check if destination is read-only. This check is repeated
        # because the backup() method could take a significant amount of
        # time for large datasets which would leave plenty of opportunity
        # for the file permissions to have been changed.
        if os.path.isfile(dst_path) and not os.access(dst_path, os.W_OK):
            os.unlink(tmp_path)  # Remove temporary file.
            msg = f'The file {dst_path!r} is read-only.'
            raise PermissionError(msg)

        # Move file to final path (tmp and dst should be on same filesystem).
        os.replace(tmp_path, dst_path)

        # Exit early when running Windows--skips fsync.
        if sys.platform == 'win32':  # Currently, there's no good way to get
            return  # <- EXIT!       # a directory descriptor on Windows.

        # Flush buffered data to permanent storage (for more info, see
        # Jeff Moyer's article https://lwn.net/Articles/457667/).
        if fsync:
            fd = os.open(dst_dirname, 0)  # Open directory descriptor.
            try:
                _best_effort_fsync(fd)
            finally:
                os.close(fd)

    @classmethod
    def open(
        cls,
        path: PathType,
        required_permissions: _schema.RequiredPermissions = 'readonly',
    ) -> 'DataAccessLayer':
        """Open a node directly from drive (does not load into memory).

        By default, ``'readonly'`` file permissions are required::

            >>> dal = DataAccessLayer.open('mynode.toron')

        If you want to disable file permission requirements, you can
        set *required_permissions* to ``None``::

            >>> dal = DataAccessLayer.open('mynode.toron', required_permissions=None)

        If you want to make sure the file can be modified, you can
        require ``'readwrite'`` permissions::

            >>> dal = DataAccessLayer.open('mynode.toron', required_permissions='readwrite')

        If a file does not satisfy the required permissions, then a
        :class:`PermissionError` is raised.

        .. warning::
            Use caution when writing to a node that has been opened
            directly. Changes are applied **immediately** to the file
            on drive and cannot be undone.

        .. tip::
            If you need to work on files that are too large to fit
            into memory but you don't want to risk changing something
            by accident, try one of the following:

            * Using ``Node.from_file(..., cache_to_drive=True)`` to
              load the node.
            * Making a copy of the file and working on that instead.

            Once you have verified that your changes are good, replace
            the original with your updated version.
        """
        path = os.path.abspath(os.fsdecode(path))
        _schema.get_connection(path, required_permissions).close()  # Verify path to Toron node file.

        obj = cls.__new__(cls)
        obj._filename = path
        obj._required_permissions = required_permissions
        obj._cleanup_item = None
        return obj

    @property
    def filename(self) -> Optional[str]:
        return getattr(self, '_filename', None)

    def _get_connection(self) -> sqlite3.Connection:
        if hasattr(self, '_connection'):
            return self._connection
        if self._filename:
            return _schema.get_connection(self._filename, self._required_permissions)
        raise RuntimeError('cannot get connection')

    @contextmanager
    def _transaction(
        self,
        method: Literal['savepoint', 'begin', None] = 'savepoint',
    ) -> Generator[sqlite3.Cursor, None, None]:
        """A context manager that yields a cursor that runs in an
        isolated transaction. If the context manager exits without
        errors, the transaction is committed. If an exception is
        raised, all changes are rolled-back::

            >>> with self._transaction() as cur:
            >>>     cur.execute(...)
        """
        # Determine transaction context manager.
        if method == 'savepoint':
            transaction_cm = _schema.savepoint
        elif method == 'begin':
            transaction_cm = _schema.begin  # type: ignore [assignment]
        elif method is None:  # Don't use transaction handling.
            transaction_cm = nullcontext  # type: ignore [assignment]
        else:
            msg = f'unknown transaction method: {method!r}'
            raise ValueError(msg)

        if hasattr(self, '_connection'):
            # In-memory database (leave connection open when finished).
            con = self._connection
            con_close = lambda: None
        else:
            # On-drive database (close connection when finished).
            if not self.filename:
                raise RuntimeError('expected filename, none found')
            con = _schema.get_connection(self.filename, self._required_permissions)
            con_close = con.close

        cur = con.cursor()
        try:
            with transaction_cm(cur):
                yield cur
        finally:
            cur.close()
            con_close()

    def __del__(self):
        if isinstance(self._cleanup_item, sqlite3.Connection):
            self._cleanup_item.close()
        elif isinstance(self._cleanup_item, str):
            os.unlink(self._cleanup_item)
            _temp_files_to_delete_atexit.discard(self._cleanup_item)
            self._cleanup_item = None
        else:
            if self._cleanup_item is not None:
                msg = f'unknown cleanup item {self._cleanup_item!r}'
                raise RuntimeError(msg)

    @staticmethod
    def _get_column_names(cursor: sqlite3.Cursor, table: str) -> List[str]:
        """Return a list of column names from the given table."""
        cursor.execute(f"PRAGMA main.table_info('{table}')")
        return [row[1] for row in cursor.fetchall()]

    @classmethod
    def _add_index_columns_make_sql(
        cls, cursor: sqlite3.Cursor, columns: Iterable[str]
    ) -> List[str]:
        """Return a list of SQL statements for adding new label columns."""
        if isinstance(columns, str):
            columns = [columns]
        columns = [_schema.normalize_identifier(col) for col in columns]

        not_allowed = {'"index_id"', '"_location_id"', '"_structure_id"'}.intersection(columns)
        if not_allowed:
            msg = f"label name not allowed: {', '.join(not_allowed)}"
            raise ValueError(msg)

        current_cols = cls._get_column_names(cursor, 'label_index')
        current_cols = [_schema.normalize_identifier(col) for col in current_cols]
        new_cols = [col for col in columns if col not in current_cols]

        if not new_cols:
            return []  # <- EXIT!

        dupes = [obj for obj, count in Counter(new_cols).items() if count > 1]
        if dupes:
            msg = f"duplicate column name: {', '.join(dupes)}"
            raise ValueError(msg)

        sql_stmnts = []

        sql_stmnts.extend(_schema.sql_drop_label_indexes())

        for col in new_cols:
            sql_stmnts.extend([
                f"ALTER TABLE main.label_index ADD COLUMN {_schema.sql_column_def_labelindex_label(col)}",
                f"ALTER TABLE main.location ADD COLUMN {_schema.sql_column_def_location_label(col)}",
                f"ALTER TABLE main.structure ADD COLUMN {_schema.sql_column_def_structure_label(col)}",
            ])

        label_cols = current_cols[1:] + new_cols  # All columns except the id column.
        sql_stmnts.extend(_schema.sql_create_label_indexes(label_cols))

        return sql_stmnts

    @classmethod
    def _rename_index_columns_apply_mapper(
        cls,
        cursor: sqlite3.Cursor,
        mapper: Union[Callable[[str], str], Mapping[str, str]],
    ) -> Tuple[List[str], List[str]]:
        column_names = cls._get_column_names(cursor, 'label_index')
        column_names = column_names[1:]  # Slice-off 'index_id'.

        if callable(mapper):
            new_column_names = [mapper(col) for col in column_names]
        elif isinstance(mapper, Mapping):
            new_column_names = [mapper.get(col, col) for col in column_names]
        else:
            msg = 'mapper must be a callable or dict-like object'
            raise ValueError(msg)

        column_names = [_schema.normalize_identifier(col) for col in column_names]
        new_column_names = [_schema.normalize_identifier(col) for col in new_column_names]

        dupes = [col for col, count in Counter(new_column_names).items() if count > 1]
        if dupes:
            zipped = zip(column_names, new_column_names)
            value_pairs = [(col, new) for col, new in zipped if new in dupes]
            formatted = [f'{col}->{new}' for col, new in value_pairs]
            msg = f'column name collisions: {", ".join(formatted)}'
            raise ValueError(msg)

        return column_names, new_column_names

    @staticmethod
    def _rename_index_columns_make_sql(
        column_names: Sequence[str], new_column_names: Sequence[str]
    ) -> List[str]:
        # The RENAME COLUMN command was added in SQLite 3.25.0 (2018-09-15).
        zipped = zip(column_names, new_column_names)
        rename_pairs = [(a, b) for a, b in zipped if a != b]

        sql_stmnts = []
        for name, new_name in rename_pairs:
            sql_stmnts.extend([
                f'ALTER TABLE main.label_index RENAME COLUMN {name} TO {new_name}',
                f'ALTER TABLE main.location RENAME COLUMN {name} TO {new_name}',
                f'ALTER TABLE main.structure RENAME COLUMN {name} TO {new_name}',
            ])
        return sql_stmnts

    def rename_index_columns(
        self, mapper: Union[Callable[[str], str], Mapping[str, str]]
    ) -> None:
        # Rename columns using native RENAME COLUMN command (only for
        # SQLite 3.25.0 or newer).
        with self._transaction() as cur:
            names, new_names = self._rename_index_columns_apply_mapper(cur, mapper)
            for stmnt in self._rename_index_columns_make_sql(names, new_names):
                cur.execute(stmnt)

    @staticmethod
    def _remove_index_columns_make_sql(
        column_names: Sequence[str], names_to_remove: Sequence[str]
    ) -> List[str]:
        """Return a list of SQL statements for removing label columns."""
        names_to_remove = [col for col in names_to_remove if col in column_names]

        if not names_to_remove:
            return []  # <- EXIT!

        sql_stmnts = []

        sql_stmnts.extend(_schema.sql_drop_label_indexes())

        for col in names_to_remove:
            sql_stmnts.extend([
                f'ALTER TABLE main.label_index DROP COLUMN {col}',
                f'ALTER TABLE main.location DROP COLUMN {col}',
                f'ALTER TABLE main.structure DROP COLUMN {col}',
            ])

        remaining_cols = [col for col in column_names if col not in names_to_remove]
        sql_stmnts.extend(_schema.sql_create_label_indexes(remaining_cols))

        return sql_stmnts

    @staticmethod
    def _coarsen_records_make_sql(
        remaining_columns: Iterable[str]
    ) -> List[str]:
        """Return a list of SQL statements to coarsen the dataset."""
        quoted_names = (_schema.normalize_identifier(col) for col in remaining_columns)
        formatted_names = ', '.join(quoted_names)

        sql_statements = []

        ################################################################
        # Consolidate records in `label_index` and `weight` tables.
        ################################################################

        # Build a temporary table with old-to-new `index_id` mapping.
        sql_statements.append(f'''
            CREATE TEMPORARY TABLE old_to_new_index_id
            AS SELECT index_id, new_index_id
            FROM main.label_index
            JOIN (SELECT MIN(index_id) AS new_index_id, {formatted_names}
                  FROM main.label_index
                  GROUP BY {formatted_names}
                  HAVING COUNT(*) > 1)
            USING ({formatted_names})
        ''')

        # Add any missing `index_id` values, needed for aggregation,
        # to the `weight` table. This is necessary because weightings
        # can be incomplete and the coarsening process may aggregate
        # records using an `index_id` that is not currently defined
        # for a particular weighting.
        sql_statements.append('''
            WITH
                MatchingRecords AS (
                    SELECT weighting_id, index_id, new_index_id
                    FROM main.weight
                    JOIN temp.old_to_new_index_id USING (index_id)
                ),
                MissingIDs AS (
                    SELECT DISTINCT weighting_id, new_index_id FROM MatchingRecords
                    EXCEPT
                    SELECT DISTINCT weighting_id, index_id FROM MatchingRecords
                )
            INSERT INTO main.weight (weighting_id, index_id, weight_value)
            SELECT weighting_id, new_index_id, 0
            FROM MissingIDs
        ''')

        # Assign summed `weight_value` to `weight` records being kept.
        if _SQLITE_VERSION_INFO >= (3, 33, 0):
            # The "UPDATE FROM" syntax was introduced in SQLite 3.33.0.
            sql_statements.append('''
                UPDATE main.weight
                SET weight_value=summed_value
                FROM (SELECT weighting_id AS old_weighting_id,
                             new_index_id,
                             SUM(weight_value) AS summed_value
                      FROM main.weight
                      JOIN temp.old_to_new_index_id USING (index_id)
                      GROUP BY weighting_id, new_index_id)
                WHERE weighting_id=old_weighting_id AND index_id=new_index_id
            ''')
        else:
            sql_statements.append('''
                WITH
                    SummedValues AS (
                        SELECT weighting_id, new_index_id, SUM(weight_value) AS summed_value
                        FROM main.weight
                        JOIN temp.old_to_new_index_id USING (index_id)
                        GROUP BY weighting_id, new_index_id
                    ),
                    RecordsToUpdate AS (
                        SELECT weight_id AS record_id, summed_value
                        FROM main.weight a
                        JOIN SummedValues b
                        ON (a.weighting_id=b.weighting_id AND a.index_id=b.new_index_id)
                    )
                UPDATE main.weight
                SET weight_value = (
                    SELECT summed_value
                    FROM RecordsToUpdate
                    WHERE weight_id=record_id
                )
                WHERE weight_id IN (SELECT record_id FROM RecordsToUpdate)
            ''')

        # Discard old `weight` records.
        sql_statements.append('''
            DELETE FROM main.weight
            WHERE index_id IN (
                SELECT index_id
                FROM temp.old_to_new_index_id
                WHERE index_id != new_index_id
            )
        ''')

        # TODO: Add missing `relation.index_id` values needed for aggregation.
        # TODO: Assign summed `proportion` to `relation` records being kept.
        # TODO: Discard old `relation` records.
        # TODO: Update `relation.mapping_level` codes.

        # Discard old `label_index` records.
        sql_statements.append('''
            DELETE FROM main.label_index
            WHERE index_id IN (
                SELECT index_id
                FROM temp.old_to_new_index_id
                WHERE index_id != new_index_id
            )
        ''')

        # Update `is_complete` for incomplete `weighting` records.
        sql_statements.append('''
            WITH
                WeightCounts AS (
                    SELECT weighting_id, COUNT(*) AS weight_count
                    FROM main.weighting
                    JOIN main.weight USING (weighting_id)
                    WHERE is_complete=0
                    GROUP BY weighting_id
                ),
                IndexCounts AS (
                    SELECT COUNT(*) AS index_count FROM main.label_index
                ),
                NewStatus AS (
                    SELECT
                        weighting_id AS record_id,
                        weight_count=index_count AS is_complete
                    FROM WeightCounts
                    CROSS JOIN IndexCounts
                )
            UPDATE main.weighting
            SET is_complete = (
                SELECT is_complete
                FROM NewStatus
                WHERE weighting_id=record_id
            )
            WHERE weighting_id IN (SELECT record_id FROM NewStatus)
        ''')

        # TODO: Update `is_locally_complete` for incomplete `edge` records.

        # Remove old-to-new temporary table for `index_id` mapping.
        sql_statements.append('DROP TABLE temp.old_to_new_index_id')

        ################################################################
        # Consolidate records in `location` and `quantity` tables.
        ################################################################

        # Build a temporary table with old-to-new `location_id` mapping.
        sql_statements.append(f'''
            CREATE TEMPORARY TABLE old_to_new_location_id
            AS SELECT _location_id, new_location_id
            FROM main.location
            JOIN (SELECT MIN(_location_id) AS new_location_id, {formatted_names}
                  FROM main.location
                  GROUP BY {formatted_names}
                  HAVING COUNT(*) > 1)
            USING ({formatted_names})
        ''')

        # Add any missing `_location_id` and `attributes` pairs,
        # needed for aggregation, to the `quantity` table. This is
        # necessary because not every `_location_id` is guaranteed
        # to have every possible combination of `attributes` values.
        # And the coarsening process may aggregate records using a
        # combination of `_location_id` and `attributes` values that
        # are not currently defined in the `quantity` table.
        sql_statements.append('''
            WITH
                MatchingRecords AS (
                    SELECT attributes, _location_id, new_location_id
                    FROM main.quantity
                    JOIN temp.old_to_new_location_id USING (_location_id)
                ),
                MissingAttributes AS (
                    SELECT DISTINCT attributes, new_location_id FROM MatchingRecords
                    EXCEPT
                    SELECT DISTINCT attributes, _location_id FROM MatchingRecords
                )
            INSERT INTO main.quantity (attributes, _location_id, quantity_value)
            SELECT attributes, new_location_id, 0
            FROM MissingAttributes;
        ''')

        # Assign summed `quantity_value` to `quantity` records being kept.
        if _SQLITE_VERSION_INFO >= (3, 33, 0):
            # The "UPDATE FROM" syntax was introduced in SQLite 3.33.0.
            sql_statements.append('''
                UPDATE main.quantity
                SET quantity_value=summed_value
                FROM (SELECT attributes AS old_attributes,
                             new_location_id,
                             SUM(quantity_value) AS summed_value
                      FROM main.quantity
                      JOIN temp.old_to_new_location_id USING (_location_id)
                      GROUP BY attributes, new_location_id)
                WHERE attributes=old_attributes AND _location_id=new_location_id
            ''')
        else:
            sql_statements.append('''
                WITH
                    SummedValues AS (
                        SELECT attributes, new_location_id, SUM(quantity_value) AS summed_value
                        FROM main.quantity
                        JOIN temp.old_to_new_location_id USING (_location_id)
                        GROUP BY attributes, new_location_id
                    ),
                    RecordsToUpdate AS (
                        SELECT a.attributes AS old_attributes, a._location_id AS record_id, b.summed_value
                        FROM main.quantity a
                        JOIN SummedValues b
                        ON (a.attributes=b.attributes AND a._location_id=b.new_location_id)
                    )
                UPDATE main.quantity
                SET quantity_value = (
                    SELECT summed_value
                    FROM RecordsToUpdate
                    WHERE _location_id=record_id AND attributes=old_attributes
                )
                WHERE _location_id IN (SELECT record_id FROM RecordsToUpdate)
            ''')

        # Discard old `quantity` records.
        sql_statements.append('''
            DELETE FROM main.quantity
            WHERE _location_id IN (
                SELECT _location_id
                FROM temp.old_to_new_location_id
                WHERE _location_id != new_location_id
            )
        ''')

        # Discard old `location` records.
        sql_statements.append('''
            DELETE FROM main.location
            WHERE _location_id IN (
                SELECT _location_id
                FROM temp.old_to_new_location_id
                WHERE _location_id != new_location_id
            )
        ''')

        # Remove old-to-new temporary table for `location_id` mapping.
        sql_statements.append('DROP TABLE temp.old_to_new_location_id')

        return sql_statements

    @classmethod
    def _remove_index_columns_execute_sql(
        cls,
        cursor: sqlite3.Cursor,
        columns: Iterable[str],
        strategy: Strategy = 'preserve',
    ) -> None:
        column_names = cls._get_column_names(cursor, 'label_index')
        column_names = column_names[1:]  # Slice-off 'index_id'.

        names_to_remove = sorted(set(columns).intersection(column_names))
        if not names_to_remove:
            return  # <- EXIT!

        names_remaining = [col for col in column_names if col not in columns]

        categories = cls._get_data_property(cursor, 'discrete_categories') or []
        categories = [set(cat) for cat in categories]
        cats_filtered = [cat for cat in categories if not cat.intersection(columns)]

        # Check for a loss of category coverage.
        cols_uncovered = set(names_remaining).difference(chain(*cats_filtered))
        if cols_uncovered:
            if strategy not in {'restructure', 'coarsenrestructure'}:
                formatted = ', '.join(repr(x) for x in sorted(cols_uncovered))
                msg = f'cannot remove, categories are undefined for remaining columns: {formatted}'
                raise ToronError(msg)

            new_categories = []
            for cat in categories:
                cat = cat.difference(names_to_remove)
                if cat and cat not in new_categories:
                    new_categories.append(cat)
        else:
            new_categories = cats_filtered

        # Check for a loss of granularity.
        cursor.execute(f'''
            SELECT 1
            FROM main.label_index
            GROUP BY {", ".join(names_remaining)}
            HAVING COUNT(*) > 1
        ''')
        if cursor.fetchone() is not None:
            if strategy not in {'coarsen', 'coarsenrestructure'}:
                msg = 'cannot remove, columns are needed to preserve granularity'
                raise ToronError(msg)

            for stmnt in cls._coarsen_records_make_sql(names_remaining):
                cursor.execute(stmnt)

        # Clear `structure` table to prevent duplicates when removing columns.
        cursor.execute('DELETE FROM main.structure')

        # Remove specified columns.
        for stmnt in cls._remove_index_columns_make_sql(column_names, names_to_remove):
            cursor.execute(stmnt)

        # Rebuild categories property and structure table.
        cls._update_categories_and_structure(cursor, new_categories)

        # TODO: Recalculate node_hash for `properties` table.

    def remove_index_columns(
        self, columns: Iterable[str], strategy: Strategy = 'preserve'
    ) -> None:
        with self._transaction() as cur:
            self._remove_index_columns_execute_sql(cur, columns, strategy)

    @classmethod
    def _add_index_records_make_sql(
        cls, cursor: sqlite3.Cursor, columns: Iterable[str]
    ) -> str:
        """Return a SQL statement adding new index records (for use
        with an executemany() call.

        Example:

            >>> dal = DataAccessLayer(...)
            >>> dal._add_index_records_make_sql(cursor, ['state', 'county'])
            'INSERT INTO label_index ("state", "county") VALUES (?, ?)'
        """
        columns = [_schema.normalize_identifier(col) for col in columns]

        existing_columns = cls._get_column_names(cursor, 'label_index')
        existing_columns = existing_columns[1:]  # Slice-off "index_id" column.
        existing_columns = [_schema.normalize_identifier(col) for col in existing_columns]

        invalid_columns = set(columns).difference(existing_columns)
        if invalid_columns:
            msg = f'invalid column name: {", ".join(invalid_columns)}'
            raise sqlite3.OperationalError(msg)

        columns_clause = ', '.join(columns)
        values_clause = ', '.join('?' * len(columns))
        return f'INSERT INTO main.label_index ({columns_clause}) VALUES ({values_clause})'

    def add_index_records(self, data: TabularData) -> None:
        iterator = make_readerlike(data)
        columns = next(iterator)

        with self._transaction() as cur:
            # Get allowed columns and build selectors values.
            allowed_columns = self._get_column_names(cur, 'label_index')
            selectors = tuple((col in allowed_columns) for col in columns)

            # Filter column names and iterator rows to allowed columns.
            columns = tuple(compress(columns, selectors))
            iterator = (tuple(compress(row, selectors)) for row in iterator)

            sql = self._add_index_records_make_sql(cur, columns)
            cur.executemany(sql, iterator)

    @staticmethod
    def _add_weights_get_new_id(
        cursor: sqlite3.Cursor,
        name: str,
        selectors: Optional[Iterable[str]] = None,
        description: Optional[str] = None,
    ) -> int:
        # This method uses the RETURNING clause which was introduced
        # in SQLite 3.35.0 (2021-03-12).
        if selectors:
            selectors = _dumps(selectors)  # Dump JSON to string.
        elif selectors is not None:
            selectors = None  # Set falsy values to None.

        sql = """
            INSERT INTO main.weighting(name, selectors, description)
            VALUES(?, ?, ?)
            RETURNING weighting_id
        """
        cursor.execute(sql, (name, selectors, description))
        return cursor.fetchone()[0]

    @classmethod
    def _add_weights_make_sql(
        cls, cursor: sqlite3.Cursor, columns: Sequence[str]
    ) -> str:
        """Return a SQL statement adding new weight value (for
        use with an executemany() call.
        """
        columns = [_schema.normalize_identifier(col) for col in columns]

        existing_columns = cls._get_column_names(cursor, 'label_index')
        existing_columns = [_schema.normalize_identifier(col) for col in existing_columns]

        invalid_columns = set(columns).difference(existing_columns)
        if invalid_columns:
            msg = f'invalid column name: {", ".join(invalid_columns)}'
            raise sqlite3.OperationalError(msg)

        where_clause = ' AND '.join(f'{col}=?' for col in columns)
        groupby_clause = ', '.join(columns)

        sql = f"""
            INSERT INTO main.weight (weighting_id, index_id, weight_value)
            SELECT ? AS weighting_id, index_id, ? AS weight_value
            FROM main.label_index
            WHERE {where_clause}
            GROUP BY {groupby_clause}
            HAVING COUNT(*)=1
        """
        return sql

    @staticmethod
    def _add_weights_set_is_complete(
        cursor: sqlite3.Cursor, weighting_id: int
    ) -> None:
        """Set the 'weighting.is_complete' value to 1 or 0 (True/False)."""
        sql = """
            UPDATE main.weighting
            SET is_complete=((SELECT COUNT(*)
                              FROM main.weight
                              WHERE weighting_id=?) = (SELECT COUNT(*)
                                                       FROM main.label_index))
            WHERE weighting_id=?
        """
        cursor.execute(sql, (weighting_id, weighting_id))

    def add_weights(
        self,
        data: TabularData,
        name: str,
        *,
        selectors: Optional[Sequence[str]],
        description: Optional[str] = None,
    ) -> None:
        iterator = make_readerlike(data)
        columns = next(iterator)

        try:
            weight_pos = columns.index(name)  # Get position of weight column.
        except ValueError:
            columns_string = ', '.join(repr(x) for x in columns)
            msg = f'Name {name!r} does not appear in columns: {columns_string}'
            raise ValueError(msg)

        with self._transaction() as cur:
            weighting_id = self._add_weights_get_new_id(cur, name, selectors, description)

            # Get allowed columns and build bitmask selectors values.
            allowed_columns = self._get_column_names(cur, 'label_index')
            bitmask_selectors = tuple((col in allowed_columns) for col in columns)

            # Filter column names and iterator rows to allowed columns.
            columns = tuple(compress(columns, bitmask_selectors))
            def mkrow(row):
                weightid_and_value = (weighting_id, row[weight_pos])
                index_labels = tuple(compress(row, bitmask_selectors))
                return weightid_and_value + index_labels
            iterator = (mkrow(row) for row in iterator)

            # Insert weight records.
            sql = self._add_weights_make_sql(cur, columns)
            cur.executemany(sql, iterator)

            # Update "weighting.is_complete" value (set to 1 or 0).
            self._add_weights_set_is_complete(cur, weighting_id)

    @staticmethod
    def _add_quantities_get_location_id(
        cursor: sqlite3.Cursor,
        labels: Mapping[str, str],
    ) -> int:
        """Return _location_id for given labels.

        IMPORTANT: To assure correct behavior, the given *labels* must
        contain all label columns present in the 'location' table.
        """
        keys = [_schema.normalize_identifier(k) for k in labels.keys()]
        values = [str(v).strip() for v in labels.values()]

        select_sql = f"""
            SELECT _location_id
            FROM main.location
            WHERE {' AND '.join(f'{k}=?' for k in keys)}
            LIMIT 2
        """
        cursor.execute(select_sql, values)
        location_id = cursor.fetchone()
        if location_id:
            if cursor.fetchone() is not None:
                raise RuntimeError(
                    f'multiple location ids for given labels: {dict(labels)!r}'
                )
            return location_id[0]  # <- EXIT!

        insert_sql = f"""
            INSERT INTO main.location({', '.join(keys)})
            VALUES({', '.join(['?'] * len(labels))})
        """
        cursor.execute(insert_sql, values)
        if not cursor.lastrowid:
            raise RuntimeError('record just inserted, lastrowid should not be None')
        return cursor.lastrowid

    @staticmethod
    def _add_quantities_warn(
        missing_attrs_count: int,
        missing_vals_count: int,
        inserted_rows_count: int,
    ) -> None:
        """If needed, emit ToronWarning with relevant information."""
        messages = []

        if missing_attrs_count:
            messages.append(
                f'skipped {missing_attrs_count} rows with no attributes'
            )

        if missing_vals_count:
            messages.append(
                f'skipped {missing_vals_count} rows with no quantity value'
            )

        if messages:
            import warnings
            msg = f'{", ".join(messages)}, inserted {inserted_rows_count} rows'
            warnings.warn(msg, category=ToronWarning, stacklevel=3)

    def add_quantities(
        self,
        data: TabularData,
        value: str,
        *,
        attributes: Optional[Iterable[str]] = None,
    ) -> None:
        """Add quantities and associated attributes. Quantity values
        are automatically associated with matching index labels.

        Parameters
        ----------
        data : Iterable[Sequence] | Iterable[Mapping] | pandas.DataFrame
            Tabular data values--must contain one or more index columns,
            one or more `attribute` columns, and a single `value`
            column.
        value : str
            Name of column which contains the quantity values.
        attributes : Iterable[str], optional
            Name of columns which contain attributes. If not given,
            attributes will default to all non-index, non-value
            columns that don't begin with an underscore ('_').

        Load quantites from an iterator of sequences::

            >>> data = [
            ...     ['idx1', 'idx2', 'attr1', 'attr2', 'counts'],
            ...     ['A', 'x', 'foo', 'corge', 12],
            ...     ['B', 'y', 'bar', 'qux', 10],
            ...     ['C', 'z', 'baz', 'quux', 15],
            ... ]
            >>> dal.add_quantities(data, 'counts')

        Load quantites using an iterator of dictionary-rows::

            >>> data = [
            ...     {'idx1': 'A', 'idx2': 'x', 'attr1': 'foo', 'attr2': 'corge', 'counts': 12},
            ...     {'idx1': 'B', 'idx2': 'y', 'attr1': 'bar', 'attr2': 'qux', 'counts': 10},
            ...     {'idx1': 'C', 'idx2': 'z', 'attr1': 'baz', 'attr2': 'quux', 'counts': 15},
            ... ]
            >>> dal.add_quantities(data, 'counts')
        """
        dict_rows = make_dictreaderlike(data)

        # Prepare data and insert quantities.
        with self._transaction() as cur:
            label_columns = self._get_column_names(cur, 'location')[1:]

            if attributes:
                def is_attr(col):  # <- Helper function.
                    return col in attributes
            else:
                def is_attr(col):  # <- Helper function.
                    return (
                        col not in label_columns
                        and not col.startswith('_')
                        and col != value
                        and col.strip() != ''
                    )

            def make_attrs_vals(row_dict):  # <- Helper function.
                quant_value = row_dict[value]
                attr_items = ((k, v) for k, v in row_dict.items() if is_attr(k))
                attr_items = ((k, v) for k, v in attr_items if v != '' and v is not None)
                attr_json = _dumps(dict(attr_items), sort_keys=True)
                return (attr_json, quant_value)

            def make_loc_dict(row_dict):  # <- Helper function.
                row_dict = {k: v for k, v in row_dict.items() if k and v}
                return {k: row_dict.get(k, '') for k in label_columns}

            missing_attrs_count = 0
            missing_vals_count = 0
            inserted_rows_count = 0

            for loc_dict, group in groupby(dict_rows, key=make_loc_dict):
                loc_id = self._add_quantities_get_location_id(cur, loc_dict)

                group = (row_dict for row_dict in group if (value in row_dict))
                attrs_vals = (make_attrs_vals(row_dict) for row_dict in group)

                for attr, val in attrs_vals:
                    if attr == '{}':
                        missing_attrs_count += 1
                        continue

                    if val == '' or val is None:
                        missing_vals_count += 1
                        continue

                    statement = """
                        INSERT INTO main.quantity (_location_id, attributes, quantity_value)
                            VALUES(?, ?, ?)
                    """
                    cur.execute(statement, (loc_id, attr, val))
                    inserted_rows_count += 1

            self._add_quantities_warn(
                missing_attrs_count,
                missing_vals_count,
                inserted_rows_count,
            )

    @staticmethod
    def _get_raw_quantities_format_args(
        index_cols: List[str], where: Dict[str, str]
    ) -> Tuple[List[str], Tuple[str, ...], Optional[Callable[[Any], bool]]]:
        """Format arguments for get_raw_quantities() and
        delete_raw_quantities() methods.

        :param List index_cols:
            A list of all index column names defined in the
            `label_index` table.
        :param Dict where:
            A dictionary of column and value requirements that will be
            used to prepare a WHERE expression and *parameters* for a
            call to `con.execute(..., parameters)`.
        :returns Tuple:
            Returns a three-tuple containing a *where-expression*
            list, an *execute-parameters* tuple, and an optional
            *attribute-selector* function.

        .. code-block::

            >>> index_cols = ['state', 'county']
            >>> where = {'state': 'OH'}
            >>> dal._get_raw_quantities_format_args(index_cols, where)
            (['"state"=?'], ('OH',), None)

        .. code-block::

            >>> index_cols = ['state', 'county']
            >>> where = {'county': 'FRANKLIN', 'census': 'TOT_MALE'}
            >>> dal._get_raw_quantities_format_args(index_cols, where)
            (['"county"=?'],
             ('FRANKLIN',),
             accepts_json_input(SimpleSelector('census', '=', 'TOT_MALE')))
        """
        normalized = [_schema.normalize_identifier(x) for x in index_cols]

        # Partition location and atttribute keys into separate dicts.
        loc_dict = {}
        attr_dict = {}
        for k, v in where.items():
            normalized_key = _schema.normalize_identifier(k)
            if normalized_key in normalized:
                loc_dict[normalized_key] = v
            else:
                attr_dict[k] = v

        # Build items for where clause.
        where_items = [f'{k}=?' for k in loc_dict.keys()]
        parameters = tuple(loc_dict.values())

        # Build function to check for matching attributes.
        if attr_dict:
            selector = CompoundSelector(
                [SimpleSelector(k, '=', v) for k, v in attr_dict.items()]
            )
            attr_func = accepts_json_input(selector)
        else:
            attr_func = None

        return where_items, parameters, attr_func

    @staticmethod
    def _get_raw_quantities_execute(
        cursor: sqlite3.Cursor,
        location_cols: List[str],
        where_items: List[str],
        parameters: Tuple[str, ...],
    ) -> Generator[Dict[str, Union[str, float]], None, None]:
        """Build query, execute, and yield results of dict rows."""
        # Build SQL query.
        normalized = [_schema.normalize_identifier(x) for x in location_cols]
        statement = f"""
            SELECT {', '.join(normalized)}, attributes, quantity_value
            FROM main.quantity
            JOIN main.location USING (_location_id)
            {'WHERE ' if where_items else ''}{' AND '.join(where_items)}
        """

        # Execute SQL query and yield results.
        cursor.execute(statement, parameters)
        for row in cursor:
            *labels, attr_dict, value = row  # Unpack row.
            row_dict = dict(zip(location_cols, labels))
            row_dict.update(attr_dict)
            row_dict['value'] = value
            yield row_dict

    def get_raw_quantities(
        self, **where: str
    ) -> Generator[Dict[str, Union[str, float]], None, None]:
        """Get raw data quantities."""
        with self._transaction(method=None) as cur:
            location_cols = self._get_column_names(cur, 'location')[1:]
            where_items, parameters, attr_func = \
                self._get_raw_quantities_format_args(location_cols, where)

            if attr_func:
                func_name = _schema.get_userfunc(cur, attr_func)
                where_items.append(f'{func_name}(attributes)=1')

            yield from self._get_raw_quantities_execute(
                cur, location_cols, where_items, parameters
            )

    @staticmethod
    def _delete_raw_quantities_execute(
        cursor: sqlite3.Cursor,
        where_items: List[str],
        parameters: Tuple[str, ...],
    ) -> int:
        """Build and execute SQL statement to delete rows."""
        # Delete quantity records that match where_items and parameters.
        statement = f"""
            DELETE FROM main.quantity
            WHERE quantity_id IN (
                SELECT quantity_id
                FROM main.quantity
                JOIN main.location USING (_location_id)
                WHERE {' AND '.join(where_items)}
            )
        """
        cursor.execute(statement, parameters)
        deleted_rowcount = cursor.rowcount

        # Delete any unused location records.
        statement = """
            DELETE FROM main.location
            WHERE _location_id IN (
                SELECT t1._location_id
                FROM main.location t1
                LEFT JOIN main.quantity t2
                    ON t1._location_id=t2._location_id
                WHERE t2._location_id IS NULL
            )
        """
        cursor.execute(statement)

        return deleted_rowcount

    def delete_raw_quantities(self, **where: str) -> None:
        """Delete data quantities."""
        if not where:
            msg = 'requires at least one key-word argument'
            raise TypeError(msg)

        with self._transaction(method='begin') as cur:
            location_cols = self._get_column_names(cur, 'location')[1:]
            where_items, parameters, attr_func = \
                self._get_raw_quantities_format_args(location_cols, where)

            if attr_func:
                func_name = _schema.get_userfunc(cur, attr_func)
                where_items.append(f'{func_name}(attributes)=1')

            self._delete_raw_quantities_execute(cur, where_items, parameters)

    @staticmethod
    def _disaggregate_make_sql_constraints(
        normalized_columns: Sequence[str],
        bitmask: Sequence[Literal[0, 1]],
        location_table_alias: str,
        index_table_alias: str,
    ) -> str:
        """Build a string of constraints on which to join the
        `location` and `label_index` tables for disaggregation.

        If a column is associated with a bitmask value of 1, then
        its condition should be `loc.COLNAME=idx.COLNAME`. But if
        a column is associated with a bitmask value of 0, then the
        condition should be `loc.COLNAME=''`.

        .. code-block::

            >>> normalized_columns = ['"A"', '"B"', '"C"']
            >>> bitmask = [1, 0, 1]
            >>> dal._disaggregate_make_sql_constraints(
            ...     normalized_columns,
            ...     bitmask,
            ...     location_table_alias='t2',
            ...     index_table_alias='t3',
            ... )
            't2."A"=t3."A" AND t2."B"=\'\' AND t2."C"=t3."C"'
        """
        # Strip trailing 0s from bitmask.
        bitmask = list(bitmask)
        try:
            while bitmask[-1] == 0:
                bitmask.pop()
        except IndexError:
            pass

        # Check that bitmask does not exceed columns.
        if len(bitmask) > len(normalized_columns):
            msg = (
                f'incompatible bitmask:\n'
                f'  columns = {", ".join(normalized_columns)}\n'
                f'  bitmask = {", ".join(str(x) for x in bitmask)}'
            )
            raise ValueError(msg)

        join_constraints = []
        for col, bit in zip_longest(normalized_columns, bitmask, fillvalue=0):
            if bit:
                constraint = f'{location_table_alias}.{col}={index_table_alias}.{col}'
            else:
                constraint = f'{location_table_alias}.{col}=\'\''
            join_constraints.append(constraint)

        return ' AND '.join(join_constraints)

    @classmethod
    def _disaggregate_make_sql(
        cls,
        normalized_columns: Sequence[str],
        bitmask: Sequence[Literal[0, 1]],
        match_selector_func: str,
        filter_attrs_func: Optional[str] = None,
    ) -> str:
        """Return SQL to disaggregate data."""
        join_constraints = cls._disaggregate_make_sql_constraints(
            normalized_columns,
            bitmask,
            location_table_alias='t2',
            index_table_alias='t3',
        )

        # Build WHERE clause if *filter_attrs_func* was given.
        if filter_attrs_func:
            where_clause = f'\n            WHERE {filter_attrs_func}(t1.attributes)=1'
        else:
            where_clause = ''

        # Build final SELECT statement.
        statement = f"""
            SELECT
                t3.index_id,
                t1.attributes,
                t1.quantity_value * IFNULL(
                    (t4.weight_value / SUM(t4.weight_value) OVER (PARTITION BY t1.quantity_id)),
                    (1.0 / COUNT(1) OVER (PARTITION BY t1.quantity_id))
                ) AS quantity_value
            FROM main.quantity t1
            JOIN main.location t2 USING (_location_id)
            JOIN main.label_index t3 ON ({join_constraints})
            JOIN main.weight t4 ON (
                t3.index_id=t4.index_id
                AND t4.weighting_id={match_selector_func}(t1.attributes)
            ){where_clause}
        """
        return statement

    def static_disaggregate(
        self, **filter_rows_where: str
    ) -> Generator[Dict[str, Union[str, float]], None, None]:
        """Return a generator that yields disaggregated quantities
        calculated using only pre-determined weights.
        """
        with self._transaction(method=None) as cur:
            # Prepare weighting_id matcher function.
            cur.execute("""
                SELECT weighting_id, selectors
                FROM main.weighting
                WHERE is_complete=1
            """)
            match_weighting_id = GetMatchingKey(cur.fetchall(), default=1)
            weighting_func_name = _schema.get_userfunc(cur, match_weighting_id)

            # Get column names from 'location'.
            columns = self._get_column_names(cur, 'location')[1:]
            normalized_cols = [_schema.normalize_identifier(col) for col in columns]

            # Get bitmask levels from structure table.
            cur.execute('SELECT * FROM main.structure')
            bitmasks = [row[2:] for row in cur]  # Slice-off id and granularity values.

            # Prepare WHERE clause items, parameters, and optional function.
            where_items, parameters, attr_func = \
                self._get_raw_quantities_format_args(columns, filter_rows_where)

            # If attribute selector function is given, get an SQL user function
            # name for it.
            if attr_func:
                attr_func_name = _schema.get_userfunc(cur, attr_func)
            else:
                attr_func_name = None

            # Build SQL statement.
            sql_statements = []
            for bitmask in bitmasks:
                sql = self._disaggregate_make_sql(
                    normalized_cols,
                    bitmask,
                    weighting_func_name,
                    attr_func_name,
                )
                sql_statements.append(sql)

            # Build a UNION of all disaggregation queries.
            disaggregated_quantities = \
                '\n            UNION ALL\n'.join(sql_statements)

            # Build a WHERE clause for final statement.
            if where_items:
                joined_items = ' AND '.join(f't1.{x}' for x in where_items)
                where_clause = f'\n                WHERE {joined_items}'
            else:
                where_clause = ''

            final_sql = f"""
                WITH
                    all_quantities AS (
                        {disaggregated_quantities}
                    )
                SELECT t1.*, t2.attributes, SUM(t2.quantity_value) AS quantity_value
                FROM main.label_index t1
                JOIN all_quantities t2 USING (index_id){where_clause}
                GROUP BY {', '.join(f't1.{x}' for x in normalized_cols)}, t2.attributes
            """

            # Execute SQL and yield result rows.
            cur.execute(final_sql, parameters)
            for row in cur:
                yield row

    @classmethod
    def _adaptive_disaggregate_make_sql(
        cls,
        normalized_columns: Sequence[str],
        bitmask: Sequence[Literal[0, 1]],
        match_selector_func: str,
        adaptive_weight_table: str,
        filter_attrs_func: Optional[str] = None,
        match_attrs_keys: Optional[Sequence[str]] = None,
    ) -> str:
        """Return SQL CTE statement to adaptively disaggregate data."""
        join_constraints = cls._disaggregate_make_sql_constraints(
            normalized_columns,
            bitmask,
            location_table_alias='t2',
            index_table_alias='t3',
        )

        # Build WHERE clause if *filter_attrs_func* was given.
        if filter_attrs_func:
            where_clause = f'\n            WHERE {filter_attrs_func}(t1.attributes)=1'
        else:
            where_clause = ''

        # Build string of args to use in the application-defined
        # user_json_object_keep() SQL function.
        if match_attrs_keys:
            # Note: The leading comma is always needed for proper syntax.
            func = lambda x: f', {_schema.sql_string_literal(x)}'
            keys_to_keep = ''.join(func(x) for x in match_attrs_keys)
        else:
            keys_to_keep = ''

        # Build final SELECT statement.
        statement = f"""
            SELECT
                t3.index_id,
                t1.attributes,
                t1.quantity_value * COALESCE(
                    (COALESCE(t5.weight_value, 0.0) / SUM(t5.weight_value) OVER (PARTITION BY t1.quantity_id)),
                    (t4.weight_value / SUM(t4.weight_value) OVER (PARTITION BY t1.quantity_id)),
                    (1.0 / COUNT(1) OVER (PARTITION BY t1.quantity_id))
                ) AS quantity_value
            FROM main.quantity t1
            JOIN main.location t2 USING (_location_id)
            JOIN main.label_index t3 ON ({join_constraints})
            JOIN main.weight t4 ON (
                t3.index_id=t4.index_id
                AND t4.weighting_id={match_selector_func}(t1.attributes)
            )
            LEFT JOIN (
                SELECT
                    t5sub.index_id,
                    user_json_object_keep(t5sub.attributes{keys_to_keep}) AS attrs_subset,
                    SUM(t5sub.quantity_value) AS weight_value
                FROM {adaptive_weight_table} t5sub
                GROUP BY t5sub.index_id, user_json_object_keep(t5sub.attributes{keys_to_keep})
            ) t5 ON (
                t3.index_id=t5.index_id
                AND t5.attrs_subset=user_json_object_keep(t1.attributes{keys_to_keep})
            ){where_clause}
            UNION ALL
            SELECT index_id, attributes, quantity_value FROM {adaptive_weight_table}
        """
        return statement

    def adaptive_disaggregate(
        self,
        match_attributes: Optional[Sequence[str]] = None,
        **filter_rows_where: str,
    ) -> Generator[Dict[str, Union[str, float]], None, None]:
        """Return a generator that yields disaggregated quantities
        calculated using previously disaggregated quantities as
        weights (when available). And when no previously disaggregated
        quantities are available, static disaggregation is performed
        using pre-calculated weights.
        """
        with self._transaction(method=None) as cur:
            # Prepare weighting_id matcher function.
            cur.execute("""
                SELECT weighting_id, selectors
                FROM main.weighting
                WHERE is_complete=1
            """)
            match_weighting_id = GetMatchingKey(cur.fetchall(), default=1)
            weighting_func_name = _schema.get_userfunc(cur, match_weighting_id)

            # Get bitmask levels from structure table.
            columns = self._get_column_names(cur, 'location')[1:]
            normalized_cols = [_schema.normalize_identifier(col) for col in columns]
            cur.execute('SELECT * FROM main.structure')
            bitmasks = [row[2:] for row in cur]  # Slice-off id and granularity values.
            bitmasks.reverse()  # <- Temporary until granularity measure is implemented.

            # Prepare WHERE clause items and parameters.
            where_items, parameters, attr_func = \
                self._get_raw_quantities_format_args(columns, filter_rows_where)

            # If attribute selector function is given, get an SQL user function
            # name for it.
            if attr_func:
                attr_func_name = _schema.get_userfunc(cur, attr_func)
            else:
                attr_func_name = None

            # Prepare to build SQL statement.
            sql_statements = []
            prev_curr_and_bitmask = \
                ((f'cte{i}', f'cte{i+1}', bits) for i, bits in enumerate(bitmasks))

            # Generate first CTE--which must use static weighting.
            _, current_cte, bitmask = next(prev_curr_and_bitmask)
            sql = self._disaggregate_make_sql(
                normalized_cols,
                bitmask,
                weighting_func_name,
                attr_func_name,
            )

            cte_statement = f'{current_cte} AS ({sql})'.strip()
            sql_statements.append(cte_statement)

            # Generate additional CTEs using adaptive weighting derived
            # from the values in previous CTEs.
            for previous_cte, current_cte, bitmask in prev_curr_and_bitmask:
                sql = self._adaptive_disaggregate_make_sql(
                    normalized_cols,
                    bitmask,
                    weighting_func_name,
                    adaptive_weight_table=previous_cte,
                    filter_attrs_func=attr_func_name,
                    match_attrs_keys=match_attributes,
                )
                cte_statement = f'{current_cte} AS ({sql})'.strip()
                sql_statements.append(cte_statement)

            all_cte_statements = ',\n        '.join(sql_statements)

            # Build a WHERE clause for final statement.
            if where_items:
                joined_items = ' AND '.join(f't1.{x}' for x in where_items)
                where_clause = f'\n                WHERE {joined_items}'
            else:
                where_clause = ''

            # Prepare final SQL statement.
            final_sql = f"""
                WITH
                    {all_cte_statements}
                SELECT t1.*, t2.attributes, SUM(t2.quantity_value) AS quantity_value
                FROM main.label_index t1
                JOIN {current_cte} t2 USING (index_id){where_clause}
                GROUP BY t2.index_id, t2.attributes
            """

            # Execute SQL and yield result rows.
            cur.execute(final_sql, parameters)
            for row in cur:
                yield row

    @staticmethod
    def _get_data_property(cursor: sqlite3.Cursor, key: str) -> Any:
        sql = 'SELECT value FROM main.property WHERE key=?'
        cursor.execute(sql, (key,))
        result = cursor.fetchone()
        return result[0] if result else None

    def get_data(self, keys: Iterable[str]) -> Mapping[str, Any]:
        data = {}
        with self._transaction() as cur:
            for key in keys:
                if key == 'index_columns':
                    cur.execute("PRAGMA main.table_info('label_index')")
                    names = [row[1] for row in cur.fetchall()]
                    data[key] = names[1:]  # Slice-off index_id.
                elif key == 'discrete_categories':
                    categories = self._get_data_property(cur, key) or []
                    data[key] = [set(x) for x in categories]
                else:
                    data[key] = self._get_data_property(cur, key)
        return data

    @staticmethod
    def _set_data_property(
        cursor: sqlite3.Cursor, key: str, value: Any
    ) -> None:
        parameters: Tuple[str, ...]
        if value is not None:
            # Insert or update property with JSON string.
            sql = '''
                INSERT INTO main.property(key, value) VALUES(?, ?)
                  ON CONFLICT(key) DO UPDATE SET value=?
            '''
            json_value = _dumps(value, sort_keys=True)
            parameters = (key, json_value, json_value)
        else:
            # Delete property when value is `None`.
            sql = 'DELETE FROM main.property WHERE key=?'
            parameters = (key,)

        cursor.execute(sql, parameters)

    @classmethod
    def _set_data_structure(
        cls, cursor: sqlite3.Cursor, structure: Iterable[Set[str]]
    ) -> None:
        """Populates 'structure' table with bitmask made from *structure*."""
        cursor.execute('DELETE FROM main.structure')  # Delete all table records.
        if not structure:
            return  # <- EXIT!

        columns = cls._get_column_names(cursor, 'structure')
        columns = columns[1:]  # Slice-off "_structure_id" column.
        if not columns:
            msg = 'no labels defined, must first add columns'
            raise ToronError(msg)

        columns_clause = ', '.join(_schema.normalize_identifier(col) for col in columns)
        values_clause = ', '.join('?' * len(columns))
        sql = f'INSERT INTO structure ({columns_clause}) VALUES ({values_clause})'

        make_bitmask = lambda cat: tuple((col in cat) for col in columns)
        parameters = (make_bitmask(category) for category in structure)
        cursor.executemany(sql, parameters)

    @classmethod
    def _update_categories_and_structure(
        cls,
        cursor: sqlite3.Cursor,
        categories: Optional[List[Set[str]]] = None,
        *,
        minimize: bool = True,
    ) -> None:
        """Update `discrete_categories` property and `structure` table.

        Set new categories and rebuild structure table::

            >>> cur = ...
            >>> dal._update_categories_and_structure(cur, categories)

        If categories have already been minimized, you can set the
        *minimize* flag to False in order to prevent running the
        process unnecessarily::

            >>> cur = ...
            >>> dal._update_categories_and_structure(cur, categories, minimize=False)

        Refresh values if label columns have been added but there are
        no explicit category changes (only implicit ones)::

            >>> cur = ...
            >>> dal._update_categories_and_structure(cur)
        """
        if not categories:
            categories = cls._get_data_property(cursor, 'discrete_categories') or []
            categories = [set(x) for x in categories]

        if minimize:
            whole_space = set(cls._get_column_names(cursor, 'label_index')[1:])
            categories = minimize_discrete_categories(categories, [whole_space])

        list_of_lists = [list(cat) for cat in categories]  # type: ignore [union-attr]
        cls._set_data_property(cursor, 'discrete_categories', list_of_lists)

        structure = make_structure(categories)
        cls._set_data_structure(cursor, structure)

    def set_data(
        self,
        mapping_or_items: Union[Mapping[str, Any], Iterable[Tuple[str, Any]]],
    ) -> None:
        items: Iterable[Tuple[str, Any]]
        if isinstance(mapping_or_items, Mapping):
            items = mapping_or_items.items()
        else:
            items = mapping_or_items

        # Bring 'add_index_columns' action to the front of the list (it
        # should be processed first).
        items = sorted(items, key=lambda item: item[0] != 'add_index_columns')

        with self._transaction() as cur:
            for key, value in items:
                if key == 'discrete_categories':
                    self._set_data_property(cur, key, [list(cat) for cat in value])
                elif key == 'structure':
                    self._set_data_structure(cur, value)
                elif key == 'add_index_columns':
                    for stmnt in self._add_index_columns_make_sql(cur, value):
                        cur.execute(stmnt)
                    self._update_categories_and_structure(cur)
                else:
                    msg = f"can't set value for {key!r}"
                    raise ToronError(msg)

    def add_discrete_categories(
        self, discrete_categories: Iterable[Set[str]]
    ) -> None:
        data = self.get_data(['discrete_categories', 'index_columns'])
        minimized = minimize_discrete_categories(
            data['discrete_categories'],
            discrete_categories,
            [set(data['index_columns'])],
        )

        omitted = [cat for cat in discrete_categories if (cat not in minimized)]
        if omitted:
            import warnings
            formatted = ', '.join(repr(cat) for cat in omitted)
            msg = f'omitting categories already covered: {formatted}'
            warnings.warn(msg, category=ToronWarning, stacklevel=2)

        with self._transaction() as cur:
            self._update_categories_and_structure(cur, minimized, minimize=False)

    def remove_discrete_categories(
        self, discrete_categories: List[Set[str]]
    ) -> None:
        data = self.get_data(['discrete_categories', 'index_columns'])
        current_cats = data['discrete_categories']
        mandatory_cat = set(data['index_columns'])

        if mandatory_cat in discrete_categories:
            import warnings
            formatted = ', '.join(repr(x) for x in data['index_columns'])
            msg = f'cannot remove whole space: {{{mandatory_cat}}}'
            warnings.warn(msg, category=ToronWarning, stacklevel=2)
            discrete_categories.remove(mandatory_cat)  # <- Remove and continue.

        no_match = [x for x in discrete_categories if x not in current_cats]
        if no_match:
            import warnings
            formatted = ', '.join(repr(x) for x in no_match)
            msg = f'no match for categories, cannot remove: {formatted}'
            warnings.warn(msg, category=ToronWarning, stacklevel=2)

        remaining_cats = [x for x in current_cats if x not in discrete_categories]

        minimized = minimize_discrete_categories(
            remaining_cats,
            [mandatory_cat],
        )

        with self._transaction() as cur:
            self._update_categories_and_structure(cur, minimized, minimize=False)


class DataAccessLayerPre35(DataAccessLayer):
    """This is a subclass of DataAccessLayer that supports SQLite
    versions before 3.35.0 (2021-03-12).

    For full documentation, see DataAccessLayer.
    """
    @staticmethod
    def _add_weights_get_new_id(
        cursor: sqlite3.Cursor,
        name: str,
        selectors: Optional[Iterable[str]] = None,
        description: Optional[str] = None,
    ) -> int:
        # Since the `RETURNING` clause is not available before version
        # 3.35.0, this method executes a second statement using the
        # last_insert_rowid() SQLite function.
        if selectors:
            selectors = _dumps(selectors)  # Dump JSON to string.
        elif selectors is not None:
            selectors = None  # Set falsy values to None.

        sql = """
            INSERT INTO main.weighting(name, selectors, description)
            VALUES(?, ?, ?)
        """
        cursor.execute(sql, (name, selectors, description))
        cursor.execute('SELECT last_insert_rowid()')
        return cursor.fetchone()[0]

    @staticmethod
    def _remove_index_columns_make_sql(
        column_names: Sequence[str], names_to_remove: Sequence[str]
    ) -> List[str]:
        """Return a list of SQL statements for removing label columns."""
        # In SQLite versions before 3.35.0, there is no native support for the
        # DROP COLUMN command. In these older versions of SQLite the tables
        # must be rebuilt. This method prepares a sequence of operations to
        # rebuild the table structures.
        columns_to_keep = [col for col in column_names if col not in names_to_remove]
        new_labelindex_cols = [_schema.sql_column_def_labelindex_label(col) for col in columns_to_keep]
        new_location_cols = [_schema.sql_column_def_location_label(col) for col in columns_to_keep]
        new_structure_cols = [_schema.sql_column_def_structure_label(col) for col in columns_to_keep]

        statements = [
            # Rebuild 'label_index'.
            f'CREATE TABLE main.new_labelindex(index_id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
                f'{", ".join(new_labelindex_cols)})',
            f'INSERT INTO main.new_labelindex SELECT index_id, {", ".join(columns_to_keep)} FROM main.label_index',
            'DROP TABLE main.label_index',
            'ALTER TABLE main.new_labelindex RENAME TO label_index',

            # Rebuild 'location' table.
            f'CREATE TABLE main.new_location(_location_id INTEGER PRIMARY KEY, ' \
                f'{", ".join(new_location_cols)})',
            f'INSERT INTO main.new_location '
                f'SELECT _location_id, {", ".join(columns_to_keep)} FROM main.location',
            'DROP TABLE main.location',
            'ALTER TABLE main.new_location RENAME TO location',

            # Rebuild 'structure' table.
            f'CREATE TABLE main.new_structure(_structure_id INTEGER PRIMARY KEY, _granularity REAL, ' \
                f'{", ".join(new_structure_cols)})',
            f'INSERT INTO main.new_structure ' \
                f'SELECT _structure_id, _granularity, {", ".join(columns_to_keep)} FROM main.structure',
            'DROP TABLE main.structure',
            'ALTER TABLE main.new_structure RENAME TO structure',
        ]

        # Reconstruct associated indexes.
        statements.extend(_schema.sql_create_label_indexes(columns_to_keep))

        return statements

    def remove_index_columns(
        self, columns: Iterable[str], strategy: Strategy = 'preserve'
    ) -> None:
        # In versions earlier than SQLite 3.35.0, there was no support for
        # the DROP COLUMN command. This method (and other related methods
        # in the class) should implement the recommended, 12-step, ALTER
        # TABLE procedure detailed in the SQLite documentation:
        #     https://www.sqlite.org/lang_altertable.html#otheralter
        con = self._get_connection()
        try:
            con.execute('PRAGMA foreign_keys=OFF')
            cur = con.cursor()
            with _schema.savepoint(cur):
                self._remove_index_columns_execute_sql(cur, columns, strategy)

                cur.execute('PRAGMA main.foreign_key_check')
                one_result = cur.fetchone()
                if one_result:
                    msg = 'foreign key violations'
                    raise Exception(msg)
        finally:
            cur.close()
            con.execute('PRAGMA foreign_keys=ON')
            if con is not getattr(self, '_connection', None):
                con.close()


class DataAccessLayerPre25(DataAccessLayerPre35):
    """This is a subclass of DataAccessLayer that supports SQLite
    versions before 3.25.0 (2018-09-15).

    For full documentation, see DataAccessLayer.
    """
    @staticmethod
    def _rename_index_columns_make_sql(
        column_names: Sequence[str], new_column_names: Sequence[str]
    ) -> List[str]:
        # In SQLite versions before 3.25.0, there is no native support for the
        # RENAME COLUMN command. In these older versions of SQLite the tables
        # must be rebuilt. This method prepares a sequence of operations to
        # rebuild the table structures.
        new_labelindex_cols = [_schema.sql_column_def_labelindex_label(col) for col in new_column_names]
        new_location_cols = [_schema.sql_column_def_location_label(col) for col in new_column_names]
        new_structure_cols = [_schema.sql_column_def_structure_label(col) for col in new_column_names]
        statements = [
            # Rebuild 'label_index'.
            f'CREATE TABLE main.new_labelindex(index_id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
                f'{", ".join(new_labelindex_cols)})',
            f'INSERT INTO main.new_labelindex SELECT index_id, {", ".join(column_names)} FROM main.label_index',
            'DROP TABLE main.label_index',
            'ALTER TABLE main.new_labelindex RENAME TO label_index',

            # Rebuild 'location' table.
            f'CREATE TABLE main.new_location(_location_id INTEGER PRIMARY KEY, ' \
                f'{", ".join(new_location_cols)})',
            f'INSERT INTO main.new_location '
                f'SELECT _location_id, {", ".join(column_names)} FROM main.location',
            'DROP TABLE main.location',
            'ALTER TABLE main.new_location RENAME TO location',

            # Rebuild 'structure' table.
            f'CREATE TABLE main.new_structure(_structure_id INTEGER PRIMARY KEY, _granularity REAL, ' \
                f'{", ".join(new_structure_cols)})',
            f'INSERT INTO main.new_structure ' \
                f'SELECT _structure_id, _granularity, {", ".join(column_names)} FROM main.structure',
            'DROP TABLE main.structure',
            'ALTER TABLE main.new_structure RENAME TO structure',
        ]

        # Reconstruct associated indexes.
        statements.extend(
            _schema.sql_create_label_indexes(list(new_column_names))
        )

        return statements

    def rename_index_columns(
        self, mapper: Union[Callable[[str], str], Mapping[str, str]]
    ) -> None:
        # These related methods should implement the recommended, 12-step,
        # ALTER TABLE procedure detailed in the SQLite documentation:
        #     https://www.sqlite.org/lang_altertable.html#otheralter
        con = self._get_connection()
        try:
            con.execute('PRAGMA foreign_keys=OFF')
            cur = con.cursor()
            with _schema.savepoint(cur):
                names, new_names = self._rename_index_columns_apply_mapper(cur, mapper)
                for stmnt in self._rename_index_columns_make_sql(names, new_names):
                    cur.execute(stmnt)

                cur.execute('PRAGMA main.foreign_key_check')
                one_result = cur.fetchone()
                if one_result:
                    msg = 'foreign key violations'
                    raise Exception(msg)
        finally:
            cur.close()
            con.execute('PRAGMA foreign_keys=ON')
            if con is not getattr(self, '_connection', None):
                con.close()

    @classmethod
    def _disaggregate_make_sql(
        cls,
        normalized_columns: Sequence[str],
        bitmask: Sequence[Literal[0, 1]],
        match_selector_func: str,
        filter_attrs_func: Optional[str] = None,
    ) -> str:
        # In SQLite versions before 3.25.0, there is no support for "window
        # functions". Instead of using the "SUM(...) OVER (PARTITION BY ...)"
        # syntax, this implementation uses correlated subqueries to achieve
        # the same result.
        join_constraints = cls._disaggregate_make_sql_constraints(
            normalized_columns,
            bitmask,
            location_table_alias='t2',
            index_table_alias='t3',
        )

        subquery_join_constraints = cls._disaggregate_make_sql_constraints(
            normalized_columns,
            bitmask,
            location_table_alias='sub2',
            index_table_alias='sub3',
        )

        # Build WHERE clause if *filter_attrs_func* was given.
        if filter_attrs_func:
            where_clause = f'\n            WHERE {filter_attrs_func}(t1.attributes)=1'
        else:
            where_clause = ''

        statement = f"""
            SELECT
                t3.index_id,
                t1.attributes,
                t1.quantity_value * IFNULL(
                    (t4.weight_value / (
                        SELECT SUM(sub4.weight_value)
                        FROM main.quantity sub1
                        JOIN main.location sub2 USING (_location_id)
                        JOIN main.label_index sub3 ON ({subquery_join_constraints})
                        JOIN main.weight sub4 USING (index_id)
                        WHERE sub1.quantity_id=t1.quantity_id
                            AND sub4.weighting_id=t4.weighting_id
                    )),
                    (1.0 / (
                        SELECT COUNT(1)
                        FROM main.quantity sub1
                        JOIN main.location sub2 USING (_location_id)
                        JOIN main.label_index sub3 ON ({subquery_join_constraints})
                        WHERE sub1.quantity_id=t1.quantity_id
                    ))
                ) AS quantity_value
            FROM main.quantity t1
            JOIN main.location t2 USING (_location_id)
            JOIN main.label_index t3 ON ({join_constraints})
            JOIN main.weight t4 ON (
                t3.index_id=t4.index_id
                AND t4.weighting_id={match_selector_func}(t1.attributes)
            ){where_clause}
        """
        return statement

    @classmethod
    def _adaptive_disaggregate_make_sql(
        cls,
        normalized_columns: Sequence[str],
        bitmask: Sequence[Literal[0, 1]],
        match_selector_func: str,
        adaptive_weight_table: str,
        filter_attrs_func: Optional[str] = None,
        match_attrs_keys: Optional[Sequence[str]] = None,
    ) -> str:
        """Return SQL CTE statement to adaptively disaggregate data."""
        join_constraints = cls._disaggregate_make_sql_constraints(
            normalized_columns,
            bitmask,
            location_table_alias='t2',
            index_table_alias='t3',
        )

        subquery_join_constraints = cls._disaggregate_make_sql_constraints(
            normalized_columns,
            bitmask,
            location_table_alias='sub2',
            index_table_alias='sub3',
        )

        # Build WHERE clause if *filter_attrs_func* was given.
        if filter_attrs_func:
            where_clause = f'\n            WHERE {filter_attrs_func}(t1.attributes)=1'
        else:
            where_clause = ''

        # Build string of args to use in the application-defined
        # user_json_object_keep() SQL function.
        if match_attrs_keys:
            # Note: The leading comma is always needed for proper syntax.
            func = lambda x: f', {_schema.sql_string_literal(x)}'
            keys_to_keep = ''.join(func(x) for x in match_attrs_keys)
        else:
            keys_to_keep = ''

        # Build final SELECT statement.
        statement = f"""
            SELECT
                t3.index_id,
                t1.attributes,
                t1.quantity_value * COALESCE(
                    (COALESCE(t5.weight_value, 0.0) / (
                        SELECT SUM(sub4.weight_value)
                        FROM main.quantity sub1
                        JOIN main.location sub2 USING (_location_id)
                        JOIN main.label_index sub3 ON ({subquery_join_constraints})
                        LEFT JOIN (
                            SELECT
                                sub4sub.index_id,
                                user_json_object_keep(sub4sub.attributes{keys_to_keep}) AS attrs_subset,
                                SUM(sub4sub.quantity_value) AS weight_value
                            FROM {adaptive_weight_table} sub4sub
                            GROUP BY sub4sub.index_id, user_json_object_keep(sub4sub.attributes{keys_to_keep})
                        ) sub4 ON (
                            sub3.index_id=sub4.index_id
                            AND sub4.attrs_subset=user_json_object_keep(sub1.attributes{keys_to_keep})
                        )
                        WHERE sub1.quantity_id=t1.quantity_id
                    )),
                    (t4.weight_value / (
                        SELECT SUM(sub4.weight_value)
                        FROM main.quantity sub1
                        JOIN main.location sub2 USING (_location_id)
                        JOIN main.label_index sub3 ON ({subquery_join_constraints})
                        JOIN main.weight sub4 USING (index_id)
                        WHERE sub1.quantity_id=t1.quantity_id
                            AND sub4.weighting_id=t4.weighting_id
                    )),
                    (1.0 / (
                        SELECT COUNT(1)
                        FROM main.quantity sub1
                        JOIN main.location sub2 USING (_location_id)
                        JOIN main.label_index sub3 ON ({subquery_join_constraints})
                        WHERE sub1.quantity_id=t1.quantity_id
                    ))
                ) AS quantity_value
            FROM main.quantity t1
            JOIN main.location t2 USING (_location_id)
            JOIN main.label_index t3 ON ({join_constraints})
            JOIN main.weight t4 ON (
                t3.index_id=t4.index_id
                AND t4.weighting_id={match_selector_func}(t1.attributes)
            )
            LEFT JOIN (
                SELECT
                    t5sub.index_id,
                    user_json_object_keep(t5sub.attributes{keys_to_keep}) AS attrs_subset,
                    SUM(t5sub.quantity_value) AS weight_value
                FROM {adaptive_weight_table} t5sub
                GROUP BY t5sub.index_id, user_json_object_keep(t5sub.attributes{keys_to_keep})
            ) t5 ON (
                t3.index_id=t5.index_id
                AND t5.attrs_subset=user_json_object_keep(t1.attributes{keys_to_keep})
            ){where_clause}
            UNION ALL
            SELECT index_id, attributes, quantity_value FROM {adaptive_weight_table}
        """
        return statement


class DataAccessLayerPre24(DataAccessLayerPre25):
    """This is a subclass of DataAccessLayer that supports SQLite
    versions before 3.24.0 (2018-06-04).

    For full documentation, see DataAccessLayer.
    """
    @staticmethod
    def _set_data_property(
        cursor: sqlite3.Cursor, key: str, value: Any
    ) -> None:
        parameters: Tuple[str, ...]
        if value is not None:
            sql = 'INSERT OR REPLACE INTO main.property(key, value) VALUES (?, ?)'
            parameters = (key, _dumps(value, sort_keys=True))
        else:
            sql = 'DELETE FROM main.property WHERE key=?'
            parameters = (key,)

        cursor.execute(sql, parameters)


# Set the DataAccessLayer class appropriate for the current version of SQLite.
dal_class: Type[DataAccessLayer]

if _SQLITE_VERSION_INFO < (3, 24, 0):
    dal_class = DataAccessLayerPre24
elif _SQLITE_VERSION_INFO < (3, 25, 0):
    dal_class = DataAccessLayerPre25
elif _SQLITE_VERSION_INFO < (3, 35, 0):
    dal_class = DataAccessLayerPre35
else:
    dal_class = DataAccessLayer
