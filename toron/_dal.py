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
    _data_to_dict_rows,
)


if sys.platform != 'win32' and hasattr(fcntl, 'F_FULLFSYNC'):
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
    _best_effort_fsync = os.fsync


_SQLITE_VERSION_INFO = sqlite3.sqlite_version_info
_temp_files_to_delete_atexit: Set[str] = set()


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


atexit.register(_delete_leftover_temp_files)  # <- Register!.


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
            dir=dst_dirname,
            delete=False,
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

        # Move file to final path.
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

        If you want to make sure the file can be modified, you can
        require ``'readwrite'`` permissions. Use this mode with caution
        since changes are applied immediately to the file on drive and
        cannot be undone::

            >>> dal = DataAccessLayer.open('mynode.toron', required_permissions='readwrite')

        You can also open a node file without requiring any specific
        permissions::

            >>> dal = DataAccessLayer.open('mynode.toron', required_permissions=None)

        If you need to work on files that are too large to fit into
        memory but you don't want to risk damaging the original node,
        you can use ``from_file()`` with the ``cache_to_drive=True``
        option.
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
    def _add_columns_make_sql(
        cls, cursor: sqlite3.Cursor, columns: Iterable[str]
    ) -> List[str]:
        """Return a list of SQL statements for adding new label columns."""
        if isinstance(columns, str):
            columns = [columns]
        columns = [_schema.normalize_identifier(col) for col in columns]

        not_allowed = {'"element_id"', '"_location_id"', '"_structure_id"'}.intersection(columns)
        if not_allowed:
            msg = f"label name not allowed: {', '.join(not_allowed)}"
            raise ValueError(msg)

        current_cols = cls._get_column_names(cursor, 'element')
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
                f"ALTER TABLE main.element ADD COLUMN {_schema.sql_column_def_element_label(col)}",
                f"ALTER TABLE main.location ADD COLUMN {_schema.sql_column_def_location_label(col)}",
                f"ALTER TABLE main.structure ADD COLUMN {_schema.sql_column_def_structure_label(col)}",
            ])

        label_cols = current_cols[1:] + new_cols  # All columns except the id column.
        sql_stmnts.extend(_schema.sql_create_label_indexes(label_cols))

        return sql_stmnts

    @classmethod
    def _rename_columns_apply_mapper(
        cls,
        cursor: sqlite3.Cursor,
        mapper: Union[Callable[[str], str], Mapping[str, str]],
    ) -> Tuple[List[str], List[str]]:
        column_names = cls._get_column_names(cursor, 'element')
        column_names = column_names[1:]  # Slice-off 'element_id'.

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
    def _rename_columns_make_sql(
        column_names: Sequence[str], new_column_names: Sequence[str]
    ) -> List[str]:
        # The RENAME COLUMN command was added in SQLite 3.25.0 (2018-09-15).
        zipped = zip(column_names, new_column_names)
        rename_pairs = [(a, b) for a, b in zipped if a != b]

        sql_stmnts = []
        for name, new_name in rename_pairs:
            sql_stmnts.extend([
                f'ALTER TABLE main.element RENAME COLUMN {name} TO {new_name}',
                f'ALTER TABLE main.location RENAME COLUMN {name} TO {new_name}',
                f'ALTER TABLE main.structure RENAME COLUMN {name} TO {new_name}',
            ])
        return sql_stmnts

    def rename_columns(
        self, mapper: Union[Callable[[str], str], Mapping[str, str]]
    ) -> None:
        # Rename columns using native RENAME COLUMN command (only for
        # SQLite 3.25.0 or newer).
        with self._transaction() as cur:
            names, new_names = self._rename_columns_apply_mapper(cur, mapper)
            for stmnt in self._rename_columns_make_sql(names, new_names):
                cur.execute(stmnt)

    @staticmethod
    def _remove_columns_make_sql(
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
                f'ALTER TABLE main.element DROP COLUMN {col}',
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

        # Build a temporary table with old-to-new `element_id` mapping.
        sql_statements.append(f'''
            CREATE TEMPORARY TABLE old_to_new_element_id
            AS SELECT element_id, new_element_id
            FROM main.element
            JOIN (SELECT MIN(element_id) AS new_element_id, {formatted_names}
                  FROM main.element
                  GROUP BY {formatted_names}
                  HAVING COUNT(*) > 1)
            USING ({formatted_names})
        ''')

        # Add missing `weight.element_id` values needed for aggregation.
        sql_statements.append('''
            WITH
                MatchingRecords AS (
                    SELECT weighting_id, element_id, new_element_id
                    FROM main.weight
                    JOIN temp.old_to_new_element_id USING (element_id)
                ),
                MissingElements AS (
                    SELECT DISTINCT weighting_id, new_element_id FROM MatchingRecords
                    EXCEPT
                    SELECT DISTINCT weighting_id, element_id FROM MatchingRecords
                )
            INSERT INTO main.weight (weighting_id, element_id, value)
            SELECT weighting_id, new_element_id, 0
            FROM MissingElements
        ''')

        # Assign summed `value` to `weight` records being kept.
        if _SQLITE_VERSION_INFO >= (3, 33, 0):
            # The "UPDATE FROM" syntax was introduced in SQLite 3.33.0.
            sql_statements.append('''
                UPDATE main.weight
                SET value=summed_value
                FROM (SELECT weighting_id AS old_weighting_id,
                             new_element_id,
                             SUM(value) AS summed_value
                      FROM main.weight
                      JOIN temp.old_to_new_element_id USING (element_id)
                      GROUP BY weighting_id, new_element_id)
                WHERE weighting_id=old_weighting_id AND element_id=new_element_id
            ''')
        else:
            sql_statements.append('''
                WITH
                    SummedValues AS (
                        SELECT weighting_id, new_element_id, SUM(value) AS summed_value
                        FROM main.weight
                        JOIN temp.old_to_new_element_id USING (element_id)
                        GROUP BY weighting_id, new_element_id
                    ),
                    RecordsToUpdate AS (
                        SELECT weight_id AS record_id, summed_value
                        FROM main.weight a
                        JOIN SummedValues b
                        ON (a.weighting_id=b.weighting_id AND a.element_id=b.new_element_id)
                    )
                UPDATE main.weight
                SET value = (
                    SELECT summed_value
                    FROM RecordsToUpdate
                    WHERE weight_id=record_id
                )
                WHERE weight_id IN (SELECT record_id FROM RecordsToUpdate)
            ''')

        # Discard old `weight` records.
        sql_statements.append('''
            DELETE FROM main.weight
            WHERE element_id IN (
                SELECT element_id
                FROM temp.old_to_new_element_id
                WHERE element_id != new_element_id
            )
        ''')

        # TODO: Add missing `relation.element_id` values needed for aggregation.
        # TODO: Assign summed `proportion` to `relation` records being kept.
        # TODO: Discard old `relation` records.
        # TODO: Update `relation.mapping_level` codes.

        # Discard old `element` records.
        sql_statements.append('''
            DELETE FROM main.element
            WHERE element_id IN (
                SELECT element_id
                FROM temp.old_to_new_element_id
                WHERE element_id != new_element_id
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
                ElementCounts AS (
                    SELECT COUNT(*) AS element_count FROM main.element
                ),
                NewStatus AS (
                    SELECT
                        weighting_id AS record_id,
                        weight_count=element_count AS is_complete
                    FROM WeightCounts
                    CROSS JOIN ElementCounts
                )
            UPDATE main.weighting
            SET is_complete = (
                SELECT is_complete
                FROM NewStatus
                WHERE weighting_id=record_id
            )
            WHERE weighting_id IN (SELECT record_id FROM NewStatus)
        ''')

        # TODO: Update `is_complete` for incomplete `edge` records.

        # Remove old-to-new temporary table for `element_id` mapping.
        sql_statements.append('DROP TABLE temp.old_to_new_element_id')

        # TODO: Build a temporary table with old-to-new `location_id` mapping.
        # TODO: Add missing `quantity._location_id` values needed for aggregation.
        # TODO: Assign summed `value` to `quantity` records being kept.
        # TODO: Discard old `location` records.
        # TODO: Remove old-to-new temporary table for `location_id` mapping.

        return sql_statements

    @classmethod
    def _remove_columns_execute_sql(
        cls,
        cursor: sqlite3.Cursor,
        columns: Iterable[str],
        strategy: Strategy = 'preserve',
    ) -> None:
        column_names = cls._get_column_names(cursor, 'element')
        column_names = column_names[1:]  # Slice-off 'element_id'.

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
            FROM main.element
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
        for stmnt in cls._remove_columns_make_sql(column_names, names_to_remove):
            cursor.execute(stmnt)

        # Rebuild categories property and structure table.
        cls._update_categories_and_structure(cursor, new_categories)

        # TODO: Recalculate node_hash for `properties` table.

    def remove_columns(
        self, columns: Iterable[str], strategy: Strategy = 'preserve'
    ) -> None:
        with self._transaction() as cur:
            self._remove_columns_execute_sql(cur, columns, strategy)

    @classmethod
    def _add_elements_make_sql(
        cls, cursor: sqlite3.Cursor, columns: Iterable[str]
    ) -> str:
        """Return a SQL statement adding new element records (for use
        with an executemany() call.

        Example:

            >>> dal = DataAccessLayer(...)
            >>> dal._make_sql_new_elements(cursor, ['state', 'county'])
            'INSERT INTO element ("state", "county") VALUES (?, ?)'
        """
        columns = [_schema.normalize_identifier(col) for col in columns]

        existing_columns = cls._get_column_names(cursor, 'element')
        existing_columns = existing_columns[1:]  # Slice-off "element_id" column.
        existing_columns = [_schema.normalize_identifier(col) for col in existing_columns]

        invalid_columns = set(columns).difference(existing_columns)
        if invalid_columns:
            msg = f'invalid column name: {", ".join(invalid_columns)}'
            raise sqlite3.OperationalError(msg)

        columns_clause = ', '.join(columns)
        values_clause = ', '.join('?' * len(columns))
        return f'INSERT INTO main.element ({columns_clause}) VALUES ({values_clause})'

    def add_elements(
        self, iterable: Iterable[Sequence[str]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        iterator = iter(iterable)
        if not columns:
            columns = next(iterator)

        with self._transaction() as cur:
            # Get allowed columns and build selectors values.
            allowed_columns = self._get_column_names(cur, 'element')
            selectors = tuple((col in allowed_columns) for col in columns)

            # Filter column names and iterator rows to allowed columns.
            columns = tuple(compress(columns, selectors))
            iterator = (tuple(compress(row, selectors)) for row in iterator)

            sql = self._add_elements_make_sql(cur, columns)
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

        existing_columns = cls._get_column_names(cursor, 'element')
        existing_columns = [_schema.normalize_identifier(col) for col in existing_columns]

        invalid_columns = set(columns).difference(existing_columns)
        if invalid_columns:
            msg = f'invalid column name: {", ".join(invalid_columns)}'
            raise sqlite3.OperationalError(msg)

        where_clause = ' AND '.join(f'{col}=?' for col in columns)
        groupby_clause = ', '.join(columns)

        sql = f"""
            INSERT INTO main.weight (weighting_id, element_id, value)
            SELECT ? AS weighting_id, element_id, ? AS value
            FROM main.element
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
                                                       FROM main.element))
            WHERE weighting_id=?
        """
        cursor.execute(sql, (weighting_id, weighting_id))

    def add_weights(
        self,
        iterable: Iterable[Sequence[Union[str, float, int]]],
        columns: Optional[Sequence[str]] = None,
        *,
        name: str,
        selectors: Optional[Sequence[str]],
        description: Optional[str] = None,
    ) -> None:
        iterator = iter(iterable)
        if not columns:
            columns = tuple(next(iterator))  # type: ignore [arg-type]
            if not all(isinstance(x, str) for x in columns):
                msg = ''
                raise TypeError(msg)

        try:
            weight_pos = columns.index(name)  # Get position of weight column.
        except ValueError:
            columns_string = ', '.join(repr(x) for x in columns)
            msg = f'Name {name!r} does not appear in columns: {columns_string}'
            raise ValueError(msg)

        with self._transaction() as cur:
            weighting_id = self._add_weights_get_new_id(cur, name, selectors, description)

            # Get allowed columns and build bitmask selectors values.
            allowed_columns = self._get_column_names(cur, 'element')
            bitmask_selectors = tuple((col in allowed_columns) for col in columns)

            # Filter column names and iterator rows to allowed columns.
            columns = tuple(compress(columns, bitmask_selectors))
            def mkrow(row):
                weightid_and_value = (weighting_id, row[weight_pos])
                element_labels = tuple(compress(row, bitmask_selectors))
                return weightid_and_value + element_labels
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
        data: Union[Iterable[Mapping], Iterable[Sequence]],
        value: str,
        *,
        attributes: Optional[Iterable[str]] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        """Add quantities and associated attributes. Quantity values
        are automatically associated with matching element labels.

        Parameters
        ----------
        data : Iterable[Mapping] | Iterable[Sequence]
            Iterable of rows or dict-rows that contain the data to be
            loaded. Must contain one or more `element` columns, one or
            more `attribute` columns, and a single `value` column.
        value : str
            Name of column which contains the quantity values.
        attributes : Iterable[str], optional
            Name of columns which contain attributes. If not given,
            attributes will default to all non-element, non-value
            columns that don't begin with an underscore ('_').
        columns : Sequence[str], optional
            Optional sequence of data column names--must be given when
            *data* does not contain fieldname information.

        Load quantites with a header row::

            >>> data = [
            ...     ['elem1', 'elem2', 'attr1', 'attr2', 'quant'],
            ...     ['A', 'x', 'foo', 'corge', 12],
            ...     ['B', 'y', 'bar', 'qux', 10],
            ...     ['C', 'z', 'baz', 'quux', 15],
            ... ]
            >>> dal.add_quantities(data, 'quant')

        Load quantites using a specified *columns* argument::

            >>> data = [
            ...     ['A', 'x', 'foo', 'corge', 12],
            ...     ['B', 'y', 'bar', 'qux', 10],
            ...     ['C', 'z', 'baz', 'quux', 15],
            ... ]
            >>> dal.add_quantities(data, 'quant', columns=['elem1', 'elem2', 'attr1', 'attr2', 'quant'])

        Load quantites using a dictionary-rows::

            >>> data = [
            ...     {'elem1': 'A', 'elem2': 'x', 'attr1': 'foo', 'attr2': 'corge', 'quant': 12},
            ...     {'elem1': 'B', 'elem2': 'y', 'attr1': 'bar', 'attr2': 'qux', 'quant': 10},
            ...     {'elem1': 'C', 'elem2': 'z', 'attr1': 'baz', 'attr2': 'quux', 'quant': 15},
            ... ]
            >>> dal.add_quantities(data, 'counts')
        """
        dict_rows = _data_to_dict_rows(data, columns)

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
                        INSERT INTO main.quantity (_location_id, attributes, value)
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
        location_cols: List[str], where: Dict[str, str]
    ) -> Tuple[List[str], Tuple[str, ...], Optional[Callable[[Any], bool]]]:
        """Format arguments for get_raw_quantities() and
        delete_raw_quantities() methods.
        """
        normalized = [_schema.normalize_identifier(x) for x in location_cols]

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
            SELECT {', '.join(normalized)}, attributes, value
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
    def _disaggregate_make_sql_parts(
        columns: Sequence[str],
        bitmask: Sequence[Literal[0, 1]],
    ) -> Tuple[List[str], List[str], List[str]]:
        """Make SQL parts used in _disaggregate_make_sql() function.

        Returns a 3-tuple containing select items, join-using items,
        and where-clause items.

        The first item in the tuple should contain all of the label
        columns used in the element/location/structure tables.

        The second item in the tuple should contain only those labels
        that are selected by the bitmask.

        The third item in the tuple should contain WHERE clause
        conditions. The labels selected by the bitmask should be
        not-equal-to empty string and the items *not* selected by
        the bitmask should be equal-to empty string.

        .. code-block::

            >>> columns = ['A', 'B', 'C', 'D']
            >>> bitmask = [1, 0, 1, 0]
            >>> dal._disaggregate_make_sql_parts(columns, bitmask)
            (['"A"', '"B"', '"C"', '"D"'],
             ['"A"', '"C"'],
             ['"A"!=\'\'', '"B"=\'\'', '"C"!=\'\'', '"D"=\'\''])
        """
        # Strip trailing 0s from bitmask.
        bitmask = list(bitmask)
        try:
            while bitmask[-1] == 0:
                bitmask.pop()
        except IndexError:
            pass

        # Check that bitmask does not exceed columns.
        if len(bitmask) > len(columns):
            msg = (
                f'incompatible bitmask:\n'
                f'  columns = {columns}\n'
                f'  bitmask = {bitmask}'
            )
            raise ValueError(msg)

        # Build and return SQL parts for disaggregate query.
        select_items = [_schema.normalize_identifier(col) for col in columns]
        join_using_items = list(compress(select_items, bitmask))
        zipped = zip_longest(select_items, bitmask, fillvalue=0)
        where_clause_items = [f"{a}{'!=' if b else '='}''" for a, b in zipped]
        return (select_items, join_using_items, where_clause_items)

    @classmethod
    def _disaggregate_make_sql(
        cls,
        columns: Sequence[str],
        bitmask: Sequence[Literal[0, 1]],
        match_selector_func: str,
    ) -> str:
        """Return SQL to disaggregate data."""
        select_items, join_using_items, where_clause_items = \
            cls._disaggregate_make_sql_parts(columns, bitmask)

        if join_using_items:
            element_join_constraint = f"USING ({', '.join(join_using_items)})"
        else:
            element_join_constraint = 'ON TRUE'

        statement = f"""
            SELECT
                t3.element_id,
                {', '.join(f't3.{x}' for x in select_items)},
                t1.attributes,
                t1.value * IFNULL(
                    (t4.value / SUM(t4.value) OVER (PARTITION BY t1.quantity_id)),
                    (1.0 / COUNT(1) OVER (PARTITION BY t1.quantity_id))
                ) AS value
            FROM main.quantity t1
            JOIN main.location t2 USING (_location_id)
            JOIN main.element t3 {element_join_constraint}
            JOIN main.weight t4 ON (
                t3.element_id=t4.element_id
                AND t4.weighting_id={match_selector_func}(t1.attributes)
            )
            WHERE {' AND '.join(f't2.{x}' for x in where_clause_items)}
        """
        return statement

    def disaggregate(self) -> Generator[Dict[str, Union[str, float]], None, None]:
        """Return a generator that yields disaggregated quantities."""
        with self._transaction(method=None) as cur:
            # Prepare weighting_id matcher function.
            cur.execute("""
                SELECT weighting_id, selectors
                FROM main.weighting
                WHERE is_complete=1
            """)
            match_weighting_id = GetMatchingKey(cur.fetchall(), default=1)
            func_name = _schema.get_userfunc(cur, match_weighting_id)

            # Get bitmask levels from structure table.
            columns = self._get_column_names(cur, 'location')[1:]
            bitmasks = cur.execute('SELECT * FROM main.structure').fetchall()

            # Build SQL statement.
            sql_statements = []
            for row in bitmasks:
                bitmask = row[1:]  # Slice-off the id value.
                sql = self._disaggregate_make_sql(columns, bitmask, func_name)
                sql_statements.append(sql)

            final_sql = '\n            UNION ALL\n'.join(sql_statements)

            # Execute SQL and yield result rows.
            cur.execute(final_sql)
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
                if key == 'column_names':
                    cur.execute("PRAGMA main.table_info('element')")
                    names = [row[1] for row in cur.fetchall()]
                    data[key] = names[1:]  # Slice-off element_id.
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
            whole_space = set(cls._get_column_names(cursor, 'element')[1:])
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

        # Bring 'add_columns' action to the front of the list (it
        # should be processed first).
        items = sorted(items, key=lambda item: item[0] != 'add_columns')

        with self._transaction() as cur:
            for key, value in items:
                if key == 'discrete_categories':
                    self._set_data_property(cur, key, [list(cat) for cat in value])
                elif key == 'structure':
                    self._set_data_structure(cur, value)
                elif key == 'add_columns':
                    for stmnt in self._add_columns_make_sql(cur, value):
                        cur.execute(stmnt)
                    self._update_categories_and_structure(cur)
                else:
                    msg = f"can't set value for {key!r}"
                    raise ToronError(msg)

    def add_discrete_categories(
        self, discrete_categories: Iterable[Set[str]]
    ) -> None:
        data = self.get_data(['discrete_categories', 'column_names'])
        minimized = minimize_discrete_categories(
            data['discrete_categories'],
            discrete_categories,
            [set(data['column_names'])],
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
        data = self.get_data(['discrete_categories', 'column_names'])
        current_cats = data['discrete_categories']
        mandatory_cat = set(data['column_names'])

        if mandatory_cat in discrete_categories:
            import warnings
            formatted = ', '.join(repr(x) for x in data['column_names'])
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
    def _remove_columns_make_sql(
        column_names: Sequence[str], names_to_remove: Sequence[str]
    ) -> List[str]:
        """Return a list of SQL statements for removing label columns."""
        # In SQLite versions before 3.35.0, there is no native support for the
        # DROP COLUMN command. In these older versions of SQLite the tables
        # must be rebuilt. This method prepares a sequence of operations to
        # rebuild the table structures.
        columns_to_keep = [col for col in column_names if col not in names_to_remove]
        new_element_cols = [_schema.sql_column_def_element_label(col) for col in columns_to_keep]
        new_location_cols = [_schema.sql_column_def_location_label(col) for col in columns_to_keep]
        new_structure_cols = [_schema.sql_column_def_structure_label(col) for col in columns_to_keep]

        statements = [
            # Rebuild 'element' table.
            f'CREATE TABLE main.new_element(element_id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
                f'{", ".join(new_element_cols)})',
            f'INSERT INTO main.new_element SELECT element_id, {", ".join(columns_to_keep)} FROM main.element',
            'DROP TABLE main.element',
            'ALTER TABLE main.new_element RENAME TO element',

            # Rebuild 'location' table.
            f'CREATE TABLE main.new_location(_location_id INTEGER PRIMARY KEY, ' \
                f'{", ".join(new_location_cols)})',
            f'INSERT INTO main.new_location '
                f'SELECT _location_id, {", ".join(columns_to_keep)} FROM main.location',
            'DROP TABLE main.location',
            'ALTER TABLE main.new_location RENAME TO location',

            # Rebuild 'structure' table.
            f'CREATE TABLE main.new_structure(_structure_id INTEGER PRIMARY KEY, ' \
                f'{", ".join(new_structure_cols)})',
            f'INSERT INTO main.new_structure ' \
                f'SELECT _structure_id, {", ".join(columns_to_keep)} FROM main.structure',
            'DROP TABLE main.structure',
            'ALTER TABLE main.new_structure RENAME TO structure',
        ]

        # Reconstruct associated indexes.
        statements.extend(_schema.sql_create_label_indexes(columns_to_keep))

        return statements

    def remove_columns(
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
                self._remove_columns_execute_sql(cur, columns, strategy)

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
    def _rename_columns_make_sql(
        column_names: Sequence[str], new_column_names: Sequence[str]
    ) -> List[str]:
        # In SQLite versions before 3.25.0, there is no native support for the
        # RENAME COLUMN command. In these older versions of SQLite the tables
        # must be rebuilt. This method prepares a sequence of operations to
        # rebuild the table structures.
        new_element_cols = [_schema.sql_column_def_element_label(col) for col in new_column_names]
        new_location_cols = [_schema.sql_column_def_location_label(col) for col in new_column_names]
        new_structure_cols = [_schema.sql_column_def_structure_label(col) for col in new_column_names]
        statements = [
            # Rebuild 'element' table.
            f'CREATE TABLE main.new_element(element_id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
                f'{", ".join(new_element_cols)})',
            f'INSERT INTO main.new_element SELECT element_id, {", ".join(column_names)} FROM main.element',
            'DROP TABLE main.element',
            'ALTER TABLE main.new_element RENAME TO element',

            # Rebuild 'location' table.
            f'CREATE TABLE main.new_location(_location_id INTEGER PRIMARY KEY, ' \
                f'{", ".join(new_location_cols)})',
            f'INSERT INTO main.new_location '
                f'SELECT _location_id, {", ".join(column_names)} FROM main.location',
            'DROP TABLE main.location',
            'ALTER TABLE main.new_location RENAME TO location',

            # Rebuild 'structure' table.
            f'CREATE TABLE main.new_structure(_structure_id INTEGER PRIMARY KEY, ' \
                f'{", ".join(new_structure_cols)})',
            f'INSERT INTO main.new_structure ' \
                f'SELECT _structure_id, {", ".join(column_names)} FROM main.structure',
            'DROP TABLE main.structure',
            'ALTER TABLE main.new_structure RENAME TO structure',
        ]

        # Reconstruct associated indexes.
        statements.extend(
            _schema.sql_create_label_indexes(list(new_column_names))
        )

        return statements

    def rename_columns(
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
                names, new_names = self._rename_columns_apply_mapper(cur, mapper)
                for stmnt in self._rename_columns_make_sql(names, new_names):
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
        columns: Sequence[str],
        bitmask: Sequence[Literal[0, 1]],
        match_selector_func: str,
    ) -> str:
        # In SQLite versions before 3.25.0, there is no support for "window
        # functions". Instead of using the "SUM(...) OVER (PARTITION BY ...)"
        # syntax, this implementation uses a correlated subquery to achieve
        # the same result.
        select_items, join_using_items, where_clause_items = \
            cls._disaggregate_make_sql_parts(columns, bitmask)

        if join_using_items:
            element_join_constraint = f"USING ({', '.join(join_using_items)})"
        else:
            element_join_constraint = 'ON 1'

        statement = f"""
            SELECT
                t3.element_id,
                {', '.join(f't3.{x}' for x in select_items)},
                t1.attributes,
                t1.value * IFNULL(
                    (t4.value / (SELECT SUM(sub4.value)
                                 FROM main.quantity sub1
                                 JOIN main.location sub2 USING (_location_id)
                                 JOIN main.element sub3 {element_join_constraint}
                                 JOIN main.weight sub4 USING (element_id)
                                 WHERE sub1.quantity_id=t1.quantity_id
                                       AND sub4.weighting_id=t4.weighting_id)),
                    (1.0 / (SELECT COUNT(1)
                            FROM main.quantity sub1
                            JOIN main.location sub2 USING (_location_id)
                            JOIN main.element sub3 {element_join_constraint}
                            JOIN main.weight sub4 USING (element_id)
                            WHERE sub1.quantity_id=t1.quantity_id
                                  AND sub4.weighting_id=t4.weighting_id))
                ) AS value
            FROM main.quantity t1
            JOIN main.location t2 USING (_location_id)
            JOIN main.element t3 {element_join_constraint}
            JOIN main.weight t4 ON (
                t3.element_id=t4.element_id
                AND t4.weighting_id={match_selector_func}(t1.attributes)
            )
            WHERE {' AND '.join(f't2.{x}' for x in where_clause_items)}
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
