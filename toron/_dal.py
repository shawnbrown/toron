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
)
from json import dumps as _dumps
from ._typing import (
    Callable,
    Dict,
    Generator,
    Iterable,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Set,
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
from ._exceptions import ToronError
from ._exceptions import ToronWarning


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

    def to_file(self, path: PathType, fsync: bool = True):
        """Write node data to a file.

        .. code-block::

            >>> from toron._dal import dal_class
            >>> dal = dal_class()
            >>> ...
            >>> dal.to_file('mynode.toron')

        On Unix systems (e.g., Linux, macOS), calling with
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
            transaction_cm = _schema.begin
        elif method is None:
            transaction_cm = nullcontext  # No transaction handling.
        else:
            msg = f'unknown transaction method: {method!r}'
            raise ValueError(msg)

        if hasattr(self, '_connection'):
            # If using an in-memory database, use the persistent
            # connection and leave it open when finished.
            cur = self._connection.cursor()
            try:
                with transaction_cm(cur):
                    yield cur
            finally:
                cur.close()
        else:
            # If using an on-drive database, create a new
            # connection and close it when finished.
            filename = self.filename  # Assign locally to limit dot-lookups.
            if not filename:
                raise RuntimeError('expected filename, none found')
            con = _schema.get_connection(filename, self._required_permissions)
            cur = con.cursor()
            try:
                with transaction_cm(cur):
                    yield cur
            finally:
                cur.close()
                con.close()

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
    def _get_column_names(cursor, table):
        """Return a list of column names from the given table."""
        cursor.execute(f"PRAGMA main.table_info('{table}')")
        return [row[1] for row in cursor.fetchall()]

    @classmethod
    def _add_columns_make_sql(cls, cursor, columns):
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
    def _rename_columns_apply_mapper(cls, cursor, mapper):
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
            value_pairs = [f'{col}->{new}' for col, new in value_pairs]
            msg = f'column name collisions: {", ".join(value_pairs)}'
            raise ValueError(msg)

        return column_names, new_column_names

    @staticmethod
    def _rename_columns_make_sql(column_names, new_column_names):
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

    def rename_columns(self, mapper):
        # Rename columns using native RENAME COLUMN command (only for
        # SQLite 3.25.0 or newer).
        with self._transaction() as cur:
            names, new_names = self._rename_columns_apply_mapper(cur, mapper)
            for stmnt in self._rename_columns_make_sql(names, new_names):
                cur.execute(stmnt)

    @staticmethod
    def _remove_columns_make_sql(column_names, names_to_remove):
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

    @classmethod
    def _coarsen_records_make_sql(cls, cursor, remaining_columns):
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
        cls, cursor, columns, strategy: Strategy ='preserve'
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

            for stmnt in cls._coarsen_records_make_sql(cursor, names_remaining):
                cursor.execute(stmnt)

        # Clear `structure` table to prevent duplicates when removing columns.
        cursor.execute('DELETE FROM main.structure')

        # Remove specified columns.
        for stmnt in cls._remove_columns_make_sql(column_names, names_to_remove):
            cursor.execute(stmnt)

        # Rebuild categories property and structure table.
        cls._update_categories_and_structure(cursor, new_categories)

        # TODO: Recalculate node_hash for `properties` table.

    def remove_columns(self, columns, strategy: Strategy = 'preserve'):
        with self._transaction() as cur:
            self._remove_columns_execute_sql(cur, columns, strategy)

    @classmethod
    def _add_elements_make_sql(cls, cursor, columns):
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

    def add_elements(self, iterable, columns=None):
        iterator = iter(iterable)
        if not columns:
            columns = next(iterator)

        with self._transaction() as cur:
            # Get allowed columns and build selectors values.
            allowed_columns = self._get_column_names(cur, 'element')
            selectors = tuple((col in allowed_columns) for col in columns)

            # Filter column names and iterator rows to allowed columns.
            columns = compress(columns, selectors)
            iterator = (tuple(compress(row, selectors)) for row in iterator)

            sql = self._add_elements_make_sql(cur, columns)
            cur.executemany(sql, iterator)

    @staticmethod
    def _add_weights_get_new_id(cursor, name, selectors=None, description=None):
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
    def _add_weights_make_sql(cls, cursor, columns):
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
    def _add_weights_set_is_complete(cursor, weighting_id):
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

    def add_weights(self, iterable, columns=None, *, name, selectors, description=None):
        iterator = iter(iterable)
        if not columns:
            columns = tuple(next(iterator))

        try:
            weight_pos = columns.index(name)  # Get position of weight column.
        except ValueError:
            columns_string = ', '.join(repr(x) for x in columns)
            msg = f'Name {name!r} does not appear in columns: {columns_string}'
            raise ValueError(msg)

        with self._transaction() as cur:
            weighting_id = self._add_weights_get_new_id(cur, name, selectors, description)

            # Get allowed columns and build selectors values.
            allowed_columns = self._get_column_names(cur, 'element')
            selectors = tuple((col in allowed_columns) for col in columns)

            # Filter column names and iterator rows to allowed columns.
            columns = compress(columns, selectors)
            def mkrow(row):
                weightid_and_value = (weighting_id, row[weight_pos])
                element_labels = tuple(compress(row, selectors))
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
            more `attribute` columns, and one `value` column.
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
        # Normalize data as dict_rows.
        iter_data = iter(data)
        first_element = next(iter_data)
        if isinstance(first_element, Sequence):
            if not columns:
                columns = first_element
            else:
                iter_data = chain([first_element], iter_data)
            dict_rows = (dict(zip(columns, row)) for row in iter_data)
        elif isinstance(first_element, Mapping):
            dict_rows = chain([first_element], iter_data)  # type: ignore [assignment]
        else:
            msg = (f'data must contain mappings or sequences, '
                   f'got type {type(first_element)}')
            raise TypeError(msg)

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

    def get_raw_quantities(
        self, **where: str
    ) -> Iterable[Dict[str, Union[str, float]]]:
        """Get raw data quantities."""
        with self._transaction(method=None) as cur:
            label_cols = self._get_column_names(cur, 'location')[1:]
            normalized_cols = [_schema.normalize_identifier(x) for x in label_cols]

            where_items = {}
            filter_items = {}
            for key, val in where.items():
                normalized_key = _schema.normalize_identifier(key)
                if normalized_key in normalized_cols:
                    where_items[normalized_key] = val
                else:
                    filter_items[key] = val

            statement = f"""
                SELECT {', '.join(normalized_cols)}, attributes, value
                FROM main.quantity
                JOIN main.location USING (_location_id)
            """
            if where_items:
                conditions = [f'{x}=?' for x in where_items]
                parameters = tuple(where_items.values())
                statement = statement + f'WHERE {" AND ".join(conditions)}'
                cur.execute(statement, parameters)
            else:
                cur.execute(statement)

            if filter_items:
                def where_func(row_dict):
                    for k, v in filter_items.items():
                        if row_dict[k] != v:
                            return False
                    return True
            else:
                where_func = lambda x: True

            for row in cur:
                *labels, attr_dict, value = row  # Unpack row.
                row_dict = dict(zip(label_cols, labels))
                row_dict.update(attr_dict)
                row_dict['value'] = value
                if where_func(row_dict):
                    yield row_dict

    @staticmethod
    def _get_data_property(cursor, key):
        sql = 'SELECT value FROM main.property WHERE key=?'
        cursor.execute(sql, (key,))
        result = cursor.fetchone()
        return result[0] if result else None

    def get_data(self, keys):
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
    def _set_data_property(cursor, key, value):
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
    def _set_data_structure(cls, cursor, structure):
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
    def _update_categories_and_structure(cls, cursor, categories=None, *, minimize=True):
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

        list_of_lists = [list(cat) for cat in categories]
        cls._set_data_property(cursor, 'discrete_categories', list_of_lists)

        structure = make_structure(categories)
        cls._set_data_structure(cursor, structure)

    def set_data(self, mapping_or_items):
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

    def add_discrete_categories(self, discrete_categories):
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

    def remove_discrete_categories(self, discrete_categories):
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
    def _add_weights_get_new_id(cursor, name, selectors, description=None):
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
    def _remove_columns_make_sql(column_names, names_to_remove):
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

    def remove_columns(self, columns, strategy: Strategy ='preserve'):
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
    def _rename_columns_make_sql(column_names, new_column_names):
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
        statements.extend(_schema.sql_create_label_indexes(new_column_names))

        return statements

    def rename_columns(self, mapper):
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


class DataAccessLayerPre24(DataAccessLayerPre25):
    """This is a subclass of DataAccessLayer that supports SQLite
    versions before 3.24.0 (2018-06-04).

    For full documentation, see DataAccessLayer.
    """
    @staticmethod
    def _set_data_property(cursor, key, value):
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
