"""Data access layer to interact with Toron node files."""

import atexit
import os
import sqlite3
import sys
import tempfile
import uuid
from collections import (
    Counter,
    defaultdict,
)
from contextlib import contextmanager, nullcontext
from itertools import (
    chain,
    compress,
    groupby,
    zip_longest,
)
from json import dumps as _dumps
from json import loads as _loads
from .selectors import (
    CompoundSelector,
    SimpleSelector,
    accepts_json_input,
    parse_selector,
    GetMatchingKey,
)
from ._typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    Iterator,
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
from .categories import make_structure
from .categories import minimize_discrete_categories
from ._xmapper import xMapper
from ._utils import (
    ToronError,
    ToronWarning,
    TabularData,
    make_readerlike,
    make_dictreaderlike,
    make_hash,
    eagerly_initialize,
    NOVALUE,
    XQuantityIterator,
)
from ._schema import BitFlags2


NoValueType: TypeAlias = NOVALUE.__class__


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
    # The absolute path of the file where the node data was loaded from
    # or was most recently saved to (if any). If the node was not loaded
    # from a file and has never been explicitly saved to a file, then
    # this attribute should be None.
    _absolute_data_source: Optional[str] = None

    # The absolute path of the file where the node instance's data is
    # currently stored (if any). This attribute should only be populated
    # when the instance's data is located on drive. This happens when the
    # node is opened directly from the drive or when ``cache_to_drive=True``
    # is used. If the node data is located in memory, this attribute should
    # be None.
    _absolute_working_path: Optional[str] = None

    # NOTE: To be clear, the above attributes ``_absolute_data_source``
    # and ``_absolute_working_path`` are independent. If node data is
    # loaded from a file into memory, then it should have a "data source"
    # but since the active database is running in memory, it should not
    # have a "working path".

    _required_permissions: _schema.RequiredPermissions
    _cleanup_item: Optional[Union[str, sqlite3.Connection]]
    _unique_id: Optional[str] = None

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

        self._unique_id = str(uuid.uuid4())  # UUID 4 for most random value.
        self._set_data_property(con.cursor(), 'unique_id', self._unique_id)

        # Assign object attributes.
        if cache_to_drive:
            con.close()  # Close on-drive connection (only open when accessed).
            self._absolute_data_source = None
            self._absolute_working_path = target_path
            self._required_permissions = 'readwrite'
            self._cleanup_item = target_path
        else:
            self._absolute_data_source = None
            self._absolute_working_path = None
            self._connection = con  # Keep connection open (in-memory database
                                    # is discarded once closed).
            self._required_permissions = None
            self._cleanup_item = con

    @property
    def unique_id(self):
        """Unique identifier for the node object."""
        if not self._unique_id:
            with self._transaction(method=None) as cur:
                self._unique_id = self._get_data_property(cur, 'unique_id')
        return self._unique_id

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
        source_path = os.path.abspath(os.fsdecode(path))
        source_con = _schema.get_raw_connection(source_path, access_mode='ro')

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
            obj._absolute_data_source = source_path
            obj._absolute_working_path = target_path
            obj._required_permissions = 'readwrite'
            obj._cleanup_item = target_path
        else:
            obj._absolute_data_source = source_path
            obj._absolute_working_path = None
            obj._connection = target_con
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
        obj._absolute_data_source = path
        obj._absolute_working_path = path
        obj._required_permissions = required_permissions
        obj._cleanup_item = None
        return obj

    @property
    def data_source(self) -> Optional[str]:
        """A relative file path to the data source for this node."""
        if self._absolute_data_source:
            return os.path.relpath(self._absolute_data_source)
        return None

    def _get_connection(self) -> sqlite3.Connection:
        if hasattr(self, '_connection'):
            return self._connection
        if self._absolute_working_path:
            return _schema.get_connection(self._absolute_working_path, self._required_permissions)
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

        # Get connection and open a new cursor..
        if hasattr(self, '_connection'):  # Access in-memory database.
            con = self._connection
        elif self._absolute_working_path:  # Load on-drive database.
            con = _schema.get_connection(
                self._absolute_working_path,
                self._required_permissions,
            )
        else:
            msg = (f"{self} should have an '_absolute_working_path' or "
                   f"a '_connection' attribute but neither was found")
            raise RuntimeError(msg)
        cur = con.cursor()

        # Yield the cursor object and clean-up when finished.
        try:
            with transaction_cm(cur):
                yield cur
        finally:
            try:
                cur.close()
            except sqlite3.ProgrammingError as err:
                if str(err) == 'Cannot operate on a closed database.':
                    pass  # SQLite raises this error in PyPy, ignore it.
                else:
                    raise

            if self._absolute_working_path:
                con.close()  # Close connection if database is on-drive.

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
        """Return a list of SQL statements for adding new index columns."""
        if isinstance(columns, str):
            columns = [columns]
        columns = [_schema.normalize_identifier(col) for col in columns]

        not_allowed = {'"index_id"', '"_location_id"', '"_structure_id"'}.intersection(columns)
        if not_allowed:
            msg = f"column name not allowed: {', '.join(not_allowed)}"
            raise ValueError(msg)

        current_cols = cls._get_column_names(cursor, 'node_index')
        current_cols = [_schema.normalize_identifier(col) for col in current_cols]
        new_cols = [col for col in columns if col not in current_cols]

        if not new_cols:
            return []  # <- EXIT!

        dupes = [obj for obj, count in Counter(new_cols).items() if count > 1]
        if dupes:
            msg = f"duplicate column name: {', '.join(dupes)}"
            raise ValueError(msg)

        sql_stmnts = []

        sql_stmnts.extend(_schema.sql_drop_label_column_indexes())

        for col in new_cols:
            sql_stmnts.extend([
                f"ALTER TABLE main.node_index ADD COLUMN {_schema.sql_column_def_nodeindex_label(col)}",
                f"ALTER TABLE main.location ADD COLUMN {_schema.sql_column_def_location_label(col)}",
                f"ALTER TABLE main.structure ADD COLUMN {_schema.sql_column_def_structure_label(col)}",
            ])

        label_cols = current_cols[1:] + new_cols  # All columns except the id column.
        sql_stmnts.extend(_schema.sql_create_node_indexes(label_cols))

        return sql_stmnts

    @classmethod
    def _rename_index_columns_apply_mapper(
        cls,
        cursor: sqlite3.Cursor,
        mapper: Union[Callable[[str], str], Mapping[str, str]],
    ) -> Tuple[List[str], List[str]]:
        column_names = cls._get_column_names(cursor, 'node_index')
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
                f'ALTER TABLE main.node_index RENAME COLUMN {name} TO {new_name}',
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
        """Return a list of SQL statements for removing index columns."""
        names_to_remove = [col for col in names_to_remove if col in column_names]

        if not names_to_remove:
            return []  # <- EXIT!

        sql_stmnts = []

        sql_stmnts.extend(_schema.sql_drop_label_column_indexes())

        for col in names_to_remove:
            sql_stmnts.extend([
                f'ALTER TABLE main.node_index DROP COLUMN {col}',
                f'ALTER TABLE main.location DROP COLUMN {col}',
                f'ALTER TABLE main.structure DROP COLUMN {col}',
            ])

        remaining_cols = [col for col in column_names if col not in names_to_remove]
        sql_stmnts.extend(_schema.sql_create_node_indexes(remaining_cols))

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
        # Consolidate records in `node_index` and `weight` tables.
        ################################################################

        # Build a temporary table with old-to-new `index_id` mapping.
        sql_statements.append(f'''
            CREATE TEMPORARY TABLE old_to_new_index_id
            AS SELECT index_id, new_index_id
            FROM main.node_index
            JOIN (SELECT MIN(index_id) AS new_index_id, {formatted_names}
                  FROM main.node_index
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
                SET weight_value=c.summed_weight_value
                FROM (
                    SELECT a.weighting_id,
                           b.new_index_id,
                           SUM(a.weight_value) AS summed_weight_value
                      FROM main.weight a
                      JOIN temp.old_to_new_index_id b USING (index_id)
                      GROUP BY a.weighting_id, b.new_index_id
                ) AS c
                WHERE main.weight.weighting_id=c.weighting_id
                      AND main.weight.index_id=c.new_index_id
            ''')
        else:
            sql_statements.append('''
                WITH
                    SummedValues AS (
                        SELECT a.weighting_id, b.new_index_id, SUM(a.weight_value) AS summed_weight_value
                        FROM main.weight a
                        JOIN temp.old_to_new_index_id b USING (index_id)
                        GROUP BY a.weighting_id, b.new_index_id
                    ),
                    RecordsToUpdate AS (
                        SELECT a.weight_id AS record_id, b.summed_weight_value
                        FROM main.weight a
                        JOIN SummedValues b
                        ON (a.weighting_id=b.weighting_id AND a.index_id=b.new_index_id)
                    )
                UPDATE main.weight
                SET weight_value = (
                    SELECT summed_weight_value
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

        # Add any missing `edge_id`, `other_index_id`, `mapping_level`
        # and `index_id` values needed for aggregation to the `relation`
        # table. This is necessary because not every `other_index_id`
        # is guaranteed to have every possible combination of `index_id`
        # and `mapping_level` values. And the coarsening process may
        # aggregate records using a combination of `index_id` and
        # `mapping_level` values that are not currently defined in the
        # `relation` table.
        sql_statements.append('''
            WITH
                MatchingRecords AS (
                    SELECT edge_id, other_index_id, mapping_level, index_id, new_index_id
                    FROM main.relation
                    JOIN temp.old_to_new_index_id USING (index_id)
                ),
                MissingIDs AS (
                    SELECT DISTINCT edge_id, other_index_id, mapping_level, new_index_id FROM MatchingRecords
                    EXCEPT
                    SELECT DISTINCT edge_id, other_index_id, mapping_level, index_id FROM MatchingRecords
                )
            INSERT INTO main.relation (edge_id, other_index_id, mapping_level, index_id, relation_value, proportion)
            SELECT edge_id, other_index_id, mapping_level, new_index_id, 0.0, 0.0
            FROM MissingIDs
        ''')

        # Assign summed values and proportions to `relation` records being kept.
        if _SQLITE_VERSION_INFO >= (3, 33, 0):
            # The "UPDATE FROM" syntax was introduced in SQLite 3.33.0.
            sql_statements.append('''
                UPDATE main.relation
                SET relation_value=c.summed_relation_value,
                    proportion=c.summed_proportion
                FROM (
                    SELECT a.edge_id,
                           a.other_index_id,
                           b.new_index_id,
                           SUM(a.relation_value) AS summed_relation_value,
                           SUM(a.proportion) AS summed_proportion,
                           a.mapping_level
                    FROM main.relation a
                    JOIN temp.old_to_new_index_id b USING (index_id)
                    GROUP BY a.edge_id, a.other_index_id, b.new_index_id, a.mapping_level
                ) AS c
                WHERE main.relation.edge_id=c.edge_id
                    AND main.relation.other_index_id=c.other_index_id
                    AND main.relation.index_id=c.new_index_id
                    AND main.relation.mapping_level IS c.mapping_level
            ''')
        else:
            sql_statements.append('''
                WITH
                    SummedValues AS (
                        SELECT a.edge_id,
                               a.other_index_id,
                               b.new_index_id,
                               SUM(a.relation_value) AS summed_relation_value,
                               SUM(a.proportion) AS summed_proportion,
                               a.mapping_level
                        FROM main.relation a
                        JOIN temp.old_to_new_index_id b ON (a.index_id=b.index_id)
                        GROUP BY a.edge_id, a.other_index_id, b.new_index_id, a.mapping_level
                    ),
                    RecordsToUpdate AS (
                        SELECT a.relation_id AS record_id,
                               b.summed_relation_value,
                               b.summed_proportion
                        FROM main.relation a
                        JOIN SummedValues b ON (a.edge_id=b.edge_id
                                                AND a.other_index_id=b.other_index_id
                                                AND a.index_id=b.new_index_id
                                                AND a.mapping_level IS b.mapping_level)
                    )
                UPDATE main.relation
                SET relation_value = (
                        SELECT summed_relation_value
                        FROM RecordsToUpdate
                        WHERE relation_id=record_id
                    ),
                    proportion = (
                        SELECT summed_proportion
                        FROM RecordsToUpdate
                        WHERE relation_id=record_id
                    )
                WHERE relation_id IN (SELECT record_id FROM RecordsToUpdate)
            ''')

        # Discard old `relation` records.
        sql_statements.append('''
            DELETE FROM main.relation
            WHERE index_id IN (
                SELECT index_id
                FROM temp.old_to_new_index_id
                WHERE index_id != new_index_id
            )
        ''')

        # Discard old `node_index` records.
        sql_statements.append('''
            DELETE FROM main.node_index
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
                    SELECT COUNT(*) AS index_count
                    FROM main.node_index
                    WHERE index_id > 0  /* <- exclude undefined record */
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

        # Update `is_locally_complete` for incomplete `edge` records.
        sql_statements.append('''
            WITH
                EdgeCounts AS (
                    SELECT edge_id, COUNT(DISTINCT index_id) AS mapped_count
                    FROM main.edge
                    JOIN main.relation USING (edge_id)
                    WHERE is_locally_complete=0
                    GROUP BY edge_id
                ),
                IndexCounts AS (
                    SELECT COUNT(*) AS index_count
                    FROM main.node_index
                ),
                NewStatus AS (
                    SELECT
                        edge_id AS record_id,
                        mapped_count=index_count AS is_locally_complete
                    FROM EdgeCounts
                    CROSS JOIN IndexCounts
                )
            UPDATE main.edge
            SET is_locally_complete = (
                SELECT is_locally_complete
                FROM NewStatus
                WHERE edge_id=record_id
            )
            WHERE edge_id IN (SELECT record_id FROM NewStatus)
        ''')

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
                    SELECT attribute_id, _location_id, new_location_id
                    FROM main.quantity
                    JOIN temp.old_to_new_location_id USING (_location_id)
                ),
                MissingAttributes AS (
                    SELECT DISTINCT attribute_id, new_location_id FROM MatchingRecords
                    EXCEPT
                    SELECT DISTINCT attribute_id, _location_id FROM MatchingRecords
                )
            INSERT INTO main.quantity (attribute_id, _location_id, quantity_value)
            SELECT attribute_id, new_location_id, 0
            FROM MissingAttributes;
        ''')

        # Assign summed `quantity_value` to `quantity` records being kept.
        if _SQLITE_VERSION_INFO >= (3, 33, 0):
            # The "UPDATE FROM" syntax was introduced in SQLite 3.33.0.
            sql_statements.append('''
                UPDATE main.quantity
                SET quantity_value=c.summed_value
                FROM (
                    SELECT a.attribute_id,
                           b.new_location_id,
                           SUM(a.quantity_value) AS summed_value
                    FROM main.quantity a
                    JOIN temp.old_to_new_location_id b USING (_location_id)
                    GROUP BY a.attribute_id, b.new_location_id
                ) AS c
                WHERE main.quantity.attribute_id=c.attribute_id
                      AND _location_id=c.new_location_id
            ''')
        else:
            sql_statements.append('''
                WITH
                    SummedValues AS (
                        SELECT a.attribute_id, b.new_location_id, SUM(a.quantity_value) AS summed_quantity_value
                        FROM main.quantity a
                        JOIN temp.old_to_new_location_id b USING (_location_id)
                        GROUP BY a.attribute_id, b.new_location_id
                    ),
                    RecordsToUpdate AS (
                        SELECT a.quantity_id AS record_id, b.summed_quantity_value
                        FROM main.quantity a
                        JOIN SummedValues b ON (a.attribute_id=b.attribute_id
                                                AND a._location_id=b.new_location_id)
                    )
                UPDATE main.quantity
                SET quantity_value = (
                    SELECT summed_quantity_value
                    FROM RecordsToUpdate
                    WHERE quantity_id=record_id
                )
                WHERE quantity_id IN (SELECT record_id FROM RecordsToUpdate)
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

    def _remove_index_columns_execute_sql(
        self,
        cursor: sqlite3.Cursor,
        columns: Iterable[str],
        *,
        preserve_structure: bool,
        preserve_granularity: bool,
        preserve_edges: bool,
        #match_limit: Optional[Union[int, float]] = 1,
    ) -> None:
        column_names = self._get_column_names(cursor, 'node_index')
        column_names = column_names[1:]  # Slice-off 'index_id'.

        names_to_remove = sorted(set(columns).intersection(column_names))
        if not names_to_remove:
            return  # <- EXIT!

        names_remaining = [col for col in column_names if col not in columns]

        categories = self._get_data_property(cursor, 'discrete_categories') or []
        categories = [set(cat) for cat in categories]
        cats_filtered = [cat for cat in categories if not cat.intersection(columns)]

        # Check for a loss of category coverage.
        cols_uncovered = set(names_remaining).difference(chain(*cats_filtered))
        if cols_uncovered:
            if preserve_structure:
                formatted = ', '.join(repr(x) for x in sorted(cols_uncovered))
                msg = f'cannot remove, categories are undefined for remaining columns: {formatted}'
                raise ToronError(msg)

            new_categories = []
            for cat in categories:
                cat = cat.difference(names_to_remove)
                if cat and (cat not in new_categories):
                    new_categories.append(cat)
        else:
            new_categories = cats_filtered

        # Check for a loss of granularity and coarsen if appropriate.
        cursor.execute(f'''
            SELECT 1
            FROM main.node_index
            GROUP BY {", ".join(names_remaining)}
            HAVING COUNT(*) > 1
        ''')
        if cursor.fetchone() is not None:
            if preserve_granularity:
                msg = 'cannot remove, columns are needed to preserve granularity'
                raise ToronError(msg)

            for stmnt in self._coarsen_records_make_sql(names_remaining):
                cursor.execute(stmnt)

            self._refresh_index_hash(cursor)

        # Clear `structure` table to prevent duplicates when removing columns.
        cursor.execute('DELETE FROM main.structure')

        # Remove specified columns.
        for stmnt in self._remove_index_columns_make_sql(column_names, names_to_remove):
            cursor.execute(stmnt)

        # Rebuild categories property and structure table.
        self._update_categories_and_structure(cursor, new_categories)

        # Get old mapping_level values.
        cursor.execute("""
            SELECT DISTINCT mapping_level
            FROM main.relation
            WHERE mapping_level IS NOT NULL
        """)
        old_mapping_levels = [x[0] for x in cursor]  # Unwrap single-item result.

        # Make new mapping levels for remaining columns.
        selectors = tuple((col in names_remaining) for col in column_names)
        new_mapping_levels = [
            BitFlags2(compress(x, selectors)) for x in old_mapping_levels
        ]

        # Get the set of mapping levels allowed by the new structure.
        cursor.execute('SELECT * FROM main.structure')
        allowed_levels = {BitFlags2(row[2:]) for row in cursor}
        allowed_levels.remove(BitFlags2())  # All 0s mapping_level not allowed.

        # Find mapping levels that would become unrepresentable.
        unrepresentable = []
        for old_level, new_level in zip(old_mapping_levels, new_mapping_levels):
            if new_level not in allowed_levels:
                unrepresentable.append(old_level)

        if unrepresentable:
            if preserve_edges:
                # Raise error, cancelling column removal.
                def func(old_level):
                    old_names = compress(column_names, old_level)
                    old_names = (repr(name) for name in old_names)
                    return f"  * {', '.join(old_names)}"

                msg_levels = '\n'.join(func(x) for x in unrepresentable)
                raise ToronError(
                    f'cannot remove; columns are needed to preserve ambiguous '
                    f'relations that use the following levels of granularity:\n\n'
                    f'{msg_levels}\n\nTo remove columns, reify the edges or use '
                    f'`preserve_edges=False` to delete unrepresentable relations.'
                )
            else:
                # Delete unrepresentable relations and continue.
                cursor.executemany(
                    'DELETE FROM main.relation WHERE mapping_level=?',
                    ([old_level] for old_level in unrepresentable)
                )

        # Update mapping_level values.
        parameters: Iterable[Tuple[BitFlags2, BitFlags2]]
        parameters = zip(new_mapping_levels, old_mapping_levels)
        parameters = ((a, b) for (a, b) in parameters if a != b)
        cursor.executemany(
            'UPDATE main.relation SET mapping_level=? WHERE mapping_level=?',
            parameters,
        )

        # Get edges that contain ambiguous relations.
        cursor.execute("""
            SELECT *
            FROM main.edge
            WHERE edge_id IN (
                SELECT DISTINCT edge_id
                FROM main.relation
                WHERE mapping_level IS NOT NULL
            )
        """)
        colnames = tuple(x[0] for x in cursor.description)
        edges_with_ambiguity = [dict(zip(colnames, x)) for x in cursor]

        for edge_dict in edges_with_ambiguity:
            # Get edge and filter to defined relations (no unmapped records).
            incoming_edge = self._get_incoming_edge(
                cursor,
                edge_dict['edge_id'],
                edge_dict['name']
            )
            defined_relations = (x for x in incoming_edge if x[0] is not None)

            # Load relations into xMapper object and rebuild them.
            mapper = xMapper(data=defined_relations, name=edge_dict['name'])
            mapper.assign_matches_by_id('left')
            mapper.find_matches(
                dal_or_node=self,
                side='right',
                match_limit=12,  # <- Hard-coded value for development only.
                weight_name=edge_dict['name'],
                allow_overlapping=False,
            )
            mapper_relations = mapper.get_relations('right')

            # Delete old edge (deletion cascades to relation records).
            cursor.execute(
                'DELETE FROM main.edge WHERE edge_id=?',
                [edge_dict['edge_id']],
            )

            # If foreign keys are off, deletion will not cascade and
            # we need to delete the associated relations explicitly.
            # This happens when using versions of SQLite older than
            # 3.35.0 which do not support the DROP COLUMN command.
            cursor.execute('PRAGMA foreign_keys')
            foreign_keys_off = not cursor.fetchone()[0]
            if foreign_keys_off:
                cursor.execute(
                    'DELETE FROM main.relation WHERE edge_id=?',
                    [edge_dict['edge_id']],
                )

            # Add new edge and rebuilt relations.
            new_edge_id = self._add_edge_get_new_id(
                cursor=cursor,
                unique_id=edge_dict['other_unique_id'],
                name=edge_dict['name'],
                description=edge_dict['description'],
                selectors=edge_dict['selectors'],
                filename_hint=edge_dict['other_filename_hint'],
                is_default=edge_dict['is_default'],
                #user_properties=edge_dict['user_properties'],  # <- ADD THIS!
            )
            self._add_edge_relations(cursor, new_edge_id, mapper_relations)  # type: ignore [arg-type]
            mapper.close()  # Close database connection inside xMapper.
            self._refresh_proportions(cursor, new_edge_id)
            self._refresh_other_index_hash(cursor, new_edge_id)
            self._refresh_is_locally_complete(cursor, new_edge_id)

    def remove_index_columns(
        self,
        columns: Iterable[str],
        *,
        preserve_structure: bool = True,
        preserve_granularity: bool = True,
        preserve_edges: bool = True,
    ) -> None:
        with self._transaction() as cur:
            self._remove_index_columns_execute_sql(
                cur,
                columns,
                preserve_structure=preserve_structure,
                preserve_granularity=preserve_granularity,
                preserve_edges=preserve_edges,
            )

    @classmethod
    def _refresh_index_hash(cls, cursor: sqlite3.Cursor) -> None:
        """Refresh the index_hash in the 'property' table.

        The index hash should be refreshed after any INSERT or DELETE
        on the 'node_index' table.
        """
        cursor.execute("""
            SELECT index_id FROM main.node_index
            WHERE index_id > 0
            ORDER BY index_id
        """)
        unpacked_values = (x[0] for x in cursor)  # Unpack 1-tuple rows.
        hash_value = make_hash(unpacked_values)
        cls._set_data_property(cursor, 'index_hash', hash_value)

    def index_columns(self) -> Sequence[str]:
        """Return the node file's index columns."""
        with self._transaction(method=None) as cur:
            columns = self._get_column_names(cur, 'node_index')
        return columns[1:]

    @classmethod
    def _add_index_records_make_sql(
        cls, cursor: sqlite3.Cursor, columns: Iterable[str]
    ) -> str:
        """Return a SQL statement adding new index records (for use
        with an executemany() call.

        Example:

            >>> dal = DataAccessLayer(...)
            >>> dal._add_index_records_make_sql(cursor, ['state', 'county'])
            'INSERT INTO node_index ("state", "county") VALUES (?, ?)'
        """
        columns = [_schema.normalize_identifier(col) for col in columns]

        existing_columns = cls._get_column_names(cursor, 'node_index')
        existing_columns = existing_columns[1:]  # Slice-off "index_id" column.
        existing_columns = [_schema.normalize_identifier(col) for col in existing_columns]

        invalid_columns = set(columns).difference(existing_columns)
        if invalid_columns:
            msg = f'invalid column name: {", ".join(invalid_columns)}'
            raise sqlite3.OperationalError(msg)

        columns_clause = ', '.join(columns)
        values_clause = ', '.join('?' * len(columns))
        return f'INSERT INTO main.node_index ({columns_clause}) VALUES ({values_clause})'

    def add_index_records(self, data: TabularData) -> None:
        iterator = make_readerlike(data)
        columns = next(iterator)

        with self._transaction() as cur:
            # Get allowed columns and build selectors values.
            allowed_columns = self._get_column_names(cur, 'node_index')
            selectors = tuple((col in allowed_columns) for col in columns)

            # Filter column names and iterator rows to allowed columns.
            columns = tuple(compress(columns, selectors))
            iterator = (tuple(compress(row, selectors)) for row in iterator)

            sql = self._add_index_records_make_sql(cur, columns)
            cur.executemany(sql, iterator)

            # Refresh granularity and index_hash to account for new records.
            self._refresh_granularity(cur)
            self._refresh_index_hash(cur)

    def _select_index_records(
        self, cursor: sqlite3.Cursor, **where: Union[str, int]
    ) -> Generator[Sequence, None, None]:
        """Returns an iterator that yields index records."""
        if where:
            where_expr, parameters = self._format_select_params(where)
            sql = f'SELECT * FROM main.node_index WHERE {where_expr}'
        else:
            parameters = {}
            sql = 'SELECT * FROM main.node_index'

        cursor.execute(sql, parameters)
        for row in cursor:
            yield row

    @eagerly_initialize
    def index_records(
        self, **where: Union[str, int]
    ) -> Generator[Sequence, None, None]:
        """Returns an iterator that yields index records.

        .. code-block::

            >>> for x in dal.index_records():
            ...     print(x)
        """
        with self._transaction(method=None) as cur:
            if where:
                columns = self._get_column_names(cur, 'node_index')
                for key in where.keys():
                    if key not in columns:
                        raise KeyError(key)

            yield from self._select_index_records(cur, **where)

    @eagerly_initialize
    def index_records_grouped(
        self, where_dicts: Iterable[Dict[str, Union[str, int]]]
    ) -> Generator[Sequence, None, None]:
        """Returns index records grouped by where conditions.

        The return value is modeled after the itertools.groupby()
        behavior which returns a key and group for each item::

            >>> results = dal.index_records_grouped([
            ...     {'state': 'CA', 'town': 'Los Angeles'},
            ...     {'state': 'CA', 'town': 'San Francisco'},
            ... ])
            >>> key, group = next(results)
            >>> key
            {'state': 'CA', 'town': 'Los Angeles'}
            >>> list(group)
            [(410, 'CA', 'Los Angeles', 'Bel Air'),
             (411, 'CA', 'Los Angeles', 'Hollywood'),
             (412, 'CA', 'Los Angeles', 'Venice'),
             ...]
            >>> key, group = next(results)
            >>> key
            {'state': 'CA', 'town': 'San Francisco'}
            >>> list(group)
            [(527, 'CA', 'San Francisco', 'Mid-Market'),
             (528, 'CA', 'San Francisco', 'Mission District'),
             (529, 'CA', 'San Francisco', 'Russian Hill'),
             ...]
        """
        with self._transaction(method=None) as cur:
            columns = self._get_column_names(cur, 'node_index')

            for where in where_dicts:
                if where:
                    for key in where.keys():
                        if key not in columns:
                            raise KeyError(key)

                group = self._select_index_records(cur, **where)
                yield (where, group)

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

        existing_columns = cls._get_column_names(cursor, 'node_index')
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
            FROM main.node_index
            WHERE index_id > 0 AND {where_clause}
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
                                                       FROM main.node_index
                                                       WHERE index_id > 0))
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
        make_default: Union[bool, NoValueType] = NOVALUE,
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
            allowed_columns = self._get_column_names(cur, 'node_index')
            bitmask_selectors = tuple((col in allowed_columns) for col in columns)

            # Filter column names and iterator rows to allowed columns.
            columns = tuple(compress(columns, bitmask_selectors))
            def mkrow(row):
                weight_value = row[weight_pos]
                index_labels = compress(row, bitmask_selectors)
                return (weighting_id, weight_value) + tuple(index_labels)
            iterator = (mkrow(row) for row in iterator)

            # Filter to rows where weight value is not None.
            iterator = (row for row in iterator if row[1] is not None)

            # Insert weight records.
            sql = self._add_weights_make_sql(cur, columns)
            cur.executemany(sql, iterator)

            # Update "weighting.is_complete" value (set to 1 or 0).
            self._add_weights_set_is_complete(cur, weighting_id)

            # Set as default if *make_default* is True or if *make_default*
            # is unspecified and this is the first weighting.
            if make_default is NOVALUE:
                cur.execute('SELECT COUNT(*) FROM main.weighting')
                if cur.fetchone()[0] == 1:
                    self._set_data_property(cur, 'default_weighting', name)
                    # TODO: Warn that default_weighting was automatically set.
            elif make_default:
                self._set_data_property(cur, 'default_weighting', name)

    @eagerly_initialize
    def weight_records(
        self,
        name: Optional[str] = None,
        **where: Union[str, int],
    ) -> Generator[Sequence, None, None]:
        """Returns an iterator that yields weight records.

        .. code-block::

            >>> for x in dal.weighting_records():
            ...     print(x)
        """
        with self._transaction(method=None) as cur:
            if where:
                columns = self._get_column_names(cur, 'node_index')
                for key in where.keys():
                    if key not in columns:
                        raise KeyError(key)

            if name is None:
                name = self._get_data_property(cur, 'default_weighting')

            cur.execute('SELECT name from main.weighting')
            weighting_names = [x[0] for x in cur]
            if name not in weighting_names:
                raise KeyError(name)

            sql = """
                SELECT index_id, weight_value
                FROM main.node_index t1
                LEFT JOIN main.weight t2 USING (index_id)
                LEFT JOIN main.weighting t3 ON (
                    t2.weighting_id=t3.weighting_id
                    AND t3.name=:weight_name
                )
            """
            if where:
                where_expr, parameters = self._format_select_params(where)
                parameters['weight_name'] = name
                sql = f'{sql}    WHERE {where_expr}'
            else:
                parameters = {'weight_name': name}

            cur.execute(sql, parameters)
            for row in cur:
                yield row

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
            second_location_id = cursor.fetchone()
            if second_location_id:
                msg  = f'multiple location ids for given labels: {dict(labels)!r}'
                raise RuntimeError(msg)
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
    def _add_quantities_get_attribute_id(
        cursor: sqlite3.Cursor,
        attribute_value: str,
    ) -> int:
        """Return attribute_id for given attribute value. If attribute
        does not exist, create it and return its id.
        """
        # If attribute already exists, return its attribute_id.
        select_sql = """
            SELECT attribute_id
            FROM main.attribute
            WHERE attribute_value=?
        """
        cursor.execute(select_sql, (attribute_value,))
        attribute_id = cursor.fetchone()
        if attribute_id:
            return attribute_id[0]  # <- EXIT!

        # If attribute does not exist, add it and return its attribute_id.
        insert_sql = 'INSERT INTO main.attribute(attribute_value) VALUES(?)'
        cursor.execute(insert_sql, (attribute_value,))
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
        are associated with matching index records.

        Parameters
        ----------
        data : Iterable[Sequence] | Iterable[Mapping]
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

                    attribute_id = self._add_quantities_get_attribute_id(cur, attr)

                    statement = """
                        INSERT INTO main.quantity (_location_id, attribute_id, quantity_value)
                            VALUES(?, ?, ?)
                    """
                    cur.execute(statement, (loc_id, attribute_id, val))
                    inserted_rows_count += 1

            self._add_quantities_warn(
                missing_attrs_count,
                missing_vals_count,
                inserted_rows_count,
            )

    @staticmethod
    def _format_select_params(
        where: Dict[str, Union[str, int]],
        connecting_op: Literal['AND', 'OR'] = 'AND',
        start_num: int = 1,
        table_qualifier: str = '',
    ) -> Tuple[str, Dict[str, Union[str, int]]]:
        """Format WHERE clause and parameters dictionary for use in
        SELECT queries.

        :param Dict where:
            A dictionary of column and value requirements that will be
            used to prepare a WHERE expression and *parameters* for a
            call to `con.execute(..., parameters)`.
        :param str connecting_op:
            Logical operator to connect where clause items.
        :param int start_num:
            Number to start with for "autoparam" names.
        :param str table_qualifier:
            If given, a table qualifier is used as a prefix for any
            column references.
        :returns Tuple:
            Returns a two-tuple containing a *where-expression* string
            and an *execute-parameters* dictionary that follow the
            DBAPI2 convention for "named style" parameters.

        .. code-block::

            >>> where_expr, parameters = dal._format_select_params({
            ...     'state': 'OH',
            ...     'town': 'Cleveland',
            ... })
            >>> where_expr
            '"state"=:autoparam1 AND "town"=:autoparam2'
            >>> parameters
            {'autoparam1': 'OH', 'autoparam2': 'Cleveland'}

        Using the returned values in a SQL query:

        .. code-block::

            >>> sql = f'SELECT * FROM mytable WHERE {where_expr}'
            >>> cursor.execute(sql, parameters)
        """
        if table_qualifier and not table_qualifier.endswith('.'):
            table_qualifier = f'{table_qualifier}.'

        conditions = []
        parameters = {}
        for num, (k, v) in enumerate(where.items(), start=start_num):
            param_name = f'autoparam{num}'
            col_name = _schema.normalize_identifier(k)
            conditions.append(f'{table_qualifier}{col_name}=:{param_name}')
            parameters[param_name] = v

        where_expr = f' {connecting_op} '.join(conditions)
        return where_expr, parameters

    @staticmethod
    def _get_raw_quantities_format_args(
        index_cols: List[str], where: Dict[str, str]
    ) -> Tuple[List[str], Tuple[str, ...], Optional[Callable[[Any], bool]]]:
        """Format arguments for get_raw_quantities() and
        delete_raw_quantities() methods.

        :param List index_cols:
            A list of all index column names defined in the
            `node_index` table.
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

        # Partition location and attribute keys into separate dicts.
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
            SELECT {', '.join(normalized)}, attribute_value, quantity_value
            FROM main.quantity
            JOIN main.attribute USING (attribute_id)
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
                where_items.append(f'{func_name}(attribute_value)=1')

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
                JOIN main.attribute USING (attribute_id)
                JOIN main.location USING (_location_id)
                WHERE {' AND '.join(where_items)}
            )
        """
        cursor.execute(statement, parameters)
        deleted_rowcount = cursor.rowcount

        # TODO: Check if we need to delete unused attribute records.

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
                where_items.append(f'{func_name}(attribute_value)=1')

            self._delete_raw_quantities_execute(cur, where_items, parameters)

    @staticmethod
    def _disaggregate_make_sql_constraints(
        normalized_columns: Sequence[str],
        bitmask: Sequence[Literal[0, 1]],
        location_table_alias: str,
        index_table_alias: str,
    ) -> str:
        """Build a string of constraints on which to join the
        `location` and `node_index` tables for disaggregation.

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
            where_clause = f'\n            WHERE {filter_attrs_func}(t1b.attribute_value)=1'
        else:
            where_clause = ''

        # Build final SELECT statement.
        statement = f"""
            SELECT
                t3.index_id,
                t1b.attribute_value,
                t1.quantity_value * IFNULL(
                    (t4.weight_value / SUM(t4.weight_value) OVER (PARTITION BY t1.quantity_id)),
                    (1.0 / COUNT(1) OVER (PARTITION BY t1.quantity_id))
                ) AS quantity_value
            FROM main.quantity t1
            JOIN main.attribute t1b USING (attribute_id)
            JOIN main.location t2 USING (_location_id)
            JOIN main.node_index t3 ON ({join_constraints})
            JOIN main.weight t4 ON (
                t3.index_id=t4.index_id
                AND t4.weighting_id={match_selector_func}(t1b.attribute_value)
            ){where_clause}
        """
        return statement

    def static_disaggregate(
        self, **filter_rows_where: str
    ) -> Iterable[Tuple[int, Dict[str, str], float]]:
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

            ## TEMPORARY PATCH FOR REFACTORING
            parse_all = lambda y: [parse_selector(z) for z in y]
            list_of_selectors = [(x, parse_all(y)) for (x, y) in cur.fetchall()]
            match_weighting_id = GetMatchingKey(list_of_selectors, default=1)  # type: ignore [arg-type]
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

            # Define and execute SELECT query.
            final_sql = f"""
                WITH
                    all_quantities AS (
                        {disaggregated_quantities}
                    )
                SELECT t1.index_id, t2.attribute_value, SUM(t2.quantity_value) AS quantity_value
                FROM main.node_index t1
                JOIN all_quantities t2 USING (index_id){where_clause}
                GROUP BY {', '.join(f't1.{x}' for x in normalized_cols)}, t2.attribute_value
            """
            cur.execute(final_sql, parameters)

            # Annotate row variable and yield selected results.
            row: Tuple[int, Dict[str, str], float]
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
            where_clause = f'\n            WHERE {filter_attrs_func}(t1b.attribute_value)=1'
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
                t1b.attribute_value,
                t1.quantity_value * COALESCE(
                    (COALESCE(t5.weight_value, 0.0) / SUM(t5.weight_value) OVER (PARTITION BY t1.quantity_id)),
                    (t4.weight_value / SUM(t4.weight_value) OVER (PARTITION BY t1.quantity_id)),
                    (1.0 / COUNT(1) OVER (PARTITION BY t1.quantity_id))
                ) AS quantity_value
            FROM main.quantity t1
            JOIN main.attribute t1b USING (attribute_id)
            JOIN main.location t2 USING (_location_id)
            JOIN main.node_index t3 ON ({join_constraints})
            JOIN main.weight t4 ON (
                t3.index_id=t4.index_id
                AND t4.weighting_id={match_selector_func}(t1b.attribute_value)
            )
            LEFT JOIN (
                SELECT
                    t5sub.index_id,
                    user_json_object_keep(t5sub.attribute_value{keys_to_keep}) AS attrs_subset,
                    SUM(t5sub.quantity_value) AS weight_value
                FROM {adaptive_weight_table} t5sub
                GROUP BY t5sub.index_id, user_json_object_keep(t5sub.attribute_value{keys_to_keep})
            ) t5 ON (
                t3.index_id=t5.index_id
                AND t5.attrs_subset=user_json_object_keep(t1b.attribute_value{keys_to_keep})
            ){where_clause}
            UNION ALL
            SELECT index_id, attribute_value, quantity_value FROM {adaptive_weight_table}
        """
        return statement

    def adaptive_disaggregate(
        self,
        match_attributes: Optional[Sequence[str]] = None,
        **filter_rows_where: str,
    ) -> Iterable[Tuple[int, Dict[str, str], float]]:
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

            ## TEMPORARY PATCH FOR REFACTORING
            parse_all = lambda y: [parse_selector(z) for z in y]
            list_of_selectors = [(x, parse_all(y)) for (x, y) in cur.fetchall()]
            match_weighting_id = GetMatchingKey(list_of_selectors, default=1)  # type: ignore [arg-type]
            weighting_func_name = _schema.get_userfunc(cur, match_weighting_id)

            # Get bitmask levels from structure table.
            columns = self._get_column_names(cur, 'location')[1:]
            normalized_cols = [_schema.normalize_identifier(col) for col in columns]
            cur.execute('SELECT * FROM main.structure ORDER BY _granularity DESC')
            bitmasks = [row[2:] for row in cur]  # Slice-off id and granularity values.

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

            # Define and execute SELECT query.
            final_sql = f"""
                WITH
                    {all_cte_statements}
                SELECT t1.index_id, t2.attribute_value, SUM(t2.quantity_value) AS quantity_value
                FROM main.node_index t1
                JOIN {current_cte} t2 USING (index_id){where_clause}
                GROUP BY t2.index_id, t2.attribute_value
            """
            cur.execute(final_sql, parameters)

            # Annotate row variable and yield selected results.
            row: Tuple[int, Dict[str, str], float]
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
                    cur.execute("PRAGMA main.table_info('node_index')")
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

        cls._refresh_granularity(cursor)

    @staticmethod
    def _structure(cursor: sqlite3.Cursor) -> Sequence[Tuple]:
        """Sequence of bitmask tuples representing the node structure."""
        cursor.execute('SELECT * FROM main.structure')
        bitmasks = (row[2:] for row in cursor)  # Slice-off id and granularity.
        return [tuple(bits) for bits in bitmasks]

    def structure(self) -> Sequence[Tuple]:
        """Sequence of bitmask tuples representing the node structure."""
        with self._transaction(method=None) as cur:
            return self._structure(cur)

    @staticmethod
    def _refresh_granularity_sql(columns: Sequence[str]) -> str:
        r"""Return a SQL statement to UPDATE a single structure record.

        When executing the returned SQL, a parameters dictionary must
        also be given that specifies ``'partition_cardinality'`` and
        ``'structure_id'`` values:

        .. code-block:: python

            sql = _refresh_granularity_sql(['col1', 'col2', 'col3])

            params = {'partition_cardinality': 91856, 'structure_id': 4}
            cursor.execute(sql, params)

        The SQL statement here implements the "granularity measure
        of a partition" as described on p. 293 of:

            MARK J. WIERMAN (1999) MEASURING UNCERTAINTY IN ROUGH SET
            THEORY, International Journal of General Systems, 28:4-5,
            283-297, DOI: 10.1080/03081079908935239

        In PROBABILISTIC APPROACHES TO ROUGH SETS (Y. Y. Yao, 2003),
        Yiyu Yao presents this same equation in Eq. (6), shown here:

        .. code-block:: none

                       m
                      ___
                      \    |A_i|
            log |U| - /    ───── log |A_i|
                      ‾‾‾   |U|
                      i=1

            TeX notation:

                \[\log_{2}|U|-\sum_{i=1}^m \frac{|A_i|}{|U|}\log_{2}|A_i|\]
        """
        if columns:
            columns = [_schema.normalize_identifier(col) for col in columns]
            groupby_clause = f'\n                    GROUP BY {", ".join(columns)}'
        else:
            groupby_clause = ''

        sql = f"""
            WITH
                subset (cardinality) AS (
                    SELECT CAST(COUNT(*) AS REAL)
                    FROM main.node_index
                    WHERE index_id > 0{groupby_clause}
                ),
                summand (uncertainty) AS (
                    SELECT ((subset.cardinality / :partition_cardinality)
                            * LOG2(subset.cardinality))
                    FROM subset
                ),
                granularity (value) AS (
                    SELECT LOG2(:partition_cardinality) - SUM(uncertainty)
                    FROM summand
                )
            UPDATE main.structure
            SET _granularity = (SELECT value FROM granularity)
            WHERE _structure_id=:structure_id
        """
        return sql

    @classmethod
    def _refresh_granularity(cls, cursor: sqlite3.Cursor) -> None:
        """Refresh the granularity measure in the structure table.

        The granularity should be refreshed after any of the following
        actions:

        * Rebuild of the 'structure' table (this happens via the
          _set_data_structure() method which gets called after adding
          or removing discrete categories and when adding or removing
          a column in the 'node_index' table).
        * Records are changed in the 'node_index' table (after INSERT,
          DELETE, and UPDATE queries).
        """
        all_columns = cls._get_column_names(cursor, 'node_index')[1:]

        cursor.execute("""
            SELECT CAST(COUNT(*) AS REAL)
            FROM main.node_index
            WHERE index_id > 0
        """)
        node_cardinality = cursor.fetchone()[0]

        cursor.execute('SELECT * FROM main.structure')
        structure_records = cursor.fetchall()
        for record in structure_records:
            structure_id, _, *bitmask = record
            columns = list(compress(all_columns, bitmask))
            sql = cls._refresh_granularity_sql(columns)
            parameters = {
                'partition_cardinality': node_cardinality,
                'structure_id': structure_id,
            }
            cursor.execute(sql, parameters)

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

        Refresh values if index columns have been added but there are
        no explicit category changes (only implicit ones)::

            >>> cur = ...
            >>> dal._update_categories_and_structure(cur)
        """
        if not categories:
            categories = cls._get_data_property(cursor, 'discrete_categories') or []
            categories = [set(x) for x in categories]

        if minimize:
            whole_space = set(cls._get_column_names(cursor, 'node_index')[1:])
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

    @staticmethod
    def _add_edge_get_new_id(
        cursor: sqlite3.Cursor,
        unique_id: str,
        name: str,
        description: Union[str, None, NoValueType] = NOVALUE,
        selectors: Union[Iterable[str], None, NoValueType] = NOVALUE,
        filename_hint: Union[str, None, NoValueType] = NOVALUE,
        is_default: Union[bool, NoValueType] = NOVALUE,
    ) -> int:
        """Add a new edge or update existing edge, returns 'edge_id'."""
        # Build SQL and add new edge.
        sql = """
            INSERT INTO main.edge(
                name, description, selectors, other_unique_id, other_filename_hint
            )
            VALUES (
                :name, :description, :selectors, :unique_id, :filename_hint
            )
        """
        parameters = {
            'name': name,
            'description': description or None,
            'selectors': _dumps(selectors) if selectors else None,
            'unique_id': unique_id,
            'filename_hint': filename_hint or None,
        }
        try:
            cursor.execute(sql, parameters)
        except sqlite3.IntegrityError:
            msg = f'edge named {name!r} already exists between these nodes'
            raise ToronError(msg)

        # Build SQL to handle 'is_default' flag.
        if is_default is NOVALUE:
            # If unspecified, set default flag to TRUE if it's the first edge.
            sql= """
                UPDATE main.edge
                SET is_default=CASE
                    WHEN 1=(SELECT COUNT(*)
                            FROM main.edge
                            WHERE other_unique_id=:unique_id)
                    THEN 1
                    ELSE NULL
                END
                WHERE other_unique_id=:unique_id AND name=:name
            """
        elif is_default:
            # Set default flag to TRUE and set all others to NULL.
            sql = """
                UPDATE main.edge
                SET is_default=CASE WHEN name=:name THEN 1 ELSE NULL END
                WHERE other_unique_id=:unique_id
            """
        else:
            # Set default flag to NULL regardless of other edges.
            sql = """
                UPDATE main.edge
                SET is_default=NULL
                WHERE other_unique_id=:unique_id AND name=:name
            """
        cursor.execute(sql, {'unique_id': unique_id, 'name': name})

        # Get newly created edge_id.
        sql = """
            SELECT edge_id, is_default
            FROM main.edge
            WHERE other_unique_id=:unique_id AND name=:name
        """
        cursor.execute(sql, {'unique_id': unique_id, 'name': name})
        edge_id, assigned_default_state = cursor.fetchone()

        #if is_default is NOVALUE and assigned_default_state:
        #    pass  # TODO: Warn that default was automatically set to TRUE.

        return edge_id

    @staticmethod
    def _add_edge_relations(
        cursor: sqlite3.Cursor,
        edge_id: int,
        relations: Iterable[Tuple[int, int, float, Union[BitFlags2, None]]],
    ) -> None:
        """Add incoming edge from other node.

        The undefined-to-undefined relation is always included with a
        weight of 0.
        """
        sql = """
            INSERT OR REPLACE INTO main.relation (
                edge_id,
                other_index_id,
                index_id,
                relation_value,
                mapping_level
            )
            VALUES (?, ?, ?, ?, ?)
        """
        relations = chain(relations, [(0, 0, 0.0, None)])  # Append "undefined" relation.
        params_iter = ((edge_id, a, b, c, d) for a, b, c, d in relations)
        cursor.executemany(sql, params_iter)

    @staticmethod
    def _refresh_proportions(cursor: sqlite3.Cursor, edge_id: int) -> None:
        """Recalculate and assign 'proportion' values for an edge."""
        # Get a list of incoming IDs (i.e. 'other_index_id' values) for
        # the specified edge (not including 0, the undefined point).
        sql = """
            SELECT DISTINCT other_index_id
            FROM main.relation
            WHERE edge_id=? AND other_index_id>0
        """
        all_other_index_ids = [x[0] for x in cursor.execute(sql, (edge_id,))]

        for other_index_id in all_other_index_ids:
            # Get all relations associated with an incoming ID.
            sql = """
                SELECT relation_id, relation_value
                FROM main.relation
                WHERE edge_id=? AND other_index_id=?
            """
            cursor.execute(sql, (edge_id, other_index_id))
            results = cursor.fetchall()

            # Get total of values from associated relations. If total
            # is 0 or None, then relations are weighted evenly.
            total_val = sum(row[1] for row in results)
            if not total_val:
                total_val = len(results)
                results = [(rel_id, 1) for rel_id, _ in results]

            # Calculate and assign proportions for associated relations.
            sql = """
                UPDATE main.relation
                SET proportion=?
                WHERE relation_id=?
            """
            params_iter = ((val/total_val, rel_id) for rel_id, val in results)
            cursor.executemany(sql, params_iter)

        # Set proportion to 0.0 for any relations between the incoming
        # undefined point (ID 0) and a locally defined point (ID > 0).
        sql = """
            UPDATE main.relation
            SET proportion=0.0
            WHERE edge_id=? AND other_index_id=0 AND index_id>0
        """
        cursor.execute(sql, (edge_id,))

        # Set proportion to 1.0 for the relation between the incoming
        # undefined point (ID 0) and the locally undefined point (ID 0).
        sql = """
            UPDATE main.relation
            SET proportion=1.0
            WHERE edge_id=? AND other_index_id=0 AND index_id=0
        """
        cursor.execute(sql, (edge_id,))

    @staticmethod
    def _refresh_other_index_hash(
        cursor: sqlite3.Cursor,
        edge_ids: Optional[Union[int, Iterable[int]]] = None,
    ) -> None:
        """Refresh 'other_index_hash' for given edge_id values. If no
        edge_id values are given, the hashes for all edges will be
        refreshed.
        """
        if isinstance(edge_ids, int):
            edge_ids = [edge_ids]
        elif edge_ids is None:
            cursor.execute('SELECT edge_id FROM main.edge')
            edge_ids = [x[0] for x in cursor]  # Eagerly unpack as list.
        elif not isinstance(edge_ids, Iterable):
            msg = (f'edge_ids must be an integer, an iterable of integers, '
                   f'or None, got {edge_ids!r}')
            raise ValueError(msg)

        for edge_id in edge_ids:
            cursor.execute("""
                SELECT DISTINCT other_index_id
                FROM main.relation
                WHERE other_index_id > 0 AND edge_id=?
                ORDER BY other_index_id
            """, (edge_id,))
            unpacked_values = (x[0] for x in cursor)  # Unpack 1-tuple rows.
            hash_value = make_hash(unpacked_values)
            sql = 'UPDATE main.edge SET other_index_hash=? WHERE edge_id=?'
            cursor.execute(sql, (hash_value, edge_id))

    @staticmethod
    def _refresh_is_locally_complete(
        cursor: sqlite3.Cursor, edge_id: int
    ) -> None:
        """Refresh 'edge.is_locally_complete' (sets to 1 or 0, True/False).

        Note: When determining if edges are locally complete, the
        undefined record (index_id 0) should always be included in
        the count.
        """
        sql = """
            UPDATE main.edge
            SET is_locally_complete=((SELECT COUNT(DISTINCT index_id)
                                      FROM main.relation
                                      WHERE edge_id=?) = (SELECT COUNT(*)
                                                          FROM main.node_index))
            WHERE edge_id=?
        """
        cursor.execute(sql, (edge_id, edge_id))

    def add_incoming_edge(
        self,
        unique_id: str,
        name: str,
        relations: Iterable[Tuple[int, int, float, Union[BitFlags2, None]]],
        description: Union[str, None, NoValueType] = NOVALUE,
        selectors: Union[Iterable[str], None, NoValueType] = NOVALUE,
        filename_hint: Union[str, None, NoValueType] = NOVALUE,
        make_default: Union[bool, NoValueType] = NOVALUE,
    ) -> None:
        """Add an incoming edge from another node.

        Parameters
        ----------
        unique_id : str
            The unique_id string of the node that the edge is coming
            from.
        name : str
            A name used to identify the edge.
        relations : Iterable[Tuple[int, int, float, Union[BitFlags2, None]]]
            An iterable of tuples containing the relationship
            information. Each tuple should contain four items:
            (other_index_id, index_id, relation_value, mapping_level)
        description : str (optional)
            An optional description describing the relationship and its
            weight values.
        selectors : list of selector strings (optional)
            Any selectors used to match attributes.
        filename_hint : str
            The filename of the node that the edge is coming from.
        make_default : bool (optional)
            A flag to determine if the edge should be used as the
            default edge when attributes cannot be matched using
            any of the edge's selectors.

        .. code-block::

            dal.add_incoming_edge(
                unique_id='00000000-0000-0000-0000-000000000000',
                name='pop20+',
                relations=[(1, 1, 110.0, None), (2, 2, 120.0, None), ...],
                description='Population Ages 20 and up.',
                selectors=['[category="pop"]'],
                filename_hint='other-file.toron',
            )

        When adding the first edge between two nodes, the edge will
        automatically be set as default when the *make_default* arg
        is not specified.
        """
        with self._transaction(method='begin') as cur:
            edge_id = self._add_edge_get_new_id(
                cur, unique_id, name, description, selectors, filename_hint, make_default
            )
            self._add_edge_relations(cur, edge_id, relations)
            self._refresh_proportions(cur, edge_id)
            self._refresh_other_index_hash(cur, edge_id)
            self._refresh_is_locally_complete(cur, edge_id)

    def edit_incoming_edge(
        self,
        unique_id: str,
        name: str,
        *,
        #relations: Iterable[Tuple[int, int, Union[float, None]]],
        description: Union[str, None, NoValueType] = NOVALUE,
        selectors: Union[Iterable[str], None, NoValueType] = NOVALUE,
        filename_hint: Union[str, None, NoValueType] = NOVALUE,
        is_default: Union[bool, NoValueType] = NOVALUE,
    ) -> None:
        """Edit the properties of an incoming edge."""
        with self._transaction(method='begin') as cur:
            # Build list of properties to SET.
            set_items = []
            parameters: Dict[str, Optional[str]] = {}

            if description is not NOVALUE:
                set_items.append('description=:description')
                parameters['description'] = description

            if selectors is not NOVALUE:
                set_items.append('selectors=:selectors')
                parameters['selectors'] = _dumps(selectors) if selectors else None

            if filename_hint is not NOVALUE:
                set_items.append('other_filename_hint=:filename_hint')
                parameters['filename_hint'] = filename_hint

            # Execute SQL to SET properties.
            if set_items:
                sql = f"""
                    UPDATE main.edge
                    SET {', '.join(set_items)}
                    WHERE other_unique_id=:unique_id AND name=:name
                """
                parameters.update({'unique_id': unique_id, 'name': name})
                cur.execute(sql, parameters)

            # Build and execute SQL for handling 'is_default' flag.
            if is_default is not NOVALUE:
                if is_default:
                    sql = """
                        UPDATE main.edge
                        SET is_default=CASE WHEN name=:name THEN 1 ELSE NULL END
                        WHERE other_unique_id=:unique_id
                    """
                else:
                    sql = """
                        UPDATE main.edge
                        SET is_default=NULL
                        WHERE other_unique_id=:unique_id AND name=:name
                    """
                cur.execute(sql, {'unique_id': unique_id, 'name': name})

    @staticmethod
    def _get_incoming_edge_reconstructed_make_sql(
        edge_id: int,
        column_names: Sequence[str],
    ) -> Tuple[str, Dict[str, int]]:
        """Return a SQL string and parameter dictionary to get incoming
        edge--for use with a cursor.execute() call.

        :param edge_id: The "edge.edge_id" of the incoming edge.
        :param column_names: The full list of node_index label columns
            used by the local node.

        .. warning::
            For proper operation, the given *column_names* MUST include
            all label columns defined in the local node's "node_index"
            table and they must appear in the same order in which they
            are currently defined in the table. If this requirement is
            not satisfied, ambiguous mappings cannot be reconstructed
            with any degree of certainty.

        .. code-block:: python

            >>> sql, parameters = self._get_incoming_edge_reconstructed_make_sql(
            ...     edge_id=42,
            ...     column_names=['A', 'B', 'C'],
            ... )
            >>> self.cur.execute(sql, parameters)

        The SQL query returned by this function requires the user
        defined function "user_apply_bit_flag". Executing this query
        uses any "relation.mapping_level" bit flags to reconstruct
        ambiguous mappings as they were originally given.
        """
        normalized_labels = [_schema.normalize_identifier(x) for x in column_names]

        func = lambda i, x: f'user_apply_bit_flag(a.{x}, b.mapping_level, {i}) AS {x}'
        qualified_bit_flag_masks = [func(i, x) for (i, x) in enumerate(normalized_labels)]

        formatted_labels = f', '.join(normalized_labels)

        sql = f"""
            WITH
                RelationValues AS (
                    SELECT
                        a.other_index_id,
                        a.index_id,
                        a.relation_value,
                        a.mapping_level
                    FROM main.relation a
                    JOIN main.edge b USING (edge_id)
                    WHERE
                        b.edge_id=:edge_id
                ),
                ReconstructedLevels AS (
                    SELECT
                        b.other_index_id,
                        b.relation_value,
                        {f', '.join(qualified_bit_flag_masks)}
                    FROM main.node_index a
                    LEFT JOIN RelationValues b USING (index_id)
                ),
                ReconstructedMapping AS (
                    SELECT
                        other_index_id,
                        SUM(relation_value) AS relation_value,
                        {formatted_labels}
                    FROM ReconstructedLevels
                    GROUP BY
                        other_index_id,
                        {formatted_labels}
                )
            SELECT *
            FROM ReconstructedMapping
            ORDER BY {formatted_labels}
        """
        parameters = {'edge_id': edge_id}
        return sql, parameters

    @staticmethod
    def _get_incoming_edge_reified_make_sql(
        edge_id: int,
        column_names: Sequence[str],
    ) -> Tuple[str, Dict[str, int]]:
        """Return a SQL statement and parameters suitable for building
        a reified correspondence mapping.

        :param edge_id: The "edge.edge_id" of the incoming edge.
        :param column_names: The full list of node_index label columns
            used by the local node.

        .. warning::
            For proper operation, the given *column_names* MUST include
            all label columns defined in the local node's "node_index"
            table. If this requirement is not satisfied, then the query
            cannot reliably build a distinct correspondence mapping to
            match the node's index records.

        .. code-block:: python

            >>> sql, parameters = self._get_incoming_edge_reified_make_sql(
            ...     edge_id=42,
            ...     column_names=['A', 'B', 'C'],
            ... )
            >>> self.cur.execute(sql, parameters)
        """
        normalized_labels = [_schema.normalize_identifier(x) for x in column_names]

        sql = f"""
            WITH
                RelationValues AS (
                    SELECT
                        a.other_index_id,
                        a.index_id,
                        a.relation_value,
                        a.mapping_level
                    FROM main.relation a
                    JOIN main.edge b USING (edge_id)
                    WHERE
                        b.edge_id=:edge_id
                ),
                ReifiedMapping AS (
                    SELECT
                        b.other_index_id,
                        b.relation_value,
                        {f', '.join(f'a.{x}' for x in normalized_labels)},
                        b.mapping_level
                    FROM main.node_index a
                    LEFT JOIN RelationValues b USING (index_id)
                )
            SELECT *
            FROM ReifiedMapping
            ORDER BY {f', '.join(normalized_labels)}
        """
        parameters = {'edge_id': edge_id}
        return sql, parameters

    @classmethod
    def _get_incoming_edge(
        cls,
        cursor: sqlite3.Cursor,
        edge_id: int,
        value_column_name: str = 'value',
        reified: bool = False,
    ) -> Generator[Tuple, None, None]:
        """Yields row tuples from the correspondence mapping of the
        edge specified by *edge_id*.

        :param cursor: Cursor object for local node instance.
        :param edge_id: The `edge.edge_id` for the specified edge.
        :param value_column_name: The column name to use for the
            edge's relation values.
        :param reified: Optional flag indicating whether to return
            reified mapping data. If True, the ambiguous fields will
            be noted in the result records.
        :return: A generator that yields a header tuple followed by
            row tuples for each individual relation in the edge.

        See :meth:`DataAccessLayer.get_incoming_edge` for related info.
        """
        # Get column names for local geography.
        column_names = cls._get_column_names(cursor, 'node_index')
        column_names = column_names[1:]  # Slice-off 'index_id'.

        if not reified:
            # Query data for reconstructed mapping.
            sql, parameters = cls._get_incoming_edge_reconstructed_make_sql(
                edge_id=edge_id,
                column_names=column_names,
            )
            cursor.execute(sql, parameters)
            query_results: Iterator = eagerly_initialize(cursor)

            # Define header for reconstructed mapping.
            header = tuple(chain(['other_index_id', value_column_name], column_names))
        else:
            # Query data for reified mapping.
            sql, parameters = cls._get_incoming_edge_reified_make_sql(
                edge_id=edge_id,
                column_names=column_names,
            )
            cursor.execute(sql, parameters)

            # Define helper function to replace BitFlags2 with description
            # of columns that were left unspecified in original mapping.
            def func(row):
                mapping_level = row[-1]  # Last value is BitFlags2 or None.
                if mapping_level is None:
                    return row  # <- EXIT! Return unchanged.
                inverted_level = [(not bit) for bit in mapping_level]
                ambiguous_fields = compress(column_names, inverted_level)
                ambiguous_desc = ', '.join(ambiguous_fields)
                return tuple(chain(row[:-1], [ambiguous_desc]))

            query_results = (func(row) for row in cursor)  # Apply func().
            query_results = eagerly_initialize(query_results)

            # Define header for reified mapping.
            header = tuple(chain(
                ['other_index_id', value_column_name], column_names, ['ambiguous_fields']
            ))

        # Yield header row and query results.
        yield header
        for row in query_results:
            yield row

    def get_incoming_edge(
        self,
        other_unique_id: str,
        name: str,
        reified: bool = False,
    ) -> Generator[Tuple, None, None]:
        """Yields row tuples from the correspondence mapping of the
        edge specified by *other_unique_id* and *name*.

        By default, any ambiguous relations will be returned in their
        collapsed, partially specified form. Optionally, users can set
        ``reified=True`` to see the concretized, individual relations
        and their calculated weights which are created by the ambiguous
        relations.

        :param other_unique_id: Unique identifier of the connected node.
        :param name: Name of the incoming edge--there can be multiple
            edges between the same two nodes (each with a different
            name).
        :param reified: Optional flag indicating whether to return
            reified mapping data. If True, the ambiguous fields will
            be noted in the result records.
        :return: A generator that yields a header tuple followed by
            row tuples for each individual relation in the edge.

        .. code-block:: python

            >>> generator = dal.get_incoming_edge(
            ...     other_unique_id='00000000-0000-0000-0000-000000000000',
            ...     name='population',
            ... )
            >>> for row in generator:
            >>>     print(row)
        """
        with self._transaction(method=None) as cur:
            # Get edge_id using `edge.other_unique_id` and `edge.name`.
            cur.execute(
                """
                    SELECT edge_id
                    FROM main.edge
                    WHERE
                        other_unique_id=:other_unique_id
                        AND name=:edge_name
                """,
                {'other_unique_id': other_unique_id, 'edge_name': name}
            )
            edge_id = cur.fetchone()[0]

            # Get generator for specified edge.
            generator = self._get_incoming_edge(
                cursor=cur,
                edge_id=edge_id,
                value_column_name=name,
                reified=reified,
            )

            # Yield relations (includes header row as first item).
            for relation in generator:
                yield relation

    @staticmethod
    def _translate_generator(
        cursor: sqlite3.Cursor, data: XQuantityIterator
    ) -> Iterable[Tuple[int, Dict[str, str], float]]:
        """Translate incoming *data* to use index_id values from the
        database associated with the given *cursor*. Data records are
        matched to an edge by the matching data.unique_id (the unique
        ID of the source node) and each record's attribute dictionary.
        If a record's attribute dictionary cannot be matched to an
        edge, the default edge is used instead.

        :param cursor: Cursor object for local node instance.
        :param data: Incoming data iterator.
        :return: A generator that yields tuple rows suitable for
            constructing a new XQuantityIterator instance.
        """
        # Get default 'edge_id' for matching 'other_unique_id'.
        sql = """
            SELECT edge_id, name, is_locally_complete
            FROM main.edge
            WHERE other_unique_id=? AND is_default=1
        """
        cursor.execute(sql, (data.unique_id,))
        default_edge_id, default_name, is_locally_complete = cursor.fetchone()
        if not is_locally_complete:
            msg = f'default edge {default_name!r} is not complete'
            raise RuntimeError(msg)

        # Get 'edge_id' and 'selectors' values.
        sql = """
            SELECT edge_id, selectors
            FROM main.edge
            WHERE other_unique_id=? AND is_locally_complete=1
        """
        cursor.execute(sql, (data.unique_id,))

        ## TEMPORARY PATCH FOR REFACTORING
        parse_all = lambda y: [parse_selector(z) for z in y]
        list_of_selectors = [(x, parse_all(y)) for (x, y) in cursor.fetchall()]
        get_edge_id = GetMatchingKey(list_of_selectors, default=default_edge_id)  # type: ignore [arg-type]

        grouped = groupby(data, key=lambda x: x[0])  # Group by other_index_id.

        for other_index_id, group in grouped:
            sql = """
                SELECT edge_id, index_id, proportion
                FROM main.edge
                JOIN main.relation USING (edge_id)
                WHERE
                    other_unique_id=? AND is_locally_complete=1
                    AND other_index_id=?
            """
            cursor.execute(sql, (data.unique_id, other_index_id))

            proportions: Dict[int, Dict[int, float]] = defaultdict(dict)
            for edge_id, index_id, proportion in cursor:
                proportions[edge_id][index_id] = proportion

            for _, attributes, quantity_value in group:
                edge_id = get_edge_id(attributes)
                for index_id, proportion in proportions[edge_id].items():
                    yield index_id, attributes, quantity_value * proportion

    def translate(
        self, data: XQuantityIterator,
    ) -> XQuantityIterator:
        """Compute crosswalk for incoming data and return result."""
        with self._transaction(method=None) as cur:
            iterator = XQuantityIterator(
                self.unique_id,  # Use destination ID.
                self._translate_generator(cur, data),  # Translate to destination.
                _attribute_keys=data.attribute_keys,  # Reuse keys from source.
            )

        return iterator


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
        """Return a list of SQL statements for removing index columns."""
        # In SQLite versions before 3.35.0, there is no native support for the
        # DROP COLUMN command. In these older versions of SQLite the tables
        # must be rebuilt. This method prepares a sequence of operations to
        # rebuild the table structures.
        columns_to_keep = [col for col in column_names if col not in names_to_remove]
        new_nodeindex_cols = [_schema.sql_column_def_nodeindex_label(col) for col in columns_to_keep]
        new_location_cols = [_schema.sql_column_def_location_label(col) for col in columns_to_keep]
        new_structure_cols = [_schema.sql_column_def_structure_label(col) for col in columns_to_keep]

        statements = [
            # Rebuild 'node_index'.
            f'CREATE TABLE main.new_nodeindex(index_id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
                f'{", ".join(new_nodeindex_cols)})',
            f'INSERT INTO main.new_nodeindex SELECT index_id, {", ".join(columns_to_keep)} FROM main.node_index',
            'DROP TABLE main.node_index',
            'ALTER TABLE main.new_nodeindex RENAME TO node_index',

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
        statements.extend(_schema.sql_create_node_indexes(columns_to_keep))

        return statements

    def remove_index_columns(
        self,
        columns: Iterable[str],
        *,
        preserve_structure: bool = True,
        preserve_granularity: bool = True,
        preserve_edges: bool = True,
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
                self._remove_index_columns_execute_sql(
                    cur,
                    columns,
                    preserve_structure=preserve_structure,
                    preserve_granularity=preserve_granularity,
                    preserve_edges=preserve_edges,
                )

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
        new_nodeindex_cols = [_schema.sql_column_def_nodeindex_label(col) for col in new_column_names]
        new_location_cols = [_schema.sql_column_def_location_label(col) for col in new_column_names]
        new_structure_cols = [_schema.sql_column_def_structure_label(col) for col in new_column_names]
        statements = [
            # Rebuild 'node_index'.
            f'CREATE TABLE main.new_nodeindex(index_id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
                f'{", ".join(new_nodeindex_cols)})',
            f'INSERT INTO main.new_nodeindex SELECT index_id, {", ".join(column_names)} FROM main.node_index',
            'DROP TABLE main.node_index',
            'ALTER TABLE main.new_nodeindex RENAME TO node_index',

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
            _schema.sql_create_node_indexes(list(new_column_names))
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
            where_clause = f'\n            WHERE {filter_attrs_func}(t1b.attribute_value)=1'
        else:
            where_clause = ''

        statement = f"""
            SELECT
                t3.index_id,
                t1b.attribute_value,
                t1.quantity_value * IFNULL(
                    (t4.weight_value / (
                        SELECT SUM(sub4.weight_value)
                        FROM main.quantity sub1
                        JOIN main.location sub2 USING (_location_id)
                        JOIN main.node_index sub3 ON ({subquery_join_constraints})
                        JOIN main.weight sub4 USING (index_id)
                        WHERE sub1.quantity_id=t1.quantity_id
                            AND sub4.weighting_id=t4.weighting_id
                    )),
                    (1.0 / (
                        SELECT COUNT(1)
                        FROM main.quantity sub1
                        JOIN main.location sub2 USING (_location_id)
                        JOIN main.node_index sub3 ON ({subquery_join_constraints})
                        WHERE sub1.quantity_id=t1.quantity_id
                    ))
                ) AS quantity_value
            FROM main.quantity t1
            JOIN main.attribute t1b USING (attribute_id)
            JOIN main.location t2 USING (_location_id)
            JOIN main.node_index t3 ON ({join_constraints})
            JOIN main.weight t4 ON (
                t3.index_id=t4.index_id
                AND t4.weighting_id={match_selector_func}(t1b.attribute_value)
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
            where_clause = f'\n            WHERE {filter_attrs_func}(t1b.attribute_value)=1'
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
                t1b.attribute_value,
                t1.quantity_value * COALESCE(
                    (COALESCE(t5.weight_value, 0.0) / (
                        SELECT SUM(sub4.weight_value)
                        FROM main.quantity sub1
                        JOIN main.attribute sub1b USING (attribute_id)
                        JOIN main.location sub2 USING (_location_id)
                        JOIN main.node_index sub3 ON ({subquery_join_constraints})
                        LEFT JOIN (
                            SELECT
                                sub4sub.index_id,
                                user_json_object_keep(sub4sub.attribute_value{keys_to_keep}) AS attrs_subset,
                                SUM(sub4sub.quantity_value) AS weight_value
                            FROM {adaptive_weight_table} sub4sub
                            GROUP BY sub4sub.index_id, user_json_object_keep(sub4sub.attribute_value{keys_to_keep})
                        ) sub4 ON (
                            sub3.index_id=sub4.index_id
                            AND sub4.attrs_subset=user_json_object_keep(sub1b.attribute_value{keys_to_keep})
                        )
                        WHERE sub1.quantity_id=t1.quantity_id
                    )),
                    (t4.weight_value / (
                        SELECT SUM(sub4.weight_value)
                        FROM main.quantity sub1
                        JOIN main.location sub2 USING (_location_id)
                        JOIN main.node_index sub3 ON ({subquery_join_constraints})
                        JOIN main.weight sub4 USING (index_id)
                        WHERE sub1.quantity_id=t1.quantity_id
                            AND sub4.weighting_id=t4.weighting_id
                    )),
                    (1.0 / (
                        SELECT COUNT(1)
                        FROM main.quantity sub1
                        JOIN main.location sub2 USING (_location_id)
                        JOIN main.node_index sub3 ON ({subquery_join_constraints})
                        WHERE sub1.quantity_id=t1.quantity_id
                    ))
                ) AS quantity_value
            FROM main.quantity t1
            JOIN main.attribute t1b USING (attribute_id)
            JOIN main.location t2 USING (_location_id)
            JOIN main.node_index t3 ON ({join_constraints})
            JOIN main.weight t4 ON (
                t3.index_id=t4.index_id
                AND t4.weighting_id={match_selector_func}(t1b.attribute_value)
            )
            LEFT JOIN (
                SELECT
                    t5sub.index_id,
                    user_json_object_keep(t5sub.attribute_value{keys_to_keep}) AS attrs_subset,
                    SUM(t5sub.quantity_value) AS weight_value
                FROM {adaptive_weight_table} t5sub
                GROUP BY t5sub.index_id, user_json_object_keep(t5sub.attribute_value{keys_to_keep})
            ) t5 ON (
                t3.index_id=t5.index_id
                AND t5.attrs_subset=user_json_object_keep(t1b.attribute_value{keys_to_keep})
            ){where_clause}
            UNION ALL
            SELECT index_id, attribute_value, quantity_value FROM {adaptive_weight_table}
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
if _SQLITE_VERSION_INFO < (3, 21, 0):
    msg = f'Internal SQLite version {sqlite3.sqlite_version} not supported.'
    raise RuntimeError(msg)
elif _SQLITE_VERSION_INFO < (3, 24, 0):
    dal_class = DataAccessLayerPre24
elif _SQLITE_VERSION_INFO < (3, 25, 0):
    dal_class = DataAccessLayerPre25
elif _SQLITE_VERSION_INFO < (3, 35, 0):
    dal_class = DataAccessLayerPre35
else:
    dal_class = DataAccessLayer
