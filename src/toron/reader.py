"""NodeReader implementation for the Toron project."""

import os
import sqlite3
import weakref
from contextlib import (
    closing,
    contextmanager,
    nullcontext,
    suppress,
)
from itertools import chain, groupby
from json import dumps, loads
from tempfile import NamedTemporaryFile

from toron._typing import (
    Callable,
    ContextManager,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Self,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
    overload,
    TYPE_CHECKING,
)
from toron.data_models import Index
from toron.data_service import make_get_crosswalk_id_func
from toron._utils import check_type

if TYPE_CHECKING:
    import pandas as pd
    from toron import TopoNode


class NodeReader(object):
    """An iterator for base level TopoNode data."""
    _data: Optional[Generator[Tuple[Union[str, float], ...], None, None]]
    _current_working_path: Optional[str]
    _in_memory_connection: Optional[sqlite3.Connection]
    _index_columns: Tuple[str, ...]
    _attr_keys: Tuple[str, ...]
    close: weakref.finalize

    def __init__(
        self,
        data: Iterator[Tuple[int, Dict[str, str], Optional[float]]],
        node: 'TopoNode',
        cache_to_drive: bool = False,
    ) -> None:
        """Initialize a new NodeReader instance."""

        # `NodeReader` data is managed using a temporary SQLite
        # database with the following schema:
        #
        #    +--------------+
        #    | quant_data   |    +--------------+
        #    +--------------+    | attr_data    |
        #    | index_id     |    +--------------+
        #    | attr_data_id |<---| attr_data_id |
        #    | quant_value  |    | attributes   |
        #    +--------------+    | crosswalk_id |
        #                        +--------------+

        connection_cm: ContextManager[sqlite3.Connection]

        # Set up database and connection context manager (cm).
        if cache_to_drive:
            with closing(NamedTemporaryFile(delete=False)) as f:
                filepath = os.path.realpath(f.name)  # resolve symlinks with realpath
            connection = None
            connection_cm = closing(sqlite3.connect(filepath))
        else:
            filepath = None
            connection = sqlite3.connect(':memory:')
            connection_cm = nullcontext(connection)

        # Create tables, insert records, and accumulate `attr_keys`.
        with connection_cm as con:
            try:
                cur = con.executescript("""
                    PRAGMA main.synchronous = OFF;

                    CREATE TABLE main.attr_data (
                        attr_data_id INTEGER PRIMARY KEY,
                        attributes TEXT NOT NULL,
                        crosswalk_id INTEGER DEFAULT NULL,
                        UNIQUE (attributes)
                    );

                    CREATE TABLE main.quant_data (
                        index_id INTEGER NOT NULL,
                        attr_data_id INTEGER NOT NULL,
                        quant_value REAL,
                        FOREIGN KEY(attr_data_id) REFERENCES attr_data(attr_data_id)
                    );
                """)

                # Load data into tables.
                get_attr_data_id = self._get_attr_data_id_add_if_missing
                attr_keys: Set[str] = set()
                for index_id, attributes, quant_value in data:
                    attr_keys.update(attributes)
                    attr_data_id = get_attr_data_id(cur, attributes)
                    sql = """
                        INSERT INTO main.quant_data (index_id, attr_data_id, quant_value)
                        VALUES (?, ?, ?)
                    """
                    cur.execute(sql, (index_id, attr_data_id, quant_value))

                con.commit()
            except Exception:
                con.rollback()
                raise

        # Assign instance attributes.
        self._current_working_path = filepath
        self._in_memory_connection = connection
        self._data = None  # <- Assigned only when iteration begins.
        self._node = node
        self._index_columns = node.index_columns
        self._attr_keys = tuple(sorted(attr_keys))

        # Assign `close()` method (gets a callable finalizer object).
        self.close = weakref.finalize(self, self._finalizer)

    @staticmethod
    def _get_attr_data_id_add_if_missing(
        cur: sqlite3.Cursor, attributes: Dict[str, str]
    ) -> int:
        """Get associated 'attr_data_id' and add if missing."""
        parameters = (dumps(attributes, sort_keys=True),)

        sql = 'SELECT attr_data_id FROM attr_data WHERE attributes=?'
        cur.execute(sql, parameters)
        result = cur.fetchone()
        if result:
            return result[0]

        sql = 'INSERT INTO main.attr_data (attributes) VALUES (?)'
        cur.execute(sql, parameters)
        return cast(int, cur.lastrowid)  # Cast because we know it exists (just inserted).

    def _finalizer(self) -> None:
        """Close `_data` generator and remove temporary database file."""
        if self._data:
            self._data.close()

        if self._in_memory_connection:
            self._in_memory_connection.close()

        if self._current_working_path:
            with suppress(FileNotFoundError):
                os.unlink(self._current_working_path)

    @property
    def index_columns(self) -> List[str]:
        """The index (row labels) of the NodeReader."""
        return list(self._index_columns)

    @property
    def columns(self) -> List[str]:
        """All column labels of the NodeReader."""
        return list(self._index_columns + self._attr_keys + ('value',))

    def to_pandas(self, index: bool = False) -> 'pd.DataFrame':
        """Return data as a pandas DataFrame object."""
        try:
            import pandas as pd
        except ImportError:
            msg = (
                "Missing optional dependency 'pandas'.  Install pandas to "
                "use this method."
            )
            raise ImportError(msg) from None

        df = pd.DataFrame(self, columns=self.columns)
        string_cols = df.columns[:-1]  # Slice-off "value" column (float64).
        for col in string_cols:  # Using loop for memory efficiency.
            df[col] = df[col].astype('string')

        if index:
            df.set_index(self.index_columns, inplace=True)

        return df

    def __iter__(self) -> Self:
        """Returns self (iterator protocol)."""
        return self

    def __next__(self) -> Tuple[Union[str, float], ...]:
        """Return the next item from the NodeReader."""
        try:
            return next(self._data)  # type: ignore [arg-type]
        except TypeError:
            self._data = self._generate_reader_output()
            return next(self._data)

    @contextmanager
    def _managed_connection(
        self
    ) -> Generator[sqlite3.Connection, None, None]:
        """Acquire and manage a connection to a NodeReader's database."""
        in_memory_connection = self._in_memory_connection
        on_drive_path = self._current_working_path

        if in_memory_connection and on_drive_path:
            raise RuntimeError(
                'NodeReader must have _in_memory_connection or '
                '_in_memory_connection, but not both'
            )

        if on_drive_path:
            connection = sqlite3.connect(on_drive_path)
            connection.execute('PRAGMA main.synchronous = OFF')
        elif in_memory_connection:
            connection = in_memory_connection
        else:
            raise RuntimeError('unable to establish connection')

        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            if on_drive_path:
                connection.close()

    def _generate_reader_output(
        self
    ) -> Generator[Tuple[Union[str, float], ...], None, None]:
        """Return generator that iterates over NodeReader data."""
        attr_keys = self._attr_keys  # Assign locally to reduce dot-lookups.
        with self._node._managed_cursor() as node_cur:
            index_repo = self._node._dal.IndexRepository(node_cur)
            with self._managed_connection() as con:
                cur = con.execute("""
                    SELECT index_id, attributes, SUM(quant_value) AS quant_value
                    FROM main.quant_data
                    JOIN main.attr_data USING (attr_data_id)
                    GROUP BY index_id, attributes
                """)
                for index_id, attributes, quant_value in cur:
                    labels = cast(Index, index_repo.get(index_id)).labels
                    get_attr_value = loads(attributes).get  # Assign get() method directly.
                    attr_vals = tuple(get_attr_value(x) for x in attr_keys)
                    yield labels + attr_vals + (quant_value,)

    def translate(self, node: 'TopoNode') -> None:
        """Translate quantities to use the index of the target node.

        This method modifies the NodeReader in place and does not
        return a value.
        """
        # Get `old_index_hash` from source node.
        with self._node._managed_cursor() as node_cur:
            property_repo = self._node._dal.PropertyRepository(node_cur)
            old_index_hash = check_type(property_repo.get('index_hash'), str)

        # Translate "quant_data" table to use the index of the new *node*.
        with node._managed_cursor() as node_cur:
            relation_repo = node._dal.RelationRepository(node_cur)

            get_crosswalk_id = make_get_crosswalk_id_func(
                node_reference=self._node.unique_id,
                crosswalk_repo=node._dal.CrosswalkRepository(node_cur),
                other_index_hash=old_index_hash,
            )

            with self._managed_connection() as con:
                cur1 = con.cursor()
                cur2 = con.cursor()

                # Update 'crosswalk_id' to use ids from new node.
                cur1.execute('SELECT attr_data_id, attributes FROM main.attr_data')
                for attr_data_id, attributes in cur1:
                    attributes_obj = loads(attributes)
                    crosswalk_id = get_crosswalk_id(attributes_obj)
                    cur2.execute(
                        'UPDATE main.attr_data SET crosswalk_id=? WHERE attr_data_id=?',
                        (crosswalk_id, attr_data_id),
                    )

                # Create and populate 'new_quant_data' table.
                cur1.execute("""
                    CREATE TABLE main.new_quant_data (
                        index_id INTEGER NOT NULL,
                        attr_data_id INTEGER NOT NULL,
                        quant_value REAL,
                        FOREIGN KEY(attr_data_id) REFERENCES attr_data(attr_data_id)
                    )
                """)
                cur1.execute("""
                    SELECT index_id, attr_data_id, quant_value, crosswalk_id
                    FROM main.quant_data
                    JOIN main.attr_data USING (attr_data_id)
                """)
                for index_id, attr_data_id, quant_value, crosswalk_id in cur1:
                        rels = relation_repo.find_by_ids(
                            crosswalk_id=crosswalk_id,
                            other_index_id=index_id,
                        )
                        for rel in rels:
                            cur2.execute(
                                'INSERT INTO main.new_quant_data VALUES (?, ?, ?)',
                                (rel.index_id, attr_data_id, quant_value * rel.proportion),
                            )

                # Replace the old quantity table with the new table.
                cur1.execute('DROP TABLE main.quant_data')
                cur1.execute('ALTER TABLE main.new_quant_data RENAME TO quant_data')

        self._node = node  # Replace old node reference with the new node.
        self._index_columns = node.index_columns

    def __rshift__(self, other: 'TopoNode') -> 'NodeReader':
        """Translate quantities to the index of the *other* node."""
        self.translate(other)
        return self


def format_column(parts: List[str]) -> Union[Tuple[str, ...], str]:
    """Make *parts* into a label to use for a pivoted column.

    JSON arrays are returned as tuples::

        >>> format_column(['foo', 'bar', 'baz'])
        ('foo', 'bar', 'baz')

    Trailing empty strings are remove::

        >>> format_column(['foo', 'bar', ''])
        ('foo', 'bar')

    Arrays with only a single value are unwrapped::

        >>> format_column(['', 'bar', ''])
        'bar'

    Leading empty strings are not removed in multi-item lists::

        >>> format_column(['', 'bar', 'baz'])
        ('', 'bar', 'baz')
    """
    while parts and parts[-1] == '':
        parts.pop()  # Remove trailing empty strings.
    non_empty = tuple(filter(None, parts))  # Get non-empty values.

    if len(non_empty) == 1:
        return non_empty[0]  # Return string if single item.
    return tuple(parts)  # Return tuple of string if multiple items.


def pivot_reader(
    reader: NodeReader,
    columns: Iterable[str],
    #max_width: Optional[int] = 256,
) -> Generator[Sequence[Union[str, float, Tuple[Optional[str], ...]]], None, None]:
    """An experimental pivot implementation for ``NodeReader`` data."""
    # TODO: Fix type hinting for yield statements.

    columns = list(columns)

    with reader._managed_connection() as con:
        cur1 = con.cursor()
        cur2 = con.cursor()
        cur1.execute("""
            CREATE TEMPORARY TABLE pivot_temp (
                attr_data_id INTEGER NOT NULL,
                pivot_attrs TEXT NOT NULL
            )
        """)
        try:
            cur1.execute('SELECT attr_data_id, attributes FROM main.attr_data')
            for attr_data_id, attributes in cur1:
                attrs_dict = loads(attributes)
                pivot_attrs = [attrs_dict.get(x, '') for x in columns]

                if not any(pivot_attrs):
                    continue  # Skip to next if pivot attrs are all empty.

                cur2.execute(
                    'INSERT INTO temp.pivot_temp VALUES (?, ?)',
                    (attr_data_id, dumps(pivot_attrs)),
                )

            # Get distinct list of columns after populating table.
            cur1.execute("""
                SELECT DISTINCT pivot_attrs
                FROM temp.pivot_temp
                ORDER BY pivot_attrs
            """)
            pivoted_columns = [x[0] for x in cur1]  # Unwrap single item results.

            # Format and yield header row.
            formatted_columns = [format_column(loads(x)) for x in pivoted_columns]
            header_row = list(reader._node.index_columns) + formatted_columns
            yield header_row

            # Yield data rows.
            cur1.execute("""
                SELECT index_id, pivot_attrs, SUM(quant_value) AS quant_value
                FROM main.quant_data
                JOIN temp.pivot_temp USING (attr_data_id)
                GROUP BY index_id, pivot_attrs
                ORDER BY index_id
            """)
            with reader._node._managed_cursor() as node_cur:
                index_repo = reader._node._dal.IndexRepository(node_cur)
                for index_id, group in groupby(cur1, key=lambda row: row[0]):
                    label_vals = cast(Index, index_repo.get(index_id)).labels
                    row_dict = {row[1]: row[2] for row in group}
                    data_vals: List[Optional[float]]
                    data_vals = [row_dict.get(col) for col in pivoted_columns]
                    yield list(label_vals) + data_vals  # type: ignore [operator]

        finally:
            cur1.execute('DROP TABLE temp.pivot_temp')


def pivot_reader_to_pandas(
    reader: NodeReader,
    columns: Iterable[str],
    #max_width: Optional[int] = 256,
    index: bool = False
    ) -> 'pd.DataFrame':
    """An experimental pivot-to-pandas implementation for ``NodeReader``."""
    try:
        import pandas as pd
    except ImportError:
        msg = (
            "Missing optional dependency 'pandas'.  Install pandas to "
            "use this method."
        )
        raise ImportError(msg) from None

    pivoted_data = pivot_reader(reader, columns)
    pivoted_columns = next(pivoted_data)

    df = pd.DataFrame(pivoted_data, columns=pivoted_columns)  # type: ignore [call-overload]
    for col in reader.index_columns:  # Using loop for memory efficiency.
        df[col] = df[col].astype('string')

    if index:
        df.set_index(reader.index_columns, inplace=True)

    return df
