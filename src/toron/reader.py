"""NodeReader implementation for the Toron project."""

import os
import sqlite3
import weakref
from contextlib import closing, contextmanager, suppress
from itertools import chain
from json import dumps, loads
from tempfile import NamedTemporaryFile

from toron._typing import (
    Callable,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Self,
    Set,
    Tuple,
    Union,
    cast,
    overload,
    TYPE_CHECKING,
)
from toron.data_models import Index
from toron.data_service import (
    find_crosswalks_by_node_reference,
)
from toron.selectors import (
    parse_selector,
    get_greatest_unique_specificity,
)

if TYPE_CHECKING:
    import pandas as pd
    from toron import TopoNode
    from toron.data_models import Crosswalk


def _create_reader_schema(cur: sqlite3.Cursor, schema: str = 'main') -> None:
    """Create database tables for NodeReader instance."""
    cur.executescript(f"""
        CREATE TABLE {schema}.attr_data (
            attr_data_id INTEGER PRIMARY KEY,
            attributes TEXT NOT NULL,
            matched_crosswalk_id INTEGER DEFAULT NULL,
            UNIQUE (attributes)
        );
        CREATE TABLE {schema}.quant_data (
            index_id INTEGER NOT NULL,
            attr_data_id INTEGER NOT NULL,
            quant_value REAL,
            FOREIGN KEY(attr_data_id) REFERENCES attr_data(attr_data_id)
        );
    """)


def _get_attr_data_id_add_if_missing(
    cur: sqlite3.Cursor, attributes: Dict[str, str]
) -> int:
    """Get associated 'attr_data_id' and add if missing."""
    parameters = (dumps(attributes, sort_keys=False),)

    sql = 'SELECT attr_data_id FROM attr_data WHERE attributes=?'
    cur.execute(sql, parameters)
    result = cur.fetchone()
    if result:
        return result[0]

    sql = 'INSERT INTO main.attr_data (attributes) VALUES (?)'
    cur.execute(sql, parameters)
    return cast(int, cur.lastrowid)  # Cast because we know it exists (just inserted).


def _insert_quant_data_get_attr_keys(
    cur: sqlite3.Cursor,
    data: Iterator[Tuple[int, Dict[str, str], Optional[float]]],
) -> Set[str]:
    """Insert 'quant_data' values and get associated attribute keys."""
    attr_keys: Set[str] = set()
    for index_id, attributes, quant_value in data:
        attr_keys.update(attributes)
        attr_data_id = _get_attr_data_id_add_if_missing(cur, attributes)
        sql = """
            INSERT INTO main.quant_data (index_id, attr_data_id, quant_value)
            VALUES (?, ?, ?)
        """
        cur.execute(sql, (index_id, attr_data_id, quant_value))

    return attr_keys


def _insert_raw_quant_data(
    cur: sqlite3.Cursor,
    data: Iterator[Tuple[int, int, Optional[float]]],
) -> None:
    """Insert raw 'quant_data' by id values."""
    sql = """
        INSERT INTO main.quant_data (index_id, attr_data_id, quant_value)
        VALUES (?, ?, ?)
    """
    cur.executemany(sql, data)


@contextmanager
def _managed_reader_connection(
    reader: 'NodeReader'
) -> Generator[sqlite3.Connection, None, None]:
    """Acquire and manage a connection to a NodeReader's database."""
    in_memory_connection = reader._in_memory_connection
    on_drive_path = reader._current_working_path

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
    reader: 'NodeReader'
) -> Generator[Tuple[Union[str, float], ...], None, None]:
    """Return generator that iterates over NodeReader data."""
    with reader._node._managed_cursor() as node_cur:
        index_repo = reader._node._dal.IndexRepository(node_cur)
        with _managed_reader_connection(reader) as con:
            cur = con.execute("""
                SELECT index_id, attributes, SUM(quant_value) AS quant_value
                FROM main.quant_data
                JOIN main.attr_data USING (attr_data_id)
                GROUP BY index_id, attributes
            """)
            for index_id, attributes, quant_value in cur:
                labels = cast(Index, index_repo.get(index_id)).labels
                attr_vals = tuple(loads(attributes).values())
                attr_dict = loads(attributes)
                attr_vals = tuple(attr_dict.get(x, '') for x in reader._attr_keys)
                yield labels + attr_vals + (quant_value,)


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
        if cache_to_drive:
            # Create temp file and get its path (resolve symlinks with realpath).
            with closing(NamedTemporaryFile(delete=False)) as f:
                filepath = os.path.realpath(f.name)
            connection = None

            with closing(sqlite3.connect(filepath)) as temp_con:
                try:
                    # Create tables, insert records, and accumulate `attr_keys`.
                    cur = temp_con.cursor()
                    cur.execute('PRAGMA main.synchronous = OFF')
                    _create_reader_schema(cur)
                    attr_keys = _insert_quant_data_get_attr_keys(cur, data)
                    temp_con.commit()
                except Exception:
                    temp_con.rollback()
                    raise

            self._initializer(filepath, connection, attr_keys, node)

        else:
            filepath = None
            connection = sqlite3.connect(':memory:')  # Persistent connection.

            try:
                # Create tables, insert records, and accumulate `attr_keys`.
                cur = connection.cursor()
                _create_reader_schema(cur)
                attr_keys = _insert_quant_data_get_attr_keys(cur, data)
                connection.commit()
            except Exception:
                connection.rollback()
                raise

            self._initializer(filepath, connection, attr_keys, node)

    @overload
    def _initializer(
        self,
        filepath: None,
        connection: sqlite3.Connection,
        attr_keys: Iterable[str],
        node: 'TopoNode',
    ) -> None:
        ...
    @overload
    def _initializer(
        self,
        filepath: str,
        connection: None,
        attr_keys: Iterable[str],
        node: 'TopoNode',
    ) -> None:
        ...
    def _initializer(self, filepath, connection, attr_keys, node):
        """Assign instance attributes and `close()` method."""
        # Assign instance attributes.
        self._current_working_path = filepath
        self._in_memory_connection = connection
        self._data = None  # <- Assigned only when iteration begins.
        self._node = node
        self._index_columns = node.index_columns
        self._attr_keys = tuple(sorted(attr_keys))

        # Assign `close()` method (gets a callable finalizer object).
        self.close = weakref.finalize(self, self._finalizer)

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
        return list(self._index_columns)

    @property
    def columns(self) -> List[str]:
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
        return self

    def __next__(self) -> Tuple[Union[str, float], ...]:
        try:
            return next(self._data)  # type: ignore [arg-type]
        except TypeError:
            self._data = _generate_reader_output(self)
            return next(self._data)

    def translate(self, node: 'TopoNode') -> None:
        """Translate quantities to use the index of the target node."""
        # Get `old_index_hash` from source node.
        with self._node._managed_cursor() as node_cur:
            property_repo = self._node._dal.PropertyRepository(node_cur)
            old_index_hash = property_repo.get('index_hash')

        with node._managed_cursor() as node_cur:
            crosswalk_repo = node._dal.CrosswalkRepository(node_cur)
            relation_repo = node._dal.RelationRepository(node_cur)

            # Make `get_crosswalk_id()` function.
            crosswalks = find_crosswalks_by_node_reference(
                node_reference=self._node.unique_id,
                crosswalk_repo=crosswalk_repo,
            )
            if not crosswalks:
                raise RuntimeError('no crosswalk found connecting nodes')
            crosswalks = [x for x in crosswalks if x.other_index_hash == old_index_hash]
            if not crosswalks:
                raise RuntimeError('crosswalks are out of date, need to relink')
            get_crosswalk_id = _make_get_crosswalk_id_func(crosswalks)

            with _managed_reader_connection(self) as con:
                cur1 = con.cursor()
                cur2 = con.cursor()

                # Update 'matched_crosswalk_id' to use ids from new node.
                cur1.execute('SELECT attr_data_id, attributes FROM main.attr_data')
                for attr_data_id, attributes in cur1:
                    attributes_obj = loads(attributes)
                    matched_crosswalk_id = get_crosswalk_id(attributes_obj)
                    cur2.execute(
                        'UPDATE main.attr_data SET matched_crosswalk_id=? WHERE attr_data_id=?',
                        (matched_crosswalk_id, attr_data_id),
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
                    SELECT index_id, attr_data_id, quant_value, matched_crosswalk_id
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


def _make_get_crosswalk_id_func(
    crosswalks: List['Crosswalk']
) -> Callable[[Dict[str, str]], int]:
    """Build a `get_crosswalk_id_func()` to match crosswalks."""
    # Get the default crosswalk and make sure it's locally complete.
    default_crosswalk_id = None
    for crosswalk in crosswalks:
        if crosswalk.is_default:
            default_crosswalk_id = crosswalk.id
            break
    else:  # IF NO BREAK!
        raise RuntimeError('no default crosswalk found for node')

    # Build dict of index id values and attribute selector objects.
    crosswalks = [x for x in crosswalks if x.is_locally_complete and x.selectors]
    func = lambda selectors: [parse_selector(s) for s in selectors]
    selector_dict = {x.id: func(x.selectors) for x in crosswalks}

    # Define function to match the crosswalk with the greatest unique
    # specificity (closes over selector_dict and default_crosswalk_id).
    def get_crosswalk_id_func(attributes: Dict[str, str]) -> int:
        crosswalk_id = get_greatest_unique_specificity(
            row_dict=attributes,
            selector_dict=selector_dict,
            default=default_crosswalk_id,
        )
        return crosswalk_id

    return get_crosswalk_id_func
