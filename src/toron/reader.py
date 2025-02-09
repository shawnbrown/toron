"""NodeReader implementation for the Toron project."""

import os
import sqlite3
import weakref
from contextlib import closing, suppress
from json import dumps, loads
from tempfile import NamedTemporaryFile

from toron._typing import (
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
    TYPE_CHECKING,
)
from toron.data_models import Index

if TYPE_CHECKING:
    from toron import TopoNode


def _create_reader_schema(cur: sqlite3.Cursor) -> None:
    """Create database tables for NodeReader instance."""
    cur.executescript("""
        CREATE TABLE attr_data (
            attr_data_id INTEGER PRIMARY KEY,
            attributes TEXT NOT NULL,
            matched_crosswalk_id INTEGER DEFAULT NULL,
            UNIQUE (attributes)
        );
        CREATE TABLE quant_data (
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


def _generate_reader_output(
    reader: 'NodeReader'
) -> Generator[Tuple[Union[str, float], ...], None, None]:
    """Return generator that iterates over NodeReader data."""
    with reader._node._managed_cursor() as node_cur:
        index_repo = reader._node._dal.IndexRepository(node_cur)
        with closing(sqlite3.connect(reader._filepath)) as con:
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
    _filepath: str
    _index_columns: Tuple[str, ...]
    _attr_keys: Tuple[str, ...]
    close: weakref.finalize

    def __init__(
        self,
        data: Iterator[Tuple[int, Dict[str, str], Optional[float]]],
        node: 'TopoNode',
    ) -> None:
        # Create temp file and get its path (resolve symlinks with realpath).
        with closing(NamedTemporaryFile(delete=False)) as f:
            filepath = os.path.realpath(f.name)

        # Create tables, insert records, and accumulate `attr_keys`.
        with closing(sqlite3.connect(filepath)) as con:
            try:
                cur = con.cursor()
                _create_reader_schema(cur)
                attr_keys = _insert_quant_data_get_attr_keys(cur, data)
                con.commit()
            except Exception:
                con.rollback()
                raise

        self._initializer(filepath, attr_keys, node)

    def _initializer(
        self, filepath: str, attr_keys: Iterable[str], node: 'TopoNode'
    ) -> None:
        """Assign instance attributes and `close()` method."""
        # Assign instance attributes.
        self._filepath = filepath
        self._data = None
        self._node = node
        self._index_columns = self._node.index_columns
        self._attr_keys = tuple(sorted(attr_keys))

        # Assign `close()` method (gets a callable finalizer object).
        self.close = weakref.finalize(self, self._finalizer)

    def _finalizer(self) -> None:
        """Close `_data` generator and remove temporary database file."""
        if self._data:
            self._data.close()

        with suppress(FileNotFoundError):
            os.unlink(self._filepath)

    @property
    def index_columns(self) -> List[str]:
        return list(self._index_columns)

    @property
    def columns(self) -> List[str]:
        return list(self._index_columns + self._attr_keys + ('value',))

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Tuple[Union[str, float], ...]:
        try:
            return next(self._data)  # type: ignore [arg-type]
        except TypeError:
            self._data = _generate_reader_output(self)
            return next(self._data)
