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
    Iterator,
    List,
    Optional,
    Self,
    Set,
    Tuple,
    Union,
    cast,
)
from toron.data_models import Index
from toron.node import TopoNode


class NodeReader(object):
    """An iterator for base level TopoNode data."""
    def __init__(
        self,
        data: Iterator[Tuple[int, Dict[str, str], Optional[float]]],
        node: TopoNode,
    ) -> None:
        # Create temp file and get its path (resolve symlinks with realpath).
        with closing(NamedTemporaryFile(delete=False)) as f:
            self._filepath = os.path.realpath(f.name)

        # Assign finalizer as a `close()` method.
        self.close = weakref.finalize(self, self._cleanup)

        # Create tables, insert records, and accumulate `attr_keys`.
        attr_keys: Set[str] = set()
        with closing(sqlite3.connect(self._filepath)) as con:
            try:
                cur = con.executescript("""
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
                for index_id, attributes, quant_value in data:
                    attr_keys.update(attributes)
                    attr_data_id = self._add_attr_get_id(cur, attributes)
                    sql = """
                        INSERT INTO main.quant_data (index_id, attr_data_id, quant_value)
                        VALUES (?, ?, ?)
                    """
                    cur.execute(sql, (index_id, attr_data_id, quant_value))

                con.commit()
            except Exception:
                con.rollback()
                raise

        self._data: Optional[Generator[Tuple[Union[str, float], ...], None, None]]
        self._data = None
        self._node = node
        self._index_columns = self._node.index_columns
        self._attr_keys = tuple(sorted(attr_keys))

    @property
    def index_columns(self) -> List[str]:
        return list(self._index_columns)

    @property
    def columns(self) -> List[str]:
        return list(self._index_columns + self._attr_keys + ('value',))

    @staticmethod
    def _add_attr_get_id(cur: sqlite3.Cursor, attributes: Dict[str, str]):
        parameters = (dumps(attributes, sort_keys=False),)

        sql = 'SELECT attr_data_id FROM attr_data WHERE attributes=?'
        cur.execute(sql, parameters)
        result = cur.fetchone()
        if result:
            return result[0]

        sql = 'INSERT INTO main.attr_data (attributes) VALUES (?)'
        cur.execute(sql, parameters)
        return cur.lastrowid  # Row id of the last inserted row.

    def _generate_records(
        self
    ) -> Generator[Tuple[Union[str, float], ...], None, None]:
        with self._node._managed_cursor() as node_cur:
            index_repo = self._node._dal.IndexRepository(node_cur)
            with closing(sqlite3.connect(self._filepath)) as con:
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
                    attr_vals = tuple(attr_dict.get(x, '') for x in self._attr_keys)
                    yield labels + attr_vals + (quant_value,)

    def _cleanup(self):
        if self._data:
            self._data.close()

        with suppress(FileNotFoundError):
            os.unlink(self._filepath)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Tuple[Union[str, float], ...]:
        try:
            return next(self._data)  # type: ignore [arg-type]
        except TypeError:
            self._data = self._generate_records()
            return next(self._data)
