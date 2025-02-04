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
    Optional,
    Self,
    Tuple,
)


class NodeReader(object):
    """An iterator for base level TopoNode data."""
    def __init__(
        self,
        data: Iterator[Tuple[int, Dict[str, str], Optional[float]]],
    ) -> None:
        # Create temp file and get its path (resolve symlinks with realpath).
        with closing(NamedTemporaryFile(delete=False)) as f:
            self._filepath = os.path.realpath(f.name)

        # Assign finalizer as a `close()` method.
        self.close = weakref.finalize(self, self._cleanup)

        # Create tables and insert records.
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

        self._data: Optional[Generator[Tuple[int, Dict[str, str], Optional[float]], None, None]]
        self._data = None

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
    ) -> Generator[Tuple[int, Dict[str, str], Optional[float]], None, None]:
        with closing(sqlite3.connect(self._filepath)) as con:
            cur = con.execute("""
                SELECT index_id, attributes, SUM(quant_value) AS quant_value
                FROM main.quant_data
                JOIN main.attr_data USING (attr_data_id)
                GROUP BY index_id, attributes
            """)
            for index_id, attributes, quant_value in cur:
                yield index_id, loads(attributes), quant_value

    def _cleanup(self):
        if self._data:
            self._data.close()

        with suppress(FileNotFoundError):
            os.unlink(self._filepath)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Tuple[int, Dict[str, str], Optional[float]]:
        try:
            return next(self._data)
        except TypeError:
            self._data = self._generate_records()
            return next(self._data)
