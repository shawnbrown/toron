"""IndexRepository and related objects using SQLite."""

import sqlite3

from toron._typing import (
    Optional,
    Sequence,
    Tuple,
)

from . import schema
from .base_classes import Index, BaseIndexRepository


class IndexRepository(BaseIndexRepository):
    def __init__(self, data_reader: sqlite3.Cursor) -> None:
        """Initialize a new IndexRepository instance."""
        self._cursor = data_reader

    def add(self, value: str, *values: str) -> None:
        """Add a record to the repository."""
        values = (value,) + values
        qmarks = ", ".join("?" * len(values))
        sql = f'INSERT INTO node_index VALUES (NULL, {qmarks})'
        self._cursor.execute(sql, values)

    def get(self, id: int) -> Optional[Index]:
        """Get a record from the repository."""
        self._cursor.execute(
            'SELECT * FROM main.node_index WHERE index_id=?', (id,)
        )
        record = self._cursor.fetchone()
        if record:
            return Index(*record)
        return None

    def update(self, record: Index) -> None:
        """Update a record in the repository."""
        columns = ', '.join(self.get_columns())
        qmarks = ', '.join('?' * len(record.values))
        sql = f"""
            UPDATE main.node_index
            SET ({columns}) = ({qmarks})
            WHERE index_id=?
        """
        self._cursor.execute(sql, record.values + (record.id,))

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.node_index WHERE index_id=?', (id,)
        )
