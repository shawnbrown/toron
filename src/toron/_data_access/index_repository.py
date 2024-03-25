"""IndexRepository and related objects using SQLite."""

import sqlite3

from toron._typing import (
    Optional,
    Sequence,
    Tuple,
)

from .base_classes import Index, BaseIndexRepository


class IndexRepository(BaseIndexRepository):
    def __init__(self, data_reader: sqlite3.Cursor) -> None:
        """Initialize a new IndexRepository instance."""
        self._cursor = data_reader

    def add(self, value: str, *values: str) -> None:
        """Add a record to the repository."""
        raise NotImplementedError

    def get(self, id: int) -> Optional[Index]:
        """Get a record from the repository."""
        raise NotImplementedError

    def update(self, record: Index) -> None:
        """Update a record in the repository."""
        raise NotImplementedError

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        raise NotImplementedError

    def add_columns(self, *columns: str):
        """Add new columns to the repository."""
        self._cursor.execute("""
            DROP INDEX IF EXISTS main.unique_nodeindex_index
        """)

        for column in columns:
            self._cursor.execute(f"""
                ALTER TABLE main.node_index
                  ADD COLUMN {column}
                  TEXT
                  NOT NULL
                  CHECK ({column} != '')
                  DEFAULT '-'
            """)

        self._cursor.execute(f"""
            CREATE UNIQUE INDEX main.unique_nodeindex_index
              ON node_index({', '.join(columns)})
        """)

    def get_columns(self) -> Tuple[str, ...]:
        """Get a tuple of column names from the repository."""
        self._cursor.execute(f"PRAGMA main.table_info('node_index')")
        columns = tuple(row[1] for row in self._cursor.fetchall())
        return columns[1:]  # Return columns (slicing-off index_id).
