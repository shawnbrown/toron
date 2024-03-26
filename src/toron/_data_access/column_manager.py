"""ColumnManager and related objects using SQLite."""

import sqlite3

from toron._typing import (
    Dict,
    Iterable,
    Tuple,
)

from . import schema
from .base_classes import BaseColumnManager


class ColumnManager(BaseColumnManager):
    def __init__(self, data_reader: sqlite3.Cursor) -> None:
        """Initialize a new instance."""
        self._cursor = data_reader

    def add_columns(self, *columns: str) -> None:
        """Add new label columns."""
        schema.drop_schema_index_constraints(self._cursor)

        for column in columns:
            self._cursor.execute(f"""
                ALTER TABLE main.node_index
                  ADD COLUMN {column}
                  TEXT
                  NOT NULL
                  CHECK ({column} != '')
                  DEFAULT '-'
            """)

        schema.create_schema_index_constraints(self._cursor)

    def get_columns(self) -> Tuple[str, ...]:
        """Get a tuple of label column names."""
        self._cursor.execute(f"PRAGMA main.table_info('node_index')")
        columns = tuple(row[1] for row in self._cursor.fetchall())
        return columns[1:]  # Return columns (slicing-off index_id).

    def update_columns(self, mapping: Dict[str, str]) -> None:
        """Update label column names."""
        raise NotImplementedError

    def delete_columns(self, columns: Iterable[str]) -> None:
        """Delete label columns."""
        raise NotImplementedError
