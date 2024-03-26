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
        raise NotImplementedError

    def get_columns(self) -> Tuple[str, ...]:
        """Get a tuple of label column names."""
        raise NotImplementedError

    def update_columns(self, mapping: Dict[str, str]) -> None:
        """Update label column names."""
        raise NotImplementedError

    def delete_columns(self, columns: Iterable[str]) -> None:
        """Delete label columns."""
        raise NotImplementedError
