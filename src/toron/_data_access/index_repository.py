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

    def add(self, values: Sequence[str]) -> None:
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

    def get_columns(self) -> Tuple[str, ...]:
        """Get a tuple of column names from the repository."""
        raise NotImplementedError
