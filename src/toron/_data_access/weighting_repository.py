"""WeightingRepository and related objects using SQLite."""

import sqlite3
from json import dumps as json_dumps

from toron._typing import (
    Optional,
    Sequence,
    Union,
)

from .base_classes import Weighting, BaseWeightingRepository


class WeightingRepository(BaseWeightingRepository):
    def __init__(self, data_reader: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = data_reader

    def add(
        self,
        name: str,
        description: Optional[str] = None,
        selectors: Optional[Union[Sequence[str], str]] = None,
        is_complete: bool = False,
    ) -> None:
        """Add a record to the repository."""
        if selectors:
            if isinstance(selectors, str):
                selectors = [selectors]
            selectors = json_dumps(selectors)

        sql = """
            INSERT INTO main.weighting (name, description, selectors, is_complete)
            VALUES (?, ?, ?, ?)
        """
        self._cursor.execute(sql, (name, description, selectors, is_complete))

    def get(self, id: int) -> Optional[Weighting]:
        """Get a record from the repository."""
        self._cursor.execute(
            'SELECT * FROM main.weighting WHERE weighting_id=?', (id,)
        )
        record = self._cursor.fetchone()
        if record:
            return Weighting(*record)
        return None

    def update(self, record: Weighting) -> None:
        """Update a record in the repository."""
        sql = f"""
            UPDATE main.weighting
            SET
                name=?,
                description=?,
                selectors=?,
                is_complete=?
            WHERE weighting_id=?
        """
        parameters = [
            record.name,
            record.description,
            json_dumps(record.selectors),
            record.is_complete,
            record.id,
        ]
        self._cursor.execute(sql, parameters)

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.weighting WHERE weighting_id=?', (id,)
        )
