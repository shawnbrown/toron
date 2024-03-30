"""WeightRepository and related objects using SQLite."""

import sqlite3
from dataclasses import asdict

from toron._typing import Optional

from .base_classes import Weight, BaseWeightRepository


class WeightRepository(BaseWeightRepository):
    def __init__(self, data_reader: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = data_reader

    def add(self, weighting_id: int, index_id: int, value: int) -> None:
        """Add a record to the repository."""
        sql = """
            INSERT INTO main.weight (weighting_id, index_id, weight_value)
            VALUES (?, ?, ?)
        """
        self._cursor.execute(sql, (weighting_id, index_id, value))

    def get(self, id: int) -> Optional[Weight]:
        """Get a record from the repository."""
        self._cursor.execute(
            'SELECT * FROM main.weight WHERE weight_id=?', (id,)
        )
        record = self._cursor.fetchone()
        if record:
            return Weight(*record)
        return None

    def update(self, record: Weight) -> None:
        """Update a record in the repository."""
        sql = f"""
            UPDATE main.weight
            SET
                weighting_id=:weighting_id,
                index_id=:index_id,
                weight_value=:value
            WHERE weight_id=:id
        """
        self._cursor.execute(sql, asdict(record))

    def delete(self, id: int):
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.weight WHERE weight_id=?', (id,)
        )

    #def find_by_weighting_id(self, weighting_id: int) -> Iterable[Weight]:
    #    """Filter to records associated with the given weighting."""
