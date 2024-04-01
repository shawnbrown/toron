"""QuantityRepository and related objects using SQLite."""

import sqlite3
from dataclasses import asdict

from toron._typing import Optional

from .base_classes import Quantity, BaseQuantityRepository


class QuantityRepository(BaseQuantityRepository):
    def __init__(self, data_reader: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = data_reader

    def add(self, location_id: int, attribute_id: int, value: float) -> None:
        """Add a record to the repository."""
        sql = """
            INSERT INTO main.quantity (_location_id, attribute_id, quantity_value)
            VALUES (?, ?, ?)
        """
        self._cursor.execute(sql, (location_id, attribute_id, value))

    def get(self, id: int) -> Optional[Quantity]:
        """Get a record from the repository."""
        self._cursor.execute(
            'SELECT * FROM main.quantity WHERE quantity_id=?', (id,)
        )
        record = self._cursor.fetchone()
        if record:
            return Quantity(*record)
        return None

    def update(self, record: Quantity) -> None:
        """Update a record in the repository."""
        sql = f"""
            UPDATE main.quantity
            SET
                _location_id=:location_id,
                attribute_id=:attribute_id,
                quantity_value=:value
            WHERE quantity_id=:id
        """
        self._cursor.execute(sql, asdict(record))

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.quantity WHERE quantity_id=?', (id,)
        )

    #def find_by_attribute_id(self, attribute_id: int) -> Iterable[Quantity]:
    #    """Filter to records associated with the given attribute."""
