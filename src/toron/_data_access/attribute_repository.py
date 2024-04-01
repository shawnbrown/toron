"""AttributeRepository and related objects using SQLite."""

import sqlite3
from json import dumps as json_dumps

from toron._typing import (
    Dict,
    Optional,
)

from .base_classes import Attribute, BaseAttributeRepository


class AttributeRepository(BaseAttributeRepository):
    def __init__(self, data_reader: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = data_reader

    def add(self, value: Dict[str, str]) -> None:
        """Add a record to the repository."""
        sql = 'INSERT INTO main.attribute (attribute_value) VALUES (?)'
        self._cursor.execute(sql, (json_dumps(value, sort_keys=True),))

    def get(self, id: int) -> Optional[Attribute]:
        """Get a record from the repository."""
        self._cursor.execute(
            'SELECT * FROM main.attribute WHERE attribute_id=?', (id,)
        )
        record = self._cursor.fetchone()
        if record:
            return Attribute(*record)
        return None

    def update(self, record: Attribute) -> None:
        """Update a record in the repository."""
        self._cursor.execute(
            'UPDATE main.attribute SET attribute_value=? WHERE attribute_id=?',
            (json_dumps(record.value, sort_keys=True), record.id),
        )

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.attribute WHERE attribute_id=?', (id,)
        )
