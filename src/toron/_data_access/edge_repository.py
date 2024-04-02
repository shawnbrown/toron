"""EdgeRepository and related objects using SQLite."""

import sqlite3
from json import dumps as json_dumps

from toron._typing import (
    Dict,
    List,
    Optional,
    Union,
)

from .base_classes import JsonTypes, Edge, BaseEdgeRepository


class EdgeRepository(BaseEdgeRepository):
    def __init__(self, data_reader: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = data_reader

    def add(
        self,
        name: str,
        other_unique_id: str,
        *,
        other_filename_hint: Optional[str] = None,
        other_index_hash: Optional[str] = None,
        description: Optional[str] = None,
        selectors: Optional[Union[List[str], str]] = None,
        user_properties: Optional[Dict[str, JsonTypes]] = None,
        is_locally_complete: bool = False,
        is_default: bool = False,
    ) -> None:
        """Add a record to the repository."""
        if isinstance(selectors, str):
            selectors = [selectors]

        sql = """
            INSERT INTO main.edge (
                name,
                other_unique_id,
                other_filename_hint,
                other_index_hash,
                description,
                selectors,
                user_properties,
                is_locally_complete,
                is_default
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        parameters = (
            name,
            other_unique_id,
            other_filename_hint,
            other_index_hash,
            description,
            json_dumps(selectors) if selectors else None,
            json_dumps(user_properties) if user_properties else None,
            is_locally_complete,
            True if is_default else None,
        )
        self._cursor.execute(sql, parameters)

    def get(self, id: int) -> Optional[Edge]:
        """Get a record from the repository."""
        self._cursor.execute(
            'SELECT * FROM main.edge WHERE edge_id=?', (id,)
        )
        record = self._cursor.fetchone()
        if record:
            a, b, c, d, e, f, g, h, i, j = record  # Faster to unpack all than to slice.
            return Edge(a, b, c, d, e, f, g, h, bool(i), bool(j))
        return None

    def update(self, record: Edge) -> None:
        """Update a record in the repository."""
        sql = f"""
            UPDATE main.edge
            SET
                name=?,
                other_unique_id=?,
                other_filename_hint=?,
                other_index_hash=?,
                description=?,
                selectors=?,
                user_properties=?,
                is_locally_complete=?,
                is_default=?
            WHERE edge_id=?
        """
        parameters = [
                record.name,
                record.other_unique_id,
                record.other_filename_hint,
                record.other_index_hash,
                record.description,
                json_dumps(record.selectors) if record.selectors else None,
                json_dumps(record.user_properties) if record.user_properties else None,
                record.is_locally_complete,
                True if record.is_default else None,
                record.id,
        ]
        self._cursor.execute(sql, parameters)

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.edge WHERE edge_id=?', (id,)
        )
