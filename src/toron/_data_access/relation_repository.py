"""RelationRepository and related objects using SQLite."""

import sqlite3

from toron._typing import Optional

from .base_classes import Relation, BaseRelationRepository


class RelationRepository(BaseRelationRepository):
    def __init__(self, data_reader: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = data_reader

    def add(
        self,
        edge_id: int,
        other_index_id: int,
        index_id: int,
        value: float,
        proportion: Optional[float] = None,
        mapping_level: Optional[bytes] = None,
    ) -> None:
        """Add a record to the repository."""
        sql = """
            INSERT INTO main.relation (
                edge_id,
                other_index_id,
                index_id,
                relation_value,
                proportion,
                mapping_level
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """
        parameters = (
            edge_id,
            other_index_id,
            index_id,
            value,
            proportion,
            bytes(mapping_level) if mapping_level else None,
        )
        self._cursor.execute(sql, parameters)

    def get(self, id: int) -> Optional[Relation]:
        """Get a record from the repository."""
        self._cursor.execute(
            'SELECT * FROM main.relation WHERE relation_id=?', (id,)
        )
        record = self._cursor.fetchone()
        if record:
            return Relation(*record)
        return None

    def update(self, record: Relation) -> None:
        """Update a record in the repository."""
        sql = f"""
            UPDATE main.relation
            SET edge_id=?,
                other_index_id=?,
                index_id=?,
                relation_value=?,
                proportion=?,
                mapping_level=?
            WHERE relation_id=?
        """
        parameters = (
            record.edge_id,
            record.other_index_id,
            record.index_id,
            record.value,
            record.proportion,
            record.mapping_level,
            record.id,
        )
        self._cursor.execute(sql, parameters)

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.relation WHERE relation_id=?', (id,)
        )
