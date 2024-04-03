"""IndexRepository and related objects using SQLite."""

import sqlite3
from dataclasses import asdict
from json import dumps as json_dumps

from toron._typing import (
    List,
    Optional,
    Union,
)

from . import schema
from .base_classes import (
    Index, BaseIndexRepository,
    Location, BaseLocationRepository,
    Structure, BaseStructureRepository,
    Weighting, BaseWeightingRepository,
    Weight, BaseWeightRepository,
)


class IndexRepository(BaseIndexRepository):
    def __init__(self, data_reader: sqlite3.Cursor) -> None:
        """Initialize a new IndexRepository instance."""
        self._cursor = data_reader

    def add(self, value: str, *values: str) -> None:
        """Add a record to the repository."""
        values = (value,) + values
        qmarks = ", ".join("?" * len(values))
        sql = f'INSERT INTO main.node_index VALUES (NULL, {qmarks})'
        self._cursor.execute(sql, values)

    def get(self, id: int) -> Optional[Index]:
        """Get a record from the repository."""
        self._cursor.execute(
            'SELECT * FROM main.node_index WHERE index_id=?', (id,)
        )
        record = self._cursor.fetchone()
        if record:
            return Index(*record)
        return None

    def update(self, record: Index) -> None:
        """Update a record in the repository."""
        self._cursor.execute(f"PRAGMA main.table_info('node_index')")
        columns = ', '.join(row[1] for row in self._cursor.fetchall()[1:])
        qmarks = ', '.join('?' * len(record.values))
        sql = f"""
            UPDATE main.node_index
            SET ({columns}) = ({qmarks})
            WHERE index_id=?
        """
        self._cursor.execute(sql, record.values + (record.id,))

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.node_index WHERE index_id=?', (id,)
        )


class LocationRepository(BaseLocationRepository):
    def __init__(self, data_reader: sqlite3.Cursor) -> None:
        """Initialize a new PropertyRepository instance."""
        self._cursor = data_reader

    def add(self, value: str, *values: str) -> None:
        """Add a record to the repository."""
        values = (value,) + values
        qmarks = ", ".join("?" * len(values))
        sql = f'INSERT INTO main.location VALUES (NULL, {qmarks})'
        self._cursor.execute(sql, values)

    def get(self, id: int) -> Optional[Location]:
        """Get a record from the repository."""
        self._cursor.execute(
            'SELECT * FROM main.location WHERE _location_id=?', (id,)
        )
        record = self._cursor.fetchone()
        if record:
            return Location(*record)
        return None

    def update(self, record: Location) -> None:
        """Update a record in the repository."""
        self._cursor.execute(f"PRAGMA main.table_info('location')")
        columns = ', '.join(row[1] for row in self._cursor.fetchall()[1:])
        qmarks = ', '.join('?' * len(record.values))
        sql = f"""
            UPDATE main.location
            SET ({columns}) = ({qmarks})
            WHERE _location_id=?
        """
        self._cursor.execute(sql, record.values + (record.id,))

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.location WHERE _location_id=?', (id,)
        )


class StructureRepository(BaseStructureRepository):
    def __init__(self, data_reader: sqlite3.Cursor) -> None:
        """Initialize a new StructureRepository instance."""
        self._cursor = data_reader

    def add(self, value: str, *values: str) -> None:
        """Add a record to the repository."""
        values = (value,) + values
        qmarks = ', '.join('?' * len(values))
        sql = f'INSERT INTO main.structure VALUES (NULL, NULL, {qmarks})'
        self._cursor.execute(sql, values)

    def get(self, id: int) -> Optional[Structure]:
        """Get a record from the repository."""
        self._cursor.execute(
            'SELECT * FROM main.structure WHERE _structure_id=?', (id,)
        )
        record = self._cursor.fetchone()
        if record:
            return Structure(*record)
        return None

    def get_all(self) -> List[Structure]:
        """Get all records sorted from most to least granular."""
        self._cursor.execute(
            'SELECT * FROM main.structure ORDER BY _granularity DESC'
        )
        return self._cursor.fetchall()

    def update(self, record: Structure) -> None:
        """Update a record in the repository."""
        self._cursor.execute(f"PRAGMA main.table_info('structure')")
        columns = ', '.join(row[1] for row in self._cursor.fetchall()[2:])
        qmarks = ', '.join('?' * len(record.values))
        sql = f"""
            UPDATE main.structure
            SET (_granularity, {columns}) = (?, {qmarks})
            WHERE _structure_id=?
        """
        parameters = (record.granularity,) + record.values + (record.id,)
        self._cursor.execute(sql, parameters)

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.structure WHERE _structure_id=?', (id,)
        )


class WeightingRepository(BaseWeightingRepository):
    def __init__(self, data_reader: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = data_reader

    def add(
        self,
        name: str,
        description: Optional[str] = None,
        selectors: Optional[Union[List[str], str]] = None,
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

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.weight WHERE weight_id=?', (id,)
        )

    #def find_by_weighting_id(self, weighting_id: int) -> Iterable[Weight]:
    #    """Filter to records associated with the given weighting."""
