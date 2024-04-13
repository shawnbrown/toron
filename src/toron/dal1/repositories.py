"""IndexRepository and related objects using SQLite."""

import sqlite3
from dataclasses import asdict
from json import dumps as json_dumps

from toron._typing import (
    Dict,
    List,
    Optional,
    Union,
)

from . import schema
from ..data_models import (
    Index, BaseIndexRepository,
    Location, BaseLocationRepository,
    Structure, BaseStructureRepository,
    Weighting, BaseWeightingRepository,
    Weight, BaseWeightRepository,
    Attribute, BaseAttributeRepository,
    Quantity, BaseQuantityRepository,
    Crosswalk, BaseCrosswalkRepository,
    Relation, BaseRelationRepository,
    JsonTypes, BasePropertyRepository,
)


class IndexRepository(BaseIndexRepository):
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new IndexRepository instance."""
        self._cursor = cursor

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
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new PropertyRepository instance."""
        self._cursor = cursor

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
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new StructureRepository instance."""
        self._cursor = cursor

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
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = cursor

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
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = cursor

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


class AttributeRepository(BaseAttributeRepository):
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = cursor

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


class QuantityRepository(BaseQuantityRepository):
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = cursor

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


class CrosswalkRepository(BaseCrosswalkRepository):
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = cursor

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
            INSERT INTO main.crosswalk (
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

    def get(self, id: int) -> Optional[Crosswalk]:
        """Get a record from the repository."""
        self._cursor.execute(
            'SELECT * FROM main.crosswalk WHERE crosswalk_id=?', (id,)
        )
        record = self._cursor.fetchone()
        if record:
            a, b, c, d, e, f, g, h, i, j = record  # Faster to unpack all than to slice.
            return Crosswalk(a, b, c, d, e, f, g, h, bool(i), bool(j))
        return None

    def update(self, record: Crosswalk) -> None:
        """Update a record in the repository."""
        sql = f"""
            UPDATE main.crosswalk
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
            WHERE crosswalk_id=?
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
            'DELETE FROM main.crosswalk WHERE crosswalk_id=?', (id,)
        )


class RelationRepository(BaseRelationRepository):
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = cursor

    def add(
        self,
        crosswalk_id: int,
        other_index_id: int,
        index_id: int,
        value: float,
        proportion: Optional[float] = None,
        mapping_level: Optional[bytes] = None,
    ) -> None:
        """Add a record to the repository."""
        sql = """
            INSERT INTO main.relation (
                crosswalk_id,
                other_index_id,
                index_id,
                relation_value,
                proportion,
                mapping_level
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """
        parameters = (
            crosswalk_id,
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
            SET crosswalk_id=?,
                other_index_id=?,
                index_id=?,
                relation_value=?,
                proportion=?,
                mapping_level=?
            WHERE relation_id=?
        """
        parameters = (
            record.crosswalk_id,
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


class PropertyRepository(BasePropertyRepository):
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new PropertyRepository instance."""
        self._cursor = cursor

    def add(self, key: str, value: JsonTypes) -> None:
        """Add an item to the repository."""
        self._cursor.execute(
            'INSERT INTO main.property (key, value) VALUES (?, ?)',
            (key, json_dumps(value)),
        )

    def get(self, key: str) -> JsonTypes:
        """Retrieve an item from the repository."""
        self._cursor.execute(
            'SELECT value FROM main.property WHERE key=?',
            (key,),
        )
        result = self._cursor.fetchone()
        if result:
            return result[0]
        return None

    def update(self, key: str, value: JsonTypes) -> None:
        """Update an item in the repository."""
        self._cursor.execute(
            'UPDATE main.property SET value=? WHERE key=?',
            (json_dumps(value), key),
        )

    def delete(self, key: str):
        """Remove an item from the repository."""
        self._cursor.execute(
            'DELETE FROM main.property WHERE key=?',
            (key,),
        )
