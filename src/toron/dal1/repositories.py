"""IndexRepository and related objects using SQLite."""

import sqlite3
from dataclasses import asdict
from itertools import chain
from json import dumps as json_dumps

from toron._typing import (
    Any,
    Dict,
    Iterator,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

from .schema import (
    SQLITE_ENABLE_JSON1,
    format_identifier,
)
from ..data_models import (
    Index, BaseIndexRepository,
    Location, BaseLocationRepository,
    Structure, BaseStructureRepository,
    WeightGroup, BaseWeightGroupRepository,
    Weight, BaseWeightRepository,
    AttributeGroup, BaseAttributeRepository,
    Quantity, BaseQuantityRepository,
    Crosswalk, BaseCrosswalkRepository,
    Relation, BaseRelationRepository,
    JsonTypes, BasePropertyRepository,
)


class IndexRepository(BaseIndexRepository):
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new IndexRepository instance."""
        self._cursor = cursor

    def add(self, label: str, *labels: str) -> None:
        """Add a record to the repository."""
        labels = (label,) + labels
        qmarks = ', '.join('?' * len(labels))
        sql = f'INSERT INTO main.node_index VALUES (NULL, {qmarks})'
        try:
            self._cursor.execute(sql, labels)
        except sqlite3.IntegrityError as err:
            raise ValueError(str(err))

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
        qmarks = ', '.join('?' * len(record.labels))
        sql = f"""
            UPDATE main.node_index
            SET ({columns}) = ({qmarks})
            WHERE index_id=?
        """
        self._cursor.execute(sql, record.labels + (record.id,))

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.node_index WHERE index_id=?', (id,)
        )

    def get_all(self, include_undefined: bool = True) -> Iterator[Index]:
        """Get all records in the repository."""
        sql = 'SELECT * FROM main.node_index'
        if not include_undefined:
            sql += ' WHERE index_id != 0'

        self._cursor.execute(sql)
        return (Index(*record) for record in self._cursor)

    def get_index_ids(self, ordered: bool = False) -> Iterator[int]:
        """Get index_id values. When *ordered* is True, must return
        values in ascending order.
        """
        sql = 'SELECT index_id FROM main.node_index'
        if ordered:
            sql += ' ORDER BY index_id'
        self._cursor.execute(sql)
        return (row[0] for row in self._cursor)

    def get_cardinality(self, include_undefined: bool = True) -> int:
        """Return the number of records in the repository."""
        sql = 'SELECT COUNT(*) FROM main.node_index'
        if not include_undefined:
            sql += ' WHERE index_id != 0'

        self._cursor.execute(sql)
        return self._cursor.fetchone()[0]

    def get_distinct_labels(
        self, column: str, *columns: str, include_undefined: bool = True
    ) -> Iterator[Tuple[str, ...]]:
        """Get distinct label values for given column names."""
        columns = (column,) + columns
        formatted_cols = ', '.join(format_identifier(x) for x in columns)
        sql = f'SELECT DISTINCT {formatted_cols} FROM main.node_index'
        if not include_undefined:
            sql += ' WHERE index_id != 0'
        self._cursor.execute(sql)
        return (row for row in self._cursor)

    def find_by_label(
        self,
        criteria: Optional[Dict[str, str]],
        include_undefined: bool = True,
    ) -> Iterator[Index]:
        """Find all records in the repository that match criteria."""
        if not criteria:
            msg = 'find_by_label requires at least 1 criteria value, got 0'
            raise ValueError(msg)

        qmarks = (f'{format_identifier(k)}=?' for k in criteria.keys())
        sql = f'SELECT * FROM main.node_index WHERE {" AND ".join(qmarks)}'
        if not include_undefined:
            sql += ' AND index_id != 0'

        self._cursor.execute(sql, tuple(criteria.values()))
        return (Index(*record) for record in self._cursor)


class LocationRepository(BaseLocationRepository):
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new PropertyRepository instance."""
        self._cursor = cursor

    def add(self, label: str, *labels: str) -> None:
        """Add a record to the repository."""
        labels = (label,) + labels
        qmarks = ", ".join("?" * len(labels))
        sql = f'INSERT INTO main.location VALUES (NULL, {qmarks})'
        self._cursor.execute(sql, labels)

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
        qmarks = ', '.join('?' * len(record.labels))
        sql = f"""
            UPDATE main.location
            SET ({columns}) = ({qmarks})
            WHERE _location_id=?
        """
        self._cursor.execute(sql, record.labels + (record.id,))

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.location WHERE _location_id=?', (id,)
        )

    def get_label_columns(self) -> Tuple[str, ...]:
        """Get a tuple of label column names."""
        self._cursor.execute(f"PRAGMA main.table_info('location')")
        columns = tuple(row[1] for row in self._cursor.fetchall())
        return columns[1:]  # Return columns (slicing-off _location_id).

    def find_by_label(
        self,
        criteria: Optional[Dict[str, str]],
    ) -> Iterator[Location]:
        """Find all records in the repository that match criteria."""
        if not criteria:
            msg = 'find_by_label requires at least 1 criteria value, got 0'
            raise ValueError(msg)

        qmarks = (f'{format_identifier(k)}=?' for k in criteria.keys())
        sql = f'SELECT * FROM main.location WHERE {" AND ".join(qmarks)}'
        self._cursor.execute(sql, tuple(criteria.values()))
        return (Location(*record) for record in self._cursor)

    def find_by_structure(self, structure: Structure) -> Iterable[Location]:
        """Find records that match the given structure's bit pattern."""
        columns = self.get_label_columns()
        func = lambda a, b: f"{format_identifier(a)} {'!=' if b else '='} ''"
        conditions = list(func(a, b) for a, b in zip(columns, structure.bits))
        self._cursor.execute(
            f'SELECT * FROM main.location WHERE {" AND ".join(conditions)}'
        )
        return (Location(*record) for record in self._cursor)


class StructureRepository(BaseStructureRepository):
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new StructureRepository instance."""
        self._cursor = cursor

    def add(
        self, granularity: Optional[float], bit: int, *bits: int
    ) -> None:
        """Add a record to the repository."""
        bits = (bit,) + bits
        qmarks = ', '.join('?' * len(bits))
        sql = f'INSERT INTO main.structure VALUES (NULL, ?, {qmarks})'
        self._cursor.execute(sql, (granularity,) + bits)

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
        return [Structure(*record) for record in self._cursor]

    def update(self, record: Structure) -> None:
        """Update a record in the repository."""
        self._cursor.execute(f"PRAGMA main.table_info('structure')")
        columns = ', '.join(row[1] for row in self._cursor.fetchall()[2:])
        qmarks = ', '.join('?' * len(record.bits))
        sql = f"""
            UPDATE main.structure
            SET (_granularity, {columns}) = (?, {qmarks})
            WHERE _structure_id=?
        """
        parameters = (record.granularity,) + record.bits + (record.id,)
        self._cursor.execute(sql, parameters)

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.structure WHERE _structure_id=?', (id,)
        )


class WeightGroupRepository(BaseWeightGroupRepository):
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
            INSERT INTO main.weight_group (name, description, selectors, is_complete)
            VALUES (?, ?, ?, ?)
        """
        self._cursor.execute(sql, (name, description, selectors, is_complete))

    def get(self, id: int) -> Optional[WeightGroup]:
        """Get a record from the repository."""
        self._cursor.execute(
            'SELECT * FROM main.weight_group WHERE weight_group_id=?', (id,)
        )
        record = self._cursor.fetchone()
        if record:
            return WeightGroup(*record)
        return None

    def get_by_name(self, name: str) -> Optional[WeightGroup]:
        """Get record from the repository with matching name."""
        self._cursor.execute(
            'SELECT * FROM main.weight_group WHERE name=?', (name,)
        )
        record = self._cursor.fetchone()
        if record:
            return WeightGroup(*record)
        return None

    def get_all(self) -> List[WeightGroup]:
        """Get all weight_group records sorted by name."""
        self._cursor.execute('SELECT * FROM main.weight_group ORDER BY name')
        return [WeightGroup(*record) for record in self._cursor]

    def update(self, record: WeightGroup) -> None:
        """Update a record in the repository."""
        sql = f"""
            UPDATE main.weight_group
            SET
                name=?,
                description=?,
                selectors=?,
                is_complete=?
            WHERE weight_group_id=?
        """
        parameters = [
            record.name,
            record.description,
            json_dumps(record.selectors) if record.selectors else None,
            record.is_complete,
            record.id,
        ]
        self._cursor.execute(sql, parameters)

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.weight_group WHERE weight_group_id=?', (id,)
        )


class WeightRepository(BaseWeightRepository):
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = cursor

    def add(self, weight_group_id: int, index_id: int, value: int) -> None:
        """Add a record to the repository."""
        sql = """
            INSERT INTO main.weight (weight_group_id, index_id, weight_value)
            VALUES (?, ?, ?)
        """
        self._cursor.execute(sql, (weight_group_id, index_id, value))

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
                weight_group_id=:weight_group_id,
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

    #def find_by_weight_group_id(self, weight_group_id: int) -> Iterable[Weight]:
    #    """Filter to records associated with the given weight group."""

    def get_by_weight_group_id_and_index_id(
        self,
        weight_group_id: int,
        index_id: int,
    ) -> Optional[Weight]:
        """Get record with matching weight_group_id and index_id."""
        self._cursor.execute(
            'SELECT * FROM main.weight WHERE weight_group_id=? AND index_id=?',
            (weight_group_id, index_id)
        )
        record = self._cursor.fetchone()
        if record:
            return Weight(*record)
        return None

    def find_by_index_id(self, index_id: int) -> Iterator[Weight]:
        """Find all records with matching index_id."""
        self._cursor.execute(
            'SELECT * FROM main.weight WHERE index_id=?', (index_id,)
        )
        for record in self._cursor:
            yield Weight(*record)

    def weight_group_is_complete(self, weight_group_id: int) -> bool:
        """Return True if there's a weight for every index record."""
        self._cursor.execute(
            """
                SELECT 1
                FROM main.node_index a
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM main.weight b
                    WHERE b.weight_group_id=? AND a.index_id=b.index_id
                ) AND a.index_id != 0
                LIMIT 1
            """,
            (weight_group_id,),
        )
        is_partial = bool(self._cursor.fetchall())
        return not is_partial


class AttributeRepository(BaseAttributeRepository):
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = cursor

    def add(self, value: Dict[str, str]) -> None:
        """Add a record to the repository."""
        if '' in value.keys() or '' in value.values():
            msg = f'keys and values cannot be empty strings, got: {value!r}'
            raise ValueError(msg)

        sql = 'INSERT INTO main.attribute_group (attribute_value) VALUES (?)'
        self._cursor.execute(sql, (json_dumps(value, sort_keys=True),))

    def get(self, id: int) -> Optional[AttributeGroup]:
        """Get a record from the repository."""
        self._cursor.execute(
            'SELECT * FROM main.attribute_group WHERE attribute_group_id=?', (id,)
        )
        record = self._cursor.fetchone()
        if record:
            return AttributeGroup(*record)
        return None

    def update(self, record: AttributeGroup) -> None:
        """Update a record in the repository."""
        attributes = record.attributes

        if '' in attributes.keys() or '' in attributes.values():
            msg = f'keys and values cannot be empty strings, got: {attributes!r}'
            raise ValueError(msg)

        self._cursor.execute(
            'UPDATE main.attribute_group SET attribute_value=? WHERE attribute_group_id=?',
            (json_dumps(attributes, sort_keys=True), record.id),
        )

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.attribute_group WHERE attribute_group_id=?', (id,)
        )

    def get_by_value(self, value: Dict[str, str]) -> Optional[AttributeGroup]:
        """Get the record matching the given value."""
        self._cursor.execute(
            'SELECT * FROM main.attribute_group WHERE attribute_value=?',
            (json_dumps(value, sort_keys=True),)
        )
        record = self._cursor.fetchone()
        if record:
            return AttributeGroup(*record)
        return None

    def find_all(self) -> Iterable[AttributeGroup]:
        """Get all records in the repository."""
        self._cursor.execute('SELECT * FROM main.attribute_group')
        return (AttributeGroup(*record) for record in self._cursor)

    if SQLITE_ENABLE_JSON1:
        def find_by_criteria(self, **criteria) -> Iterable[AttributeGroup]:
            """Find records matching given criteria values."""
            # If one or more keys is not a simple alpha-numeric string,
            # then call the unoptimized parent class' method instead.
            # This is done because SQLite's JSON "PATH arguments" are
            # not well defined for keys with special characters.
            if any(key and not key.isalnum() for key in criteria.keys()):
                return super().find_by_criteria(**criteria)

            # Format keys as SQLite JSON "PATH arguments". See SQLite's JSON
            # docs for details <https://sqlite.org/json1.html#path_arguments>.
            formatted_items = [(f'$.{k}', v) for k, v in criteria.items()]

            expression = 'json_extract(attribute_value, ?) IS ?'
            where_clause = ' AND '.join([expression] * len(formatted_items))
            sql = f'SELECT * FROM main.attribute_group WHERE {where_clause}'

            flattened_items = list(chain.from_iterable(formatted_items))

            self._cursor.execute(sql, flattened_items)
            return (AttributeGroup(*record) for record in self._cursor)

        def get_all_attribute_names(self) -> List[str]:
            """Return a sorted list of distinct attribute names."""
            self._cursor.execute("""
                SELECT DISTINCT
                    json_each.key
                FROM
                    main.attribute_group,
                    json_each(attribute_value)
            """)
            return sorted(x[0] for x in self._cursor)


class QuantityRepository(BaseQuantityRepository):
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = cursor

    def add(self, location_id: int, attribute_id: int, value: float) -> None:
        """Add a record to the repository."""
        sql = """
            INSERT INTO main.quantity (_location_id, attribute_group_id, quantity_value)
            VALUES (?, ?, ?)
        """
        self._cursor.execute(sql, (location_id, attribute_id, value))

    def get(self, id: int) -> Optional[Quantity]:
        """Get a record from the repository."""
        self._cursor.execute(
            'SELECT * FROM main.quantity WHERE quantity_id=?', (id,)
        )
        quantity = self._cursor.fetchone()
        if quantity:
            quantity_id, loc_id, attr_id, val = quantity
            return Quantity(quantity_id, loc_id, attr_id, float(val))
        return None

    def update(self, record: Quantity) -> None:
        """Update a record in the repository."""
        sql = f"""
            UPDATE main.quantity
            SET
                _location_id=:location_id,
                attribute_group_id=:attribute_id,
                quantity_value=:value
            WHERE quantity_id=:id
        """
        self._cursor.execute(sql, asdict(record))

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.quantity WHERE quantity_id=?', (id,)
        )

    def find_by_location_id(self, location_id: int) -> Iterator[Quantity]:
        """Find records with matching location id."""
        self._cursor.execute(
            'SELECT * FROM main.quantity WHERE _location_id=?',
            (location_id,),
        )
        for quantity in self._cursor:
            quantity_id, loc_id, attr_id, val = quantity
            yield Quantity(quantity_id, loc_id, attr_id, float(val))

    def find_by_ids(
        self,
        *,
        location_id: Optional[int] = None,
        attribute_id:  Optional[int] = None,
    ) -> Iterator[Quantity]:
        """Find records with matching location and attribute ids."""
        criteria = []
        if location_id is not None:
            criteria.append('_location_id=:location_id')
        if attribute_id is not None:
            criteria.append('attribute_group_id=:attribute_group_id')

        if criteria:
            sql = f'SELECT * FROM main.quantity WHERE {" AND ".join(criteria)}'
            parameters = {
                'location_id': location_id,
                'attribute_group_id': attribute_id,
            }
            self._cursor.execute(sql, parameters)

            for quantity in self._cursor:
                quantity_id, loc_id, attr_id, val = quantity
                yield Quantity(quantity_id, loc_id, attr_id, float(val))


class CrosswalkRepository(BaseCrosswalkRepository):
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = cursor

    def add(
        self,
        other_unique_id: str,
        other_filename_hint: Union[str, None],
        name: str,
        *,
        description: Optional[str] = None,
        selectors: Optional[Union[List[str], str]] = None,
        is_default: bool = False,
        user_properties: Optional[Dict[str, JsonTypes]] = None,
        other_index_hash: Optional[str] = None,
        is_locally_complete: bool = False,
    ) -> None:
        """Add a record to the repository."""
        if isinstance(selectors, str):
            selectors = [selectors]

        sql = """
            INSERT INTO main.crosswalk (
                other_unique_id,
                other_filename_hint,
                name,
                description,
                selectors,
                is_default,
                user_properties,
                other_index_hash,
                is_locally_complete
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        parameters = (
            other_unique_id,
            other_filename_hint,
            name,
            description,
            json_dumps(selectors) if selectors else None,
            True if is_default else None,
            json_dumps(user_properties) if user_properties else None,
            other_index_hash,
            is_locally_complete,
        )
        self._cursor.execute(sql, parameters)

    @staticmethod
    def _make_crosswalk(values: Iterable[Any]) -> Crosswalk:
        """Normalize row of 'crosswalk' values and return Crosswalk."""
        a, b, c, d, e, f, g, h, i, j = values  # Faster to unpack all than to slice.
        return Crosswalk(
            id=a,
            other_unique_id=b,
            other_filename_hint=c,
            name=d,
            description=e,
            selectors=f,
            is_default=bool(g),
            user_properties=h,
            other_index_hash=i,
            is_locally_complete=bool(j),
        )

    def get(self, id: int) -> Optional[Crosswalk]:
        """Get a record from the repository."""
        sql = """
            SELECT
                crosswalk_id,
                other_unique_id,
                other_filename_hint,
                name,
                description,
                selectors,
                is_default,
                user_properties,
                other_index_hash,
                is_locally_complete
            FROM main.crosswalk
            WHERE crosswalk_id=?
        """
        self._cursor.execute(sql, (id,))
        record = self._cursor.fetchone()
        return self._make_crosswalk(record) if record else None

    def get_all(self) -> List[Crosswalk]:
        """Get all records from the repository."""
        self._cursor.execute('SELECT * FROM main.crosswalk')
        return [self._make_crosswalk(row) for row in self._cursor]

    def update(self, record: Crosswalk) -> None:
        """Update a record in the repository."""
        sql = f"""
            UPDATE main.crosswalk
            SET
                other_unique_id=?,
                other_filename_hint=?,
                name=?,
                description=?,
                selectors=?,
                is_default=?,
                user_properties=?,
                other_index_hash=?,
                is_locally_complete=?
            WHERE crosswalk_id=?
        """
        parameters = [
                record.other_unique_id,
                record.other_filename_hint,
                record.name,
                record.description,
                json_dumps(record.selectors) if record.selectors else None,
                True if record.is_default else None,
                json_dumps(record.user_properties) if record.user_properties else None,
                record.other_index_hash,
                record.is_locally_complete,
                record.id,
        ]
        self._cursor.execute(sql, parameters)

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.crosswalk WHERE crosswalk_id=?', (id,)
        )

    def find_by_other_unique_id(
        self, other_unique_id: str
    ) -> Iterator[Crosswalk]:
        """Find all records with matching other_unique_id."""
        self._cursor.execute(
            'SELECT * FROM main.crosswalk WHERE other_unique_id=?',
            (other_unique_id,),
        )
        for record in self._cursor:
            yield self._make_crosswalk(record)

    def find_by_other_filename_hint(
        self, other_filename_hint: str
    ) -> Iterator[Crosswalk]:
        """Find all records with matching other_filename_hint."""
        self._cursor.execute(
            'SELECT * FROM main.crosswalk WHERE other_filename_hint=?',
            (other_filename_hint,),
        )
        for record in self._cursor:
            yield self._make_crosswalk(record)


class RelationRepository(BaseRelationRepository):
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new repository instance."""
        self._cursor = cursor

    if sqlite3.sqlite_version_info >= (3, 32, 0):
        def add(
            self,
            crosswalk_id: int,
            other_index_id: int,
            index_id: int,
            mapping_level: Union[bytes, None],
            value: float,
            proportion: Optional[float] = None,
        ) -> None:
            """Add a record to the repository."""
            sql = """
                INSERT INTO main.relation (
                    crosswalk_id,
                    other_index_id,
                    index_id,
                    relation_value,
                    mapping_level,
                    proportion
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """
            parameters = (
                crosswalk_id,
                other_index_id,
                index_id,
                value,
                bytes(mapping_level) if mapping_level else None,
                proportion,
            )
            self._cursor.execute(sql, parameters)
    else:
        # Prior to SQLite 3.32.0, column affinity was not always applied before
        # computing CHECK constraints. For proper behavior, 'other_index_id'
        # and 'value' need to be converted *before* inserting or updating.
        def add(
            self,
            crosswalk_id: int,
            other_index_id: int,
            index_id: int,
            mapping_level: Union[bytes, None],
            value: float,
            proportion: Optional[float] = None,
        ) -> None:
            """Add a record to the repository."""
            sql = """
                INSERT INTO main.relation (
                    crosswalk_id,
                    other_index_id,
                    index_id,
                    relation_value,
                    mapping_level,
                    proportion
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """
            parameters = (
                crosswalk_id,
                int(other_index_id),
                index_id,
                float(value),
                bytes(mapping_level) if mapping_level else None,
                proportion,
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

    if sqlite3.sqlite_version_info >= (3, 32, 0):
        def update(self, record: Relation) -> None:
            """Update a record in the repository."""
            sql = f"""
                UPDATE main.relation
                SET crosswalk_id=?,
                    other_index_id=?,
                    index_id=?,
                    mapping_level=?,
                    relation_value=?,
                    proportion=?
                WHERE relation_id=?
            """
            parameters = (
                record.crosswalk_id,
                record.other_index_id,
                record.index_id,
                record.mapping_level,
                record.value,
                record.proportion,
                record.id,
            )
            self._cursor.execute(sql, parameters)
    else:
        # Prior to SQLite 3.32.0, column affinity was not always applied before
        # computing CHECK constraints. For proper behavior, 'other_index_id'
        # and 'value' need to be converted *before* inserting or updating.
        def update(self, record: Relation) -> None:
            """Update a record in the repository."""
            sql = f"""
                UPDATE main.relation
                SET crosswalk_id=?,
                    other_index_id=?,
                    index_id=?,
                    mapping_level=?,
                    relation_value=?,
                    proportion=?
                WHERE relation_id=?
            """
            parameters = (
                record.crosswalk_id,
                int(record.other_index_id),
                record.index_id,
                record.mapping_level,
                float(record.value),
                record.proportion,
                record.id,
            )
            self._cursor.execute(sql, parameters)

    def delete(self, id: int) -> None:
        """Delete a record from the repository."""
        self._cursor.execute(
            'DELETE FROM main.relation WHERE relation_id=?', (id,)
        )

    def get_distinct_other_index_ids(
        self, crosswalk_id: int, ordered: bool = False
    ) -> Iterator[int]:
        """Get distinct other_index_id values for the given crosswalk.
        When *ordered* is True, must return values in ascending order.
        """
        sql = f"""
            SELECT DISTINCT other_index_id
            FROM main.relation
            WHERE crosswalk_id=?
            {'ORDER BY other_index_id' if ordered else ''}
        """
        self._cursor.execute(sql, (crosswalk_id,))
        return (row[0] for row in self._cursor)

    def find_by_ids(
        self,
        *,
        crosswalk_id: Optional[int] = None,
        other_index_id: Optional[int] = None,
        index_id: Optional[int] = None,
    ) -> Iterator[Relation]:
        """Find all records with matching combination of id values."""
        criteria = []
        if crosswalk_id is not None:
            criteria.append('crosswalk_id=:crosswalk_id')
        if other_index_id is not None:
            criteria.append('other_index_id=:other_index_id')
        if index_id is not None:
            criteria.append('index_id=:index_id')

        if criteria:
            sql = f'SELECT * FROM main.relation WHERE {" AND ".join(criteria)}'
            parameters = {
                'crosswalk_id': crosswalk_id,
                'other_index_id': other_index_id,
                'index_id': index_id,
            }
            self._cursor.execute(sql, parameters)

            for record in self._cursor:
                yield Relation(*record)

    def crosswalk_is_complete(self, crosswalk_id: int) -> bool:
        """Return True if there's a relation for every index record."""
        self._cursor.execute(
            """
                SELECT 1
                FROM main.node_index a
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM main.relation b
                    WHERE b.crosswalk_id=? AND a.index_id=b.index_id
                ) AND a.index_id != 0
                LIMIT 1
            """,
            (crosswalk_id,),
        )
        is_partial = bool(self._cursor.fetchall())
        return not is_partial


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

    def add_or_update(self, key: str, value: JsonTypes) -> None:
        """Add a new item or update an existing item in the repository."""
        self._cursor.execute(
            'INSERT OR REPLACE INTO main.property (key, value) VALUES (?, ?)',
            (key, json_dumps(value)),
        )
