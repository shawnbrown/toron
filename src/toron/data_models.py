"""Abstract base classes to define the data access API.

A compatible back-end for the DataAccessLayer class must implement
all of the base classes given in this sub-module.
"""

import os
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from itertools import groupby

from toron._typing import (
    Any,
    Dict,
    Final,
    Generic,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Self,
    Sequence,
    Tuple,
    TypeAlias,
    TypeVar,
    Union,
    overload,
)


T1 = TypeVar('T1')
T2 = TypeVar('T2')

JsonTypes: TypeAlias = Union[
    Dict[str, 'JsonTypes'], List['JsonTypes'], str, int, float, bool, None
]

# Magic number to use as file signature for Toron files.
TORON_MAGIC_NUMBER: Final[bytes] = b'\x01\x2d\x84\xc8'


class BaseDataConnector(ABC, Generic[T1, T2]):
    @abstractmethod
    def __init__(self, **kwds) -> None:
        """Initialize a new node instance."""

    @property
    @abstractmethod
    def unique_id(self) -> str:
        """Unique identifier for the node object."""

    @abstractmethod
    def acquire_connection(self) -> T1:
        """Return an appropriate object to interact with a node's data.

        If a node's storage backend is a database, the connection
        might be a DBAPI2 Connection. If other storage backends are
        implemented, the "connection" could be an HDF5 group object,
        a collection of Parquet tables, etc.
        """

    @abstractmethod
    def release_connection(self, connection: T1) -> None:
        """Release the acquired data *connection*.

        This method should release the given data *connection* object
        if it's acceptable to do so. In the case of connections to
        a temporary database, this method may not do anything at all
        (since closing such a connection would delete the data). But
        if the connection is a reference to an on-drive file or other
        persistent storage, then this method should release it to
        free up system resources (file handles, network connections,
        etc.).
        """

    @abstractmethod
    def acquire_cursor(self, connection: T1) -> T2:
        """Return an appropriate object to interact with a node's data.

        If a node's storage backend is a relational database, the
        *cursor* might be a DBAPI2 Cursor. If other storage backends
        are implemented, the *cursor* could be an HDF5 dataset, a
        Parquet table, etc.

        For certain backends, the "connection" and the "cursor"
        might be the same object. If this is the case, then this
        method should simply return the connection given to it.
        """

    @abstractmethod
    def release_cursor(self, cursor: T2) -> None:
        """Release the acquired data *cursor*.

        If the "connection" and "cursor" are the same object, this
        method should pass without doing anything to the object and
        allow ``release_connection()`` to do the final clean-up.
        """

    @abstractmethod
    def transaction_begin(self, cursor: T2) -> None:
        """Begin a new transaction.

        If the back-end is a SQL database, this method should execute
        a "BEGIN TRANSACTION" statement. If the back-end is some other
        data store, this method should take the necessary steps to mark
        the start of a series of data changes, to ensure that they are
        treated as a single unit that either completes entirely or is
        rolled back if any part fails.
        """

    @abstractmethod
    def transaction_rollback(self, cursor: T2) -> None:
        """Roll-back the transaction.

        If the back-end is a SQL database, this method should execute a
        "ROLLBACK TRANSACTION" statement. If the back-end is some other
        data store, this method must take steps to undo all changes to
        the data since beginning of the transaction, restoring its
        original state.
        """

    @abstractmethod
    def transaction_commit(self, cursor: T2) -> None:
        """Commit the transaction.

        If the back-end is a SQL database, this method should execute
        a "COMMIT TRANSACTION" statement. If the back-end is some other
        data store, this method should finalize and save all data
        changes made within the transaction.
        """

    @abstractmethod
    def to_file(
        self, path: Union[str, bytes, os.PathLike], *, fsync: bool = True
    ) -> None:
        """Write node data to a file.

        Parameters
        ----------
        path : :py:term:`path-like-object`
            File path where the node data should be saved.
        fsync : bool, default True
            Immediately flush any cached data to drive storage.

            On systems where it's not possible to guarantee that data
            is flushed, this method should still make a best-effort
            attempt to do so.
        """

    @classmethod
    @abstractmethod
    def from_file(
        cls, path: Union[str, bytes, os.PathLike]
    ) -> Self:
        """Read a node file into a new data connector object.

        Parameters
        ----------
        path : :py:term:`path-like-object`
            File path containing the node data.
        """


class BaseColumnManager(ABC):
    """Manage node's label columns (add, get, update, and delete)."""
    @abstractmethod
    def __init__(self, cursor: Any) -> None:
        """Initialize a new instance."""

    @abstractmethod
    def add_columns(self, column: str, *columns: str) -> None:
        """Add new label columns."""

    @abstractmethod
    def get_columns(self) -> Tuple[str, ...]:
        """Get a tuple of label column names."""

    @abstractmethod
    def rename_columns(self, mapping: Dict[str, str]) -> None:
        """Rename label columns."""

    @abstractmethod
    def drop_columns(self, column: str, *columns: str) -> None:
        """Remove label columns."""


@dataclass(init=False)
class Index(object):
    """
    Index(1, 'foo', 'bar')
    Index(id=1, labels=('foo', 'bar'))
    """
    id: int
    labels: Tuple[str, ...]

    @overload
    def __init__(self, id: int, *args: str) -> None:
        ...
    @overload
    def __init__(self, id: int, *, labels: Tuple[str, ...]) -> None:
        ...
    def __init__(self, id, *args, labels=tuple()):
        if args and labels:
            raise TypeError('must provide either *args or labels')
        self.id = id
        self.labels = args or labels


class BaseIndexRepository(ABC):
    """The IndexRepository holds id and label values for a node's index.

    ``index_id`` (INTEGER)
        * This is used as a primary key and can appear in user facing
          data results.
        * The same set of labels should never be reused for the life
          of the node.
            - In SQLite, this requirement can be satisfied by defining a
              column using ``index_id INTEGER PRIMARY KEY AUTOINCREMENT``.
              The ``AUTOINCREMENT`` keyword prevents the reuse of record
              ids from previously deleted rows.
    Label Columns (TEXT)
        * Additional columns can be added with the ``add_columns()``
          method.
        * Label values in the Node Index table must never be empty
          strings or NULL.
        * If a column has no value for a given record, a dash/hyphen
          (``"-"``) should be used.

    The ``index_id`` value ``0`` is reserved for the "undefined point".
    It is used in correspondence mappings for external records that
    cannot be linked to local records.

    A record's labels must be unique within the Node Index table.
    """
    @abstractmethod
    def __init__(self, cursor: Any) -> None:
        """Initialize a new IndexRepository instance."""

    @abstractmethod
    def add(self, label: str, *labels: str) -> None:
        """Add a record to the repository.

        Duplicate or invalid data should raise a ValueError.
        """

    @abstractmethod
    def get(self, id: int) -> Optional[Index]:
        """Get a record from the repository."""

    @abstractmethod
    def update(self, record: Index) -> None:
        """Update a record in the repository."""

    @abstractmethod
    def delete(self, id: int) -> None:
        """Delete a record from the repository."""

    @abstractmethod
    def get_all(self, include_undefined: bool = True) -> Iterator[Index]:
        """Get all records in the repository."""

    def get_cardnality(self, include_undefined: bool = True) -> int:
        """Return the number of records in the repository."""
        return sum(1 for _ in self.get_all(include_undefined))

    @abstractmethod
    def get_distinct_labels(
        self, column: str, *columns: str, include_undefined: bool = True
    ) -> Iterator[Tuple[str, ...]]:
        """Get distinct label values for given column names."""

    @abstractmethod
    def find_by_label(
        self,
        criteria: Optional[Dict[str, str]],
        include_undefined: bool = True,
    ) -> Iterator[Index]:
        """Find all records where labels match criteria.

        If criteria is an empty dict, should raise ValueError.
        """


class Location(Index):
    """
    Location(1, 'foo', 'bar')
    Location(id=1, labels=('foo', 'bar'))
    """


class BaseLocationRepository(ABC):
    @abstractmethod
    def __init__(self, cursor: Any) -> None:
        """Initialize a new LocationRepository instance."""

    @abstractmethod
    def add(self, label: str, *labels: str) -> None:
        """Add a record to the repository."""

    @abstractmethod
    def get(self, id: int) -> Optional[Location]:
        """Get a record from the repository."""

    @abstractmethod
    def update(self, record: Location) -> None:
        """Update a record in the repository."""

    @abstractmethod
    def delete(self, id: int) -> None:
        """Delete a record from the repository."""

    #def filter_by_structure(self, structure: Structure) -> Iterable[Location]:
    #    """Filter to records that match the given structure."""


@dataclass(init=False)
class Structure(object):
    """
    Structure(1, None, 1, 0)
    Structure(id=1, granularity=None, bits=(1, 0))
    """
    id: int
    granularity: Union[float, None]
    bits: Tuple[Literal[0, 1], ...]

    @overload
    def __init__(
        self,
        id: int,
        granularity: Union[float, None],
        *args: Literal[0, 1],
    ) -> None:
        ...
    @overload
    def __init__(
        self,
        id: int,
        granularity: Union[float, None],
        *,
        bits: Tuple[Literal[0, 1], ...],
    ) -> None:
        ...
    def __init__(self, id, granularity, *args, bits=tuple()):
        if args and bits:
            raise TypeError('must provide either *args or bits')
        self.id = id
        self.granularity = granularity
        self.bits = args or bits


class BaseStructureRepository(ABC):
    @abstractmethod
    def __init__(self, cursor: Any) -> None:
        """Initialize a new StructureRepository instance."""

    @abstractmethod
    def add(self, bit: int, *bits: int) -> None:
        """Add a record to the repository."""

    @abstractmethod
    def get(self, id: int) -> Optional[Structure]:
        """Get a record from the repository."""

    @abstractmethod
    def get_all(self) -> List[Structure]:
        """Get all records sorted from most to least granular."""

    @abstractmethod
    def update(self, record: Structure) -> None:
        """Update a record in the repository."""

    @abstractmethod
    def delete(self, id: int) -> None:
        """Delete a record from the repository."""


@dataclass
class WeightGroup(object):
    """WeightGroup record."""
    id: int
    name: str
    description: Optional[str]
    selectors: Optional[List[str]]
    is_complete: bool = False


class BaseWeightGroupRepository(ABC):
    @abstractmethod
    def __init__(self, cursor: Any) -> None:
        """Initialize a new repository instance."""

    @abstractmethod
    def add(
        self,
        name: str,
        description: Optional[str] = None,
        selectors: Optional[Union[List[str], str]] = None,
        is_complete: bool = False,
    ) -> None:
        """Add a record to the repository."""

    @abstractmethod
    def get(self, id: int) -> Optional[WeightGroup]:
        """Get a record from the repository."""

    @abstractmethod
    def get_by_name(self, name: str) -> Optional[WeightGroup]:
        """Get record from the repository with matching name."""

    @abstractmethod
    def get_all(self) -> List[WeightGroup]:
        """Get all records in the repository sorted by name."""

    @abstractmethod
    def update(self, record: WeightGroup) -> None:
        """Update a record in the repository."""

    @abstractmethod
    def delete(self, id: int) -> None:
        """Delete a record from the repository."""


@dataclass
class Weight(object):
    """Weight record."""
    id: int
    weight_group_id: int
    index_id: int
    value: float


class BaseWeightRepository(ABC):
    @abstractmethod
    def __init__(self, cursor: Any) -> None:
        """Initialize a new repository instance."""

    @abstractmethod
    def add(self, weight_group_id: int, index_id: int, value: int) -> None:
        """Add a record to the repository."""

    @abstractmethod
    def get(self, id: int) -> Optional[Weight]:
        """Get a record from the repository."""

    @abstractmethod
    def update(self, record: Weight) -> None:
        """Update a record in the repository."""

    @abstractmethod
    def delete(self, id: int) -> None:
        """Delete a record from the repository."""

    #@abstractmethod
    #def find_by_weight_group_id(self, weight_group_id: int) -> Iterable[Weight]:
    #    """Filter to records associated with the given weight group."""

    @abstractmethod
    def get_by_weight_group_id_and_index_id(
        self,
        weight_group_id: int,
        index_id: int,
    ) -> Optional[Weight]:
        """Get record with matching weight_group_id and index_id."""

    @abstractmethod
    def find_by_index_id(self, index_id: int) -> Iterator[Weight]:
        """Find all records with matching index_id."""

    def merge_by_index_id(
        self, index_ids: Union[Iterable[int], int], target: int
    ) -> None:
        """Merge weight records by given index_id values."""
        if not isinstance(index_ids, Iterable):
            index_ids = [index_ids]
        index_ids = {target}.union(index_ids)  # Always include target in ids.

        old_weight_ids = set()
        summed_values: defaultdict = defaultdict(float)
        for index_id in index_ids:
            for weight in self.find_by_index_id(index_id):
                old_weight_ids.add(weight.id)
                summed_values[weight.weight_group_id] += weight.value

        for weight_id in old_weight_ids:
            self.delete(weight_id)

        for weight_group_id, value in summed_values.items():
            self.add(
                weight_group_id=weight_group_id,
                index_id=target,  # <- Target index_id.
                value=value,
            )


@dataclass
class Attribute(object):
    """Attribute record."""
    id: int
    value: Dict[str, str]


class BaseAttributeRepository(ABC):
    @abstractmethod
    def __init__(self, cursor: Any) -> None:
        """Initialize a new repository instance."""

    @abstractmethod
    def add(self, value: Dict[str, str]) -> None:
        """Add a record to the repository."""

    @abstractmethod
    def get(self, id: int) -> Optional[Attribute]:
        """Get a record from the repository."""

    @abstractmethod
    def update(self, record: Attribute) -> None:
        """Update a record in the repository."""

    @abstractmethod
    def delete(self, id: int) -> None:
        """Delete a record from the repository."""

    #@abstractmethod
    #def find_by_criteria(self, **criteria) -> Iterable[Attribute]:
    #    """Filter to records associated matching the given criteria."""


@dataclass
class Quantity(object):
    """Quantity record."""
    id: int
    location_id: int
    attribute_id: int
    value: float


class BaseQuantityRepository(ABC):
    @abstractmethod
    def __init__(self, cursor: Any) -> None:
        """Initialize a new repository instance."""

    @abstractmethod
    def add(self, location_id: int, attribute_id: int, value: float) -> None:
        """Add a record to the repository."""

    @abstractmethod
    def get(self, id: int) -> Optional[Quantity]:
        """Get a record from the repository."""

    @abstractmethod
    def update(self, record: Quantity) -> None:
        """Update a record in the repository."""

    @abstractmethod
    def delete(self, id: int) -> None:
        """Delete a record from the repository."""

    #@abstractmethod
    #def find_by_attribute_id(self, attribute_id: int) -> Iterable[Quantity]:
    #    """Filter to records associated with the given attribute."""


@dataclass
class Crosswalk(object):
    """Crosswalk record."""
    id: int
    other_unique_id: str
    other_filename_hint: Union[str, None]
    name: str
    description: Optional[str] = None
    selectors: Optional[List[str]] = None
    is_default: bool = False
    user_properties: Optional[Dict[str, JsonTypes]] = None
    other_index_hash: Optional[str] = None
    is_locally_complete: bool = False


class BaseCrosswalkRepository(ABC):
    @abstractmethod
    def __init__(self, cursor: Any) -> None:
        """Initialize a new repository instance."""

    @abstractmethod
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

    @abstractmethod
    def get(self, id: int) -> Optional[Crosswalk]:
        """Get a record from the repository."""

    @abstractmethod
    def get_all(self) -> List[Crosswalk]:
        """Get all records from the repository."""

    @abstractmethod
    def update(self, record: Crosswalk) -> None:
        """Update a record in the repository."""

    @abstractmethod
    def delete(self, id: int) -> None:
        """Delete a record from the repository."""

    @abstractmethod
    def find_by_other_unique_id(
        self, other_unique_id: str
    ) -> Iterator[Crosswalk]:
        """Find all records with matching other_unique_id."""

    @abstractmethod
    def find_by_other_filename_hint(
        self, other_filename_hint: str
    ) -> Iterator[Crosswalk]:
        """Find all records with matching other_filename_hint."""


@dataclass
class Relation(object):
    """Relation record."""
    id: int
    crosswalk_id: int
    other_index_id: int
    index_id: int
    value: float
    proportion: Optional[float] = None
    mapping_level: Optional[bytes] = None


class BaseRelationRepository(ABC):
    @abstractmethod
    def __init__(self, cursor: Any) -> None:
        """Initialize a new repository instance."""

    @abstractmethod
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

    @abstractmethod
    def get(self, id: int) -> Optional[Relation]:
        """Get a record from the repository."""

    @abstractmethod
    def update(self, record: Relation) -> None:
        """Update a record in the repository."""

    @abstractmethod
    def delete(self, id: int) -> None:
        """Delete a record from the repository."""

    #@abstractmethod
    #def find_by_crosswalk_id(self, crosswalk_id: int) -> Iterable[Relation]:
    #    """Filter to records associated with the given crosswalk."""

    @abstractmethod
    def find_by_crosswalk_id_and_index_id(
        self, crosswalk_id: int, index_id: int
    ) -> Iterator[Relation]:
        """Find all records with matching crosswalk_id and index_id."""

    @abstractmethod
    def find_by_index_id(self, index_id: int) -> Iterator[Relation]:
        """Find all records with matching index_id."""

    def merge_by_index_id(
        self, index_ids: Union[Iterable[int], int], target: int
    ) -> None:
        """Merge relation records by given index_id values."""
        if not isinstance(index_ids, Iterable):
            index_ids = [index_ids]
        index_ids = {target}.union(index_ids)  # Always include target in ids.

        relation_ids = set()
        relation_sums: defaultdict = defaultdict(lambda: (0.0, 0.0))
        for index_id in index_ids:
            for rel in self.find_by_index_id(index_id):
                relation_ids.add(rel.id)
                key = (rel.crosswalk_id, rel.other_index_id, rel.mapping_level)
                v, p = relation_sums[key]  # Unpack value and proportion.
                try:
                    proportion = p + rel.proportion
                except TypeError:
                    proportion = None
                relation_sums[key] = ((v + rel.value), proportion)

        for relation_id in relation_ids:
            self.delete(relation_id)

        for key, (value, proportion) in relation_sums.items():
            crosswalk_id, other_index_id, mapping_level = key
            self.add(
                crosswalk_id=crosswalk_id,
                other_index_id=other_index_id,
                index_id=target,  # <- Target index_id.
                value=value,
                proportion=proportion,
                mapping_level=mapping_level,
            )

    @abstractmethod
    def find_by_other_index_id(self, other_index_id: int) -> Iterator[Relation]:
        """Find all records with matching other_index_id."""

    def refresh_proportions(
        self, other_index_ids: Union[Iterable[int], int]
    ) -> None:
        """Refresh proportions for records with matching other_index_ids."""
        if not isinstance(other_index_ids, Iterable):
            other_index_ids = [other_index_ids]

        keyfunc = lambda x: (x.crosswalk_id, x.other_index_id)
        for other_index_id in other_index_ids:
            relations = self.find_by_other_index_id(other_index_id)

            sorted_rels = sorted(relations, key=keyfunc)
            if not sorted_rels:
                continue  # <- Skip to next item.

            for _, group in groupby(sorted_rels, key=keyfunc):
                grouped_rels = list(group)
                values_sum = sum(x.value for x in grouped_rels)
                for relation in grouped_rels:
                    try:
                        relation.proportion = relation.value / values_sum
                    except ZeroDivisionError:
                        relation.proportion = 1 / len(grouped_rels)
                    self.update(relation)


class BasePropertyRepository(ABC):
    @abstractmethod
    def __init__(self, cursor: Any) -> None:
        """Initialize a new PropertyRepository instance."""

    @abstractmethod
    def add(self, key: str, value: JsonTypes) -> None:
        """Add an item to the repository."""

    @abstractmethod
    def get(self, key: str) -> JsonTypes:
        """Retrieve an item from the repository."""

    @abstractmethod
    def update(self, key: str, value: JsonTypes) -> None:
        """Update an item in the repository."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Remove an item from the repository."""
