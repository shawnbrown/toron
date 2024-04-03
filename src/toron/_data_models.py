"""Abstract base classes for data access objects."""

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass

from toron._typing import (
    Any,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Self,
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


class BaseDataConnector(ABC, Generic[T1, T2]):
    @abstractmethod
    def __init__(self) -> None:
        """Initialize a new node instance."""

    @property
    @abstractmethod
    def unique_id(self) -> str:
        """Unique identifier for the node object."""

    @abstractmethod
    def acquire_resource(self) -> T1:
        """Return an appropriate object to interact with a node's data.

        If a node's storage backend is a database, the resource
        might be a DBAPI2 Connection. If other storage backends are
        implemented, the resource could be an HDF5 group object, a
        collection of Parquet tables, etc.
        """

    @abstractmethod
    def release_resource(self, resource: T1) -> None:
        """Release the acquired data *resource*.

        This method should release the given data *resource* object
        if it's acceptable to do so. In the case of connections to
        a temporary database, this method may not do anything at all
        (since closing such a connection would delete the data). But
        if the resource is a connection to an on-drive file or other
        persistent storage, then this method should release it to
        free up system resources (file handles, network connections,
        etc.).
        """

    @abstractmethod
    def acquire_data_reader(self, resource: T1) -> T2:
        """Return an appropriate object to interact with a node's data.

        If a node's storage backend is a relational database, the
        *data_reader* might be a DBAPI2 Cursor. If other storage
        backends are implemented, the *data_reader* could be an HDF5
        dataset, a Parquet table, etc.

        For certain backends, the "resource" and the "data_reader"
        might be the same object. If this is the case, then this
        method should simply return the resource given to it.
        """

    @abstractmethod
    def release_data_reader(self, data_reader: T2) -> None:
        """Release the acquired data *data_reader*.

        If the "resource" and "data_reader" are the same object,
        this method should pass without doing anything to the object
        and allow ``release_resource()`` to do the final clean-up.
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
    def __init__(self, data_reader: Any) -> None:
        """Initialize a new instance."""

    @abstractmethod
    def add_columns(self, column: str, *columns: str) -> None:
        """Add new label columns."""

    @abstractmethod
    def get_columns(self) -> Tuple[str, ...]:
        """Get a tuple of label column names."""

    @abstractmethod
    def update_columns(self, mapping: Dict[str, str]) -> None:
        """Update label column names."""

    @abstractmethod
    def delete_columns(self, column: str, *columns: str) -> None:
        """Delete label columns."""


@dataclass(init=False)
class Index(object):
    """
    Index(1, 'foo', 'bar')
    Index(id=1, values=('foo', 'bar'))
    """
    id: int
    values: Tuple[str, ...]

    @overload
    def __init__(self, id: int, *args: str) -> None:
        ...
    @overload
    def __init__(self, id: int, *, values: Tuple[str, ...]) -> None:
        ...
    def __init__(self, id, *args, values=tuple()):
        if args and values:
            raise TypeError('must provide either *args or values')
        self.id = id
        self.values = args or values


class BaseIndexRepository(ABC):
    """The IndexRepository holds id and label values for a node's index.

    ``index_id`` (INTEGER)
        * This is used as a primary key and can appear in user facing
          data results.
        * Values should never be reused for the life of the node.
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
    def __init__(self, data_reader: Any) -> None:
        """Initialize a new IndexRepository instance."""

    @abstractmethod
    def add(self, value: str, *values: str) -> None:
        """Add a record to the repository."""

    @abstractmethod
    def get(self, id: int) -> Optional[Index]:
        """Get a record from the repository."""

    @abstractmethod
    def update(self, record: Index) -> None:
        """Update a record in the repository."""

    @abstractmethod
    def delete(self, id: int) -> None:
        """Delete a record from the repository."""

    #@abstractmethod
    #def get_all(self) -> Iterator[Index]:
    #    """Get all records in the repository."""

    #@abstractmethod
    #def find(self, **criteria: str) -> Iterator[Index]:
    #    """Find all records in the repository that match criteria."""


class Location(Index):
    """
    Location(1, 'foo', 'bar')
    Location(id=1, values=('foo', 'bar'))
    """


class BaseLocationRepository(ABC):
    @abstractmethod
    def __init__(self, data_reader: Any) -> None:
        """Initialize a new LocationRepository instance."""

    @abstractmethod
    def add(self, value: str, *values: str) -> None:
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
    Structure(id=1, None, values=(1, 0))
    """
    id: int
    granularity: Union[float, None]
    values: Tuple[Literal[0, 1], ...]

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
        values: Tuple[Literal[0, 1], ...],
    ) -> None:
        ...
    def __init__(self, id, granularity, *args, values=tuple()):
        if args and values:
            raise TypeError('must provide either *args or values')
        self.id = id
        self.granularity = granularity
        self.values = args or values


class BaseStructureRepository(ABC):
    @abstractmethod
    def __init__(self, data_reader: Any) -> None:
        """Initialize a new StructureRepository instance."""

    @abstractmethod
    def add(self, value: str, *values: str) -> None:
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
class Weighting(object):
    """Weighting record."""
    id: int
    name: str
    description: Optional[str]
    selectors: Optional[List[str]]
    is_complete: bool = False


class BaseWeightingRepository(ABC):
    @abstractmethod
    def __init__(self, data_reader: Any) -> None:
        """Initialize a new repository instance."""

    @abstractmethod
    def add(
        self,
        name: str,
        description: Optional[str],
        selectors: Optional[Union[List[str], str]],
        is_complete: bool = False,
    ) -> None:
        """Add a record to the repository."""

    @abstractmethod
    def get(self, id: int) -> Optional[Weighting]:
        """Get a record from the repository."""

    @abstractmethod
    def update(self, record: Weighting) -> None:
        """Update a record in the repository."""

    @abstractmethod
    def delete(self, id: int) -> None:
        """Delete a record from the repository."""


@dataclass
class Weight(object):
    """Weight record."""
    id: int
    weighting_id: int
    index_id: int
    value: float


class BaseWeightRepository(ABC):
    @abstractmethod
    def __init__(self, data_reader: Any) -> None:
        """Initialize a new repository instance."""

    @abstractmethod
    def add(self, weighting_id: int, index_id: int, value: int) -> None:
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
    #def find_by_weighting_id(self, weighting_id: int) -> Iterable[Weight]:
    #    """Filter to records associated with the given weighting."""


@dataclass
class Attribute(object):
    """Attribute record."""
    id: int
    value: Dict[str, str]


class BaseAttributeRepository(ABC):
    @abstractmethod
    def __init__(self, data_reader: Any) -> None:
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
    def __init__(self, data_reader: Any) -> None:
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
class Edge(object):
    """Edge record."""
    id: int
    name: str
    other_unique_id: str
    other_filename_hint: Optional[str] = None
    other_index_hash: Optional[str] = None
    description: Optional[str] = None
    selectors: Optional[List[str]] = None
    user_properties: Optional[Dict[str, JsonTypes]] = None
    is_locally_complete: bool = False
    is_default: bool = False


class BaseEdgeRepository(ABC):
    @abstractmethod
    def __init__(self, data_reader: Any) -> None:
        """Initialize a new repository instance."""

    @abstractmethod
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

    @abstractmethod
    def get(self, id: int) -> Optional[Edge]:
        """Get a record from the repository."""

    @abstractmethod
    def update(self, record: Edge) -> None:
        """Update a record in the repository."""

    @abstractmethod
    def delete(self, id: int) -> None:
        """Delete a record from the repository."""


@dataclass
class Relation(object):
    """Relation record."""
    id: int
    edge_id: int
    other_index_id: int
    index_id: int
    value: float
    proportion: Optional[float] = None
    mapping_level: Optional[bytes] = None


class BaseRelationRepository(ABC):
    @abstractmethod
    def __init__(self, data_reader: Any) -> None:
        """Initialize a new repository instance."""

    @abstractmethod
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
    #def find_by_edge_id(self, edge_id: int) -> Iterable[Relation]:
    #    """Filter to records associated with the given edge."""


class BasePropertyRepository(ABC):
    @abstractmethod
    def __init__(self, data_reader: Any) -> None:
        """Initialize a new PropertyRepository instance.

        If a node's storage backend is a database, the *data_reader*
        might be a DBAPI2 Cursor. If other storage backends are
        implemented, the *data_reader* could be an HDF5 dataset, a
        Parquet table, etc.
        """

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