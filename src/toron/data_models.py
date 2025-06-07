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
    Set,
    Tuple,
    TypeAlias,
    TypeVar,
    Union,
    cast,
    overload,
    TYPE_CHECKING,  # <- Temporary.
)

if TYPE_CHECKING:  # <- Temporary (remove after moving QuantityIterator).
    import pandas as pd
    from .node import TopoNode


T1 = TypeVar('T1')
T2 = TypeVar('T2')

JsonTypes: TypeAlias = Union[
    Dict[str, 'JsonTypes'], List['JsonTypes'], str, int, float, bool, None
]

# Magic number to use as file signature for Toron files.
TORON_MAGIC_NUMBER: Final[bytes] = b'\x01\x2d\x84\xc8'


# Reserved identifiers should not be used for index columns,
# attribute keys, or weight group names. In addition to the
# common set of reserved identifiers here, individual backends
# (like DAL1) may define additional ones.
COMMON_RESERVED_IDENTIFIERS: Final[Set[str]] = {
    'index_id',
    'value',
    'weight',
}


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
    def transaction_is_active(self, cursor: T2) -> bool:
        """Return True if a transaction is active, otherwise False."""

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
        cls, path: Union[str, bytes, os.PathLike], *args: Any, **kwds: Any
    ) -> Self:
        """Read a node file into a new data connector object.

        Parameters
        ----------
        path : :py:term:`path-like-object`
            File path containing the node data.

        .. note::
            If a concrete method adds additional arguments, they should
            be defined as keyword-only arguments.
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
        * Label values in the TopoNode Index table must never be empty
          strings or NULL.
        * If a column has no value for a given record, a dash/hyphen
          (``"-"``) should be used.

    The ``index_id`` value ``0`` is reserved for the "undefined point".
    It is used in correspondence mappings for external records that
    cannot be linked to local records.

    A record's labels must be unique within the TopoNode Index table.
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
    def get(self, id: int) -> Index:
        """Get a record from the repository.

        If no index matches the given *id*, a ``KeyError`` is raised.
        """

    @abstractmethod
    def update(self, record: Index) -> None:
        """Update a record in the repository."""

    @abstractmethod
    def delete(self, id: int) -> None:
        """Delete a record from the repository."""

    @abstractmethod
    def get_label_names(self) -> List[str]:
        """Return a list of label column names."""

    @abstractmethod
    def find_all(self, include_undefined: bool = True) -> Iterator[Index]:
        """Find all records in the repository."""

    @abstractmethod
    def find_all_index_ids(self, ordered: bool = False) -> Iterator[int]:
        """Find all index_id values. When *ordered* is True, must
        return values in ascending order.
        """

    @abstractmethod
    def find_unmatched_index_ids(self, crosswalk_id: int) -> Iterator[int]:
        """Find index_id values missing from the specified crosswalk.

        It should raise an exception if the given *crosswalk_id* does
        not exist.
        """

    @abstractmethod
    def find_distinct_labels(
        self, column: str, *columns: str, include_undefined: bool = True
    ) -> Iterator[Tuple[str, ...]]:
        """Find distinct label values for given column names."""

    @abstractmethod
    def filter_by_label(
        self,
        criteria: Dict[str, str],
        include_undefined: bool = True,
    ) -> Iterator[Index]:
        """Filter to Index objects whose labels match *criteria* items.

        If *criteria* is an empty dict, no filtering is applied and all
        Index objects are returned.
        """
        criteria_items = criteria.items()
        label_names = self.get_label_names()
        for record in self.find_all(include_undefined):
            if criteria_items <= set(zip(label_names, record.labels)):
                yield record

    @abstractmethod
    def filter_index_ids_by_label(
        self,
        criteria: Dict[str, str],
        include_undefined: bool = True,
    ) -> Iterator[int]:
        """Filter to 'index_id' integers whose labels match *criteria*
        items.

        If *criteria* is an empty dict, no filtering is applied and all
        'index_id' integers are returned.
        """
        for index in self.filter_by_label(criteria, include_undefined):
            yield index.id

    @abstractmethod
    def get_cardinality(self, include_undefined: bool = True) -> int:
        """Return the number of unique records in the repository.

        A concrete DAL should implement an optimized version of this
        method. But as a stop-gap, this unoptimized base implementation
        can be called with ``super()``:

        .. code-block::

            def get_cardinality(self, include_undefined: bool = True) -> int:
                return super().get_cardinality(include_undefined)
        """
        return sum(1 for _ in self.find_all(include_undefined))


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
    def get(self, id: int) -> Location:
        """Get a record from the repository.

        If no location matches the given *id*, a ``KeyError`` is raised.
        """

    @abstractmethod
    def update(self, record: Location) -> None:
        """Update a record in the repository."""

    @abstractmethod
    def delete(self, id: int) -> None:
        """Delete a record from the repository."""

    @abstractmethod
    def get_label_columns(self) -> Tuple[str, ...]:
        """Return a tuple of label column names."""

    @abstractmethod
    def find_all(self) -> Iterator[Location]:
        """Find all location records."""

    @abstractmethod
    def find_by_label(
        self,
        criteria: Optional[Dict[str, str]],
    ) -> Iterator[Location]:
        """Find all records where labels match criteria.

        If criteria is an empty dict, should raise ValueError.
        """

    @abstractmethod
    def find_by_structure(self, structure: 'Structure') -> Iterable[Location]:
        """Find records that match the given structure's bit pattern."""

    def get_by_labels_add_if_missing(self, labels: dict) -> Location:
        """Return the location that matches given *labels* dict. If
        there is no matching location, a new location is added and then
        returned.

        The given *labels* dictionary must include items for all label
        columns.
        """
        columns = self.get_label_columns()

        if set(labels.keys()) != set(columns):
            given_cols = ', '.join(str(x) for x in labels.keys())
            required_cols = ', '.join(str(x) for x in columns)
            raise ValueError(
                f'requires all label columns, got: {given_cols or "nothing"} '
                f'(needs {required_cols})'
            )

        location_record = next(self.find_by_label(labels), None)

        if not location_record:
            self.add(*(labels[k] for k in columns))
            location_record = next(self.find_by_label(labels))

        return location_record


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
    def add(
        self, granularity: Optional[float], bit: int, *bits: int
    ) -> None:
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

    def get_by_bits(
        self, bits: Sequence[Literal[0, 1]]
    ) -> Optional[Structure]:
        """Get record with the matching bit pattern."""
        bits = tuple(bits)
        for structure in self.get_all():
            if structure.bits == bits:
                return structure
        return None


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
    def add(self, weight_group_id: int, index_id: int, value: float) -> None:
        """Add a record to the repository.

        This method must raise a ValueError if any of the following
        conditions are true:

        * The given *index_id* is ``0``, the "undefined record".
        * The given *value* is a negative number.
        * The repository already contains a Weight with the given
          *weight_group_id* and *index_id*.
        """

    @abstractmethod
    def get(self, id: int) -> Optional[Weight]:
        """Get a record from the repository."""

    @abstractmethod
    def update(self, record: Weight) -> None:
        """Update a record in the repository.

        If this method is called to update a weight with a negative
        value, it must raise an exception.
        """

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
    ) -> Weight:
        """Get record with matching *weight_group_id* and *index_id*.

        .. code-block::

            >>> weight_repo.get_by_weight_group_id_and_index_id(
            ...     weight_group_id=4, index_id=102
            ... )
            Weight(id=733, weight_group_id=4, index_id=102, value=84.75)

        If given the undefined record (index_id 0), returns a dummy
        weight with the same `weight_group_id` and a value of `0.0`::

            >>> weight_repo.get_by_weight_group_id_and_index_id(
            ...     weight_group_id=4, index_id=0
            ... )
            Weight(id=-1, weight_group_id=4, index_id=0, value=0.0)

        If no weight matches the given id values, a ``KeyError`` is
        raised.
        """

    @abstractmethod
    def find_by_index_id(self, index_id: int) -> Iterator[Weight]:
        """Find all records with matching index_id."""

    @abstractmethod
    def weight_group_is_complete(self, weight_group_id: int) -> bool:
        """Return True if there's a weight for every index record."""

    def add_or_resolve(
        self,
        weight_group_id: int,
        index_id: int,
        value: float,
        on_conflict: Literal['abort', 'skip', 'overwrite', 'sum'] = 'abort',
    ) -> Literal['inserted', 'skipped', 'overwritten', 'summed']:
        """Add a record to the repository or resolve conflict.

        Any conflicts are resolved according to the ``on_conflict``
        argument:

        +-----------------+--------------------------------------------+
        | ``on_conflict`` | description                                |
        +=================+============================================+
        | ``'abort'``     | raise an error when conflict arises        |
        |                 | (same as ``add()`` method)                 |
        +-----------------+--------------------------------------------+
        | ``'skip'``      | ignore the conflict and exit without error |
        +-----------------+--------------------------------------------+
        | ``'overwrite'`` | replace value of conflicting record with   |
        |                 | new value                                  |
        +-----------------+--------------------------------------------+
        | ``'sum'``       | replace value of conflicting record with   |
        |                 | the sum of existing and new values         |
        +-----------------+--------------------------------------------+

        This function returns a **result code** string to indicate the
        action performed:

        * ``'inserted'``
        * ``'skipped'``
        * ``'overwritten'``
        * ``'summed'``
        """
        try:
            self.add(weight_group_id, index_id, value)
            return 'inserted'  # <- EXIT!
        except ValueError:
            if index_id == 0 or value < 0.0:
                raise  # Reraise error if undefined record or negative value.

            if on_conflict == 'skip':
                return 'skipped'  # <- EXIT!

            weight = self.get_by_weight_group_id_and_index_id(
                weight_group_id,
                index_id,
            )
            weight = cast(Weight, weight)

            if on_conflict == 'overwrite':
                weight.value = value  # Replace value.
                self.update(weight)
                return 'overwritten'  # <- EXIT!

            if on_conflict == 'sum':
                weight.value += value  # Sum values.
                self.update(weight)
                return 'summed'  # <- EXIT!

            if on_conflict == 'abort':
                msg = (
                    f"a weight record already exists for weight_group_id "
                    f"{weight_group_id} and index_id {index_id}; change load "
                    f"behavior by setting on_conflict to 'skip', 'overwrite', "
                    f"or 'sum'"
                )
                raise Exception(msg)

            raise ValueError(
                f"on_conflict must be 'abort', 'skip', 'overwrite', or 'sum'; "
                f"got {on_conflict!r}"
            )

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



AttributesDict: TypeAlias = Dict[str, str]


@dataclass
class AttributeGroup(object):
    """AttributeGroup record."""
    id: int
    attributes: AttributesDict


class BaseAttributeGroupRepository(ABC):
    @abstractmethod
    def __init__(self, cursor: Any) -> None:
        """Initialize a new repository instance."""

    @abstractmethod
    def add(self, value: Dict[str, str]) -> None:
        """Add a record to the repository."""

    @abstractmethod
    def get(self, id: int) -> Optional[AttributeGroup]:
        """Get a record from the repository."""

    @abstractmethod
    def update(self, record: AttributeGroup) -> None:
        """Update a record in the repository."""

    @abstractmethod
    def delete(self, id: int) -> None:
        """Delete a record from the repository."""

    @abstractmethod
    def get_by_value(self, value: Dict[str, str]) -> Optional[AttributeGroup]:
        """Get the record matching the given value."""

    @abstractmethod
    def find_all(self) -> Iterable[AttributeGroup]:
        """Get all records in the repository."""

    def find_by_criteria(self, **criteria: str) -> Iterable[AttributeGroup]:
        """Find records matching given criteria values."""
        for attribute_group in self.find_all():
            attributes = attribute_group.attributes
            if all(attributes.get(k) == v for k, v in criteria.items()):
                yield attribute_group

    def get_by_value_add_if_missing(self, value: Dict[str, str]) -> AttributeGroup:
        """Return the attribute-group that matches given value. If there
        is no matching attribute-group, a new record is added and then
        returned.
        """
        attribute_group = self.get_by_value(value)
        if attribute_group:
            return attribute_group

        self.add(value)
        attribute_group = self.get_by_value(value)
        if attribute_group:
            return attribute_group

        raise RuntimeError('expected attribute-group was not created')

    def get_all_attribute_names(self) -> List[str]:
        """Return a sorted list of distinct attribute names."""
        attribute_names: Set[str] = set()
        for attr_grp in self.find_all():
            attribute_names.update(attr_grp.attributes.keys())
        return sorted(attribute_names)


@dataclass
class Quantity(object):
    """Quantity record."""
    id: int
    location_id: int
    attribute_group_id: int
    value: float


class BaseQuantityRepository(ABC):
    @abstractmethod
    def __init__(self, cursor: Any) -> None:
        """Initialize a new repository instance."""

    @abstractmethod
    def add(self, location_id: int, attribute_group_id: int, value: float) -> None:
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

    @abstractmethod
    def find_by_location_id(self, location_id: int) -> Iterator[Quantity]:
        """Find records with matching location id."""

    @abstractmethod
    def find_by_ids(
        self,
        *,
        location_id: Optional[int] = None,
        attribute_group_id:  Optional[int] = None,
    ) -> Iterator[Quantity]:
        """Find records with matching location and attribute-group ids."""

    @abstractmethod
    def find_by_multiple(
        self,
        structure: Structure,
        attribute_id_filter: Optional[List[int]] = None,
    ) -> Iterator[Quantity]:
        """Find all quantities matching given structure and ids and
        return records ordered by `location_id`.

        If *attribute_id_filter* is given, only those records with
        matching attribute id values will be returned. If it's `None`,
        no records wil lbe filtered. But if an empty list is provided,
        then no records will be returned at all.

        .. note::
            If it's possible to do so efficiently, the returned
            `Quantity` records should be ordered by their `location_id`
            properties. Doing so will speed-up disaggregation which
            uses `itertools.groupby()` to reduce the number of index
            record lookups.

        .. note::
            While part of the "quantity" repository, this method must
            also interact with the back-end "location" data. This steps
            outside the normal scope of repository methods.

            Using loosely coupled methods helps to keep the design
            flexible, but it can limit the ability to implement data
            access optimizations. In this project's critical loops,
            we need to fetch many Quantity objects quickly, so
            performance is important.
        """


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
    mapping_level: Union[bytes, None]
    value: float
    proportion: Optional[float] = None


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
        mapping_level: Union[bytes, None],
        value: float,
        proportion: Optional[float] = None,
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

    @abstractmethod
    def get_distinct_other_index_ids(
        self, crosswalk_id: int, ordered: bool = False
    ) -> Iterator[int]:
        """Get distinct other_index_id values for the given crosswalk.
        When *ordered* is True, must return values in ascending order.
        """

    @abstractmethod
    def find_by_ids(
        self,
        *,
        crosswalk_id: Optional[int] = None,
        other_index_id: Optional[int] = None,
        index_id: Optional[int] = None,
    ) -> Iterator[Relation]:
        """Find all records with matching combination of id values."""

    @abstractmethod
    def get_index_id_cardinality(
        self, crosswalk_id: int, include_undefined: bool = True
    ) -> int:
        """Return the number of unique index_id values in the crosswalk."""

    @abstractmethod
    def crosswalk_is_complete(self, crosswalk_id: int) -> bool:
        """Return True if there's a relation for every index record."""

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
            for rel in self.find_by_ids(index_id=index_id):
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
                mapping_level=mapping_level,
                proportion=proportion,
            )

    def refresh_proportions(
        self, crosswalk_id: int, other_index_id: int
    ) -> None:
        """Refresh proportions for records with matching crosswalk_id
        and other_index_id.
        """
        relations = list(self.find_by_ids(
            crosswalk_id=crosswalk_id, other_index_id=other_index_id
        ))

        if other_index_id == 0:
            # Set the proportion to 0.0 for undefined-to-defined relations.
            # And set the proportion to 1.0 for the undefined-to-undefined
            # relation.
            for relation in relations:
                relation.proportion = 0.0 if relation.index_id != 0 else 1.0
                self.update(relation)
            return  # <- EXIT!

        values_sum = sum(rel.value for rel in relations)
        for relation in relations:
            try:
                relation.proportion = relation.value / values_sum
            except ZeroDivisionError:
                relation.proportion = 1 / len(relations)

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

    def add_or_update(self, key: str, value: JsonTypes) -> None:
        """Add a new item or update an existing item in the repository."""
        try:
            self.add(key, value)
        except Exception:
            self.update(key, value)


class QuantityIterator(object):
    """An iterator for disaggregated quantity data."""
    def __init__(
        self,
        unique_id: str,
        index_hash: str,
        domain: Dict[str, str],
        data: Iterable[Tuple[Index, AttributesDict, Optional[float]]],
        label_names: Sequence[str],
        attribute_keys: Iterable[str],
    ):
        self._unique_id = unique_id
        self._index_hash = index_hash

        if domain:
            domain_names, domain_values = zip(*sorted(domain.items()))
            self._domain_names = domain_names
            self._domain_values = domain_values
        else:
            self._domain_names = tuple()
            self._domain_values = tuple()

        self._data = iter(data)
        self._label_names = tuple(label_names)
        self._attribute_keys = tuple(attribute_keys)

    @property
    def unique_id(self) -> str:
        return self._unique_id

    @property
    def index_hash(self) -> str:
        return self._index_hash

    @property
    def domain(self) -> Dict[str, str]:
        return dict(zip(self._domain_names, self._domain_values))

    @property
    def data(self) -> Iterator[Tuple[Index, AttributesDict, Optional[float]]]:
        return self._data

    @property
    def label_names(self) -> Tuple[str, ...]:
        return self._label_names

    @property
    def attribute_keys(self) -> Tuple[str, ...]:
        return self._attribute_keys

    @property
    def columns(self) -> Tuple[str, ...]:
        return self._label_names + self._domain_names + self._attribute_keys + ('value',)

    def __next__(self) -> Tuple[Union[str, float, None], ...]:
        index, attributes, quantity = next(self._data)
        attr_vals = tuple(attributes.get(x) for x in self._attribute_keys)
        return index.labels + self._domain_values + attr_vals + (quantity,)

    def __iter__(self):
        return self

    def __rshift__(self, other: 'TopoNode') -> 'QuantityIterator':
        """Translate quantities to the index of the *other* node."""
        # TODO: Update this method and fix import after this
        # class is moved into a different module.
        from toron.graph import translate
        return translate(self, other)

    def to_pandas(self, index: bool = False) -> 'pd.DataFrame':
        """Return data as a pandas DataFrame object."""
        try:
            import pandas as pd
        except ImportError:
            msg = (
                "Missing optional dependency 'pandas'.  Install pandas to "
                "use this method."
            )
            raise ImportError(msg) from None

        df = pd.DataFrame(self, columns=self.columns)
        string_cols = df.columns[:-1]  # Slice-off "value" column (float64).
        for col in string_cols:  # Using loop for memory efficiency.
            df[col] = df[col].astype('string')

        if index:
            df.set_index(list(self.label_names), inplace=True)

        return df
