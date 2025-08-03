"""DataAccessLayer and related helper functions."""

import os
import sys
from dataclasses import dataclass
from ._typing import (
    Callable,
    Dict,
    Optional,
    Set,
    Type,
    Union,
)

from .data_models import (
    TORON_MAGIC_NUMBER,
    COMMON_RESERVED_IDENTIFIERS,
    BaseDataConnector,
    BaseColumnManager,
    BaseIndexRepository,
    BaseLocationRepository,
    BaseStructureRepository,
    BaseWeightGroupRepository,
    BaseWeightRepository,
    BaseAttributeGroupRepository,
    BaseQuantityRepository,
    BaseCrosswalkRepository,
    BaseRelationRepository,
    BasePropertyRepository,
)


if sys.version_info >= (3, 10, 0):
    _kwds = {'frozen': True, 'kw_only': True}
else:
    _kwds = {'frozen': True}

@dataclass(**_kwds)
class DataAccessLayer(object):
    """A namespace for related data access classes."""
    backend: str
    reserved_identifiers: Set[str]
    DataConnector: Type[BaseDataConnector]
    ColumnManager: Type[BaseColumnManager]
    IndexRepository: Type[BaseIndexRepository]
    LocationRepository: Type[BaseLocationRepository]
    StructureRepository: Type[BaseStructureRepository]
    WeightGroupRepository: Type[BaseWeightGroupRepository]
    WeightRepository: Type[BaseWeightRepository]
    AttributeGroupRepository: Type[BaseAttributeGroupRepository]
    QuantityRepository: Type[BaseQuantityRepository]
    CrosswalkRepository: Type[BaseCrosswalkRepository]
    RelationRepository: Type[BaseRelationRepository]
    PropertyRepository: Type[BasePropertyRepository]
    optimizations: Dict[str, Callable]


_loaded_backends: Dict[str, DataAccessLayer] = {}


def get_data_access_layer(backend: Optional[str] = None) -> DataAccessLayer:
    """Load and return a DataAccessLayer instance for a given backend.

    DAL1 is the only available backend and there is no current plan
    to add another (although the structure exists to do so).
    """
    if not backend:
        backend = 'DAL1'  # If omitted, use 'DAL1' (the default backend).

    if backend in _loaded_backends:
        return _loaded_backends[backend]  # <- EXIT!

    # The keyword syntax below is verbose but allows for reliable
    # type checking. Type checking with `**kwds` currently requires
    # TypedDict and Unpack which adds complexity and lines of code.

    if backend == 'DAL1':
        from . import dal1 as mod

        dal = DataAccessLayer(
            backend=backend,
            reserved_identifiers=mod.RESERVED_IDENTIFIERS,
            DataConnector=mod.DataConnector,
            ColumnManager=mod.ColumnManager,
            IndexRepository=mod.IndexRepository,
            LocationRepository=mod.LocationRepository,
            StructureRepository=mod.StructureRepository,
            WeightGroupRepository=mod.WeightGroupRepository,
            WeightRepository=mod.WeightRepository,
            AttributeGroupRepository=mod.AttributeGroupRepository,
            QuantityRepository=mod.QuantityRepository,
            CrosswalkRepository=mod.CrosswalkRepository,
            RelationRepository=mod.RelationRepository,
            PropertyRepository=mod.PropertyRepository,
            optimizations=mod.optimizations,
        )
        _loaded_backends[backend] = dal
        return dal

    #if backend == 'DAL#':
    #    dal = ...
    #    return dal

    msg = f'could not find data backend {backend!r}'
    raise RuntimeError(msg)


def get_backend_from_path(path: Union[str, bytes, os.PathLike]) -> str:
    """Inspect file and return appropriate backend string.
    If the file type is not supported, returns ``None``.

    .. note::
        When inspecting files, this function should make a best
        effort to avoid importing the back-end modules themselves.
        Doing so could load multiple back-ends into memory when
        only one is needed.
    """
    with open(path, 'rb') as f:
        header = f.read(72)  # Read first 72 bytes from file.

    # Check for SQLite header, 'application_id', and 'user_version'.
    # See: https://www.sqlite.org/fileformat.html#the_database_header
    if header.startswith(b'SQLite format 3\x00'):
        application_id = header[68:72]
        user_version = header[60:64]
        if application_id == TORON_MAGIC_NUMBER and user_version == b'DAL1':
            return 'DAL1'

    ## HDF5 Format Signature (not used, included as an example).
    #hdf5_magic_number = b'\x89\x48\x44\x46\x0d\x0a\x1a\x0a'
    #if header.startswith(hdf5_magic_number):
    #    return 'DAL#'

    msg = f'{path!r} does not appear to be a Toron file'
    raise ValueError(msg)
