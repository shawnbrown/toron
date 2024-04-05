"""DataAccessLayer and related helper functions."""

import os
import sys
from dataclasses import dataclass
from ._typing import (
    Dict,
    Optional,
    Type,
    Union,
)

from ._data_models import (
    BaseDataConnector,
    BaseColumnManager,
    BaseIndexRepository,
    BaseLocationRepository,
    BaseStructureRepository,
    BaseWeightingRepository,
    BaseWeightRepository,
    BaseAttributeRepository,
    BaseQuantityRepository,
    BaseEdgeRepository,
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
    DataConnector: Type[BaseDataConnector]
    ColumnManager: Type[BaseColumnManager]
    IndexRepository: Type[BaseIndexRepository]
    LocationRepository: Type[BaseLocationRepository]
    StructureRepository: Type[BaseStructureRepository]
    WeightingRepository: Type[BaseWeightingRepository]
    WeightRepository: Type[BaseWeightRepository]
    AttributeRepository: Type[BaseAttributeRepository]
    QuantityRepository: Type[BaseQuantityRepository]
    EdgeRepository: Type[BaseEdgeRepository]
    RelationRepository: Type[BaseRelationRepository]
    PropertyRepository: Type[BasePropertyRepository]


_loaded_backends: Dict[str, DataAccessLayer] = {}


def get_data_access_layer(backend: str = 'DAL1') -> DataAccessLayer:
    """Load and return a DataAccessLayer instance for a given backend.

    DAL1 is the only available backend and there is no current plan
    to add another (although the structure exists to do so).
    """
    if backend in _loaded_backends:
        return _loaded_backends[backend]  # <- EXIT!

    # The keyword syntax below is verbose but allows for reliable
    # type checking. Type checking with `**kwds` currently requires
    # TypedDict and Unpack which adds complexity and lines of code.

    if backend == 'DAL1':
        from . import _dal1 as mod
        dal = DataAccessLayer(
            backend=backend,
            DataConnector=mod.DataConnector,
            ColumnManager=mod.ColumnManager,
            IndexRepository=mod.IndexRepository,
            LocationRepository=mod.LocationRepository,
            StructureRepository=mod.StructureRepository,
            WeightingRepository=mod.WeightingRepository,
            WeightRepository=mod.WeightRepository,
            AttributeRepository=mod.AttributeRepository,
            QuantityRepository=mod.QuantityRepository,
            EdgeRepository=mod.EdgeRepository,
            RelationRepository=mod.RelationRepository,
            PropertyRepository=mod.PropertyRepository,
        )
        _loaded_backends[backend] = dal
        return dal

    #if backend == 'DAL#':
    #    dal = ...
    #    return dal

    msg = f'could not find data backend {backend!r}'
    raise RuntimeError(msg)


def get_backend_from_path(
    path: Union[str, bytes, os.PathLike]
) -> Optional[str]:
    """Inspect file and return appropriate backend string.

    .. note::
        When inspecting files, this function should make a best
        effort to avoid importing the back-end modules themselves.
        Doing so could load multiple back-ends into memory when
        only one is needed.
    """
    with open(path, 'rb') as f:
        header = f.read(64)  # Read first 64 bytes from file.

    # Check for SQLite header and the 'PRAGMA user_version' for DAL1.
    # See: https://www.sqlite.org/fileformat.html#the_database_header
    if header.startswith(b'SQLite format 3\x00'):
        dal1_magic_number = 0x012D84C8  # Hardcoded to avoid import.
        pragma_user_version = int.from_bytes(header[60:64], byteorder='big')
        if pragma_user_version == dal1_magic_number:
            return 'DAL1'

    ## HDF5 Format Signature (not used, included as an example).
    #hdf5_magic_number = b'\x89\x48\x44\x46\x0d\x0a\x1a\x0a'
    #if header.startswith(hdf5_magic_number):
    #    return 'DAL#'

    return None
