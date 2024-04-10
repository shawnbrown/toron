"""Back-end implementation for data access API using SQLite."""

from .data_connector import DataConnector
from .column_manager import ColumnManager
from .repositories import (
    IndexRepository,
    LocationRepository,
    StructureRepository,
    WeightingRepository,
    WeightRepository,
    AttributeRepository,
    QuantityRepository,
    EdgeRepository,
    RelationRepository,
    PropertyRepository,
)
from .schema import DAL1_MAGIC_NUMBER