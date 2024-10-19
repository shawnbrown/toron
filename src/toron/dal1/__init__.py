"""Back-end implementation for data access API using SQLite."""

if __import__('sqlite3').sqlite_version_info < (3, 21, 0):
    raise RuntimeError(
        f"This build of Python {__import__('platform').python_version()} "
        f"is bundled with SQLite {__import__('sqlite3').sqlite_version}. "
        f"But Toron's \"DAL1\" backend requires SQLite 3.21.0 or newer.\n"
        f"\n"
        f"Please use an updated Python build with a newer version of SQLite."
    )


from .data_connector import DataConnector
from .column_manager import (
    ColumnManager,
    legacy_rename_columns,
    legacy_drop_columns,
)
from .repositories import (
    IndexRepository,
    LocationRepository,
    StructureRepository,
    WeightGroupRepository,
    WeightRepository,
    AttributeGroupRepository,
    QuantityRepository,
    CrosswalkRepository,
    RelationRepository,
    PropertyRepository,
)
from .schema import RESERVED_IDENTIFIERS
