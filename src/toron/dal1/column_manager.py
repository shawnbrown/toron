"""ColumnManager and related objects using SQLite."""

import sqlite3
from itertools import chain

from toron._typing import (
    Dict,
    Tuple,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from toron import Node

from . import schema
from ..data_models import BaseColumnManager


class ColumnManager(BaseColumnManager):
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        """Initialize a new instance."""
        self._cursor = cursor

    def add_columns(self, column: str, *columns: str) -> None:
        """Add new label columns."""
        schema.drop_schema_constraints(self._cursor)

        for column in chain([column], columns):
            self._cursor.execute(f"""
                ALTER TABLE main.node_index ADD COLUMN
                    {schema.column_def_node_index(column)}
            """)
            self._cursor.execute(f"""
                ALTER TABLE main.location ADD COLUMN
                    {schema.column_def_location(column)}
            """)
            self._cursor.execute(f"""
                ALTER TABLE main.structure ADD COLUMN
                    {schema.column_def_structure(column)}
            """)

        schema.create_schema_constraints(self._cursor)

    def get_columns(self) -> Tuple[str, ...]:
        """Get a tuple of label column names."""
        self._cursor.execute(f"PRAGMA main.table_info('node_index')")
        columns = tuple(row[1] for row in self._cursor.fetchall())
        return columns[1:]  # Return columns (slicing-off index_id).

    def rename_columns(self, mapping: Dict[str, str]) -> None:
        """Rename label columns."""

        if sqlite3.sqlite_version_info < (3, 25, 0):
            msg = (
                f"This feature requires SQLite 3.25.0 or newer. The current running "
                f"Python is bundled with SQLite {sqlite3.sqlite_version}.\n"
                f"\n"
                f"Use the helper function 'toron.dal1.legacy_rename_columns(...)' instead."
            )
            raise Exception(msg)

        for name, new_name in mapping.items():
            self._cursor.execute(f"""
                ALTER TABLE main.node_index
                    RENAME COLUMN {name} TO {new_name}
            """)
            self._cursor.execute(f"""
                ALTER TABLE main.location
                    RENAME COLUMN {name} TO {new_name}
            """)
            self._cursor.execute(f"""
                ALTER TABLE main.structure
                    RENAME COLUMN {name} TO {new_name}
            """)

    def drop_columns(self, column: str, *columns: str) -> None:
        """Remove label columns."""

        if sqlite3.sqlite_version_info < (3, 35, 5):
            msg = (
                f"This feature requires SQLite 3.35.5 or newer. The current running "
                f"Python is bundled with SQLite {sqlite3.sqlite_version}.\n"
                f"\n"
                f"Use the helper function 'toron.dal1.legacy_drop_columns(...)' instead."
            )
            raise Exception(msg)

        columns_to_delete = \
            set(chain([column], columns)).intersection(self.get_columns())

        schema.drop_schema_constraints(self._cursor)

        for column in columns_to_delete:
            column = schema.format_identifier(column)
            self._cursor.execute(
                f'ALTER TABLE main.node_index DROP COLUMN {column}'
            )
            self._cursor.execute(
                f'ALTER TABLE main.location DROP COLUMN {column}'
            )
            self._cursor.execute(
                f'ALTER TABLE main.structure DROP COLUMN {column}'
            )

        schema.create_schema_constraints(self._cursor)


def verify_foreign_key_check(cursor: sqlite3.Cursor) -> None:
    """Run SQLite's "PRAGMA foreign_key_check" to verify that schema
    changes did not break any foreign key constraints. If there are
    foreign key violations, raise a RuntimeError--if not, then pass
    without error.
    """
    cursor.execute('PRAGMA main.foreign_key_check')
    first_ten_violations = cursor.fetchmany(size=10)

    if not first_ten_violations:
        return  # <- EXIT!

    formatted = '\n  '.join(str(x) for x in first_ten_violations)
    msg = (
        f'Legacy support for SQLite {sqlite3.sqlite_version} encountered '
        f'unexpected foreign key violations:\n  {formatted}'
    )
    additional_count = sum(1 for row in cursor)  # Count remaining.
    if additional_count:
        msg = (
            f'{msg}\n'
            f'  ...\n'
            f'  Additionally, {additional_count} more violations occurred.'
        )
    raise RuntimeError(msg)


def legacy_rename_columns(node: 'Node', mapping: Dict[str, str]) -> None:
    """Rename label columns (for legacy SQLite versions).

    RENAME COLUMN support was added in SQLite 3.25.0 (2018-09-15).
    """
    # This function implements the recommended 12-step procedure for schema
    # changes (see https://www.sqlite.org/lang_altertable.html#otheralter).

    if node._dal.backend != 'DAL1':
        msg = f"expected Node with 'DAL1' backend, got {node._dal.backend!r}"
        raise TypeError(msg)

    with node._managed_cursor() as cursor:
        manager = ColumnManager(cursor)

        if cursor.connection.in_transaction:
            msg = 'cannot rename columns inside an existing transaction'
            raise RuntimeError(msg)

        # Build a list of new column names.
        new_columns = []
        for old_col in manager.get_columns():
            new_col = mapping.get(old_col, old_col)  # Get new name or default to old.
            if new_col in new_columns:
                raise ValueError(f'cannot create duplicate columns: {new_col}')
            new_columns.append(new_col)

        cursor.execute('PRAGMA foreign_keys=OFF')  # <- Must be outside transaction.
        try:
            cursor.execute('BEGIN TRANSACTION')
            schema.drop_schema_constraints(cursor)

            # Rebuild 'node_index' table with new column names.
            cursor.execute(f"""
                CREATE TABLE main.new_node_index(
                    index_id INTEGER PRIMARY KEY AUTOINCREMENT,  /* <- Must not reuse id values. */
                    {', '.join(schema.column_def_node_index(x) for x in new_columns)}
                )
            """)
            cursor.execute(
                'INSERT INTO main.new_node_index SELECT * FROM main.node_index'
            )
            cursor.execute('DROP TABLE main.node_index')
            cursor.execute('ALTER TABLE main.new_node_index RENAME TO node_index')

            # Rebuild 'location' table with new column names.
            cursor.execute(f"""
                CREATE TABLE main.new_location(
                    _location_id INTEGER PRIMARY KEY,
                    {', '.join(schema.column_def_location(x) for x in new_columns)}
                )
            """)
            cursor.execute(
                'INSERT INTO main.new_location SELECT * FROM main.location'
            )
            cursor.execute('DROP TABLE main.location')
            cursor.execute('ALTER TABLE main.new_location RENAME TO location')

            # Rebuild 'structure' table with new column names.
            cursor.execute(f"""
                CREATE TABLE main.new_structure(
                    _structure_id INTEGER PRIMARY KEY,
                    _granularity REAL,
                    {', '.join(schema.column_def_structure(x) for x in new_columns)}
                )
            """)
            cursor.execute(
                'INSERT INTO main.new_structure SELECT * FROM main.structure'
            )
            cursor.execute('DROP TABLE main.structure')
            cursor.execute('ALTER TABLE main.new_structure RENAME TO structure')

            # Check integrity, re-create constraints, and commit transaction.
            verify_foreign_key_check(cursor)
            schema.create_schema_constraints(cursor)
            cursor.execute('COMMIT TRANSACTION')

        except Exception as err:
            cursor.execute('ROLLBACK TRANSACTION')
            raise  # Re-raise exception.

        finally:
            cursor.execute('PRAGMA foreign_keys=ON')  # <- Must be outside transaction.


def legacy_drop_columns(node: 'Node', column: str, *columns: str) -> None:
    """Remove columns (for legacy SQLite versions).

    DROP COLUMN support was first added in SQLite 3.35.0 and important
    bugfixes were added in 3.35.5 (2021-04-19).
    """
    # This function implements the recommended 12-step procedure for schema
    # changes (see https://www.sqlite.org/lang_altertable.html#otheralter).

    if node._dal.backend != 'DAL1':
        msg = f"expected Node with 'DAL1' backend, got {node._dal.backend!r}"
        raise TypeError(msg)

    with node._managed_cursor() as cursor:
        manager = ColumnManager(cursor)

        if cursor.connection.in_transaction:
            msg = 'cannot delete columns inside an existing transaction'
            raise RuntimeError(msg)

        # Get list of columns to keep (must preserve existing order).
        columns_to_delete = tuple(chain([column], columns))
        columns_to_keep = [
            col for col in manager.get_columns() if col not in columns_to_delete
        ]
        if not columns_to_keep:
            msg = (
                'cannot delete all columns\n'
                '\n'
                'Without at least one index column, a node cannot represent '
                'any weights, quantities, or relations it might contain.'
            )
            raise RuntimeError(msg)

        formatted_columns_to_keep = [
            schema.format_identifier(x) for x in columns_to_keep
        ]

        cursor.execute('PRAGMA foreign_keys=OFF')  # <- Must be outside transaction.
        try:
            cursor.execute('BEGIN TRANSACTION')
            schema.drop_schema_constraints(cursor)

            # Rebuild 'node_index' table with columns_to_keep.
            cursor.execute(f"""
                CREATE TABLE main.new_node_index(
                    index_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {', '.join(schema.column_def_node_index(x) for x in columns_to_keep)}
                )
            """)
            cursor.execute(f"""
                INSERT INTO main.new_node_index
                SELECT index_id, {', '.join(formatted_columns_to_keep)}
                FROM main.node_index
            """)
            cursor.execute('DROP TABLE main.node_index')
            cursor.execute('ALTER TABLE main.new_node_index RENAME TO node_index')

            # Rebuild 'location' table with columns_to_keep.
            cursor.execute(f"""
                CREATE TABLE main.new_location(
                    _location_id INTEGER PRIMARY KEY,
                    {', '.join(schema.column_def_location(x) for x in columns_to_keep)}
                )
            """)
            cursor.execute(f"""
                INSERT INTO main.new_location
                SELECT _location_id, {', '.join(formatted_columns_to_keep)}
                FROM main.location
            """)
            cursor.execute('DROP TABLE main.location')
            cursor.execute('ALTER TABLE main.new_location RENAME TO location')

            # Rebuild 'structure' table with columns_to_keep.
            cursor.execute(f"""
                CREATE TABLE main.new_structure(
                    _structure_id INTEGER PRIMARY KEY,
                    _granularity REAL,
                    {', '.join(schema.column_def_structure(x) for x in columns_to_keep)}
                )
            """)
            cursor.execute(f"""
                INSERT INTO main.new_structure
                SELECT _structure_id, _granularity, {', '.join(formatted_columns_to_keep)}
                FROM main.structure
            """)
            cursor.execute('DROP TABLE main.structure')
            cursor.execute('ALTER TABLE main.new_structure RENAME TO structure')

            # Check integrity, re-create constraints, and commit transaction.
            verify_foreign_key_check(cursor)
            schema.create_schema_constraints(cursor)
            cursor.execute('COMMIT TRANSACTION')

        except Exception as err:
            cursor.execute('ROLLBACK TRANSACTION')
            raise  # Re-raise exception.

        finally:
            cursor.execute('PRAGMA foreign_keys=ON')  # <- Must be outside transaction.
