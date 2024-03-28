"""ColumnManager and related objects using SQLite."""

import sqlite3

from toron._typing import (
    Dict,
    Iterable,
    Tuple,
)

from . import schema
from .base_classes import BaseColumnManager


def verify_foreign_key_check(cursor: sqlite3.Cursor) -> None:
    """Run SQLite's "PRAGMA foreign_key_check" to verify that schema
    changes did not break any foreign key constraints. If there are
    foreign key violations, raise a RuntimeError--if not, then pass
    without error.
    """
    cursor.execute('PRAGMA main.foreign_key_check')
    first_ten_violations = cursor.fetchmany(size=1)

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


class ColumnManager(BaseColumnManager):
    def __init__(self, data_reader: sqlite3.Cursor) -> None:
        """Initialize a new instance."""
        self._cursor = data_reader

    def add_columns(self, column: str, *columns: str) -> None:
        """Add new label columns."""
        schema.drop_schema_constraints(self._cursor)

        columns = (column,) + columns
        for column in columns:
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

    if sqlite3.sqlite_version_info >= (3, 25, 0):
        # RENAME COLUMN support added in SQLite 3.25.0 (2018-09-15).
        def update_columns(self, mapping: Dict[str, str]) -> None:
            """Update label column names."""

            if self._cursor.connection.in_transaction:
                # While SQLite 3.25.0 and newer can rename columns inside
                # an existing transaction, this function blocks doing so
                # to maintain consistent behavior with legacy version.
                msg = 'cannot update columns inside an existing transaction'
                raise RuntimeError(msg)

            try:
                self._cursor.execute('BEGIN TRANSACTION')
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
                self._cursor.execute('COMMIT TRANSACTION')
            except Exception as err:
                self._cursor.execute('ROLLBACK TRANSACTION')
                raise  # Re-raise exception.
            finally:
                self._cursor.execute('PRAGMA foreign_keys=ON')

    else:
        # Legacy support: For SQLite versions older than 3.25.0, use a
        # series of operations to rebuild the tables with renamed columns
        # (see https://www.sqlite.org/lang_altertable.html#otheralter).
        def update_columns(self, mapping: Dict[str, str]) -> None:
            """Update label column names."""

            if self._cursor.connection.in_transaction:
                msg = 'cannot update columns inside an existing transaction'
                raise RuntimeError(msg)

            # Build a list of new column names.
            new_columns = []
            for old_col in self.get_columns():
                new_col = mapping.get(old_col, old_col)  # Get new name or default to old.
                if new_col in new_columns:
                    raise ValueError(f'cannot create duplicate columns: {new_col}')
                new_columns.append(new_col)

            self._cursor.execute('PRAGMA foreign_keys=OFF')  # <- Is no-op within transaction
            try:
                self._cursor.execute('BEGIN TRANSACTION')
                schema.drop_schema_constraints(self._cursor)

                # Rebuild 'node_index' table with new column names.
                self._cursor.execute(f"""
                    CREATE TABLE main.new_node_index(
                        index_id INTEGER PRIMARY KEY AUTOINCREMENT,  /* <- Must not reuse id values. */
                        {', '.join(schema.column_def_node_index(x) for x in new_columns)}
                    )
                """)
                self._cursor.execute(
                    'INSERT INTO main.new_node_index SELECT * FROM main.node_index'
                )
                self._cursor.execute('DROP TABLE main.node_index')
                self._cursor.execute('ALTER TABLE main.new_node_index RENAME TO node_index')

                # Rebuild 'location' table with new column names.
                self._cursor.execute(f"""
                    CREATE TABLE main.new_location(
                        _location_id INTEGER PRIMARY KEY,
                        {', '.join(schema.column_def_location(x) for x in new_columns)}
                    )
                """)
                self._cursor.execute(
                    'INSERT INTO main.new_location SELECT * FROM main.location'
                )
                self._cursor.execute('DROP TABLE main.location')
                self._cursor.execute('ALTER TABLE main.new_location RENAME TO location')

                # Rebuild 'structure' table with new column names.
                self._cursor.execute(f"""
                    CREATE TABLE main.new_structure(
                        _structure_id INTEGER PRIMARY KEY,
                        _granularity REAL,
                        {', '.join(schema.column_def_structure(x) for x in new_columns)}
                    )
                """)
                self._cursor.execute(
                    'INSERT INTO main.new_structure SELECT * FROM main.structure'
                )
                self._cursor.execute('DROP TABLE main.structure')
                self._cursor.execute('ALTER TABLE main.new_structure RENAME TO structure')

                # Check integrity, re-create constraints and commit transaction.
                verify_foreign_key_check(self._cursor)
                schema.create_schema_constraints(self._cursor)
                self._cursor.execute('COMMIT TRANSACTION')

            except Exception as err:
                self._cursor.execute('ROLLBACK TRANSACTION')
                raise  # Re-raise exception.

            finally:
                self._cursor.execute('PRAGMA foreign_keys=ON')

    def delete_columns(self, columns: Iterable[str]) -> None:
        """Delete label columns."""
        raise NotImplementedError
