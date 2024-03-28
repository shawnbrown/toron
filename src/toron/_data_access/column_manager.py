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

    def update_columns(self, mapping: Dict[str, str]) -> None:
        """Update label column names."""
        raise NotImplementedError

    def delete_columns(self, columns: Iterable[str]) -> None:
        """Delete label columns."""
        raise NotImplementedError
