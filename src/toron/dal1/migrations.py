"""Database schema version migration functions."""
import json
import sqlite3
from toron._typing import (
    Optional,
)

from . import schema
from toron._utils import BitFlags


def v020_to_v030_relation_table(
    cursor: sqlite3.Cursor, whole_space_level: bytes
) -> None:
    """Update 'relation' constraints for 0.2.0 to 0.3.0 migration."""
    # Create new 'relation' table with updated constraints.
    cursor.execute("""
        CREATE TABLE main.new_relation(
            relation_id INTEGER PRIMARY KEY,
            crosswalk_id INTEGER NOT NULL,
            other_index_id INTEGER NOT NULL CHECK (other_index_id != 0),
            index_id INTEGER NOT NULL,
            mapping_level BLOB_BITFLAGS NOT NULL,
            relation_value REAL NOT NULL CHECK (TYPEOF(relation_value) IN ("real", "integer") AND relation_value >= 0.0),
            proportion REAL CHECK (proportion BETWEEN 0.0 AND 1.0),
            FOREIGN KEY(crosswalk_id) REFERENCES crosswalk(crosswalk_id) ON DELETE CASCADE,
            FOREIGN KEY(index_id) REFERENCES node_index(index_id) DEFERRABLE INITIALLY DEFERRED,
            UNIQUE (crosswalk_id, other_index_id, index_id, mapping_level)
        );
    """)

    # Transfer contents into new 'relation' table making sure
    # to omit records where other_index_id is 0 and also replace
    # NULL mapping_level values with "whole space" bytes.
    cursor.execute("""
        INSERT INTO main.new_relation
        SELECT
            relation_id,
            crosswalk_id,
            other_index_id,
            index_id,
            COALESCE(mapping_level, ?),
            relation_value,
            proportion
        FROM main.relation
        WHERE other_index_id != 0
    """, (whole_space_level,))

    # Drop old 'relation' table and rename new table.
    cursor.execute('DROP TABLE main.relation')
    cursor.execute('ALTER TABLE main.new_relation RENAME TO relation')


def v020_to_v030_properties(cursor: sqlite3.Cursor) -> None:
    """Update 'property' values for 0.2.0 to 0.3.0 migration."""
    # Update domain (change `dict` to `str`).
    cursor.execute("SELECT value FROM main.property WHERE key='domain'")
    domain_result = cursor.fetchone()
    if domain_result:
        if isinstance(domain_result[0], str):
            domain_dict = json.loads(domain_result[0])
        else:
            domain_dict = domain_result[0]

        if not isinstance(domain_dict, dict):
            raise Exception(
                'in DAL1 schema 0.2.0, the domain property should be a '
                'JSON Object'
            )

        if len(domain_dict) == 1 and 'domain' in domain_dict:
            domain = domain_dict['domain']
        else:
            domain = '_'.join(f'{x}_{y}' for x, y in sorted(domain_dict.items()))
    else:
        domain = ''

    cursor.execute(
        "UPDATE main.property SET value=? WHERE key='domain'",
        (json.dumps(domain),)
    )

    # Update schema version number.
    cursor.execute("""
        UPDATE main.property
        SET value='"0.3.0"'
        WHERE key='toron_schema_version'
    """)


def apply_migrations(
    cursor: sqlite3.Cursor, mode: Optional[str] = None
) -> None:
    """Update a DAL1 node schema to the latest version."""
    # This function implements the recommended 12-step procedure for schema
    # changes (see https://www.sqlite.org/lang_altertable.html#otheralter).

    # Get current schema version.
    cursor.execute("SELECT value FROM main.property "
                   "WHERE key='toron_schema_version'")
    toron_schema_version = cursor.fetchone()[0]

    # Exit without changes if schema already uses the latest version.
    if toron_schema_version in {'0.3.0', '"0.3.0"'}:
        return  # <- EXIT!

    if mode == 'ro':
        raise Exception(
            'Node schema version is out of date, unable to update when file '
            'is open in read-only mode. Open and save the file in read-write '
            'mode to update schema.'
        )

    # Get "whole space" mapping level as bytes (to replace any NULLs in relation).
    cursor.execute("PRAGMA main.table_info('node_index')")
    whole_space_len = len(cursor.fetchall()) - 1  # Minus one because index_id not counted.
    whole_space_level = bytes(BitFlags([1] * whole_space_len))

    # Verify that we are not inside an existing transation.
    if cursor.connection.in_transaction:
        msg = 'cannot update schema version inside an existing transaction'
        raise RuntimeError(msg)

    cursor.execute('PRAGMA foreign_keys=OFF')  # <- Must be outside transaction.
    try:
        cursor.execute('BEGIN TRANSACTION')
        schema.drop_schema_constraints(cursor)

        # Apply migrations.
        if toron_schema_version in {'0.2.0', '"0.2.0"'}:
            v020_to_v030_relation_table(cursor, whole_space_level)
            v020_to_v030_properties(cursor)

        # Check integrity, re-create constraints, and commit transaction.
        schema.verify_foreign_key_check(cursor)
        schema.create_schema_constraints(cursor)
        cursor.execute('COMMIT TRANSACTION')

    except Exception as err:
        cursor.execute('ROLLBACK TRANSACTION')
        raise  # Re-raise exception.

    finally:
        cursor.execute('PRAGMA foreign_keys=ON')  # <- Must be outside transaction.
