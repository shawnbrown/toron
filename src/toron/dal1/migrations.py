"""Database schema version migration functions."""
import json
import sqlite3
from itertools import chain
from toron._typing import (
    Optional,
)

from . import schema
from toron._utils import BitFlags


def v020_to_v030_step01_link_table(cursor: sqlite3.Cursor) -> None:
    """Update 'link' tables for 0.2.0 to 0.3.0 migration."""
    # Rename table and columns 'crosswalk' -> 'link'.
    cursor.execute("""
        CREATE TABLE main.new_link(
            link_id INTEGER PRIMARY KEY,
            other_unique_id TEXT NOT NULL,
            other_filename_hint TEXT,
            name TEXT NOT NULL,
            description TEXT,
            selectors TEXT_SELECTORS,
            is_default INTEGER CHECK (is_default IS NULL OR is_default=1) DEFAULT NULL,
            user_properties TEXT_USERPROPERTIES,
            other_index_hash TEXT,
            is_locally_complete INTEGER NOT NULL CHECK (is_locally_complete IN (0, 1)) DEFAULT 0,
            UNIQUE (name, other_unique_id),
            UNIQUE (is_default, other_unique_id)
        );
    """)
    # Transfer contents from old 'crosswalk' table into new 'link' table.
    cursor.execute("""
        INSERT INTO main.new_link
        SELECT
            crosswalk_id,  /* <- link_id in new table */
            other_unique_id,
            other_filename_hint,
            name,
            description,
            selectors,
            is_default,
            user_properties,
            other_index_hash,
            is_locally_complete
        FROM main.crosswalk
    """)

    # Drop old 'crosswalk' table and rename new 'link' table.
    cursor.execute('DROP TABLE main.crosswalk')
    cursor.execute('ALTER TABLE main.new_link RENAME TO link')

def v020_to_v030_step02_relation_table(
    cursor: sqlite3.Cursor, whole_space_level: bytes
) -> None:
    """Update 'mapping' tables for 0.2.0 to 0.3.0 migration."""
    # Rename table and columns 'relation' -> 'mapping'.
    cursor.execute("""
        CREATE TABLE main.new_mapping(
            mapping_id INTEGER PRIMARY KEY,
            link_id INTEGER NOT NULL,
            other_index_id INTEGER NOT NULL CHECK (TYPEOF(other_index_id) = 'integer'),
            index_id INTEGER NOT NULL,
            mapping_level BLOB_BITFLAGS NOT NULL,
            mapping_value REAL NOT NULL CHECK (TYPEOF(mapping_value) IN ('real', 'integer') AND mapping_value >= 0.0),
            proportion REAL CHECK (proportion BETWEEN 0.0 AND 1.0 OR proportion IS NULL),
            CHECK (other_index_id != 0 OR index_id != 0),
            FOREIGN KEY(link_id) REFERENCES link(link_id) ON DELETE CASCADE,
            FOREIGN KEY(index_id) REFERENCES node_index(index_id) DEFERRABLE INITIALLY DEFERRED,
            UNIQUE (link_id, other_index_id, index_id, mapping_level)
        );
    """)

    # Transfer contents into new 'mapping' table making sure to omit
    # any undefined-to-undefined records (0 -> 0) and also replace NULL
    # mapping_level values with "whole space" bytes.
    cursor.execute("""
        INSERT INTO main.new_mapping
        SELECT
            relation_id,  /* <- mapping_id in new table */
            crosswalk_id,  /* <- link_id in new table */
            other_index_id,
            index_id,
            COALESCE(mapping_level, ?),
            relation_value,  /* <- mapping_value in new table */
            proportion
        FROM main.relation
        WHERE NOT (other_index_id = 0 AND index_id = 0);
    """, (whole_space_level,))

    # Drop old 'relation' table and rename new 'mapping' table.
    cursor.execute('DROP TABLE main.relation')
    cursor.execute('ALTER TABLE main.new_mapping RENAME TO mapping')


def v020_to_v030_step03_quantity_table(cursor: sqlite3.Cursor) -> None:
    """Update 'quantity' table and values for 0.2.0 to 0.3.0 migration."""
    # Create new 'quantity' table with a UNIQUE constraint.
    cursor.execute("""
        CREATE TABLE main.new_quantity(
            quantity_id INTEGER PRIMARY KEY,
            _location_id INTEGER NOT NULL,
            attribute_group_id INTEGER NOT NULL,
            quantity_value NUMERIC NOT NULL CHECK (TYPEOF(quantity_value) IN ('integer', 'real')),
            FOREIGN KEY(_location_id) REFERENCES location(_location_id) ON DELETE CASCADE,
            FOREIGN KEY(attribute_group_id) REFERENCES attribute_group(attribute_group_id) ON DELETE CASCADE,
            UNIQUE (_location_id, attribute_group_id)
        );
    """)

    # Transfer contents into new 'quantity' table making sure to sum the
    # values for records that share the same location and attribute group.
    cursor.execute("""
        INSERT INTO main.new_quantity
        SELECT
            MIN(quantity_id),
            _location_id,
            attribute_group_id,
            SUM(quantity_value)
        FROM main.quantity
        GROUP BY _location_id, attribute_group_id
    """)

    # Drop old 'quantity' table and rename new table.
    cursor.execute('DROP TABLE main.quantity')
    cursor.execute('ALTER TABLE main.new_quantity RENAME TO quantity')


def v020_to_v030_step04_rename_label_tables(cursor: sqlite3.Cursor) -> None:
    """Rename "label" tables for 0.2.0 to 0.3.0 migration."""
    cursor.execute('PRAGMA legacy_alter_table = 0')
    cursor.execute('ALTER TABLE main.node_index RENAME TO label_index')
    cursor.execute('ALTER TABLE main.location RENAME TO label_location')
    cursor.execute('ALTER TABLE main.structure RENAME TO label_structure')


def v020_to_v030_step05_properties(cursor: sqlite3.Cursor) -> None:
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

    # Add registered attributes.
    cursor.execute('SELECT attributes FROM main.attribute_group '
                   'WHERE attributes IS NOT NULL')
    generator = (
        json.loads(attr_dict) if isinstance(attr_dict, str) else attr_dict
        for (attr_dict,) in cursor  # Unpack single item in `for` clause.
    )
    attribute_keys = sorted(set(chain.from_iterable(generator)))
    cursor.execute(
        "INSERT INTO main.property VALUES('registered_attributes', ?)",
        (json.dumps(attribute_keys),)
    )

    # Update "partition_definitions" key.
    cursor.execute("""
        UPDATE main.property
        SET key='partition_definitions'
        WHERE key='discrete_categories'
    """)

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
            v020_to_v030_step01_link_table(cursor)
            v020_to_v030_step02_relation_table(cursor, whole_space_level)
            v020_to_v030_step03_quantity_table(cursor)
            v020_to_v030_step04_rename_label_tables(cursor)
            v020_to_v030_step05_properties(cursor)

        # Check integrity, re-create constraints, and commit transaction.
        schema.verify_foreign_key_check(cursor)
        schema.create_schema_constraints(cursor)
        cursor.execute('COMMIT TRANSACTION')

    except Exception as err:
        cursor.execute('ROLLBACK TRANSACTION')
        raise  # Re-raise exception.

    finally:
        cursor.execute('PRAGMA foreign_keys=ON')  # <- Must be outside transaction.
