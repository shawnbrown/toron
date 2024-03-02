"""Database schema functions and information for Toron node files.

Toron nodes are stored as individual files. The file format is managed,
internally, as a relational database. The schema for this database is
shown below as a simplified ERD (entity relationship diagram). SQL
foreign key relationships are represented with hyphen and pipe
characters (``---`` and ``|``). Other, more complex relationships are
represented with bullet points (``•••``) and these are enforced at the
application layer:

.. code-block:: text

                                       <Other Node> ••••••••
                                                           •  +-----------------+
                                    +----------------+     •  | attribute       |
    +----------------------+        | relation       |     •  +-----------------+
    | edge                 |        +----------------+     •  | attribute_id    |--+
    +----------------------+        | relation_id    |     •  | attribute_value |  |
    | edge_id              |------->| edge_id        |     •  +-----------------+  |
    | name                 |  ••••••| other_index_id |<•••••                       |
    | description          |  •  •••| index_id       |<-+     +-----------------+  |
    | selectors            |  •  •  | relation_value |  |     | quantity        |  |
    | user_properties      |  •  •  | proportion*    |  |     +-----------------+  |
    | other_unique_id      |  •  •  | mapping_level* |  |     | quantity_id     |  |
    | other_filename_hint  |  •  •  +----------------+  |  +->| _location_id    |  |
    | other_index_hash*    |<••  •                      |  |  | attribute_id    |<-+
    | is_locally_complete* |<•••••                      |  |  | quantity_value  |
    | is_default           |          +-----------------+  |  +-----------------+
    +----------------------+          |                    |
                                      |                    |  +---------------+
                      +------------+  |  +--------------+  |  | structure     |
                      | node_index |  |  | location     |  |  +---------------+
                      +------------+  |  +--------------+  |  | _structure_id |
                   +--| index_id   |--+  | _location_id |--+  | _granularity* |
                   |  | label_a    |••••>| label_a      |<••••| label_a*      |
                   |  | label_b    |••••>| label_b      |<••••| label_b*      |
                   |  | label_c    |••••>| label_c      |<••••| label_c*      |
                   |  | ...        |••••>| ...          |<••••| ...           |
                   |  +------------+     +--------------+     +---------------+
                   |
                   |  +--------------+                          +----------+
                   |  | weight       |     +--------------+     | property |
                   |  +--------------+     | weighting    |     +----------+
                   |  | weight_id    |     +--------------+     | key      |
                   |  | weighting_id |<----| weighting_id |     | value    |
                   +->| index_id     |•••  | name         |     +----------+
                      | weight_value |  •  | description  |
                      +--------------+  •  | selectors    |
                                        ••>| is_complete* |
                                           +--------------+

Asterisks (``*``) denote values that are computed at the application
layer using data from elsewhere in the schema. Toron may automatically
recompute these values as records and columns are added or removed
from certain tables.
"""

import sqlite3
from contextlib import closing

from toron._typing import (
    Callable,
)


def create_node_schema(connection: sqlite3.Connection) -> None:
    """Creates tables and sets starting values for Toron node schema.

    This function expects a *connection* to a newly-created, or
    otherwise empty database.
    """
    with closing(connection.cursor()) as cur:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur if row[0] != 'sqlite_sequence'}
        if tables:
            formatted = ', '.join(repr(x) for x in sorted(tables))
            msg = f'database must be empty; found tables: {formatted}'
            raise RuntimeError(msg)

    connection.executescript("""
        PRAGMA foreign_keys = ON;

        CREATE TABLE main.edge(
            edge_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            selectors TEXT_SELECTORS,
            user_properties TEXT_USERPROPERTIES,
            other_unique_id TEXT NOT NULL,
            other_filename_hint TEXT,
            other_index_hash TEXT,
            is_locally_complete INTEGER NOT NULL CHECK (is_locally_complete IN (0, 1)) DEFAULT 0,
            is_default INTEGER CHECK (is_default IS NULL OR is_default=1) DEFAULT NULL,
            UNIQUE (name, other_unique_id),
            UNIQUE (is_default, other_unique_id)
            /*
                Note: The column `is_default` uses 1 and NULL (instead
                of 1 and 0) so that the UNIQUE constraint can limit each
                `other_unique_id` to a single 1 but allow for multple
                NULLs since a NULL value does not test as equal to other
                NULL values.
            */
        );

        CREATE TABLE main.relation(
            relation_id INTEGER PRIMARY KEY,
            edge_id INTEGER,
            other_index_id INTEGER NOT NULL,
            index_id INTEGER,
            relation_value REAL NOT NULL CHECK (0.0 <= relation_value),
            proportion REAL CHECK (0.0 <= proportion AND proportion <= 1.0),
            mapping_level BLOB_BITFLAGS,
            FOREIGN KEY(edge_id) REFERENCES edge(edge_id) ON DELETE CASCADE,
            FOREIGN KEY(index_id) REFERENCES node_index(index_id) DEFERRABLE INITIALLY DEFERRED,
            UNIQUE (edge_id, other_index_id, index_id)
        );

        CREATE TABLE main.node_index(
            index_id INTEGER PRIMARY KEY AUTOINCREMENT  /* <- Must not reuse id values. */
            /* label columns added programmatically */
        );

        CREATE TABLE main.location(
            _location_id INTEGER PRIMARY KEY
            /* label columns added programmatically */
        );

        CREATE TABLE main.structure(
            _structure_id INTEGER PRIMARY KEY,
            _granularity REAL
            /* label columns added programmatically */
        );

        CREATE TABLE main.attribute(
            attribute_id INTEGER PRIMARY KEY,
            attribute_value TEXT_ATTRIBUTES NOT NULL,
            UNIQUE (attribute_value)
        );

        CREATE TABLE main.quantity(
            quantity_id INTEGER PRIMARY KEY,
            _location_id INTEGER,
            attribute_id INTEGER,
            quantity_value NUMERIC NOT NULL,
            FOREIGN KEY(_location_id) REFERENCES location(_location_id),
            FOREIGN KEY(attribute_id) REFERENCES attribute(attribute_id) ON DELETE CASCADE
        );

        CREATE TABLE main.weighting(
            weighting_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            selectors TEXT_SELECTORS,
            is_complete INTEGER NOT NULL CHECK (is_complete IN (0, 1)) DEFAULT 0,
            UNIQUE (name)
        );

        CREATE TABLE main.weight(
            weight_id INTEGER PRIMARY KEY,
            weighting_id INTEGER,
            index_id INTEGER CHECK (index_id > 0),
            weight_value REAL NOT NULL,
            FOREIGN KEY(weighting_id) REFERENCES weighting(weighting_id) ON DELETE CASCADE,
            FOREIGN KEY(index_id) REFERENCES node_index(index_id) DEFERRABLE INITIALLY DEFERRED,
            UNIQUE (index_id, weighting_id)
        );

        CREATE TABLE main.property(
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT_JSON
        );

        /* Set properties for Toron schema and application versions. */
        INSERT INTO main.property VALUES ('toron_schema_version', '"0.2.0"');
        INSERT INTO main.property VALUES ('toron_app_version', '"0.1.0"');

        /* Reserve id zero for an "undefined" record. */
        INSERT INTO main.node_index (index_id) VALUES (0);
    """)


def create_sql_function(
    connection: sqlite3.Connection,
    name: str,
    narg: int,
    func: Callable,
    *,
    deterministic: bool = False,
) -> None:
    """Create a user-defined SQL function."""
    try:  # `deterministic` argument added in Python 3.8
        connection.create_function(name, narg, func, deterministic=deterministic)
    except TypeError:
        connection.create_function(name, narg, func)

    # Note: SQLite versions older than 3.8.3 will raise a NotSupportedError
    # if the `deterministic` argument is used but Toron does not currently
    # support SQLite versions older than 3.21.0.
