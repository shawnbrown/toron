"""Database schema functions and information for Toron node files.

Toron nodes are stored as individual files. The file format is
managed, internally, as a relational database. The schema for this
database is shown below as a simplified ERD (entity relationship
diagram). SQL foreign key relationships are represented with hyphen
and pipe characters ('-' and '|'). Other, more complex relationships
are represented with bullet points ('•') and these are enforced at
the application layer:

                                 +------------------+
  +---------------------+        | relation         |
  | edge                |        +------------------+
  +---------------------+        | relation_id      |     •••• <Other Node>
  | edge_id             |------->| edge_id          |     •
  | name                |  ••••••| other_element_id |<•••••
  | description         |  •  •••| element_id       |<-+     +-------------+
  | type_info           |  •  •  | proportion       |  |     | quantity    |
  | optional_attributes |  •  •  | mapping_level    |  |     +-------------+
  | other_uuid          |  •  •  +------------------+  |     | quantity_id |
  | other_filename_hint |  •  •                        |  +->| location_id |
  | other_element_hash  |<••  •                        |  |  | attributes  |
  | is_complete         |<•••••       +----------------+  |  | value       |
  +---------------------+             |                   |  +-------------+
                                      |                   |
                      +------------+  |  +-------------+  |  +--------------+
                      | element    |  |  | location    |  |  | structure    |
                      +------------+  |  +-------------+  |  +--------------+
              +-------| element_id |--+  | location_id |--+  | structure_id |
              |       | label_a    |••••>| label_a     |<••••| label_a      |
              |       | label_b    |••••>| label_b     |<••••| label_b      |
              |       | label_c    |••••>| label_c     |<••••| label_c      |
              |       | ...        |••••>| ...         |<••••| ...          |
              |       +------------+     +-------------+     +--------------+
              |
              |  +----------------+                            +----------+
              |  | weight         |     +----------------+     | property |
              |  +----------------+     | weight_info    |     +----------+
              |  | weight_id      |     +----------------+     | key      |
              |  | weight_info_id |<----| weight_info_id |     | value    |
              +->| element_id     |•••  | name           |     +----------+
                 | value          |  •  | description    |
                 +----------------+  •  | type_info      |
                                     ••>| is_complete    |
                                        +----------------+
"""

import os
import sqlite3
from json import loads as _loads
from ast import literal_eval


def _is_sqlite_json1_enabled():
    """Check if SQLite implementation includes JSON1 extension."""
    # The inclusion of JSON functions is optional when compiling SQLite.
    # In versions 3.38.0 and newer, JSON functions are included by
    # default but can be disabled (opt-out policy). For older versions
    # of SQLite, JSON functions are available on an opt-in basis. It is
    # necessary to test for their presence rathern than referencing the
    # SQLite version number.
    #
    # For more information, see:
    #     https://www.sqlite.org/json1.html#compiling_in_json_support

    con = sqlite3.connect(':memory:')
    try:
        con.execute("SELECT json_valid('123')")
    except sqlite3.OperationalError:
        return False
    finally:
        con.close()
    return True


SQLITE_JSON1_ENABLED = _is_sqlite_json1_enabled()


def _is_flat_json_object(x):
    """Returns 1 if *x* is a wellformed, flat, JSON object string,
    else returns 0. This function should be registered with SQLite
    (via the create_function() method) when the JSON1 extension is
    not available.
    """
    try:
        obj = _loads(x)
    except (ValueError, TypeError):
        return 0

    if not isinstance(obj, dict):
        return 0

    for value in obj.values():
        if isinstance(value, (dict, list)):
            return 0

    return 1


if not SQLITE_JSON1_ENABLED:
    def _pre_execute_functions(con):
        try:
            con.create_function('is_flat_json_object', 1, _is_flat_json_object, deterministic=True)
        except TypeError:
            con.create_function('is_flat_json_object', 1, _is_flat_json_object)
else:
    def _pre_execute_functions(con):
        pass


_schema_script = """
    PRAGMA foreign_keys = ON;

    CREATE TABLE edge(
        edge_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        type_info TEXT_JSONFLATOBJ NOT NULL,
        optional_attributes TEXT_JSONFLATOBJ,
        other_uuid TEXT CHECK (other_uuid LIKE '________-____-____-____-____________') NOT NULL,
        other_filename_hint TEXT NOT NULL,
        other_element_hash TEXT,
        is_complete INTEGER CHECK (is_complete IN (0, 1)),
        UNIQUE (name, other_uuid)
    );

    CREATE TABLE relation(
        relation_id INTEGER PRIMARY KEY,
        edge_id INTEGER,
        other_element_id INTEGER NOT NULL,
        element_id INTEGER,
        proportion REAL CHECK (0.0 < proportion AND proportion <= 1.0) NOT NULL,
        mapping_level INTEGER NOT NULL,
        FOREIGN KEY(edge_id) REFERENCES edge(edge_id),
        FOREIGN KEY(element_id) REFERENCES element(element_id),
        UNIQUE (edge_id, other_element_id, element_id)
    );

    CREATE TABLE element(
        element_id INTEGER PRIMARY KEY AUTOINCREMENT  /* <- Must not reuse id values. */
        /* label columns added programmatically */
    );

    CREATE TABLE location(
        location_id INTEGER PRIMARY KEY
        /* label columns added programmatically */
    );

    CREATE TABLE structure(
        structure_id INTEGER PRIMARY KEY
        /* label columns added programmatically */
    );

    CREATE TABLE quantity(
        quantity_id INTEGER PRIMARY KEY,
        location_id INTEGER,
        attributes TEXT_JSONFLATOBJ NOT NULL,
        value NUMERIC NOT NULL,
        FOREIGN KEY(location_id) REFERENCES location(location_id)
    );

    CREATE TABLE weight_info(
        weight_info_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        type_info TEXT_JSONFLATOBJ NOT NULL,
        is_complete INTEGER CHECK (is_complete IN (0, 1)),
        UNIQUE (name)
    );

    CREATE TABLE weight(
        weight_id INTEGER PRIMARY KEY,
        weight_info_id INTEGER,
        element_id INTEGER,
        value REAL NOT NULL,
        FOREIGN KEY(element_id) REFERENCES element(element_id),
        FOREIGN KEY(weight_info_id) REFERENCES weight_info(weight_info_id)
    );

    CREATE TABLE property(
        key TEXT PRIMARY KEY NOT NULL,
        value TEXT_JSON
    );
"""


def _make_trigger_for_jsonflatobj(insert_or_update, table, column):
    """Return a SQL statement for creating a temporary trigger. The
    trigger is used to validate the contents of TEXT_JSONFLATOBJ type
    columns.

    The trigger will pass without error if the JSON is a wellformed
    "object" whose values are "text", "integer", "real", "true",
    "false", or "null" types.

    The trigger will raise an error if the value is:

      * not wellformed JSON
      * not an "object" type
      * an "object" type that contains one or more "object" or "array"
        types (i.e., a container of other nested containers)
    """
    if insert_or_update.upper() not in {'INSERT', 'UPDATE'}:
        msg = f"expected 'INSERT' or 'UPDATE', got {insert_or_update!r}"
        raise ValueError(msg)

    if SQLITE_JSON1_ENABLED:
        when_clause = f"""
            NEW.{column} IS NOT NULL
            AND (json_valid(NEW.{column}) = 0
                 OR json_type(NEW.{column}) != 'object'
                 OR (SELECT COUNT(*)
                     FROM json_each(NEW.{column})
                     WHERE json_each.type IN ('object', 'array')) != 0)
        """.rstrip()
    else:
        when_clause = f' is_flat_json_object(NEW.{column}) = 0'  # <- Keep leading space.

    return f'''
        CREATE TEMPORARY TRIGGER IF NOT EXISTS trg_assert_flat_{table}_{column}_{insert_or_update.lower()}
        AFTER {insert_or_update.upper()} ON main.{table} FOR EACH ROW
        WHEN{when_clause}
        BEGIN
            SELECT RAISE(
                ABORT,
                '{column} must be JSON object containing strings, numbers, true, false, or null'
            );
        END;
    '''


def _execute_post_schema_triggers(cur):
    """Create triggers for columns of declared type 'TEXT_JSONFLATOBJ'.

    Note: This function must not be executed on an empty connection.
    The table schema must exist before triggers can be created.
    """
    jsonflatobj_columns = [
        ('edge', 'type_info'),
        ('edge', 'optional_attributes'),
        ('quantity', 'attributes'),
        ('weight_info', 'type_info'),
    ]
    for table, column in jsonflatobj_columns:
        cur.execute(_make_trigger_for_jsonflatobj('INSERT', table, column))
        cur.execute(_make_trigger_for_jsonflatobj('UPDATE', table, column))


def connect(path):
    """Returns a sqlite3 connection to a Toron node file. If *path*
    doesn't exist, a new node is created at this location.
    """
    if os.path.exists(path):
        try:
            con = sqlite3.connect(path)
            _pre_execute_functions(con)
        except sqlite3.OperationalError:
            # If *path* is a directory or non-file resource, then
            # calling `connect()` will raise an OperationalError.
            raise Exception(f'path {path!r} is not a Toron Node')
    else:
        con = sqlite3.connect(path)
        _pre_execute_functions(con)
        con.executescript(_schema_script)

    cur = con.cursor()
    try:
        _execute_post_schema_triggers(cur)
    finally:
        cur.close()

    return con

