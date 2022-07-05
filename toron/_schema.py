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
  | type_info           |  •  •••| element_id       |<-+     +--------------+
  | description         |  •  •  | proportion       |  |     | quantity     |
  | user_properties     |  •  •  | mapping_level    |  |     +--------------+
  | other_uuid          |  •  •  +------------------+  |     | quantity_id  |
  | other_filename_hint |  •  •                        |  +->| _location_id |
  | other_element_hash  |<••  •                        |  |  | attributes   |
  | is_complete         |<•••••      +-----------------+  |  | value        |
  +---------------------+            |                    |  +--------------+
                                     |                    |
                     +------------+  |  +--------------+  |  +---------------+
                     | element    |  |  | location     |  |  | structure     |
                     +------------+  |  +--------------+  |  +---------------+
              +------| element_id |--+  | _location_id |--+  | _structure_id |
              |      | label_a    |••••>| label_a      |<••••| label_a       |
              |      | label_b    |••••>| label_b      |<••••| label_b       |
              |      | label_c    |••••>| label_c      |<••••| label_c       |
              |      | ...        |••••>| ...          |<••••| ...           |
              |      +------------+     +--------------+     +---------------+
              |
              |  +-------------------+                          +----------+
              |  | element_weight    |     +--------------+     | property |
              |  +-------------------+     | weighting    |     +----------+
              |  | element_weight_id |     +--------------+     | key      |
              |  | weighting_id      |<----| weighting_id |     | value    |
              +->| element_id        |•••  | name         |     +----------+
                 | value             |  •  | type_info    |
                 +-------------------+  •  | description  |
                                        ••>| is_complete  |
                                           +--------------+
"""

import itertools
import os
import re
import sqlite3
from contextlib import contextmanager
from json import loads as _loads
from typing import List
from urllib.parse import quote as urllib_parse_quote

from ._exceptions import ToronError


sqlite3.register_converter('TEXT_JSON', _loads)
sqlite3.register_converter('TEXT_ATTRIBUTES', _loads)
sqlite3.register_converter('TEXT_SELECTORS', _loads)
sqlite3.register_converter('TEXT_USERPROPERTIES', _loads)


# Check if SQLite implementation includes JSON1 extension and assign
# SQLITE_JSON1_ENABLED.
#
# The inclusion of JSON functions is optional when compiling SQLite.
# In versions 3.38.0 (2022-02-22) and newer, JSON functions are
# included by default but can be disabled (opt-out policy). For older
# versions of SQLite, JSON functions are available on an opt-in basis.
# It is necessary to test for their presence rathern than referencing
# the SQLite version number.
#
# For more information, see:
#     https://www.sqlite.org/json1.html#compiling_in_json_support
try:
    _con = sqlite3.connect(':memory:')
    _con.execute("SELECT json_valid('123')")
    SQLITE_JSON1_ENABLED = True
except sqlite3.OperationalError:
    SQLITE_JSON1_ENABLED = False
finally:
    _con.close()
    del _con


_schema_script = """
    PRAGMA foreign_keys = ON;

    CREATE TABLE edge(
        edge_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        type_info TEXT_SELECTORS NOT NULL,
        description TEXT,
        user_properties TEXT_USERPROPERTIES,
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
        proportion REAL CHECK (0.0 <= proportion AND proportion <= 1.0) NOT NULL,
        mapping_level INTEGER NOT NULL,
        FOREIGN KEY(edge_id) REFERENCES edge(edge_id) ON DELETE CASCADE,
        FOREIGN KEY(element_id) REFERENCES element(element_id) DEFERRABLE INITIALLY DEFERRED,
        UNIQUE (edge_id, other_element_id, element_id)
    );

    CREATE TABLE element(
        element_id INTEGER PRIMARY KEY AUTOINCREMENT  /* <- Must not reuse id values. */
        /* label columns added programmatically */
    );

    CREATE TABLE location(
        _location_id INTEGER PRIMARY KEY
        /* label columns added programmatically */
    );

    CREATE TABLE structure(
        _structure_id INTEGER PRIMARY KEY
        /* label columns added programmatically */
    );

    CREATE TABLE quantity(
        quantity_id INTEGER PRIMARY KEY,
        _location_id INTEGER,
        attributes TEXT_ATTRIBUTES NOT NULL,
        value NUMERIC NOT NULL,
        FOREIGN KEY(_location_id) REFERENCES location(_location_id)
    );

    CREATE TABLE weighting(
        weighting_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        type_info TEXT_SELECTORS NOT NULL,
        description TEXT,
        is_complete INTEGER CHECK (is_complete IN (0, 1)),
        UNIQUE (name)
    );

    CREATE TABLE element_weight(
        element_weight_id INTEGER PRIMARY KEY,
        weighting_id INTEGER,
        element_id INTEGER,
        value REAL NOT NULL,
        FOREIGN KEY(weighting_id) REFERENCES weighting(weighting_id) ON DELETE CASCADE,
        FOREIGN KEY(element_id) REFERENCES element(element_id) DEFERRABLE INITIALLY DEFERRED,
        UNIQUE (element_id, weighting_id)
    );

    CREATE TABLE property(
        key TEXT PRIMARY KEY NOT NULL,
        value TEXT_JSON
    );

    INSERT INTO property VALUES ('schema_version', '1');
"""


def normalize_identifier(value: str) -> str:
    """Normalize and return a delimited identifier suitable as a SQLite
    column name.

    .. code-block::

        >>> normalize_identifier('A')
        '"A"'
        >>> normalize_identifier('   A   B')
        '"A B"'
    """
    value.encode('utf-8', errors='strict')  # Raises error on surrogate codes.

    nul_pos = value.find('\x00')
    if nul_pos != -1:
        raise UnicodeEncodeError(
            'utf-8',            # encoding
            value,              # object
            nul_pos,            # start position
            nul_pos + 1,        # end position
            'NUL not allowed',  # reason
        )

    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
        value = value.replace('""', '"')
    else:
        value = ' '.join(value.split())

    value = value.replace('"', '""')
    return f'"{value}"'


def sql_drop_label_indexes() -> List[str]:
    """Return list of SQL statements to drop unique label indexes."""
    return [
        'DROP INDEX IF EXISTS unique_element_index',
        'DROP INDEX IF EXISTS unique_location_index',
        'DROP INDEX IF EXISTS unique_structure_index',
    ]


def sql_create_label_indexes(columns: List[str]) -> List[str]:
    """Return list of SQL statements to create unique label indexes."""
    formatted = ', '.join(normalize_identifier(x) for x in columns)
    return [
        f'CREATE UNIQUE INDEX unique_element_index ON element({formatted})',
        f'CREATE UNIQUE INDEX unique_location_index ON location({formatted})',
        f'CREATE UNIQUE INDEX unique_structure_index ON structure({formatted})',
    ]


# The following `sql_column_def_XYZ_label()` functions should follow
# the SQLite syntax described at:
#
#     https://www.sqlite.org/syntax/column-def.html
#
# The returned definitions are suitable for use in ALTER TABLE or
# CREATE TABLE statements.


def sql_column_def_element_label(name: str) -> str:
    """Return an `element` table column-def for a label column."""
    return f"{name} TEXT DEFAULT '-' NOT NULL CHECK ({name} != '')"


def sql_column_def_location_label(name: str) -> str:
    """Return a `location` table column-def for a label column."""
    return f"{name} TEXT DEFAULT '' NOT NULL"


def sql_column_def_structure_label(name: str) -> str:
    """Return a `structure` table column-def for a label column."""
    return f"{name} INTEGER CHECK ({name} IN (0, 1)) DEFAULT 0"


def _user_json_valid(x: str) -> bool:
    """A user-defined function to use when the SQLite JSON1 extension
    is not available (register as 'user_json_valid').

    Returns True if *x* is well-formed JSON and returns False if *x*
    is not well-formed JSON.

    This function mimics the JSON1 json_valid() function, see:
        https://www.sqlite.org/json1.html#jvalid
    """
    try:
        _loads(x)
    except (ValueError, TypeError):
        return False
    return True


def _make_trigger_for_json(insert_or_update, table, column):
    """Return a SQL statement for creating a temporary trigger. The
    trigger is used to validate the contents of TEXT_JSON type columns.
    The trigger will pass without error if the JSON is wellformed.
    """
    if insert_or_update.upper() not in {'INSERT', 'UPDATE'}:
        msg = f"expected 'INSERT' or 'UPDATE', got {insert_or_update!r}"
        raise ValueError(msg)

    if SQLITE_JSON1_ENABLED:
        json_valid_func = 'json_valid'
    else:
        json_valid_func = 'user_json_valid'

    return f'''
        CREATE TEMPORARY TRIGGER IF NOT EXISTS trigger_check_{insert_or_update.lower()}_{table}_{column}
        BEFORE {insert_or_update.upper()} ON main.{table} FOR EACH ROW
        WHEN
            NEW.{column} IS NOT NULL
            AND {json_valid_func}(NEW.{column}) = 0
        BEGIN
            SELECT RAISE(ABORT, '{table}.{column} must be wellformed JSON');
        END;
    '''


def _user_userproperties_valid(x: str) -> bool:
    """A user-defined function to use when the SQLite JSON1 extension
    is not available (register as 'user_userproperties_valid').

    Check if *x* is a wellformed TEXT_USERPROPERTIES value. A
    wellformed TEXT_USERPROPERTIES value is a string containing a
    JSON formatted "object" type (returned as a dict by the loads()
    function). Returns 1 if *x* is valid or 0 if it's not.
    """
    try:
        obj = _loads(x)
    except (ValueError, TypeError):
        return False

    return isinstance(obj, dict)


def _make_trigger_for_user_properties(insert_or_update, table, column):
    """Return a CREATE TRIGGER statement to check TEXT_USERPROPERTIES
    values. This trigger is used to check values before they are saved
    in the database.

    A wellformed TEXT_USERPROPERTIES value is a string containing
    a JSON formatted object.

    The trigger will pass without error if the value is wellformed.
    """
    if SQLITE_JSON1_ENABLED:
        userproperties_are_invalid = \
            f"(json_valid(NEW.{column}) = 0 OR json_type(NEW.{column}) != 'object')"
    else:
        userproperties_are_invalid = f'user_userproperties_valid(NEW.{column}) = 0'

    return f'''
        CREATE TEMPORARY TRIGGER IF NOT EXISTS trigger_check_{insert_or_update.lower()}_{table}_{column}
        BEFORE {insert_or_update.upper()} ON main.{table} FOR EACH ROW
        WHEN
            NEW.{column} IS NOT NULL
            AND {userproperties_are_invalid}
        BEGIN
            SELECT RAISE(ABORT, '{table}.{column} must be wellformed JSON object');
        END;
    '''


def _user_attributes_valid(x: str) -> bool:
    """A user-defined function to use when the SQLite JSON1 extension
    is not available (register as 'user_attributes_valid').

    Returns True if *x* is a wellformed TEXT_ATTRIBUTES value or return
    False if it is not wellformed. A TEXT_ATTRIBUTES value should be a
    JSON object that contains only string values.
    """
    try:
        obj = _loads(x)
    except (ValueError, TypeError):
        return False

    if not isinstance(obj, dict):
        return False

    for value in obj.values():
        if not isinstance(value, str):
            return False

    return True


def _make_trigger_for_attributes(insert_or_update, table, column):
    """Return a SQL statement for creating a temporary trigger. The
    trigger is used to validate the contents of TEXT_ATTRIBUTES
    type columns.

    The trigger will pass without error if the JSON is a wellformed
    "object" containing "text" values.

    The trigger will raise an error if the value is:

      * not wellformed JSON
      * not an "object" type
      * an "object" type that contains one or more "integer", "real",
        "true", "false", "null", "object" or "array" types
    """
    if insert_or_update.upper() not in {'INSERT', 'UPDATE'}:
        msg = f"expected 'INSERT' or 'UPDATE', got {insert_or_update!r}"
        raise ValueError(msg)

    if SQLITE_JSON1_ENABLED:
        attributes_are_invalid = f"""
            (json_valid(NEW.{column}) = 0
                 OR json_type(NEW.{column}) != 'object'
                 OR (SELECT COUNT(*)
                     FROM json_each(NEW.{column})
                     WHERE json_each.type != 'text') != 0)
        """.strip()
    else:
        attributes_are_invalid = f'user_attributes_valid(NEW.{column}) = 0'

    return f'''
        CREATE TEMPORARY TRIGGER IF NOT EXISTS trigger_check_{insert_or_update.lower()}_{table}_{column}
        BEFORE {insert_or_update.upper()} ON main.{table} FOR EACH ROW
        WHEN
            NEW.{column} IS NOT NULL
            AND {attributes_are_invalid}
        BEGIN
            SELECT RAISE(ABORT, '{table}.{column} must be a JSON object with text values');
        END;
    '''


def _add_functions_and_triggers(connection):
    """Create triggers and application-defined functions *connection*.

    Note: This function must not be executed on an empty connection.
    The table schema must exist before triggers can be created.
    """
    if not SQLITE_JSON1_ENABLED:
        try:
            connection.create_function(
                'user_json_valid', 1, _user_json_valid, deterministic=True)
            connection.create_function(
                'user_userproperties_valid', 1, _user_userproperties_valid, deterministic=True)
            connection.create_function(
                'user_attributes_valid', 1, _user_attributes_valid, deterministic=True)
        except TypeError:
            connection.create_function('user_json_valid', 1, _user_json_valid)
            connection.create_function('user_userproperties_valid', 1, _user_userproperties_valid)
            connection.create_function('user_attributes_valid', 1, _user_attributes_valid)

    connection.execute(_make_trigger_for_json('INSERT', 'property', 'value'))
    connection.execute(_make_trigger_for_json('UPDATE', 'property', 'value'))

    connection.execute(_make_trigger_for_user_properties('INSERT', 'edge', 'user_properties'))
    connection.execute(_make_trigger_for_user_properties('UPDATE', 'edge', 'user_properties'))

    jsonflatobj_columns = [
        ('edge', 'type_info'),
        ('quantity', 'attributes'),
        ('weighting', 'type_info'),
    ]
    for table, column in jsonflatobj_columns:
        connection.execute(_make_trigger_for_attributes('INSERT', table, column))
        connection.execute(_make_trigger_for_attributes('UPDATE', table, column))


def _path_to_sqlite_uri(path):
    """Convert a path into a SQLite compatible URI.

    Unlike pathlib's URI handling, SQLite accepts relative URI paths.
    For details, see:

        https://www.sqlite.org/uri.html#the_uri_path
    """
    if os.name == 'nt':  # Windows
        if re.match(r'^[a-zA-Z]:', path):
            path = os.path.abspath(path)  # If drive-letter, must be absolute.
            drive_prefix = f'/{path[:2]}'  # Must not url-quote colon after drive-letter.
            path = path[2:]
        else:
            drive_prefix = ''
        path = path.replace('\\', '/')
        path = urllib_parse_quote(path)
        path = f'{drive_prefix}{path}'
    else:
        path = urllib_parse_quote(path)

    path = re.sub('/+', '/', path)
    return f'file:{path}'


def connect(path, mode='rwc'):
    """Returns a sqlite3 connection to a Toron node file."""
    uri_path = _path_to_sqlite_uri(path)
    uri_path = f'{uri_path}?mode={mode}'

    try:
        get_connection = lambda: sqlite3.connect(
            database=uri_path,
            detect_types=sqlite3.PARSE_DECLTYPES,
            isolation_level=None,
            uri=True,
        )
        if os.path.exists(path):
            con = get_connection()
        else:
            con = get_connection()
            con.executescript(_schema_script)  # Create database schema.
    except sqlite3.OperationalError as err:
        msg = str(err).replace('database file', f'node file {path!r}')
        raise ToronError(msg)

    try:
        _add_functions_and_triggers(con)
    except (sqlite3.OperationalError, sqlite3.DatabaseError):
        # Raises OperationalError when *path* is a database with an unknown
        # schema and DatabaseError when *path* is a file but not a database.
        con.close()
        raise ToronError(f'Path is not a Toron node: {path!r}')

    cur = con.execute("SELECT value FROM property WHERE key='schema_version'")
    schema_version, *_ = cur.fetchone() or (None,)
    cur.close()

    if schema_version != 1:  # When schema version is unsupported.
        msg = f'Unsupported Toron node format: schema version {schema_version!r}'
        raise ToronError(msg)

    return con


_SAVEPOINT_NAME_GENERATOR = (f'svpnt{n}' for n in itertools.count())


class savepoint(object):
    """Context manager to wrap a block of code inside a SAVEPOINT.
    If the block exists without errors, the SAVEPOINT is released
    and the changes are committed. If an error occurs, all of the
    changes are rolled back:

        cur = con.cursor()
        with savepoint(cur):
            cur.execute(...)
    """
    def __init__(self, cursor):
        if cursor.connection.isolation_level is not None:
            isolation_level = cursor.connection.isolation_level
            msg = (
                f'isolation_level must be None, got: {isolation_level!r}\n'
                '\n'
                'For explicit transaction handling, the connection must '
                'be operating in "autocommit" mode. Turn on autocommit '
                'mode by setting "con.isolation_level = None".'
            )
            raise sqlite3.OperationalError(msg)

        self.name = next(_SAVEPOINT_NAME_GENERATOR)
        self.cursor = cursor

    def __enter__(self):
        self.cursor.execute(f'SAVEPOINT {self.name}')

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.cursor.execute(f'RELEASE {self.name}')
        else:
            self.cursor.execute(f'ROLLBACK TO {self.name}')


@contextmanager
def transaction(path_or_connection, mode=None):
    """A context manager that yields a cursor that runs in an
    isolated transaction. If the context manager exits without
    errors, the transaction is committed. If an exception is
    raised, all changes are rolled-back.
    """
    if isinstance(path_or_connection, sqlite3.Connection):
        connection = path_or_connection
        connection_close = lambda: None  # Don't close already-existing cursor.
    else:
        connection = connect(path_or_connection, mode=mode)
        connection_close = connection.close

    cursor = connection.cursor()
    try:
        with savepoint(cursor):
            yield cursor
    finally:
        cursor.close()
        connection_close()

