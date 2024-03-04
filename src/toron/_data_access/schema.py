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
from json import (
    dumps as json_dumps,
    loads as json_loads,
)

from toron._typing import (
    Callable,
    Final,
    Optional,
)
from toron._utils import BitFlags


sqlite3.register_adapter(BitFlags, bytes)

with closing(sqlite3.connect(':memory:')) as _con:
    # Check for SQLite compile-time options. When SQLite is compiled,
    # certain features can be enabled or omitted. When available, Toron
    # makes use of:
    #
    # * JSON Functions And Operators:
    #    - https://www.sqlite.org/json1.html#compiling_in_json_support
    #    - https://www.sqlite.org/compile.html#enable_json1
    # * Built-In Mathematical SQL Functions:
    #    - https://www.sqlite.org/lang_mathfunc.html#overview
    #    - https://www.sqlite.org/compile.html#enable_math_functions
    #
    # When these features are not available, Toron creates user-defined
    # functions to achieve the same functionality.

    def _succeeds(sql: str) -> bool:
        try:
            _con.execute(sql)
            return True
        except sqlite3.OperationalError:
            return False

    SQLITE_ENABLE_JSON1: Final[bool] = _succeeds("SELECT json_valid('123')")
    SQLITE_ENABLE_MATH_FUNCTIONS: Final[bool] = _succeeds('SELECT log2(64)')

    del _succeeds
    del _con


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


def create_toron_check_property_value(connection: sqlite3.Connection) -> None:
    """Create a app-defined SQL function named ``toron_check_property_value``."""
    def toron_check_property_value(x):
        try:
            json_loads(x)
        except (ValueError, TypeError):
            return 0
        return 1

    create_sql_function(connection,
                        name='toron_check_property_value',
                        narg=1,
                        func=toron_check_property_value,
                        deterministic=True)


def create_triggers_property_value(connection: sqlite3.Connection) -> None:
    """Add temp triggers to validate ``property.value`` column."""
    if SQLITE_ENABLE_JSON1:
        check_function = 'json_valid'
    else:
        check_function = 'toron_check_property_value'

    sql = f"""
        CREATE TEMPORARY TRIGGER IF NOT EXISTS trigger_check_{{event}}_property_value
        BEFORE {{event}} ON main.property FOR EACH ROW
        WHEN
            NEW.value IS NOT NULL
            AND {check_function}(NEW.value) = 0
        BEGIN
            SELECT RAISE(ABORT, 'property.value must be well-formed JSON');
        END;
    """
    with closing(connection.cursor()) as cur:
        cur.execute(sql.format(event='INSERT'))
        cur.execute(sql.format(event='UPDATE'))


def create_toron_check_attribute_value(connection: sqlite3.Connection) -> None:
    """Create a user defined SQL function named ``toron_check_attribute_value``.

    Returns True if *x* is a wellformed TEXT_ATTRIBUTES value or return
    False if it is not wellformed. A TEXT_ATTRIBUTES value should be a
    JSON object that contains only string values.

    This is used when JSON functions are not available in SQLite.
    """
    def toron_check_attribute_value(x):
        try:
            obj = json_loads(x)
        except (ValueError, TypeError):
            return 0
        if not isinstance(obj, dict):
            return 0
        for value in obj.values():
            if not isinstance(value, str):
                return 0
        return 1

    create_sql_function(connection,
                        name='toron_check_attribute_value',
                        narg=1,
                        func=toron_check_attribute_value,
                        deterministic=True)


def create_triggers_attribute_value(connection: sqlite3.Connection) -> None:
    """Add temp triggers to validate ``attribute.attribute_value`` column.

    The ``attribute_value`` column is of the type TEXT_ATTRIBUTES which
    must be a well-formed JSON "object" containing "text" values.

    The trigger will raise an error if the value is:

      * not wellformed JSON
      * not an "object" type
      * an "object" type that contains one or more "integer", "real",
        "true", "false", "null", "object" or "array" types
    """
    if SQLITE_ENABLE_JSON1:
        attributes_are_invalid = """
            (json_valid(NEW.attribute_value) = 0
                 OR json_type(NEW.attribute_value) != 'object'
                 OR (SELECT COUNT(*)
                     FROM json_each(NEW.attribute_value)
                     WHERE json_each.type != 'text') != 0)
        """.strip()
    else:
        attributes_are_invalid = f'toron_check_attribute_value(NEW.attribute_value) = 0'

    sql = f"""
        CREATE TEMPORARY TRIGGER IF NOT EXISTS trigger_check_{{event}}_attribute_attribute_value
        BEFORE {{event}} ON main.attribute FOR EACH ROW
        WHEN
            NEW.attribute_value IS NOT NULL
            AND {attributes_are_invalid}
        BEGIN
            SELECT RAISE(ABORT, 'attribute.attribute_value must be a JSON object with text values');
        END;
    """
    with closing(connection.cursor()) as cur:
        cur.execute(sql.format(event='INSERT'))
        cur.execute(sql.format(event='UPDATE'))


def create_toron_check_user_properties(connection: sqlite3.Connection) -> None:
    """Create a user defined SQL function named ``toron_check_user_properties``.

    Returns 1 if *x* is a wellformed TEXT_USERPROPERTIES value or return
    0 if it is not wellformed. A wellformed TEXT_USERPROPERTIES value is
    JSON formatted "object" containing values of any type.
    """
    def toron_check_user_properties(x):
        try:
            obj = json_loads(x)
        except (ValueError, TypeError):
            return 0
        return 1 if isinstance(obj, dict) else 0

    create_sql_function(connection,
                        name='toron_check_user_properties',
                        narg=1,
                        func=toron_check_user_properties,
                        deterministic=True)


def create_triggers_user_properties(connection: sqlite3.Connection) -> None:
    """Add temp triggers to validate ``edge.user_properties`` column.

    A well-formed TEXT_USERPROPERTIES value is a string containing
    a JSON object type.

    The trigger will pass without error if the value is well-formed.
    """
    if SQLITE_ENABLE_JSON1:
        userproperties_are_invalid = \
            f"(json_valid(NEW.user_properties) = 0 OR json_type(NEW.user_properties) != 'object')"
    else:
        userproperties_are_invalid = f'toron_check_user_properties(NEW.user_properties) = 0'

    sql = f"""
        CREATE TEMPORARY TRIGGER IF NOT EXISTS trigger_check_{{event}}_edge_user_properties
        BEFORE {{event}} ON main.edge FOR EACH ROW
        WHEN
            NEW.user_properties IS NOT NULL
            AND {userproperties_are_invalid}
        BEGIN
            SELECT RAISE(ABORT, 'edge.user_properties must be well-formed JSON object type');
        END;
    """
    with closing(connection.cursor()) as cur:
        cur.execute(sql.format(event='INSERT'))
        cur.execute(sql.format(event='UPDATE'))


def create_toron_check_selectors(connection: sqlite3.Connection) -> None:
    """Create a user defined SQL function named ``toron_check_selectors``.

    Returns 1 if *x* is a wellformed TEXT_SELECTORS value or return
    0 if it is not wellformed. A wellformed TEXT_SELECTORS value is
    JSON formatted "array" containing "string" values.
    """
    def toron_check_selectors(x):
        try:
            obj = json_loads(x)
        except (ValueError, TypeError):
            return 0
        if not isinstance(obj, list):
            return 0
        for value in obj:
            if not isinstance(value, str):
                return 0
        return 1

    create_sql_function(connection,
                        name='toron_check_selectors',
                        narg=1,
                        func=toron_check_selectors,
                        deterministic=True)


def create_triggers_selectors(connection: sqlite3.Connection) -> None:
    """Add temp triggers to validate ``edge.selectors`` and
    ``weighting.selectors`` columns.

    The trigger will pass without error when the value is a wellformed
    JSON "array" containing "text" elements.

    The trigger will raise an error when the value is:
      * not wellformed JSON
      * not an "array" type
      * an "array" type that contains one or more "integer", "real",
        "true", "false", "null", "object" or "array" elements
    """
    if SQLITE_ENABLE_JSON1:
        selectors_are_invalid = """
            (json_valid(NEW.selectors) = 0
                 OR json_type(NEW.selectors) != 'array'
                 OR (SELECT COUNT(*)
                     FROM json_each(NEW.selectors)
                     WHERE json_each.type != 'text') != 0)
        """.strip()
    else:
        selectors_are_invalid = 'toron_check_selectors(NEW.selectors) = 0'

    sql = f"""
        CREATE TEMPORARY TRIGGER IF NOT EXISTS trigger_check_{{event}}_{{table}}_selectors
        BEFORE {{event}} ON main.{{table}} FOR EACH ROW
        WHEN
            NEW.selectors IS NOT NULL
            AND {selectors_are_invalid}
        BEGIN
            SELECT RAISE(ABORT, '{{table}}.selectors must be a JSON array with text values');
        END;
    """
    with closing(connection.cursor()) as cur:
        cur.execute(sql.format(event='INSERT', table='edge'))
        cur.execute(sql.format(event='UPDATE', table='edge'))
        cur.execute(sql.format(event='INSERT', table='weighting'))
        cur.execute(sql.format(event='UPDATE', table='weighting'))


def create_log2(
    connection: sqlite3.Connection, alt_name: Optional[str] = None
) -> None:
    """Create a user defined SQL function named ``log2``.

    This function should serve as a drop-in replacement when the
    built-in ``log2`` function is not available:

        https://www.sqlite.org/lang_mathfunc.html#log2

    An *alt_name* can be given for testing and debugging.
    """
    from math import log2 as _log2  # Import math only if needed.

    def log2(x):
        try:
            return _log2(x)
        except (ValueError, TypeError):  # Return None on error to match
            return None                  # SQLite's log2 behavior.

    create_sql_function(connection,
                        name=alt_name or 'log2',
                        narg=1,
                        func=log2,
                        deterministic=True)


def create_toron_apply_bit_flag(connection: sqlite3.Connection) -> None:
    """Create a user defined SQL function named ``toron_apply_bit_flag``.

    The SQL function has the following signature:

        toron_apply_bit_flag(VALUE, BIT_FLAGS, INDEX)

    The BIT_FLAGS value is a binary blob suitable for interpretation
    as a BitFlags object. If the bit at the given INDEX is 1, then
    VALUE is returned, if the bit at the given INDEX is 0, then None
    is returned. If BIT_FLAGS itself is None, then VALUE is returned.

    The following example uses BitFlags(1, 0, 1) in its binary blob
    form ``X'A0'``::

        >>> cur.execute("SELECT toron_apply_bit_flag('foo', X'A0', 0)")
        >>> cur.fetchall()
        [('foo',)]
        >>> cur.execute("SELECT toron_apply_bit_flag('bar', X'A0', 1)")
        >>> cur.fetchall()
        [(None,)]
        >>> cur.execute("SELECT toron_apply_bit_flag('baz', X'A0', 2)")
        >>> cur.fetchall()
        [('baz',)]

    The bit at INDEX 0 is 1 so 'foo' is returned. The bit at INDEX 1
    is 0, so None is returned. And the bit at INDEX 2 is 1, so 'baz'
    is returned.
    """
    def toron_apply_bit_flag(value, bytes_bit_flags, bit_index):
        if bytes_bit_flags is None:
            return value  # <- EXIT!
        bit_flags = BitFlags(bytes_bit_flags)
        try:
            bit_flag = bit_flags[bit_index]
        except IndexError:
            bit_flag = 0
        return value if bit_flag else None

    create_sql_function(connection,
                        name='toron_apply_bit_flag',
                        narg=3,
                        func=toron_apply_bit_flag,
                        deterministic=True)


def create_toron_json_object_keep(connection: sqlite3.Connection) -> None:
    """Create a user defined SQL function named ``toron_json_object_keep``.

    Return a JSON object keeping only the given *keys*::

        >>> cur.execute(
        ...    'SELECT toron_json_object_keep(?, ?, ?)',
        ...    ('{"a": "one", "b": "two", "c": "three"}', 'a', 'b'),
        ... )
        >>> cur.fetchall()
        [('{"a": "one", "b": "two"}',)]

    If no *keys* are given, returns a complete and normalized JSON
    object::

        >>> cur.execute(
        ...    'SELECT toron_json_object_keep(?)',
        ...    ('{"a": "one", "b": "two", "c": "three"}',),
        ... )
        >>> cur.fetchall()
        [('{"a": "one", "b": "two", "c": "three"}',)]

    If *keys* are given but none of them match the keys in the JSON
    object, then None is returned::

        >>> cur.execute(
        ...    'SELECT toron_json_object_keep(?, ?, ?, ?)',
        ...    ('{"a": "one", "b": "two", "c": "three"}', 'x', 'y', 'z'),
        ... )
        >>> cur.fetchall()
        [(None,)]
    """
    def toron_json_object_keep(json_obj, *keys):
        obj = json_loads(json_obj)
        if not isinstance(obj, dict):
            class_name = obj.__class__.__name__
            msg = f'expected JSON object type, got {class_name}: {json_obj}'
            raise ValueError(msg)
        if not keys:
            return json_dumps(obj, sort_keys=True)
        obj_subset = {k: obj[k] for k in keys if k in obj}
        if obj_subset:
            return json_dumps(obj_subset, sort_keys=True)
        return None

    create_sql_function(connection,
                        name='toron_json_object_keep',
                        narg=-1,  # Using -1 to indicate variable args.
                        func=toron_json_object_keep,
                        deterministic=True)


def create_functions_and_temporary_triggers(
    connection: sqlite3.Connection
) -> None:
    """Create SQL functions and temporary triggers for Toron schema.

    .. important::

        This function should only be called with a *connection* to a
        Toron node SQL schema. It should not be called on an empty
        database or a database containing some other schema.
    """
    if not SQLITE_ENABLE_JSON1:
        create_toron_check_property_value(connection)
        create_toron_check_attribute_value(connection)
        create_toron_check_user_properties(connection)
        create_toron_check_selectors(connection)

    create_triggers_property_value(connection)
    create_triggers_attribute_value(connection)
    create_triggers_user_properties(connection)
    create_triggers_selectors(connection)

    if not SQLITE_ENABLE_MATH_FUNCTIONS:
        create_log2(connection)

    create_toron_apply_bit_flag(connection)
    create_toron_json_object_keep(connection)
