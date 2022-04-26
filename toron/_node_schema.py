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


_primitive_types = (str, int, float, bool, type(None), bytes, complex)


def get_primitive_repr(obj):
    """Return repr string for supported, non-container values."""
    for type_ in _primitive_types:
        if obj.__class__ is type_:
            obj_repr = repr(obj)
            try:
                if obj == literal_eval(obj_repr):
                    return obj_repr
            except Exception:
                return None
    return None


def serialize_list_or_tuple(obj):
    """Serialize a list or tuple of primitive items as a string."""
    for item in obj:
        if get_primitive_repr(item) is None:
            msg = f'cannot serialize item of type {item.__class__}'
            raise TypeError(msg)

    return repr(obj)


def serialize_set(obj):
    """Serialize a set of primitive items as a string."""
    member_reprs = []
    for item in obj:
        item_repr = get_primitive_repr(item)
        if item_repr is None:
            msg = f'cannot serialize member of type {item.__class__}'
            raise TypeError(msg)
        member_reprs.append(item_repr)

    return f'{{{", ".join(sorted(member_reprs))}}}'


def serialize_dict(obj):
    """Serialize a dictionary of basic types to a Python-literal
    formatted string. Keys and values must be instances of one of
    the supported types. Dictionary items do not preserve their
    original order but are serialized in alphabetical order by key.

    Supported types: str, bytes, int, float, bool, complex, NoneType
    """
    item_reprs = []
    for key, value in obj.items():
        key_repr = get_primitive_repr(key)
        if key_repr is None:
            msg = f'cannot serialize key of type {key.__class__}'
            raise TypeError(msg)

        value_repr = get_primitive_repr(value)
        if value_repr is None:
            msg = f'cannot serialize value of type {value.__class__}'
            raise TypeError(msg)

        item_reprs.append(f'{key_repr}: {value_repr}')

    return f'{{{", ".join(sorted(item_reprs))}}}'


class InvalidSerialization(object):
    """Wrapper class for strings that cannot be deserialized."""
    def __init__(self, invalid_s):
        self.data = invalid_s

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.data == other.data

    def __repr__(self):
        cls_name = self.__class__.__name__
        return f'{cls_name}({self.data!r})'


def dumps(obj):
    """Return a string representing the serialized content of *obj*."""
    obj_repr = get_primitive_repr(obj)
    if obj_repr:
        return obj_repr

    if (obj.__class__ is list) or (obj.__class__ is tuple):
        return serialize_list_or_tuple(obj)

    if obj.__class__ is set:
        return serialize_set(obj)

    if obj.__class__ is dict:
        return serialize_dict(obj)

    msg = f'cannot serialize object of type {obj.__class__}'
    raise TypeError(msg)


def loads(s, errors='strict'):
    """Return an object deserialized from a string of literals."""
    try:
        return literal_eval(s)
    except Exception as e:
        if errors == 'strict':
            raise  # Reraise original error.
        elif errors == 'warn':
            import warnings
            msg = f'cannot deserialize string: {s!r}'
            warnings.warn(msg, category=RuntimeWarning)
            return InvalidSerialization(s)
        elif errors == 'ignore':
            return None

        msg = "*errors* must be 'strict', 'warn', or 'ignore'"
        raise ValueError(msg)


def _is_sqlite_json1_supported():
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


SQLITE_JSON1_ENABLED = _is_sqlite_json1_supported()


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
    def json_type(x):
        """Returns the JSON type of the outermost element of
        *x*. The type returned is one of the following text
        values: 'null', 'true', 'false', 'integer', 'real',
        'text', 'array', or 'object'. Raises a ValueError if
        *x* is not well-formed.

        For details, see:
            https://www.sqlite.org/json1.html#jtype
        """
        if x is None:
            return None  # <- EXIT!

        obj = _loads(x)
        if isinstance(obj, dict):
            return 'object'
        if isinstance(obj, list):
            return 'array'
        if isinstance(obj, str):
            return 'text'
        if isinstance(obj, int) and not isinstance(obj, bool):
            return 'integer'
        if isinstance(obj, float):
            return 'real'
        if obj == True:
            return 'true'
        if obj == False:
            return 'false'
        if obj is None:
            return 'null'
        raise ValueError

    def json_valid(x):
        """Return 1 if *x* is well-formed JSON or return 0 if *x*
        is not well-formed.

        For details, see:
            https://www.sqlite.org/json1.html#jvalid
        """
        try:
            _loads(x)
        except (ValueError, TypeError):
            return 0
        except TypeError as err:
            print(err)
            raise
        return 1

    def json_object_is_flat(x):
        """This function has no equivalent SQLite JSON function.
        When SQLite includes JSON support, the built-in json_each()
        function is used. But since json_each() is a table-valued
        function, there is no built-in way to recreate it as an
        application defined function.

        For details, see:
            https://www.sqlite.org/json1.html#jeach
        """
        try:
            obj = _loads(x)
        except (ValueError, TypeError):
            return 0
        for value in obj.values():
            if isinstance(value, (dict, list)):
                return 0
        return 1

    def _pre_execute_functions(con):
        try:
            con.create_function('json_type', 1, json_type, deterministic=True)
            con.create_function('json_valid', 1, json_valid, deterministic=True)
            con.create_function('json_object_is_flat', 1, json_object_is_flat, deterministic=True)
            con.create_function('is_flat_json_object', 1, _is_flat_json_object, deterministic=True)
        except TypeError:
            # The `deterministic` param not supported until Python 3.8.
            con.create_function('json_type', 1, json_type)
            con.create_function('json_valid', 1, json_valid)
            con.create_function('json_object_is_flat', 1, json_object_is_flat)
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

