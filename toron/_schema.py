"""Database schema functions and information for Toron node files.

Toron nodes are stored as individual files. The file format is
managed, internally, as a relational database. The schema for this
database is shown below as a simplified ERD (entity relationship
diagram). SQL foreign key relationships are represented with hyphen
and pipe characters ('-' and '|'). Other, more complex relationships
are represented with bullet points ('•') and these are enforced at
the application layer:

                                 +----------------+
 +----------------------+        | relation       |
 | edge                 |        +----------------+
 +----------------------+        | relation_id    |     ••••• <Other Node>
 | edge_id              |------->| edge_id        |     •
 | name                 |  ••••••| other_index_id |<•••••
 | description          |  •  •••| index_id       |<-+     +----------------+
 | selectors            |  •  •  | relation_value |  |     | quantity       |
 | user_properties      |  •  •  | proportion*    |  |     +----------------+
 | other_unique_id      |  •  •  | mapping_level* |  |     | quantity_id    |
 | other_filename_hint  |  •  •  +----------------+  |  +->| _location_id   |
 | other_index_hash*    |<••  •                      |  |  | attributes     |
 | is_locally_complete* |<•••••                      |  |  | quantity_value |
 | is_default           |          +-----------------+  |  +----------------+
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

Asterisks (*) denote values that are computed at the application layer
using data from elsewhere in the schema. Toron may automatically
recompute these values as records and columns are added or removed
from certain tables.
"""

import itertools
import os
import re
import sqlite3
from collections import UserList
from contextlib import contextmanager
from json import loads as _loads
from json import dumps as _dumps
from ._typing import (
    overload,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    TypeAlias,
    Union,
)
from urllib.parse import quote as urllib_parse_quote

from ._utils import ToronError
from ._selectors import convert_text_selectors


_schema_script = """
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

    CREATE TABLE main.quantity(
        quantity_id INTEGER PRIMARY KEY,
        _location_id INTEGER,
        attributes TEXT_ATTRIBUTES NOT NULL,
        quantity_value NUMERIC NOT NULL,
        FOREIGN KEY(_location_id) REFERENCES location(_location_id)
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
        index_id INTEGER,
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
    INSERT INTO main.property VALUES ('toron_schema_version', '"0.1.0"');
    INSERT INTO main.property VALUES ('toron_app_version', '"0.1.0"');

    /* Reserve id zero for an "undefined" record. */
    INSERT INTO main.node_index (index_id) VALUES (0);
"""


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
    try:
        _con.execute("SELECT json_valid('123')")
        SQLITE_JSON1_ENABLED = True
    except sqlite3.OperationalError:
        SQLITE_JSON1_ENABLED = False

    try:
        _con.execute('SELECT log2(64)')
        SQLITE_ENABLE_MATH_FUNCTIONS = True
    except sqlite3.OperationalError:
        SQLITE_ENABLE_MATH_FUNCTIONS = False
finally:
    _con.close()
    del _con


sqlite3.register_converter('TEXT_JSON', _loads)
sqlite3.register_converter('TEXT_ATTRIBUTES', _loads)
sqlite3.register_converter('TEXT_SELECTORS', convert_text_selectors)
sqlite3.register_converter('TEXT_USERPROPERTIES', _loads)


class BitFlags(Sequence[Literal[0, 1]]):
    """A sequence of 0s and 1s used to encode multiple true/false or
    on/off values. This class can be registered with SQLite to support
    a "BLOB_BITFLAGS" data type.

    Create a BitFlags object from arguments of 0 or 1 (bit sequences
    are padded to the nearest multiple of 8)::

        >>> BitFlags(1, 1, 0, 1, 0)
        BitFlags(1, 1, 0, 1, 0, 0, 0, 0)

    Other values are converted to 0 and 1 based on their truth value::

        >>> BitFlags('x', 'x', '', 'x', '', '', '', '')
        BitFlags(1, 1, 0, 1, 0, 0, 0, 0)

    Change a BitFlags object into bytes::

        >>> bits = BitFlags(1, 1, 0, 1, 0, 0, 0, 0)
        >>> bytes(bits)
        b'\xd0'

    Create a BitFlags object from bytes::

        >>> BitFlags.from_bytes(b'\xd0')
        BitFlags(1, 1, 0, 1, 0, 0, 0, 0)

    When comparing BitFlags against other containers of ones and
    zeros, trailing zeros are ignored::

        >>> BitFlags(1, 1, 0, 1, 0, 0, 0, 0) == (1, 1, 0, 1, 0)
        True

    Register the BitFlags type with SQLite::

        >>> import sqlite3
        >>> sqlite3.register_adapter(BitFlags, bytes)
        >>> sqlite3.register_converter('BLOB_BITFLAGS', BitFlags.from_bytes)
    """
    def __init__(self, *bits: Any) -> None:
        """Initialize a new BitFlags instance."""
        data: List[Literal[0, 1]] = [(1 if x else 0) for x in bits]
        data = self._normalize_length(data)

        self._data: Tuple[Literal[0, 1], ...]
        super().__setattr__('_data', tuple(data))  # Assign to "immutable".

    @classmethod
    def from_bytes(cls, bytes_: bytes) -> 'BitFlags':
        """Take a bytes object and return a new BitFlags."""
        # Convert bytes to strings of 1s and 0s and slice-off '0b' prefix.
        binary_strings = (bin(x)[2:] for x in bytes_)

        # Format strings as big-endian, 8-bit words.
        eight_bit_words = (x.rjust(8, '0') for x in binary_strings)

        # Convert to iterable of integers (1s and 0s only).
        ones_and_zeros = (int(x) for x in ''.join(eight_bit_words))

        # Initialize and return a new BitFlags instance.
        new_inst = cls.__new__(cls)
        cls.__init__(new_inst, *ones_and_zeros)
        return new_inst

    def __bytes__(self) -> bytes:
        """Return a bytes object representing the sequence of bits."""
        bitstr = ''.join(str(x) for x in self._data)

        # Make sure length of bits is a multiple of 8.
        bitstr = bitstr.rstrip('0')
        remainder = len(bitstr) % 8
        if remainder:
            bitstr = bitstr + '0' * (8 - remainder)

        # If no data, set to zeros.
        if not bitstr:
            bitstr = '0' * 8

        # Group into 8-bit chunks and convert to bytes.
        eight_bit_chunks = (bitstr[i:i + 8] for i in range(0, len(bitstr), 8))
        return b''.join(int(x, 2).to_bytes(1, 'big') for x in eight_bit_chunks)

    @staticmethod
    def _normalize_length(values: Iterable) -> List:
        data = list(values)

        # Remove excess trailing 0 bits.
        while data:
            if data[-1] == 0:
                data.pop()
            else:
                break

        # Pad bits to a multiple of eight.
        remainder = len(data) % 8
        if remainder:
            data = data + [0] * (8 - remainder)

        # If no data, set to zeros.
        if not data:
            data = [0] * 8

        return data

    @property
    def data(self) -> Tuple[Literal[0, 1], ...]:
        """A tuple containing the contents of the BitFlags object."""
        return self._data

    @overload
    def __getitem__(self, key: int) -> Literal[0, 1]:
        ...
    @overload
    def __getitem__(self, key: slice) -> 'BitFlags':
        ...
    def __getitem__(self, key):
        """Return value at index position or slice."""
        return self._data[key]

    def __len__(self):
        """Return len() of bit flags data."""
        return len(self._data)

    def __repr__(self) -> str:
        """Return string representation of BitFlags object."""
        bits = ', '.join(str(x) for x in self._data)
        return f'{self.__class__.__name__}({bits})'

    def __eq__(self, other: Any) -> bool:
        """Return True if BitFlags == other."""
        if isinstance(other, self.__class__):
            return self._data == other._data

        if isinstance(other, Iterable):
            other = self._normalize_length(other)
            return self._data == tuple(other)

        return NotImplemented

    def __setattr__(self, *args):
        msg = f'{self.__class__.__name__!r} object does not support assignment'
        raise TypeError(msg)

    def __delattr__(self, *args):
        msg = f'{self.__class__.__name__!r} object does not support deletion'
        raise TypeError(msg)

    def __hash__(self):
        return hash((self.__class__, self._data))


sqlite3.register_adapter(BitFlags, bytes)
sqlite3.register_converter('BLOB_BITFLAGS', BitFlags.from_bytes)


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


def sql_string_literal(value: str) -> str:
    """Return *value* as a single-quoted SQL string literal.

    .. code-block::

        >>> sql_string_literal("A")
        "'A'"
        >>> sql_string_literal("   A   B")
        "'   A   B'"
        >>> sql_string_literal("O'Connell")
        "'O''Connell'"

    NOTE: IN MOST CASES, THIS FUNCTION SHOULD NOT BE USED.
    Instead, follow best practice and make a parameterized
    SQL statement using the built-in placeholder syntax and
    the execute() method's *parameters* argument. For more
    details, see "How to use placeholders to bind values
    in SQL queries" in the sqlite3 documentation:

        https://docs.python.org/3/library/sqlite3.html

    This function should only by used when the management
    of placeholders and parameters will lead to functions
    that are too-strongly coupled to be easily reasoned
    about, tested, and maintained.

    Currently, there are a few functions in Toron's data
    access layer that would suffer from added complexity if
    they did not use this function. Perhaps this function
    can be removed with future refactoring.
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

    value = value.replace("'", "''")
    return f"'{value}'"


def sql_drop_label_column_indexes() -> List[str]:
    """Return list of SQL statements to drop unique label column indexes."""
    return [
        'DROP INDEX IF EXISTS main.unique_nodeindex_index',
        'DROP INDEX IF EXISTS main.unique_location_index',
        'DROP INDEX IF EXISTS main.unique_structure_index',
    ]


def sql_create_node_indexes(columns: List[str]) -> List[str]:
    """Return list of SQL statements to create unique label indexes."""
    formatted = ', '.join(normalize_identifier(x) for x in columns)
    return [
        f'CREATE UNIQUE INDEX main.unique_nodeindex_index ON node_index({formatted})',
        f'CREATE UNIQUE INDEX main.unique_location_index ON location({formatted})',
        f'CREATE UNIQUE INDEX main.unique_structure_index ON structure({formatted})',
    ]


# The following `sql_column_def_XYZ_label()` functions should follow
# the SQLite syntax described at:
#
#     https://www.sqlite.org/syntax/column-def.html
#
# The returned definitions are suitable for use in ALTER TABLE or
# CREATE TABLE statements.


def sql_column_def_nodeindex_label(name: str) -> str:
    """Return a `node_index` column-def for a label column."""
    return f"{name} TEXT NOT NULL CHECK ({name} != '') DEFAULT '-'"


def sql_column_def_location_label(name: str) -> str:
    """Return a `location` table column-def for a label column."""
    return f"{name} TEXT NOT NULL DEFAULT ''"


def sql_column_def_structure_label(name: str) -> str:
    """Return a `structure` table column-def for a label column."""
    return f"{name} INTEGER CHECK ({name} IN (0, 1)) DEFAULT 0"


def _user_json_object_keep(
    json_obj: str, *keys: str
) -> Optional[str]:
    """Return a JSON object keeping only the given *keys*.

    .. code-block::

        >>> json_obj = '{"a": "one", "b": "two", "c": "three"}'
        >>> _user_json_object_keep(json_obj, 'a', 'b')
        '{"a": "one", "b": "two"}'

    If no *keys* are given, returns a complete and normalized JSON
    object::

        >>> _user_json_object_keep(json_obj)
        '{"a": "one", "b": "two", "c": "three"}'

    If *keys* are given but none of them match the keys in the JSON
    object, then None is returned::

        >>> print(_user_json_object_keep(json_obj, 'x', 'y', 'z'))
        None

    Register with SQLite using::

        >>> con = sqlite3.connect(...)
        >>> con.create_function(
        ...     'user_json_object_keep',
        ...     -1,
        ...     _user_json_object_keep,
        ...     deterministic=True,
        ... )
    """
    obj = _loads(json_obj)

    if not isinstance(obj, dict):
        class_name = obj.__class__.__name__
        msg = f'expected JSON object/dict type, got {class_name}: {json_obj}'
        raise ValueError(msg)

    if not keys:
        return _dumps(obj, sort_keys=True)

    try:
        obj_subset = {k: obj[k] for k in keys if k in obj}
    except TypeError:
        for key in keys:
            if not isinstance(key, str):
                class_name = key.__class__.__name__
                msg = f'given keys should be str objects, got {class_name}: {key!r}'
                raise TypeError(msg)
        raise  # If no error in *keys*, reraise original TypeError.

    if obj_subset:
        return _dumps(obj_subset, sort_keys=True)
    return None


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


def _sql_trigger_validate_json(
    insert_or_update: str, table: str, column: str
) -> str:
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


def _sql_trigger_validate_userproperties(
    insert_or_update: str, table: str, column: str
) -> str:
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


def _sql_trigger_validate_attributes(
    insert_or_update: str, table: str, column: str
) -> str:
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


def _user_selectors_valid(x: str) -> bool:
    """A user-defined function to use when the SQLite JSON1 extension
    is not available (register as 'user_attributes_valid').

    Returns True if *x* is a wellformed TEXT_SELECTORS value or return
    False if it is not wellformed. A wellformed TEXT_SELECTORS value is
    a string containing a JSON formatted "array" type (returned as a
    list by the loads() function) that contains "string" values.
    """
    try:
        obj = _loads(x)
    except (ValueError, TypeError):
        return False

    if not isinstance(obj, list):
        return False

    for value in obj:
        if not isinstance(value, str):
            return False

    return True


def _sql_trigger_validate_selectors(
    insert_or_update: str, table: str, column: str
) -> str:
    """Return a SQL statement for creating a temporary trigger. The
    trigger is used to validate the contents of TEXT_SELECTORS
    type columns.

    The trigger will pass without error when the value is a wellformed
    JSON "array" containing "text" elements.

    The trigger will raise an error when the value is:
      * not wellformed JSON
      * not an "array" type
      * an "array" type that contains one or more "integer", "real",
        "true", "false", "null", "object" or "array" elements
    """
    if insert_or_update.upper() not in {'INSERT', 'UPDATE'}:
        msg = f"expected 'INSERT' or 'UPDATE', got {insert_or_update!r}"
        raise ValueError(msg)

    if SQLITE_JSON1_ENABLED:
        selectors_are_invalid = f"""
            (json_valid(NEW.{column}) = 0
                 OR json_type(NEW.{column}) != 'array'
                 OR (SELECT COUNT(*)
                     FROM json_each(NEW.{column})
                     WHERE json_each.type != 'text') != 0)
        """.strip()
    else:
        selectors_are_invalid = f'user_selectors_valid(NEW.{column}) = 0'

    return f'''
        CREATE TEMPORARY TRIGGER IF NOT EXISTS trigger_check_{insert_or_update.lower()}_{table}_{column}
        BEFORE {insert_or_update.upper()} ON main.{table} FOR EACH ROW
        WHEN
            NEW.{column} IS NOT NULL
            AND {selectors_are_invalid}
        BEGIN
            SELECT RAISE(ABORT, '{table}.{column} must be a JSON array with text values');
        END;
    '''


def _add_functions_and_triggers(connection):
    """Create triggers and application-defined functions *connection*.

    Note: This function must not be executed on an empty connection.
    The table schema must exist before triggers can be created.
    """
    try:
        connection.create_function(
            'user_json_object_keep', -1, _user_json_object_keep, deterministic=True)
    except TypeError:
        connection.create_function('user_json_object_keep', -1, _user_json_object_keep)

    if not SQLITE_JSON1_ENABLED:
        try:
            connection.create_function(
                'user_json_valid', 1, _user_json_valid, deterministic=True)
            connection.create_function(
                'user_userproperties_valid', 1, _user_userproperties_valid, deterministic=True)
            connection.create_function(
                'user_attributes_valid', 1, _user_attributes_valid, deterministic=True)
            connection.create_function(
                'user_selectors_valid', 1, _user_selectors_valid, deterministic=True)
        except TypeError:
            connection.create_function('user_json_valid', 1, _user_json_valid)
            connection.create_function('user_userproperties_valid', 1, _user_userproperties_valid)
            connection.create_function('user_attributes_valid', 1, _user_attributes_valid)
            connection.create_function('user_selectors_valid', 1, _user_selectors_valid)

    if not SQLITE_ENABLE_MATH_FUNCTIONS:
        from math import log2 as _log2
        def log2(x):
            try:
                return _log2(x)
            except ValueError:  # Returns None on error to mimic SQLite's log
                return None     # function behavior (returns NULL for errors).

        try:
            connection.create_function('log2', 1, log2, deterministic=True)
        except TypeError:
            connection.create_function('log2', 1, log2)

    connection.execute(_sql_trigger_validate_json('INSERT', 'property', 'value'))
    connection.execute(_sql_trigger_validate_json('UPDATE', 'property', 'value'))

    connection.execute(_sql_trigger_validate_userproperties('INSERT', 'edge', 'user_properties'))
    connection.execute(_sql_trigger_validate_userproperties('UPDATE', 'edge', 'user_properties'))

    connection.execute(_sql_trigger_validate_attributes('INSERT', 'quantity', 'attributes'))
    connection.execute(_sql_trigger_validate_attributes('UPDATE', 'quantity', 'attributes'))

    connection.execute(_sql_trigger_validate_selectors('INSERT', 'edge', 'selectors'))
    connection.execute(_sql_trigger_validate_selectors('UPDATE', 'edge', 'selectors'))

    connection.execute(_sql_trigger_validate_selectors('INSERT', 'weighting', 'selectors'))
    connection.execute(_sql_trigger_validate_selectors('UPDATE', 'weighting', 'selectors'))


RequiredPermissions: TypeAlias = Literal['readonly', 'readwrite', None]


def _validate_permissions(
    path: str,
    required_permissions: RequiredPermissions,
) -> None:
    """Raise error if file does not have required permissions.

    IMPORTANT: The reason for enforcing filesystem permissions,
    rather than relying on SQLite URI access modes, is to mitigate
    the chance of data corruption. While SQLite can open files in
    read-only mode, doing so does not ensure that the database
    file on the drive will always remain safe to copy. At this
    time, SQLite makes no guarantees that use of the "ro" URI
    access mode is equivalent to using a database with read-only
    permissions enforced by the filesystem.

    In a high availability computing environment, it's possible
    that an automated backup system could copy a database file
    while a transaction is in progress. For this reason, when
    opening an existing database directly on drive, Toron requires
    the use read-only file permissions (unless the user specifies
    otherwise) to mitigate the chance that a backup process makes
    a corrupted copy.

    For related information, see section 1.2 of "How To Corrupt
    An SQLite Database File":

        https://www.sqlite.org/howtocorrupt.html
    """
    if required_permissions is None:
        return  # <- EXIT!

    if not os.path.exists(path):
        if required_permissions != 'readwrite' and required_permissions is not None:
            msg = f"file {path!r} does not exist, must require 'readwrite' " \
                  f"or None permissions, got {required_permissions!r}"
            raise ToronError(msg)
        return  # <- EXIT!

    if required_permissions == 'readonly':
        # Raise error if file has write permissions.
        if os.access(path, os.W_OK):
            msg = f"required 'readonly' permissions but {path!r} is not read-only"
            raise PermissionError(msg)
        return  # <- EXIT!

    if required_permissions == 'readwrite':
        # Raise error if file does not have write permissions.
        if not os.access(path, os.W_OK):
            msg = f"required 'readwrite' permissions but {path!r} does not " \
                  f"have write access"
            raise PermissionError(msg)
        return  # <- EXIT!

    msg = f"`required_permissions` must be 'readonly', 'readwrite', " \
          f"or None; got {required_permissions!r}"
    raise ToronError(msg)


def _make_sqlite_uri_filepath(path: str, mode: Literal['ro', 'rw', 'rwc', None]) -> str:
    """Return a SQLite compatible URI file path.

    Unlike pathlib's URI handling, SQLite accepts relative URI paths.
    For details, see:

        https://www.sqlite.org/uri.html#the_uri_path
    """
    if os.name == 'nt':  # Windows
        if re.match(r'^[a-zA-Z]:', path):
            path = os.path.abspath(path)  # Paths with drive-letter must be absolute.
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
    if mode:
        return f'file:{path}?mode={mode}'
    return f'file:{path}'


def get_raw_connection(
    path: str,
    access_mode: Literal['ro', 'rw', 'rwc', None] = None,
) -> sqlite3.Connection:
    """Open and return an SQLite 3 connection to the given *path* using
    the specified URI access mode if provided. If path is ':memory:',
    then *access_mode* is ignored.

    NOTE: This method should only establish a connection, it should
    not execute queries of any kind.
    """
    if path == ':memory:':
        normalized_path = path
        is_uri_path = False
    else:
        normalized_path = _make_sqlite_uri_filepath(path, access_mode)
        is_uri_path = True

    try:
        con = sqlite3.connect(
            database=normalized_path,
            detect_types=sqlite3.PARSE_DECLTYPES,
            isolation_level=None,
            uri=is_uri_path,
        )
    except sqlite3.OperationalError as err:
        msg = str(err).replace('database file', f'node file {path!r}')
        raise ToronError(msg)

    return con


def get_connection(
    path: str,
    required_permissions: RequiredPermissions,
    access_mode: Literal['ro', 'rw', 'rwc', None] = None,
) -> sqlite3.Connection:
    """Return an SQLite 3 connection to a Toron database containing a
    supported node schema with required file permissions, triggers, and
    functions.
    """
    if path == ':memory:':
        con = get_raw_connection(path)
        con.executescript(_schema_script)  # Create database schema.
    else:
        _validate_permissions(path, required_permissions)
        if not access_mode and required_permissions == 'readonly':
            access_mode = 'ro'

        if os.path.exists(path):
            con = get_raw_connection(path, access_mode)
        else:
            con = get_raw_connection(path, access_mode)
            con.executescript(_schema_script)  # Create database schema.

    try:
        _add_functions_and_triggers(con)
    except (sqlite3.OperationalError, sqlite3.DatabaseError):
        # SQLite raises an OperationalError when *path* is a database with
        # an unknown schema and DatabaseError when *path* is a file but not
        # a database.
        con.close()
        raise ToronError(f'Path is not a Toron node: {path!r}')

    cur = con.execute("SELECT value FROM main.property WHERE key='toron_schema_version'")
    schema_version, *_ = cur.fetchone() or (None,)
    cur.close()

    if schema_version != '0.1.0':  # When schema version is unsupported.
        msg = f'Unsupported Toron node format: schema version {schema_version}'
        raise ToronError(msg)

    return con


def _validate_isolation_level(connection):
    """Raise error if connection uses improper isolation level."""
    if connection.isolation_level is not None:
        isolation_level = connection.isolation_level
        msg = (
            f'isolation_level must be None, got: {isolation_level!r}\n'
            '\n'
            'For explicit transaction handling, the connection must '
            'be operating in "autocommit" mode. Turn on autocommit '
            'mode by setting "con.isolation_level = None".'
        )
        raise sqlite3.OperationalError(msg)


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
        _validate_isolation_level(cursor.connection)
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
def begin(cursor):
    """Context manager to handle transaction using BEGIN and COMMIT
    (or ROLLBACK if an error occurs).

    .. code-block::

        >>> cur = con.cursor()
        >>> with begin(cur):
        ...     cur.execute(...)
    """
    _validate_isolation_level(cursor.connection)
    cursor.execute(f'BEGIN TRANSACTION')
    try:
        yield None
        finalize = 'COMMIT TRANSACTION'
    except Exception:
        finalize = 'ROLLBACK TRANSACTION'
        raise
    finally:
        cursor.execute(finalize)


# A generator of names for user-defined SQL functions.
_USERFUNC_NAME_GENERATOR = (f'userfunc_{n}' for n in itertools.count())

# A registry of callable objects and their associated SQL function names.
_USERFUNC_NAME_REGISTRY: Dict[Callable, str] = {}


def _sql_function_exists(
    cursor: sqlite3.Cursor,
    name: str,
) -> bool:
    """Return True if the named SQL function is known to exist.

    .. code-block::

        >>> _sql_function_exists(cursor, 'length')
        True

    The "function_list" PRAGMA (used by this implementation) is an
    optional part of SQLite but was included by default starting in
    SQLite 3.30.0. If this PRAGMA is not available, this function
    will always return False even if the SQL function exists.
    """
    try:
        sql = 'SELECT EXISTS(SELECT 1 FROM pragma_function_list WHERE name=?)'
        cursor.execute(sql, (name,))
        return cursor.fetchone() == (1,)
    except sqlite3.OperationalError:
        return False


def _sql_create_function(
    cursor_or_connection: Union[sqlite3.Cursor, sqlite3.Connection],
    name: str,
    func: Callable,
) -> None:
    """Create a deterministic, user-defined SQL function of 1 argument.

    .. code-block::

        >>> cursor = ...
        >>> myfunc = lambda x: ...
        >>> _sql_create_function(cursor, 'myfunc', myfunc)
    """
    if isinstance(cursor_or_connection, sqlite3.Cursor):
        con = cursor_or_connection.connection
    elif isinstance(cursor_or_connection, sqlite3.Connection):
        con = cursor_or_connection
    else:
        raise TypeError

    # Call with `deterministic` arg (new in Python 3.8) or fallback.
    try:
        con.create_function(name, narg=1, func=func, deterministic=True)
    except TypeError:
        con.create_function(name, narg=1, func=func)


def get_userfunc(cursor: sqlite3.Cursor, func: Callable) -> str:
    """Get user-defined SQL function name (registers SQL function
    if needed).

    .. code-block::

        >>> cursor = ...
        >>> myfunc = lambda x: ...
        >>> func_name = get_userfunc(cursor, myfunc)

    Once created, the new function can be used in SQL statements::

        >>> cursor.execute(f'SELECT {func_name}(a) FROM mytable')
    """
    # Get function name if it's in the registry.
    name = _USERFUNC_NAME_REGISTRY.get(func)

    # If no name, register new name, create SQL func, and return name.
    if name is None:
        name = next(_USERFUNC_NAME_GENERATOR)
        _sql_create_function(cursor, name, func)
        _USERFUNC_NAME_REGISTRY[func] = name
        return name  # <- EXIT!

    # If SQL function is known to exist, return name.
    if _sql_function_exists(cursor, name):
        return name  # <- EXIT!

    # Create SQL function and return name.
    _sql_create_function(cursor, name, func)
    return name

