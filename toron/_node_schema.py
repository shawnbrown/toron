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


_schema_script = """
    PRAGMA foreign_keys = ON;

    CREATE TABLE edge(
        edge_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        type_info PYTEXT_DICT NOT NULL,
        optional_attributes PYTEXT_DICT,
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
        attributes PYTEXT_DICT NOT NULL,
        value NUMERIC NOT NULL,
        FOREIGN KEY(location_id) REFERENCES location(location_id)
    );

    CREATE TABLE weight_info(
        weight_info_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        type_info PYTEXT_DICT NOT NULL,
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
        value PYTEXT_LITERAL
    );
"""


def connect(path):
    """Returns a sqlite3 connection to a Toron node file. If *path*
    doesn't exist, a new node is created at this location.
    """
    if os.path.exists(path):
        try:
            con = sqlite3.connect(path)
        except sqlite3.OperationalError:
            # If *path* is a directory or non-file resource, then
            # calling `connect()` will raise an OperationalError.
            raise Exception(f'path {path!r} is not a Toron Node')
    else:
        con = sqlite3.connect(path)
        con.executescript(_schema_script)

    return con

