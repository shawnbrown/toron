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
              |  +-------------------+                         +----------+
              |  | element_weight    |     +-------------+     | property |
              |  +-------------------+     | weight      |     +----------+
              |  | element_weight_id |     +-------------+     | key      |
              |  | weight_id         |<----| weight_id   |     | value    |
              +->| element_id        |•••  | name        |     +----------+
                 | value             |  •  | type_info   |
                 +-------------------+  •  | description |
                                        ••>| is_complete |
                                           +-------------+
"""

import itertools
import os
import re
import sqlite3
from ast import literal_eval
from collections import Counter
from collections.abc import Mapping
from contextlib import contextmanager
from itertools import compress
from json import loads as _loads
from json import dumps as _dumps
from urllib.parse import quote as urllib_parse_quote

from ._exceptions import ToronError


sqlite3.register_converter('TEXT_JSON', _loads)
sqlite3.register_converter('TEXT_ATTRIBUTES', _loads)


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


_schema_script = """
    PRAGMA foreign_keys = ON;

    CREATE TABLE edge(
        edge_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        type_info TEXT_ATTRIBUTES NOT NULL,
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

    CREATE TABLE weight(
        weight_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        type_info TEXT_ATTRIBUTES NOT NULL,
        description TEXT,
        is_complete INTEGER CHECK (is_complete IN (0, 1)),
        UNIQUE (name)
    );

    CREATE TABLE element_weight(
        element_weight_id INTEGER PRIMARY KEY,
        weight_id INTEGER,
        element_id INTEGER,
        value REAL NOT NULL,
        FOREIGN KEY(element_id) REFERENCES element(element_id),
        FOREIGN KEY(weight_id) REFERENCES weight(weight_id),
        UNIQUE (element_id, weight_id)
    );

    CREATE TABLE property(
        key TEXT PRIMARY KEY NOT NULL,
        value TEXT_JSON
    );

    INSERT INTO property VALUES ('schema_version', '1');
"""


def _is_wellformed_json(x):
    """Return 1 if *x* is well-formed JSON or return 0 if *x* is not
    well-formed. This function should be registered with SQLite (via
    the create_function() method) when the JSON1 extension is not
    available.

    This function mimics the JSON1 json_valid() function, see:
        https://www.sqlite.org/json1.html#jvalid
    """
    try:
        _loads(x)
    except (ValueError, TypeError):
        return 0
    return 1


def _make_trigger_for_json(insert_or_update, table, column):
    """Return a SQL statement for creating a temporary trigger. The
    trigger is used to validate the contents of TEXT_JSON type columns.
    The trigger will pass without error if the JSON is wellformed.
    """
    if insert_or_update.upper() not in {'INSERT', 'UPDATE'}:
        msg = f"expected 'INSERT' or 'UPDATE', got {insert_or_update!r}"
        raise ValueError(msg)

    if SQLITE_JSON1_ENABLED:
        when_clause = f"""
            NEW.{column} IS NOT NULL
            AND json_valid(NEW.{column}) = 0
        """.rstrip()
    else:
        when_clause = f"""
            NEW.{column} IS NOT NULL
            AND is_wellformed_json(NEW.{column}) = 0
        """.rstrip()

    return f'''
        CREATE TEMPORARY TRIGGER IF NOT EXISTS trigger_check_{insert_or_update.lower()}_{table}_{column}
        BEFORE {insert_or_update.upper()} ON main.{table} FOR EACH ROW
        WHEN{when_clause}
        BEGIN
            SELECT RAISE(ABORT, '{table}.{column} must be wellformed JSON');
        END;
    '''


def _is_wellformed_user_properties(x):
    """Check if *x* is a wellformed TEXT_USERPROPERTIES value.
    A wellformed TEXT_USERPROPERTIES value is a string containing
    a JSON formatted object. Returns 1 if *x* is valid or 0 if
    it's not.

    This function should be registered as an application-defined
    SQL function and used in queries when SQLite's JSON1 extension
    is not enabled.
    """
    try:
        obj = _loads(x)
    except (ValueError, TypeError):
        return 0

    if isinstance(obj, dict):
        return 1
    return 0


def _make_trigger_for_user_properties(insert_or_update, table, column):
    """Return a CREATE TRIGGER statement to check TEXT_USERPROPERTIES
    values. This trigger is used to check values before they are saved
    in the database.

    A wellformed TEXT_USERPROPERTIES value is a string containing
    a JSON formatted object.

    The trigger will pass without error if the value is wellformed.
    """
    if SQLITE_JSON1_ENABLED:
        user_properties_check = f"(json_valid(NEW.{column}) = 0 OR json_type(NEW.{column}) != 'object')"
    else:
        user_properties_check = f'is_wellformed_user_properties(NEW.{column}) = 0'

    return f'''
        CREATE TEMPORARY TRIGGER IF NOT EXISTS trigger_check_{insert_or_update.lower()}_{table}_{column}
        BEFORE {insert_or_update.upper()} ON main.{table} FOR EACH ROW
        WHEN
            NEW.{column} IS NOT NULL
            AND {user_properties_check}
        BEGIN
            SELECT RAISE(ABORT, '{table}.{column} must be wellformed JSON object');
        END;
    '''


def _is_wellformed_attributes(x):
    """Returns 1 if *x* is a wellformed TEXT_ATTRIBUTES column
    value else returns 0. TEXT_ATTRIBUTES should be flat, JSON
    object strings. This function should be registered with SQLite
    (via the create_function() method) when the JSON1 extension
    is not available.
    """
    try:
        obj = _loads(x)
    except (ValueError, TypeError):
        return 0

    if not isinstance(obj, dict):
        return 0

    for value in obj.values():
        if not isinstance(value, str):
            return 0

    return 1


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
        when_clause = f"""
            NEW.{column} IS NOT NULL
            AND (json_valid(NEW.{column}) = 0
                 OR json_type(NEW.{column}) != 'object'
                 OR (SELECT COUNT(*)
                     FROM json_each(NEW.{column})
                     WHERE json_each.type != 'text') != 0)
        """.rstrip()
    else:
        when_clause = f"""
            NEW.{column} IS NOT NULL
            AND is_wellformed_attributes(NEW.{column}) = 0
        """.rstrip()

    return f'''
        CREATE TEMPORARY TRIGGER IF NOT EXISTS trigger_check_{insert_or_update.lower()}_{table}_{column}
        BEFORE {insert_or_update.upper()} ON main.{table} FOR EACH ROW
        WHEN{when_clause}
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
                'is_wellformed_json', 1, _is_wellformed_json, deterministic=True)
            connection.create_function(
                'is_wellformed_user_properties', 1, _is_wellformed_user_properties, deterministic=True)
            connection.create_function(
                'is_wellformed_attributes', 1, _is_wellformed_attributes, deterministic=True)
        except TypeError:
            connection.create_function('is_wellformed_json', 1, _is_wellformed_json)
            connection.create_function('is_wellformed_user_properties', 1, _is_wellformed_user_properties)
            connection.create_function('is_wellformed_attributes', 1, _is_wellformed_attributes)

    connection.execute(_make_trigger_for_json('INSERT', 'property', 'value'))
    connection.execute(_make_trigger_for_json('UPDATE', 'property', 'value'))

    connection.execute(_make_trigger_for_user_properties('INSERT', 'edge', 'user_properties'))
    connection.execute(_make_trigger_for_user_properties('UPDATE', 'edge', 'user_properties'))

    jsonflatobj_columns = [
        ('edge', 'type_info'),
        ('quantity', 'attributes'),
        ('weight', 'type_info'),
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


def _rename_columns_make_sql(column_names, new_column_names):
    """The RENAME COLUMN command was added in SQLite 3.25.0 (2018-09-15)."""
    zipped = zip(column_names, new_column_names)
    rename_pairs = [(a, b) for a, b in zipped if a != b]

    sql_stmnts = []
    for name, new_name in rename_pairs:
        sql_stmnts.extend([
            f'ALTER TABLE element RENAME COLUMN {name} TO {new_name}',
            f'ALTER TABLE location RENAME COLUMN {name} TO {new_name}',
            f'ALTER TABLE structure RENAME COLUMN {name} TO {new_name}',
        ])
    return sql_stmnts


def _rename_columns(dal, mapper):
    """Rename columns using native RENAME COLUMN command (only for
    SQLite 3.25.0 or newer).
    """
    with dal._transaction() as cur:
        names, new_names = dal._rename_columns_apply_mapper(cur, mapper)
        for stmnt in _rename_columns_make_sql(names, new_names):
            cur.execute(stmnt)


def _legacy_rename_columns_make_sql(column_names, new_column_names):
    """In SQLite versions older than 3.25.0, there was no native
    support for the RENAME COLUMN command. In these older versions
    of SQLite the tables must be rebuilt.
    """
    # To support versions of SQLite before 3.25.0, this method
    # prepares a sequence of operations to rebuild the table
    # structures.
    new_element_cols = [f"{col} TEXT DEFAULT '-' NOT NULL" for col in new_column_names]
    new_location_cols = [f"{col} TEXT" for col in new_column_names]
    new_structure_cols = [f"{col} INTEGER CHECK ({col} IN (0, 1)) DEFAULT 0" for col in new_column_names]
    statements = [
        # Rebuild 'element' table.
        f'CREATE TABLE new_element(element_id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
            f'{", ".join(new_element_cols)})',
        f'INSERT INTO new_element SELECT element_id, {", ".join(column_names)} FROM element',
        'DROP TABLE element',
        'ALTER TABLE new_element RENAME TO element',

        # Rebuild 'location' table.
        f'CREATE TABLE new_location(_location_id INTEGER PRIMARY KEY, ' \
            f'{", ".join(new_location_cols)})',
        f'INSERT INTO new_location '
            f'SELECT _location_id, {", ".join(column_names)} FROM location',
        'DROP TABLE location',
        'ALTER TABLE new_location RENAME TO location',

        # Rebuild 'structure' table.
        f'CREATE TABLE new_structure(_structure_id INTEGER PRIMARY KEY, ' \
            f'{", ".join(new_structure_cols)})',
        f'INSERT INTO new_structure ' \
            f'SELECT _structure_id, {", ".join(column_names)} FROM structure',
        'DROP TABLE structure',
        'ALTER TABLE new_structure RENAME TO structure',

        # Reconstruct associated indexes.
        f'CREATE UNIQUE INDEX unique_element_index ON element({", ".join(new_column_names)})',
        f'CREATE UNIQUE INDEX unique_structure_index ON structure({", ".join(new_column_names)})',
    ]
    return statements


def _legacy_rename_columns(dal, mapper):
    # This code should follow the recommended, 12-step ALTER TABLE
    # procedure detailed in the SQLite documentation:
    #     https://www.sqlite.org/lang_altertable.html#otheralter
    con = dal._get_connection()
    try:
        con.execute('PRAGMA foreign_keys=OFF')
        cur = con.cursor()
        with savepoint(cur):
            names, new_names = dal._rename_columns_apply_mapper(cur, mapper)
            for stmnt in _legacy_rename_columns_make_sql(names, new_names):
                cur.execute(stmnt)

            cur.execute('PRAGMA main.foreign_key_check')
            one_result = cur.fetchone()
            if one_result:
                msg = 'foreign key violations'
                raise Exception(msg)
    finally:
        cur.close()
        con.execute('PRAGMA foreign_keys=ON')
        if con is not getattr(dal, '_connection', None):
            con.close()


class DataAccessLayer(object):
    def __init__(self, path, mode='rwc'):
        if mode == 'memory':
            self._connection = connect(path, mode=mode)  # In-memory connection.
            self._transaction = lambda: transaction(self._connection)
        else:
            path = os.fspath(path)
            connect(path, mode=mode).close()  # Verify path to Toron node file.
            self._transaction = lambda: transaction(self.path, mode=mode)
        self.path = path
        self.mode = mode

    def _get_connection(self):
        if self.mode == 'memory':
            return self._connection
        return connect(self.path, mode=self.mode)

    def __del__(self):
        if hasattr(self, '_connection'):
            self._connection.close()

    @staticmethod
    def _get_column_names(cursor, table):
        """Return a list of column names from the given table."""
        cursor.execute(f"PRAGMA table_info('{table}')")
        return [row[1] for row in cursor.fetchall()]

    @staticmethod
    def _quote_identifier(value):
        """Return a quoted SQLite identifier suitable as a column name."""
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

        value = ' '.join(value.split()).replace('"', '""')
        return f'"{value}"'

    @classmethod
    def _add_columns_make_sql(cls, cursor, columns):
        """Return a list of SQL statements for adding new label columns."""
        if isinstance(columns, str):
            columns = [columns]
        columns = [cls._quote_identifier(col) for col in columns]

        not_allowed = {'"element_id"', '"_location_id"', '"_structure_id"'}.intersection(columns)
        if not_allowed:
            msg = f"label name not allowed: {', '.join(not_allowed)}"
            raise ValueError(msg)

        current_cols = cls._get_column_names(cursor, 'element')
        current_cols = [cls._quote_identifier(col) for col in current_cols]
        new_cols = [col for col in columns if col not in current_cols]

        if not new_cols:
            return []  # <- EXIT!

        dupes = [obj for obj, count in Counter(new_cols).items() if count > 1]
        if dupes:
            msg = f"duplicate column name: {', '.join(dupes)}"
            raise ValueError(msg)

        sql_stmnts = []

        sql_stmnts.extend([
            'DROP INDEX IF EXISTS unique_element_index',
            'DROP INDEX IF EXISTS unique_structure_index',
        ])

        for col in new_cols:
            sql_stmnts.extend([
                f"ALTER TABLE element ADD COLUMN {col} TEXT DEFAULT '-' NOT NULL",
                f'ALTER TABLE location ADD COLUMN {col} TEXT',
                f'ALTER TABLE structure ADD COLUMN {col} INTEGER CHECK ({col} IN (0, 1)) DEFAULT 0',
            ])

        label_cols = current_cols[1:] + new_cols  # All columns except the id column.
        label_cols = ', '.join(label_cols)
        sql_stmnts.extend([
            f'CREATE UNIQUE INDEX unique_element_index ON element({label_cols})',
            f'CREATE UNIQUE INDEX unique_structure_index ON structure({label_cols})',
        ])

        return sql_stmnts

    def add_columns(self, columns):
        with self._transaction() as cur:
            for stmnt in self._add_columns_make_sql(cur, columns):
                cur.execute(stmnt)

    @classmethod
    def _rename_columns_apply_mapper(cls, cursor, mapper):
        column_names = cls._get_column_names(cursor, 'element')
        column_names = column_names[1:]  # Slice-off 'element_id'.

        if callable(mapper):
            new_column_names = [mapper(col) for col in column_names]
        elif isinstance(mapper, Mapping):
            new_column_names = [mapper.get(col, col) for col in column_names]
        else:
            msg = 'mapper must be a callable or dict-like object'
            raise ValueError(msg)

        column_names = [cls._quote_identifier(col) for col in column_names]
        new_column_names = [cls._quote_identifier(col) for col in new_column_names]

        dupes = [col for col, count in Counter(new_column_names).items() if count > 1]
        if dupes:
            zipped = zip(column_names, new_column_names)
            value_pairs = [(col, new) for col, new in zipped if new in dupes]
            value_pairs = [f'{col}->{new}' for col, new in value_pairs]
            msg = f'column name collisions: {", ".join(value_pairs)}'
            raise ValueError(msg)

        return column_names, new_column_names

    # For _rename_columns_make_sql(), see module-level function definitinos.

    if sqlite3.sqlite_version_info >= (3, 25, 0):
        def rename_columns(self, mapper):
            return _rename_columns(self, mapper)
    else:
        def rename_columns(self, mapper):
            return _rename_columns_legacy(self, mapper)

    @classmethod
    def _add_elements_make_sql(cls, cursor, columns):
        """Return a SQL statement adding new element records (for use
        with an executemany() call.

        Example:

            >>> dal = DataAccessLayer(...)
            >>> dal._make_sql_new_elements(cursor, ['state', 'county'])
            'INSERT INTO element ("state", "county") VALUES (?, ?)'
        """
        columns = [cls._quote_identifier(col) for col in columns]

        existing_columns = cls._get_column_names(cursor, 'element')
        existing_columns = existing_columns[1:]  # Slice-off "element_id" column.
        existing_columns = [cls._quote_identifier(col) for col in existing_columns]

        invalid_columns = set(columns).difference(existing_columns)
        if invalid_columns:
            msg = f'invalid column name: {", ".join(invalid_columns)}'
            raise sqlite3.OperationalError(msg)

        columns_clause = ', '.join(columns)
        values_clause = ', '.join('?' * len(columns))
        return f'INSERT INTO element ({columns_clause}) VALUES ({values_clause})'

    def add_elements(self, iterable, columns=None):
        iterator = iter(iterable)
        if not columns:
            columns = next(iterator)

        with self._transaction() as cur:
            # Get allowed columns and build selectors values.
            allowed_columns = self._get_column_names(cur, 'element')
            selectors = tuple((col in allowed_columns) for col in columns)

            # Filter column names and iterator rows to allowed columns.
            columns = compress(columns, selectors)
            iterator = (tuple(compress(row, selectors)) for row in iterator)

            sql = self._add_elements_make_sql(cur, columns)
            cur.executemany(sql, iterator)

    if sqlite3.sqlite_version_info >= (3, 35, 0):
        # The RETURNING clause was added in SQLite 3.35.0 (released 2021-03-12).
        @staticmethod
        def _add_weights_get_new_id(cursor, name, type_info, description=None):
            type_info = _dumps(type_info, sort_keys=True)  # Dump JSON to string.
            sql = """
                INSERT INTO weight(name, type_info, description)
                VALUES(?, ?, ?)
                RETURNING weight_id
            """
            cursor.execute(sql, (name, type_info, description))
            return cursor.fetchone()[0]
    else:
        # Older versions of SQLite must use last_insert_rowid() function.
        @staticmethod
        def _add_weights_get_new_id(cursor, name, type_info, description=None):
            type_info = _dumps(type_info, sort_keys=True)  # Dump JSON to string.
            sql = """
                INSERT INTO weight(name, type_info, description)
                VALUES(?, ?, ?)
            """
            cursor.execute(sql, (name, type_info, description))
            cursor.execute('SELECT last_insert_rowid()')
            return cursor.fetchone()[0]

    @classmethod
    def _add_weights_make_sql(cls, cursor, columns):
        """Return a SQL statement adding new element_weight value (for
        use with an executemany() call.
        """
        columns = [cls._quote_identifier(col) for col in columns]

        existing_columns = cls._get_column_names(cursor, 'element')
        existing_columns = [cls._quote_identifier(col) for col in existing_columns]

        invalid_columns = set(columns).difference(existing_columns)
        if invalid_columns:
            msg = f'invalid column name: {", ".join(invalid_columns)}'
            raise sqlite3.OperationalError(msg)

        where_clause = ' AND '.join(f'{col}=?' for col in columns)
        groupby_clause = ', '.join(columns)

        sql = f"""
            INSERT INTO element_weight (weight_id, element_id, value)
            SELECT ? AS weight_id, element_id, ? AS value
            FROM element
            WHERE {where_clause}
            GROUP BY {groupby_clause}
            HAVING COUNT(*)=1
        """
        return sql

    @staticmethod
    def _add_weights_set_is_complete(cursor, weight_id):
        """Set the 'weight.is_complete' value to 1 or 0 (True/False)."""
        sql = """
            UPDATE weight
            SET is_complete=((SELECT COUNT(*)
                              FROM element_weight
                              WHERE weight_id=?) = (SELECT COUNT(*)
                                                    FROM element))
            WHERE weight_id=?
        """
        cursor.execute(sql, (weight_id, weight_id))

    def add_weights(self, iterable, columns=None, *, name, type_info, description=None):
        iterator = iter(iterable)
        if not columns:
            columns = tuple(next(iterator))

        try:
            weight_pos = columns.index(name)  # Get position of weight column.
        except ValueError:
            columns_string = ', '.join(repr(x) for x in columns)
            msg = f'Name {name!r} does not appear in columns: {columns_string}'
            raise ValueError(msg)

        with self._transaction() as cur:
            weight_id = self._add_weights_get_new_id(cur, name, type_info, description)

            # Get allowed columns and build selectors values.
            allowed_columns = self._get_column_names(cur, 'element')
            selectors = tuple((col in allowed_columns) for col in columns)

            # Filter column names and iterator rows to allowed columns.
            columns = compress(columns, selectors)
            def mkrow(row):
                weightid_and_value = (weight_id, row[weight_pos])
                element_labels = tuple(compress(row, selectors))
                return weightid_and_value + element_labels
            iterator = (mkrow(row) for row in iterator)

            # Insert element_weight records.
            sql = self._add_weights_make_sql(cur, columns)
            cur.executemany(sql, iterator)

            # Update "weight.is_complete" value (set to 1 or 0).
            self._add_weights_set_is_complete(cur, weight_id)

