"""Data access layer to interact with Toron node files."""

import os
import sqlite3
from collections import Counter
from collections.abc import Mapping
from itertools import compress
from json import dumps as _dumps

from toron._node_schema import connect
from toron._node_schema import savepoint
from toron._node_schema import transaction


class DataAccessLayer(object):
    """A data access layer to interface with the underlying SQLite
    database. This class is not part of Toron's public interface--it
    is intended to be wrapped inside a toron.Node instance.

    Open an existing file or create a new one::

        dal = DataAccessLayer('mynode.toron')

    Open an existing file::

        dal = DataAccessLayer('mynode.toron', mode='rw')

    Open a file in read-only mode::

        dal = DataAccessLayer('mynode.toron', mode='ro')

    Open an in-memory node (no file on disk)::

        dal = DataAccessLayer('mynode', mode='memory')
    """
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

    @staticmethod
    def _rename_columns_make_sql(column_names, new_column_names):
        # The RENAME COLUMN command was added in SQLite 3.25.0 (2018-09-15).
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

    def rename_columns(self, mapper):
        # Rename columns using native RENAME COLUMN command (only for
        # SQLite 3.25.0 or newer).
        with self._transaction() as cur:
            names, new_names = self._rename_columns_apply_mapper(cur, mapper)
            for stmnt in self._rename_columns_make_sql(names, new_names):
                cur.execute(stmnt)

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

    @staticmethod
    def _add_weights_get_new_id(cursor, name, type_info, description=None):
        # This method uses the RETURNING clause which was introduced
        # in SQLite 3.35.0 (2021-03-12).
        type_info = _dumps(type_info, sort_keys=True)  # Dump JSON to string.
        sql = """
            INSERT INTO weight(name, type_info, description)
            VALUES(?, ?, ?)
            RETURNING weight_id
        """
        cursor.execute(sql, (name, type_info, description))
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

    @staticmethod
    def _get_properties(cursor, keys):
        sql = f'''
            SELECT key, value
            FROM property
            WHERE key IN ({", ".join("?" * len(keys))})
        '''
        cursor.execute(sql, keys)
        return dict(cursor.fetchall())

    @staticmethod
    def _set_properties(cursor, properties):
        sql = 'DELETE FROM property WHERE key=?'
        parameters = [k for k, v in properties.items() if v is None]
        cursor.executemany(sql, parameters)

        sql = '''
            INSERT INTO property(key, value) VALUES(?, ?)
              ON CONFLICT(key) DO UPDATE SET value=?
        '''
        filtered = ((k, v) for k, v in properties.items() if v is not None)
        formatted = ((k, _dumps(v, sort_keys=True)) for k, v in filtered)
        parameters = ((k, v, v) for k, v in formatted)
        cursor.executemany(sql, parameters)


class DataAccessLayerPre35(DataAccessLayer):
    """This is a subclass of DataAccessLayer that supports SQLite
    versions before 3.35.0 (2021-03-12).

    For full documentation, see DataAccessLayer.
    """
    @staticmethod
    def _add_weights_get_new_id(cursor, name, type_info, description=None):
        # Since the `RETURNING` clause is not available before version
        # 3.35.0, this method executes a second statement using the
        # last_insert_rowid() SQLite function.
        type_info = _dumps(type_info, sort_keys=True)  # Dump JSON to string.
        sql = """
            INSERT INTO weight(name, type_info, description)
            VALUES(?, ?, ?)
        """
        cursor.execute(sql, (name, type_info, description))
        cursor.execute('SELECT last_insert_rowid()')
        return cursor.fetchone()[0]


class DataAccessLayerPre25(DataAccessLayerPre35):
    """This is a subclass of DataAccessLayer that supports SQLite
    versions before 3.25.0 (2018-09-15).

    For full documentation, see DataAccessLayer.
    """
    @staticmethod
    def _rename_columns_make_sql(column_names, new_column_names):
        # In SQLite versions before 3.25.0, there is no native support for the
        # RENAME COLUMN command. In these older versions of SQLite the tables
        # must be rebuilt. This method prepares a sequence of operations to
        # rebuild the table structures.
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

    def rename_columns(self, mapper):
        # These related methods should implement the recommended, 12-step,
        # ALTER TABLE procedure detailed in the SQLite documentation:
        #     https://www.sqlite.org/lang_altertable.html#otheralter
        con = self._get_connection()
        try:
            con.execute('PRAGMA foreign_keys=OFF')
            cur = con.cursor()
            with savepoint(cur):
                names, new_names = self._rename_columns_apply_mapper(cur, mapper)
                for stmnt in self._rename_columns_make_sql(names, new_names):
                    cur.execute(stmnt)

                cur.execute('PRAGMA main.foreign_key_check')
                one_result = cur.fetchone()
                if one_result:
                    msg = 'foreign key violations'
                    raise Exception(msg)
        finally:
            cur.close()
            con.execute('PRAGMA foreign_keys=ON')
            if con is not getattr(self, '_connection', None):
                con.close()


class DataAccessLayerPre24(DataAccessLayerPre25):
    """This is a subclass of DataAccessLayer that supports SQLite
    versions before 3.24.0 (2018-06-04).

    For full documentation, see DataAccessLayer.
    """
    @staticmethod
    def _set_properties(cursor, properties):
        sql = 'DELETE FROM property WHERE key=?'
        parameters = [k for k, v in properties.items() if v is None]
        cursor.executemany(sql, parameters)

        sql = 'INSERT OR REPLACE INTO property(key, value) VALUES (?, ?)'
        filtered = ((k, v) for k, v in properties.items() if v is not None)
        parameters = ((k, _dumps(v, sort_keys=True)) for k, v in filtered)
        cursor.executemany(sql, parameters)


# Set the DataAccessLayer class appropriate for the current version of SQLite.
_sqlite_version_info = sqlite3.sqlite_version_info
if _sqlite_version_info < (3, 24, 0):
    dal_class = DataAccessLayerPre24
elif _sqlite_version_info < (3, 25, 0):
    dal_class = DataAccessLayerPre25
elif _sqlite_version_info < (3, 35, 0):
    dal_class = DataAccessLayerPre35
else:
    dal_class = DataAccessLayer
