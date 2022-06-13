"""Data access layer to interact with Toron node files."""

import os
import sqlite3
from collections import Counter
from collections.abc import Mapping
from itertools import chain
from itertools import compress
from json import dumps as _dumps

from ._categories import make_structure
from ._categories import minimize_discrete_categories
from ._exceptions import ToronError
from ._exceptions import ToronWarning
from ._node_schema import connect
from ._node_schema import savepoint
from ._node_schema import transaction


_SQLITE_VERSION_INFO = sqlite3.sqlite_version_info


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

    @staticmethod
    def _remove_columns_make_sql(column_names, names_to_remove):
        """Return a list of SQL statements for removing label columns."""
        names_to_remove = [col for col in names_to_remove if col in column_names]

        if not names_to_remove:
            return []  # <- EXIT!

        sql_stmnts = []

        sql_stmnts.extend([
            'DROP INDEX IF EXISTS unique_element_index',
            'DROP INDEX IF EXISTS unique_structure_index',
        ])

        for col in names_to_remove:
            sql_stmnts.extend([
                f'ALTER TABLE main.element DROP COLUMN {col}',
                f'ALTER TABLE main.location DROP COLUMN {col}',
                f'ALTER TABLE main.structure DROP COLUMN {col}',
            ])

        remaining_cols = [col for col in column_names if col not in names_to_remove]
        remaining_cols = ', '.join(remaining_cols)
        sql_stmnts.extend([
            f'CREATE UNIQUE INDEX unique_element_index ON element({remaining_cols})',
            f'CREATE UNIQUE INDEX unique_structure_index ON structure({remaining_cols})',
        ])

        return sql_stmnts

    @classmethod
    def _coarsen_records_make_sql(cls, cursor, remaining_columns):
        """Return a list of SQL statements to coarsen the dataset."""
        quoted_names = (cls._quote_identifier(col) for col in remaining_columns)
        formatted_names = ', '.join(quoted_names)

        sql_statements = []

        # Build a temp table with old-to-new element_id mappings.
        sql_statements.append(f'''
            CREATE TEMPORARY TABLE old_to_new_element_id
            AS SELECT element_id, new_element_id
            FROM main.element
            JOIN (SELECT MIN(element_id) AS new_element_id, {formatted_names}
                  FROM main.element
                  GROUP BY {formatted_names}
                  HAVING COUNT(*) > 1)
            USING ({formatted_names})
        ''')

        # Assign summed `value` to the records being kept.
        if _SQLITE_VERSION_INFO >= (3, 33, 0):
            # The "UPDATE FROM" syntax was introduced in SQLite 3.33.0.
            sql_statements.append('''
                UPDATE main.element_weight
                SET value=summed_value
                FROM (SELECT weight_id AS old_weight_id,
                             new_element_id,
                             SUM(value) AS summed_value
                      FROM main.element_weight
                      JOIN temp.old_to_new_element_id USING (element_id)
                      GROUP BY weight_id, new_element_id)
                WHERE weight_id=old_weight_id AND element_id=new_element_id
            ''')
        else:
            sql_statements.append('''
                WITH
                    SummedValues AS (
                        SELECT weight_id, new_element_id, SUM(value) AS summed_value
                        FROM main.element_weight
                        JOIN temp.old_to_new_element_id USING (element_id)
                        GROUP BY weight_id, new_element_id
                    ),
                    RecordsToUpdate AS (
                        SELECT element_weight_id AS record_id, summed_value
                        FROM main.element_weight a
                        JOIN SummedValues b
                        ON (a.weight_id=b.weight_id AND a.element_id=b.new_element_id)
                    )
                UPDATE main.element_weight
                SET value = (
                    SELECT summed_value
                    FROM RecordsToUpdate
                    WHERE element_weight_id=record_id
                )
                WHERE element_weight_id IN (SELECT record_id FROM RecordsToUpdate)
            ''')

        # Discard old `element_weight` records.
        sql_statements.append('''
            DELETE FROM main.element_weight
            WHERE element_id IN (
                SELECT element_id
                FROM temp.old_to_new_element_id
                WHERE element_id != new_element_id
            )
        ''')

        # TODO: Assign proportion sums to `relation` table.
        # TODO: Discard old relations.
        # TODO: Update relation.mapping_level

        # Discard old `element` records.
        sql_statements.append('''
            DELETE FROM main.element
            WHERE element_id IN (
                SELECT element_id
                FROM temp.old_to_new_element_id
                WHERE element_id != new_element_id
            )
        ''')

        # TODO: Update weight.is_complete for incomplete weights.
        # TODO: Update edge.is_complete for incomplete edges.
        # TODO: Collapse duplicates in `location` table.
        # TODO: Collapse duplicates in `quantity` table.

        # Remove temporary table.
        sql_statements.append('DROP TABLE temp.old_to_new_element_id')

        return sql_statements

    @classmethod
    def _remove_columns_execute_sql(cls, cursor, columns, strategy='preserve'):
        column_names = cls._get_column_names(cursor, 'element')
        column_names = column_names[1:]  # Slice-off 'element_id'.

        names_to_remove = sorted(set(columns).intersection(column_names))
        if not names_to_remove:
            return  # <- EXIT!

        names_remaining = [col for col in column_names if col not in columns]

        categories = cls._get_data_property(cursor, 'discrete_categories') or []
        categories = [set(cat) for cat in categories]
        cats_filtered = [cat for cat in categories if not cat.intersection(columns)]

        # Check for a loss of category coverage.
        cols_uncovered = set(names_remaining).difference(chain(*cats_filtered))
        if cols_uncovered:
            if strategy == 'preserve':
                formatted = ', '.join(repr(x) for x in sorted(cols_uncovered))
                msg = f'cannot remove, categories are undefined for remaining columns: {formatted}'
                raise ToronError(msg)
            elif strategy == 'restructure':
                new_categories = []
                for cat in categories:
                    cat = cat.difference(names_to_remove)
                    if cat and cat not in new_categories:
                        new_categories.append(cat)
            else:
                msg = f'unknown strategy: {strategy!r}'
                raise ToronError(msg)
        else:
            new_categories = cats_filtered

        # Check for a loss of granularity.
        cursor.execute(f'''
            SELECT 1
            FROM main.element
            GROUP BY {", ".join(names_remaining)}
            HAVING COUNT(*) > 1
        ''')
        if cursor.fetchone() is not None:
            if strategy == 'preserve' or strategy == 'restructure':
                msg = 'cannot remove, columns are needed to preserve granularity'
                raise ToronError(msg)
            elif strategy == 'coarsen':
                for stmnt in cls._coarsen_records_make_sql(cursor, names_remaining):
                    cursor.execute(stmnt)

        # Clear `structure` table to prevent duplicates when removing columns.
        cursor.execute('DELETE FROM main.structure')

        # Remove specified columns.
        for stmnt in cls._remove_columns_make_sql(column_names, names_to_remove):
            cursor.execute(stmnt)

        # Rebuild categories property and structure table.
        cls._update_categories_and_structure(cursor, new_categories)

        # TODO: Recalculate node_hash for `properties` table.

    def remove_columns(self, columns, strategy='preserve'):
        with self._transaction() as cur:
            self._remove_columns_execute_sql(cur, columns, strategy)

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
    def _get_data_property(cursor, key):
        sql = 'SELECT value FROM main.property WHERE key=?'
        cursor.execute(sql, (key,))
        result = cursor.fetchone()
        return result[0] if result else None

    def get_data(self, keys):
        data = {}
        with self._transaction() as cur:
            for key in keys:
                if key == 'column_names':
                    cur.execute("PRAGMA table_info('element')")
                    names = [row[1] for row in cur.fetchall()]
                    data[key] = names[1:]  # Slice-off element_id.
                elif key == 'discrete_categories':
                    categories = self._get_data_property(cur, key) or []
                    data[key] = [set(x) for x in categories]
                else:
                    data[key] = self._get_data_property(cur, key)
        return data

    @staticmethod
    def _set_data_property(cursor, key, value):
        if value is not None:
            # Insert or update property with JSON string.
            sql = '''
                INSERT INTO property(key, value) VALUES(?, ?)
                  ON CONFLICT(key) DO UPDATE SET value=?
            '''
            json_value = _dumps(value, sort_keys=True)
            parameters = (key, json_value, json_value)
        else:
            # Delete property when value is `None`.
            sql = 'DELETE FROM property WHERE key=?'
            parameters = (key,)

        cursor.execute(sql, parameters)

    @classmethod
    def _set_data_structure(cls, cursor, structure):
        """Populates 'structure' table with bitmask made from *structure*."""
        cursor.execute('DELETE FROM structure')  # Delete all table records.
        if not structure:
            return  # <- EXIT!

        columns = cls._get_column_names(cursor, 'structure')
        columns = columns[1:]  # Slice-off "_structure_id" column.
        if not columns:
            msg = 'no labels defined, must first add columns'
            raise ToronError(msg)

        columns_clause = ', '.join(cls._quote_identifier(col) for col in columns)
        values_clause = ', '.join('?' * len(columns))
        sql = f'INSERT INTO structure ({columns_clause}) VALUES ({values_clause})'

        make_bitmask = lambda cat: tuple((col in cat) for col in columns)
        parameters = (make_bitmask(category) for category in structure)
        cursor.executemany(sql, parameters)

    @classmethod
    def _update_categories_and_structure(cls, cursor, categories=None, *, minimize=True):
        """Update `discrete_categories` property and `structure` table.

        Set new categories and rebuild structure table::

            >>> cur = ...
            >>> dal._update_categories_and_structure(cur, categories)

        If categories have already been minimized, you can set the
        *minimize* flag to False in order to prevent running the
        process unnecessarily::

            >>> cur = ...
            >>> dal._update_categories_and_structure(cur, categories, minimize=False)

        Refresh values if label columns have been added but there are
        no explicit category changes (only implicit ones)::

            >>> cur = ...
            >>> dal._update_categories_and_structure(cur)
        """
        if not categories:
            categories = cls._get_data_property(cursor, 'discrete_categories') or []
            categories = [set(x) for x in categories]

        if minimize:
            whole_space = set(cls._get_column_names(cursor, 'element')[1:])
            categories = minimize_discrete_categories(categories, [whole_space])

        list_of_lists = [list(cat) for cat in categories]
        cls._set_data_property(cursor, 'discrete_categories', list_of_lists)

        structure = make_structure(categories)
        cls._set_data_structure(cursor, structure)

    def set_data(self, mapping_or_items):
        if isinstance(mapping_or_items, Mapping):
            items = mapping_or_items.items()
        else:
            items = mapping_or_items

        # Bring 'add_columns' action to the front of the list (it
        # should be processed first).
        items = sorted(items, key=lambda item: item[0] != 'add_columns')

        with self._transaction() as cur:
            for key, value in items:
                if key == 'discrete_categories':
                    self._set_data_property(cur, key, [list(cat) for cat in value])
                elif key == 'structure':
                    self._set_data_structure(cur, value)
                elif key == 'add_columns':
                    for stmnt in self._add_columns_make_sql(cur, value):
                        cur.execute(stmnt)
                    self._update_categories_and_structure(cur)
                else:
                    msg = f"can't set value for {key!r}"
                    raise ToronError(msg)

    def add_discrete_categories(self, discrete_categories):
        data = self.get_data(['discrete_categories', 'column_names'])
        minimized = minimize_discrete_categories(
            data['discrete_categories'],
            discrete_categories,
            [set(data['column_names'])],
        )

        omitted = [cat for cat in discrete_categories if (cat not in minimized)]
        if omitted:
            import warnings
            formatted = ', '.join(repr(cat) for cat in omitted)
            msg = f'omitting categories already covered: {formatted}'
            warnings.warn(msg, category=ToronWarning, stacklevel=2)

        with self._transaction() as cur:
            self._update_categories_and_structure(cur, minimized, minimize=False)

    def remove_discrete_categories(self, discrete_categories):
        data = self.get_data(['discrete_categories', 'column_names'])
        current_cats = data['discrete_categories']
        mandatory_cat = set(data['column_names'])

        if mandatory_cat in discrete_categories:
            import warnings
            formatted = ', '.join(repr(x) for x in data['column_names'])
            msg = f'cannot remove whole space: {{{mandatory_cat}}}'
            warnings.warn(msg, category=ToronWarning, stacklevel=2)
            discrete_categories.remove(mandatory_cat)  # <- Remove and continue.

        no_match = [x for x in discrete_categories if x not in current_cats]
        if no_match:
            import warnings
            formatted = ', '.join(repr(x) for x in no_match)
            msg = f'no match for categories, cannot remove: {formatted}'
            warnings.warn(msg, category=ToronWarning, stacklevel=2)

        remaining_cats = [x for x in current_cats if x not in discrete_categories]

        minimized = minimize_discrete_categories(
            remaining_cats,
            [mandatory_cat],
        )

        with self._transaction() as cur:
            self._update_categories_and_structure(cur, minimized, minimize=False)


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

    @staticmethod
    def _remove_columns_make_sql(column_names, names_to_remove):
        """Return a list of SQL statements for removing label columns."""
        # In SQLite versions before 3.35.0, there is no native support for the
        # DROP COLUMN command. In these older versions of SQLite the tables
        # must be rebuilt. This method prepares a sequence of operations to
        # rebuild the table structures.
        columns_to_keep = [col for col in column_names if col not in names_to_remove]
        new_element_cols = [f"{col} TEXT DEFAULT '-' NOT NULL" for col in columns_to_keep]
        new_location_cols = [f"{col} TEXT" for col in columns_to_keep]
        new_structure_cols = [f"{col} INTEGER CHECK ({col} IN (0, 1)) DEFAULT 0" for col in columns_to_keep]

        statements = [
            # Rebuild 'element' table.
            f'CREATE TABLE new_element(element_id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
                f'{", ".join(new_element_cols)})',
            f'INSERT INTO new_element SELECT element_id, {", ".join(columns_to_keep)} FROM element',
            'DROP TABLE element',
            'ALTER TABLE new_element RENAME TO element',

            # Rebuild 'location' table.
            f'CREATE TABLE new_location(_location_id INTEGER PRIMARY KEY, ' \
                f'{", ".join(new_location_cols)})',
            f'INSERT INTO new_location '
                f'SELECT _location_id, {", ".join(columns_to_keep)} FROM location',
            'DROP TABLE location',
            'ALTER TABLE new_location RENAME TO location',

            # Rebuild 'structure' table.
            f'CREATE TABLE new_structure(_structure_id INTEGER PRIMARY KEY, ' \
                f'{", ".join(new_structure_cols)})',
            f'INSERT INTO new_structure ' \
                f'SELECT _structure_id, {", ".join(columns_to_keep)} FROM structure',
            'DROP TABLE structure',
            'ALTER TABLE new_structure RENAME TO structure',

            # Reconstruct associated indexes.
            f'CREATE UNIQUE INDEX unique_element_index ON element({", ".join(columns_to_keep)})',
            f'CREATE UNIQUE INDEX unique_structure_index ON structure({", ".join(columns_to_keep)})',
        ]
        return statements

    def remove_columns(self, columns, strategy='preserve'):
        # In versions earlier than SQLite 3.35.0, there was no support for
        # the DROP COLUMN command. This method (and other related methods
        # in the class) should implement the recommended, 12-step, ALTER
        # TABLE procedure detailed in the SQLite documentation:
        #     https://www.sqlite.org/lang_altertable.html#otheralter
        con = self._get_connection()
        try:
            con.execute('PRAGMA foreign_keys=OFF')
            cur = con.cursor()
            with savepoint(cur):
                self._remove_columns_execute_sql(cur, columns, strategy)

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
    def _set_data_property(cursor, key, value):
        if value is not None:
            sql = 'INSERT OR REPLACE INTO property(key, value) VALUES (?, ?)'
            parameters = (key, _dumps(value, sort_keys=True))
        else:
            sql = 'DELETE FROM property WHERE key=?'
            parameters = (key,)

        cursor.execute(sql, parameters)


# Set the DataAccessLayer class appropriate for the current version of SQLite.
if _SQLITE_VERSION_INFO < (3, 24, 0):
    dal_class = DataAccessLayerPre24
elif _SQLITE_VERSION_INFO < (3, 25, 0):
    dal_class = DataAccessLayerPre25
elif _SQLITE_VERSION_INFO < (3, 35, 0):
    dal_class = DataAccessLayerPre35
else:
    dal_class = DataAccessLayer
