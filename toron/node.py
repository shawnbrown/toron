"""Node implementation for the Toron project."""

import os
from contextlib import closing
from itertools import compress

from ._node_schema import connect
from ._node_schema import savepoint
from ._node_schema import _get_column_names
from ._node_schema import _make_sql_new_labels
from ._node_schema import _make_sql_insert_elements
from ._node_schema import _insert_weight_get_id
from ._node_schema import _make_sql_insert_element_weight
from ._node_schema import _update_weight_is_complete


class Node(object):
    def __init__(self, path, mode='rwc'):
        path = os.fspath(path)
        connect(path, mode=mode).close()  # Verify path to Toron node file.
        self._path = path
        self.mode = mode

    @property
    def path(self):
        return self._path

    def add_columns(self, columns):
        with closing(connect(self.path, mode=self.mode)) as con:
            with closing(con.cursor()) as cur:
                with savepoint(cur):
                    for stmnt in _make_sql_new_labels(cur, columns):
                        cur.execute(stmnt)

    def add_elements(self, iterable, columns=None):
        iterator = iter(iterable)
        if not columns:
            columns = next(iterator)

        with closing(connect(self.path, mode=self.mode)) as con:
            with closing(con.cursor()) as cur:
                with savepoint(cur):
                    # Get allowed columns and build selectors values.
                    allowed_columns = _get_column_names(cur, 'element')
                    selectors = tuple((col in allowed_columns) for col in columns)

                    # Filter column names and iterator rows to allowed columns.
                    columns = compress(columns, selectors)
                    iterator = (tuple(compress(row, selectors)) for row in iterator)

                    sql = _make_sql_insert_elements(cur, columns)
                    cur.executemany(sql, iterator)

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

        with closing(connect(self.path, mode=self.mode)) as con:
            with closing(con.cursor()) as cur:
                with savepoint(cur):
                    weight_id = _insert_weight_get_id(cur, name, type_info, description)

                    # Get allowed columns and build selectors values.
                    allowed_columns = _get_column_names(cur, 'element')
                    selectors = tuple((col in allowed_columns) for col in columns)

                    # Filter column names and iterator rows to allowed columns.
                    columns = compress(columns, selectors)
                    def mkrow(row):
                        weightid_and_value = (weight_id, row[weight_pos])
                        element_labels = tuple(compress(row, selectors))
                        return weightid_and_value + element_labels
                    iterator = (mkrow(row) for row in iterator)

                    # Insert element_weight records.
                    sql = _make_sql_insert_element_weight(cur, columns)
                    cur.executemany(sql, iterator)

                    # Update "weight.is_complete" value (set to 1 or 0).
                    _update_weight_is_complete(cur, weight_id)

