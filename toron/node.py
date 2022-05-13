"""Node implementation for the Toron project."""

import os
from contextlib import closing
from itertools import compress

from ._node_schema import connect
from ._node_schema import savepoint
from ._node_schema import _get_column_names
from ._node_schema import _make_sql_new_labels
from ._node_schema import _make_sql_insert_elements


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

