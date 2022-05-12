"""Node implementation for the Toron project."""

import os
from contextlib import closing

from ._node_schema import connect
from ._node_schema import savepoint
from ._node_schema import _make_sql_new_labels


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

