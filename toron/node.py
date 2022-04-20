"""Node implementation for the Toron project."""

import os
import sqlite3
from ._node_schema import _schema_script


class Node(object):
    def __init__(self, path):
        path = os.fspath(path)
        self._path = path

        if os.path.exists(path):
            try:
                con = sqlite3.connect(path)
                con.close()
            except sqlite3.OperationalError:
                # If *path* is a directory or non-file resource, then
                # calling `connect()` will raise an OperationalError.
                raise Exception(f'path {path!r} is not a Toron Node')
        else:
            con = sqlite3.connect(path)
            con.executescript(_schema_script)
            con.close()

