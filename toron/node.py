"""Node implementation for the Toron project."""

import os

from ._node_schema import connect


class Node(object):
    def __init__(self, path, mode='rwc'):
        path = os.fspath(path)
        connect(path, mode=mode).close()  # Verify path to Toron node file.
        self._path = path
        self.mode = mode

    @property
    def path(self):
        return self._path

