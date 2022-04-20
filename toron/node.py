"""Node implementation for the Toron project."""

from ._node_schema import connect


class Node(object):
    def __init__(self, path):
        path = os.fspath(path)
        self._path = path

        connect(path).close()  # Verify that path is a Toron node.

