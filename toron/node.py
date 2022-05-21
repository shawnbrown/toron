"""Node implementation for the Toron project."""

from ._node_schema import DataAccessLayer


class Node(object):
    def __init__(self, path, mode='rwc'):
        self._dal = DataAccessLayer(path, mode)
        self._path = path
        self.mode = mode

    @property
    def path(self):
        return self._path

    def add_columns(self, columns):
        self._dal.add_columns(columns)

    def add_elements(self, iterable, columns=None):
        self._dal.add_elements(iterable, columns)

    def add_weights(self, iterable, columns=None, *, name, type_info, description=None):
        self._dal.add_weights(iterable, columns,
                              name=name,
                              type_info=type_info,
                              description=description)

