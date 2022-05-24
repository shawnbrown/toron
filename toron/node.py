"""Node implementation for the Toron project."""

import sqlite3

_sqlite_version_info = sqlite3.sqlite_version_info

if _sqlite_version_info < (3, 35, 0):
    from ._dal import DataAccessLayerPre35 as dal_class
else:
    from ._dal import DataAccessLayer as dal_class


class Node(object):
    def __init__(self, path, mode='rwc'):
        self._dal = dal_class(path, mode)

    @property
    def path(self):
        return self._dal.path

    @property
    def mode(self):
        return self._dal.mode

    def add_columns(self, columns):
        self._dal.add_columns(columns)

    def add_elements(self, iterable, columns=None):
        self._dal.add_elements(iterable, columns)

    def add_weights(self, iterable, columns=None, *, name, type_info, description=None):
        self._dal.add_weights(iterable, columns,
                              name=name,
                              type_info=type_info,
                              description=description)

    def rename_columns(self, mapper):
        self._dal.rename_columns(mapper)

