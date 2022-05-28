"""Node implementation for the Toron project."""

from itertools import chain
from itertools import combinations

from ._dal import dal_class


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

    @staticmethod
    def _make_structure(discrete_categories):
        """Returns a category structure generated from a base of
        discrete categories::

            >>> node._make_structure([{'A'}, {'B'}, {'A', 'C'}])
            [set(), {'A'}, {'B'}, {'A', 'B'}, {'A', 'C'}, {'A', 'B', 'C'}]

        The generated structure is almost always a topology but that
        is not necessarily the case. There are valid collections of
        discrete categories that do not result in a valid topology::

            >>> node._make_structure([{'A', 'B'}, {'A', 'C'}])
            [set(), {'A', 'B'}, {'A', 'C'}, {'A', 'B', 'C'}]

        The above result is not a valid topology because it does not
        contain the intersection of {'A', 'B'} and {'A', 'C'}--the set
        {'A'}.
        """
        structure = []
        lengths = range(len(discrete_categories) + 1)
        for r in lengths:
            for c in combinations(discrete_categories, r):
                unioned = set().union(*c)
                if unioned not in structure:
                    structure.append(unioned)
        return structure

