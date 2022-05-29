"""Node implementation for the Toron project."""

from itertools import chain
from itertools import combinations

from ._dal import dal_class
from ._exceptions import ToronWarning


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
            [set(), {'A'}, {'B'}, {'A', 'C'}, {'A', 'B'}, {'A', 'B', 'C'}]

        The generated structure is almost always a topology but that
        is not necessarily the case. There are valid collections of
        discrete categories that do not result in a valid topology::

            >>> node._make_structure([{'A', 'B'}, {'A', 'C'}])
            [set(), {'A', 'B'}, {'A', 'C'}, {'A', 'B', 'C'}]

        The above result is not a valid topology because it does not
        contain the intersection of {'A', 'B'} and {'A', 'C'}--the set
        {'A'}.
        """
        structure = []  # Use list to preserve lexical order of input.
        for length in range(len(discrete_categories) + 1):
            for subsequence in combinations(discrete_categories, length):
                unioned = set().union(*subsequence)
                if unioned not in structure:
                    structure.append(unioned)
        return structure

    @classmethod
    def _minimize_discrete_categories(cls, *bases):
        """Returns a minimal base of discrete categories that covers
        the same generated structure as all given bases combined::

            >>> base_a = [{'A'}, {'B'}, {'B', 'C'}]
            >>> base_b = [{'A', 'C'}, {'C'}, {'C', 'D'}]
            >>> Node._minimize_discrete_categories(base_a, base_b)
            [{'A'}, {'B'}, {'C'}, {'C', 'D'}]
        """
        base_categories = []
        for category in sorted(chain(*bases), key=len):
            structure = cls._make_structure(base_categories)
            if category not in structure:
                base_categories.append(category)

        return base_categories

    def add_discrete_categories(self, discrete_categories):
        minimized = self._minimize_discrete_categories(
            self._dal.get_discrete_categories(),
            discrete_categories,
        )

        omitted = [cat for cat in discrete_categories if (cat not in minimized)]
        if omitted:
            import warnings
            formatted = ', '.join(repr(cat) for cat in omitted)
            msg = f'omitting categories already covered: {formatted}'
            warnings.warn(msg, category=ToronWarning, stacklevel=2)

        self._dal.set_discrete_categories(minimized)

