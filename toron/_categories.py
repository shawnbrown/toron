"""Category handling functions for the Toron project."""

from itertools import chain
from itertools import combinations


def make_structure(discrete_categories):
    """Returns a category structure generated from a base of
    discrete categories::

        >>> make_structure([{'A'}, {'B'}, {'B', 'C'}])
        [set(), {'A'}, {'B'}, {'B', 'C'}, {'A', 'B'}, {'A', 'B', 'C'}]

    The generated structure is almost always a topology but that
    is not necessarily the case. There are valid collections of
    discrete categories that do not result in a valid topology::

        >>> make_structure([{'A', 'B'}, {'B', 'C'}])
        [set(), {'A', 'B'}, {'B', 'C'}, {'A', 'B', 'C'}]

    The above result is not a topology because it's missing the
    intersection of {'A', 'B'} and {'B', 'C'}--the set {'B'}.
    """
    structure = []  # Use list to preserve lexical order of input.
    for length in range(len(discrete_categories) + 1):
        for subsequence in combinations(discrete_categories, length):
            unioned = set().union(*subsequence)
            if unioned not in structure:
                structure.append(unioned)
    return structure


def minimize_discrete_categories(*bases):
    """Returns a minimal base of discrete categories that covers
    the same generated structure as all given bases combined::

        >>> base_a = [{'A'}, {'B'}, {'B', 'C'}]
        >>> base_b = [{'A', 'C'}, {'C'}, {'C', 'D'}]
        >>> minimize_discrete_categories(base_a, base_b)
        [{'A'}, {'B'}, {'C'}, {'C', 'D'}]
    """
    base_categories = []
    for category in sorted(chain(*bases), key=len):
        structure = make_structure(base_categories)
        if category not in structure:
            base_categories.append(category)

    return base_categories

