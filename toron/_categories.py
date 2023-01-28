"""Category handling functions for the Toron project."""

from itertools import chain
from itertools import combinations


def make_structure(discrete_categories):
    """Returns a join-semilattice generated from a collection of
    discrete categories which can be used to define the valid levels
    of granularity in a dataset::

        >>> make_structure([{'A'}, {'B'}, {'B', 'C'}])
        [set(), {'A'}, {'B'}, {'B', 'C'}, {'A', 'B'}, {'A', 'B', 'C'}]

    While the collection of sets in the semilattice may often satisfy
    the requirements of a topology, this is not necessarily the case::

        >>> make_structure([{'A', 'B'}, {'B', 'C'}])
        [set(), {'A', 'B'}, {'B', 'C'}, {'A', 'B', 'C'}]

    The collection of sets in the above semilattice do not satisfy the
    requirements of a topology because it's missing the set {'B'}--the
    intersection of {'A', 'B'} and {'B', 'C'}.
    """
    structure = []  # Use list to preserve lexical order of input.
    for length in range(len(discrete_categories) + 1):
        for subsequence in combinations(discrete_categories, length):
            unioned = set().union(*subsequence)  # Use union() for join-semilattice.
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
