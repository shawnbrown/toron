"""Category handling functions for the Toron project."""

from itertools import chain
from itertools import combinations

from toron._typing import (
    Iterable,
    List,
    Set,
)


def make_structure(discrete_categories: List[Set[str]]) -> List[Set[str]]:
    """Return all unique unions from the given list of basic open sets.

    In Toron, each "basic open set" represents a discrete category. This
    function takes a list of these discrete categories and builds all
    possible unions among them, including the empty set. This result is
    a join-semilattice of combined sets which forms a basis used to
    organize data within a node.

    .. code-block::

        >>> make_structure([{'A'}, {'B'}, {'B', 'C'}])
        [set(), {'A'}, {'B'}, {'B', 'C'}, {'A', 'B'}, {'A', 'B', 'C'}]

    While the collection of sets may resemble a topology, it does not
    necessarily satisfy the requirements of one::

        >>> make_structure([{'A', 'B'}, {'B', 'C'}])
        [set(), {'A', 'B'}, {'B', 'C'}, {'A', 'B', 'C'}]

    The result above is not a topology because it's missing the set
    {'B'}--the intersection of {'A', 'B'} and {'B', 'C'}. If {'B'}
    does represent a discrete category, it should be provided in the
    list of input sets.

    .. admonition:: Why Not Use Topological Spaces?

        If a user accidentally includes an invalid category--one that
        doesn't actually make sense in the domain--it can lead to the
        unintended creation of derived sets that also lack meaning. And
        data can be loaded using these invalid sets making the data
        invalid, too.

        When this happens, *unions* that include an invalid category are
        typically **more specific** than their components. This extra
        specificity helps preserve context, making it easier to identify
        and correct such mistakes.

        In contrast, *intersections* involving an invalid category are
        **less specific**. These less specific sets can lack important
        context information, and if data is loaded into them, it can be
        more difficult to fix--especially if the issue isn't caught
        immediately.

        To prevent such accidental losses of specificity, Toron avoids
        using topological spaces (which require closure under finite
        intersections) and instead uses join-semilattices of basic
        categories. This approach ensures that all derived sets remain
        as specific as the original inputs, helping preserve valuable
        context for validation and correction.
    """
    structure = []  # Use list to preserve lexical order of input.
    for length in range(len(discrete_categories) + 1):
        for subsequence in combinations(discrete_categories, length):
            unioned = set().union(*subsequence)  # Use union() for join-semilattice.
            if unioned not in structure:
                structure.append(unioned)
    return structure


def minimize_discrete_categories(*bases: Iterable[Set[str]]) -> List[Set[str]]:
    """Return a minimal set of base categories sufficient to generate
    all of the given *bases*.

    .. code-block::

        >>> base_a = [{'A'}, {'B'}, {'B', 'C'}]
        >>> base_b = [{'A', 'C'}, {'C'}, {'C', 'D'}]
        >>> minimize_discrete_categories(base_a, base_b)
        [{'A'}, {'B'}, {'C'}, {'C', 'D'}]
    """
    # Categories are processed in order of increasing size to ensure
    # that smaller sets serve as the basis for building larger ones.
    base_categories: List[Set[str]] = []
    for category in sorted(chain(*bases), key=len):  # <- sort by size
        structure = make_structure(base_categories)
        if category not in structure:
            base_categories.append(category)

    return base_categories
