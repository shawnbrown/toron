"""Category handling functions for the Toron project."""

from itertools import chain
from itertools import combinations

from toron._typing import (
    Iterable,
    List,
    Set,
)


def make_structure(partition_definitions: List[Set[str]]) -> List[Set[str]]:
    """Return all unique unions from the given list of basic open sets.

    In Toron, each "basic open set" is a set of label names that defines
    a partition of the data. This function takes a list of partition
    definitions and computes all possible unions among them, including
    the empty set. The resulting join-semilattice forms the basis used
    to organize data within a node.

    .. code-block::

        >>> make_structure([{'A'}, {'B'}, {'B', 'C'}])
        [set(), {'A'}, {'B'}, {'B', 'C'}, {'A', 'B'}, {'A', 'B', 'C'}]

    While the collection of sets may resemble a topology, it does not
    necessarily satisfy the requirements of one::

        >>> make_structure([{'A', 'B'}, {'B', 'C'}])
        [set(), {'A', 'B'}, {'B', 'C'}, {'A', 'B', 'C'}]

    The result above is not a topology because it is missing the set
    {'B'}--the intersection of {'A', 'B'} and {'B', 'C'}. If a partition
    definition based on {'B'} is needed, it must be included in the
    input list.

    .. admonition:: Why Not Use Topological Spaces?

        If a user mistakenly specifies an invalid partition
        definition--one that does not correspond to a valid partition
        in the domain--it can produce derived sets that are likewise
        invalid. If data is loaded using those sets, the resulting data
        is also invalid.

        When this happens, *unions* that include an invalid partition
        definition contain more context than any of the individual
        partition definitions. This extra information helps to identify
        and correct such mistakes.

        In contrast, the intersection of two partition definitions
        contains less context than either definition alone. If data is
        loaded using such an intersection, it can be more difficult to
        identify and correct the mistake--especially if it isn't caught
        immediately.

        To prevent such issues, Toron avoids using topological spaces
        (which require closure under finite intersections) and instead
        uses a join-semilattice of partition definitions. This approach
        ensures that all derived sets preserve valuable context for
        validation and correction.
    """
    structure = []  # Use list to preserve lexical order of input.
    for length in range(len(partition_definitions) + 1):
        for subsequence in combinations(partition_definitions, length):
            unioned = set().union(*subsequence)  # Use union() for join-semilattice.
            if unioned not in structure:
                structure.append(unioned)
    return structure


def find_minimal_partition_generating_set(
    *bases: Iterable[Set[str]],
) -> List[Set[str]]:
    """Return a minimal set of partition definitions sufficient to
    generate all of the given *bases*.

    .. code-block::

        >>> base_a = [{'A'}, {'B'}, {'B', 'C'}]
        >>> base_b = [{'A', 'C'}, {'C'}, {'C', 'D'}]
        >>> find_minimal_generating_set(base_a, base_b)
        [{'A'}, {'B'}, {'C'}, {'C', 'D'}]
    """
    # Process definitions in order of increasing size so that
    # smaller definitions can be used to generate larger ones.
    definitions: List[Set[str]] = []
    for definition in sorted(chain(*bases), key=len):
        structure = make_structure(definitions)
        if definition not in structure:
            definitions.append(definition)

    return definitions
