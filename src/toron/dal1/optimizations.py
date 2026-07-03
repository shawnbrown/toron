"""Data service functions optimized for DAL1 backend."""

from toron._typing import (
    Callable,
    Dict,
    List,
    Optional,
)

from .repositories import (
    IndexRepository,
)
from .schema import (
    format_identifier,
)


def calculate_granularity(
    columns: List[str],
    index_repo: IndexRepository,
    aux_index_repo: IndexRepository,
) -> Optional[float]:
    r"""
    .. note::

        This is an optimized, drop-in replacement for the normal
        ``toron.data_service.calculate_granularity()`` function.
        It implements the granularity calculation entirely in SQL
        so that it can run natively in SQLite. In tests, this
        version ran over 12 x faster than the standard function.

        Even though not all of the function arguments are used in
        this implementation, it's important to have the exact same
        function signature of the normal function so that we can
        use it as a drop-in replacement.

    Return the granularity of a partition (as given by *columns*)

    If *columns* list is empty or if the index contains no records
    (other than the "undefined" record), then ``None`` will be returned.

    .. code-block:: python

        >>> calculate_granularity(
        ...     ['county', 'town'],
        ...     index_repo,
        ...     aux_index_repo,
        ... )
        6.71556532205684

    This function implements a Shannon entropy based metric which
    was first proposed by Mark Wierman for the "granularity measure
    of a partition" on p. 293 of:

        MARK J. WIERMAN (1999) MEASURING UNCERTAINTY IN ROUGH SET
        THEORY, International Journal of General Systems, 28:4-5,
        283-297, DOI: 10.1080/03081079908935239

    The metric uses block cardinalities to derive relative frequencies,
    whose Shannon entropy serves as a measure of the partition's
    granularity.

    In PROBABILISTIC APPROACHES TO ROUGH SETS (Y. Y. Yao, 2003),
    Yiyu Yao presents the same metric in Eq. (6), using a form
    more useful for our implementation:

    .. code-block:: none

                   m
                  ___
                  \    |A_i|
        log |U| - /    ───── log |A_i|
                  ‾‾‾   |U|
                  i=1

        TeX notation:

            \[\log_{2}|U|-\sum_{i=1}^m \frac{|A_i|}{|U|}\log_{2}|A_i|\]
    """
    if not columns:
        return None  # <- EXIT!

    total_cardinality = index_repo.get_cardinality(include_undefined=False)
    if not total_cardinality:
        return None  # <- EXIT!

    if columns:
        columns = [format_identifier(col) for col in columns]
        groupby_clause = f"\n                GROUP BY {', '.join(columns)}"
    else:
        groupby_clause = ''

    sql = f"""
        WITH
            block (cardinality) AS (
                SELECT CAST(COUNT(*) AS REAL)
                FROM main.label_index
                WHERE index_id > 0{groupby_clause}
            ),
            summand (partition_coarseness) AS (
                SELECT SUM((block.cardinality / :total_cardinality)
                           * LOG2(block.cardinality))
                FROM block
            )
        SELECT LOG2(:total_cardinality) - partition_coarseness
        FROM summand
    """
    cursor = index_repo._cursor  # Get cursor (non-public interface).
    cursor.execute(sql, {'total_cardinality': total_cardinality})
    return cursor.fetchone()[0]


# Define `optimizations` dictionary for optional function optimizations.
optimizations: Dict[str, Callable] = {
    'calculate_granularity': calculate_granularity,
}
