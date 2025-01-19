"""Data service functions optimized for DAL1 backend."""

from toron._typing import (
    Callable,
    Dict,
    List,
    Optional,
)

from . import schema
from .repositories import (
    IndexRepository,
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

    Return granularity of a given level--as defined by *columns*.

    If *columns* list is empty or if the index contains no records
    (other than the "undefined" record), then ``None`` will be returned.

    This function implements a Shannon entropy based metric for the
    "granularity measure of a partition" as described on p. 293 of:

        MARK J. WIERMAN (1999) MEASURING UNCERTAINTY IN ROUGH SET
        THEORY, International Journal of General Systems, 28:4-5,
        283-297, DOI: 10.1080/03081079908935239

    In PROBABILISTIC APPROACHES TO ROUGH SETS (Y. Y. Yao, 2003),
    Yiyu Yao presents the same equation in Eq. (6), using a form
    more useful for our implimentation:

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

    #from toron.dal1.schema import format_identifier
    if columns:
        columns = [schema.format_identifier(col) for col in columns]
        groupby_clause = f"\n                GROUP BY {', '.join(columns)}"
    else:
        groupby_clause = ''

    sql = f"""
        WITH
            subset (cardinality) AS (
                SELECT CAST(COUNT(*) AS REAL)
                FROM main.node_index
                WHERE index_id > 0{groupby_clause}
            ),
            summand (uncertainty) AS (
                SELECT ((subset.cardinality / :partition_cardinality)
                        * LOG2(subset.cardinality))
                FROM subset
            )
        SELECT LOG2(:partition_cardinality) - SUM(uncertainty)
        FROM summand
    """
    cursor = index_repo._cursor  # Get cursor (non-public interface).
    cursor.execute(sql, {'partition_cardinality': total_cardinality})
    return cursor.fetchone()[0]


# Define `optimizations` dictionary for optional function optimizations.
optimizations: Dict[str, Callable] = {
    'calculate_granularity': calculate_granularity,
}
