"""Data formatting functions for user-facing output (repr and CLI)."""
from ._typing import (
    List,
    Sequence,
    Set,
)


def sort_categories(
    discrete_categories: List[Set[str]],
    labels: Sequence[str],
) -> List[List[str]]:
    """Sort a list of categories and sort labels within categories.
    The order is determined by the given ``labels`` sequence.

    .. code-block:: none

        >>> sort_categories(
        ...     [{'state', 'town'}, {'county', 'state'}],
        ...     labels=['state', 'county', 'town'],
        ... )
        [['state', 'county', 'town'],
         ['state', 'county'],
         ['state', 'town']]
    """
    whole_space = set(labels)
    if whole_space and (whole_space not in discrete_categories):
        discrete_categories.append(whole_space)

    # Sort categories (starting with whole space first).
    catkey = lambda cat: tuple((x in cat) for x in labels)
    discrete_categories = sorted(discrete_categories, key=catkey, reverse=True)

    # Sort labels within categories.
    label_to_index = {label: i for (i, label) in enumerate(labels)}
    lblkey = lambda label: label_to_index[label]
    try:
        return [sorted(cat, key=lblkey) for cat in discrete_categories]
    except KeyError as e:
        raise ValueError(f'category label {e} missing from given labels {labels}')


def format_granularity(granularity_values: Sequence[float]) -> List[str]:
    """Return rounded representations that preserve value uniqueness.
    Values are represented with two or more decimal places of precision.

    Output uses the number of decimal places necessary to prevent unique
    input values from being rounded to the same representation. Output
    is also right-aligned for display.

    .. code-block:: none

        >>> format_granularity([12.650378635397704,
        ...                     12.647267731680174,
        ...                     8.297246124988996,
        ...                     5.303016085958896,
        ...                     5.303016085958896])
        ['12.650',
         '12.647',
         ' 8.297',
         ' 5.303',
         ' 5.303']

    In the example above, three decimal places are used because two
    decimal places are not sufficient to differentiate between the first
    and second values. The last two values are exactly the same so their
    rounded representations are also the same.
    """
    unique_values = set(granularity_values) | {0.0}  # Always include `0.0`.
    unique_len = len(unique_values)

    precision = 2
    while True:
        formatted = {f'{x:.{precision}f}' for x in unique_values}
        if len(formatted) == unique_len:
            width = max(len(x) for x in formatted)
            fmtstr = f'{{:>{width}.{precision}f}}'  # Build format-string.
            return [fmtstr.format(x) for x in granularity_values]
        precision += 1
