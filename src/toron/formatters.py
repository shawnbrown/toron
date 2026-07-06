"""Data formatting functions for user-facing output (repr and CLI)."""
from ._typing import (
    List,
    Sequence,
    Set,
    Union,
)


def sort_partition_definitions(
    definitions: List[Set[str]],
    labels: Sequence[str],
) -> List[List[str]]:
    """Sort a list of partition definitions and sort labels within
    individual definitions. The order is determined by the given
    ``labels`` sequence.

    .. code-block:: none

        >>> sort_partition_definitions(
        ...     [{'state', 'town'}, {'county', 'state'}],
        ...     labels=['state', 'county', 'town'],
        ... )
        [['state', 'county', 'town'],
         ['state', 'county'],
         ['state', 'town']]
    """
    whole_space = set(labels)
    if whole_space and (whole_space not in definitions):
        definitions.append(whole_space)

    # Sort definitions (starting with whole space first).
    catkey = lambda cat: tuple((x in cat) for x in labels)
    definitions = sorted(definitions, key=catkey, reverse=True)

    # Sort labels within definitions.
    label_to_index = {label: i for (i, label) in enumerate(labels)}
    lblkey = lambda label: label_to_index[label]
    try:
        return [sorted(cat, key=lblkey) for cat in definitions]
    except KeyError as e:
        raise ValueError(f'partition label {e} missing from given labels {labels}')


def format_granularity(
    granularity_values: Sequence[Union[float, None]]
) -> List[str]:
    """Return rounded representations that preserve value distinctness.
    Values are represented with 3 or more decimal places of precision.

    Output uses the number of decimal places necessary to prevent unique
    input values from being rounded to the same representation. Output
    is also right-aligned for display.

    .. code-block::

        >>> format_granularity([12.650378635397704,
        ...                     12.650325958503286,
        ...                     8.297246124988996,
        ...                     5.303016085958896,
        ...                     5.303016085958896])
        ['12.6504',
         '12.6503',
         ' 8.2972',
         ' 5.3030',
         ' 5.3030']

    In the example above, 4 decimal places are used because 3 decimal
    places are not sufficient to differentiate between the first and
    second values. The last two values are exactly the same so their
    rounded representations are also the same.
    """
    # Get unique values and include 0.0 so that selected rounding precision
    # is sufficient to distinguish small values from zero itself.
    unique_vals = set(granularity_values) | {0.0}

    unique_len = len(unique_vals)

    for precision in range(3, 18):
        rounded_vals = {
            f'{x:.{precision}f}' if x is not None else 'None'
            for x in unique_vals
        }
        if len(rounded_vals) == unique_len:
            width = max(len(x) for x in rounded_vals)
            none_repr = 'None'.rjust(width)
            return [
                f'{x:>{width}.{precision}f}' if x is not None else none_repr
                for x in granularity_values
            ]

    raise ValueError('cannot find a unique representation')
