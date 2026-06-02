"""Data formatting functions for user-facing output (repr and CLI)."""
from ._typing import (
    List,
    Sequence,
)


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
