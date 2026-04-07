"""Implementation for "crosswalk" command."""
from .._typing import Callable, List, Optional, Sequence


def get_location_factory(
    header_row: Sequence[str],
    label_columns: Sequence[str],
    start: Optional[int] = None,
    stop: Optional[int] = None,
) -> Callable[[Sequence], List]:
    """Return a function to get location labels from a given row.

    The function can be limited to a slice of the given values
    using the *start* and *stop* parameters.

    .. code-block:: python

        >>> header = ['foo', 'bar', 'baz', 'qux', 'foo', 'bar']
        >>> row = ['A', 'B', 'C', 100.0, 'D', 'E']
        >>> label_cols = ['foo', 'bar', 'baz']
        >>>
        >>> get_location = self._get_location_factory(header, label_cols, stop=3)
        >>> get_location(row)
        ['A', 'B', 'C']
        >>>
        >>> get_location = self._get_location_factory(header, label_cols, start=3)
        >>> get_location(row)
        ['D', 'E', '']

    If there are duplicate column names within a given slice, a
    ``ValueError`` is raised.
    """
    # Slice header (if needed) and check for duplicates.
    if start is not None or stop is not None:
        header_row = header_row[start:stop]

    if len(header_row) != len(set(header_row)):
        raise ValueError(
            f'found duplicate values in header: {header_row!r}\n'
            f'You may need to limit the columns using `start` or `stop` values.'
        )

    # Build name-to-index lookup.
    enumerated = enumerate(header_row, start or 0)
    name_to_index = {name: i for i, name in enumerated}

    # Build tuple of indexes (use -1 for missing columns).
    indexes = tuple(name_to_index.get(name, -1) for name in label_columns)

    # Define and return `get_location()` function (closes over `indexes`).
    def get_location(row):
        return [(row[i] if i != -1 else '') for i in indexes]

    return get_location
