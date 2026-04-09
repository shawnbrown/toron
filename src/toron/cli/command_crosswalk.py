"""Implementation for "crosswalk" command."""
import argparse
import logging
import uuid
from contextlib import suppress
from itertools import (
    chain,
    islice,
)
from .._typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
)

from .. import TopoNode
from ..mapper import get_mapping_value_position
from .common import (
    ExitCode,
    get_index_code_position,
)
from .._utils import ToronError


applogger = logging.getLogger('app-toron')


def get_column_positions(
    node1: TopoNode,
    node2: TopoNode,
    crosswalk_name: str,
    data: Iterable[Sequence],
    columns: Sequence[str],
) -> Tuple[Dict[str, Optional[int]], Iterator[Sequence]]:
    """Find positions and return positions dict and data iterator."""
    data_iter = iter(data)  # Must be iterator.

    value_position = get_mapping_value_position(columns, crosswalk_name)

    # Scan through data 8 rows (chunk_size) at a time looking for index
    # code columns but give up after 256 (scan_limit) rows.
    chunk_size = 8
    scan_limit = 256

    unscanned_rows = islice(data_iter, scan_limit)
    scanned_rows = []

    node1_id_bytes = uuid.UUID(node1.unique_id).bytes
    node2_id_bytes = uuid.UUID(node2.unique_id).bytes

    node1_index_pos = None
    node2_index_pos = None

    sample_rows: List[Sequence] = list(islice(unscanned_rows, chunk_size))
    while sample_rows:
        if node1_index_pos is None:
            with suppress(RuntimeError):
                node1_index_pos = get_index_code_position(sample_rows, node1_id_bytes)

        if node2_index_pos is None:
            with suppress(RuntimeError):
                node2_index_pos = get_index_code_position(sample_rows, node2_id_bytes)

        scanned_rows.extend(sample_rows)
        if (node1_index_pos is not None) and (node2_index_pos is not None):
            break
        else:
            sample_rows = list(islice(unscanned_rows, chunk_size))

    # Prepare and return result values.
    positions = {
        'node1_index_pos': node1_index_pos,
        'node2_index_pos': node2_index_pos,
        'value_position': value_position,
    }
    data_iter = chain(scanned_rows, data_iter)

    return (positions, data_iter)


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


def process_crosswalk_action(args: argparse.Namespace) -> ExitCode:
    """Write crosswalk to ``args.stdout`` or read from ``args.stdin``."""
    applogger.error('not implemented')
    return ExitCode.ERR
