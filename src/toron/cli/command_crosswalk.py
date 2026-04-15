"""Implementation for "crosswalk" command."""
import argparse
import csv
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
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from .. import TopoNode
from ..mapper import (
    get_mapping_value_position,
    Mapper,
)
from .._utils import (
    eagerly_initialize,
    normalize_tabular,
    ToronError,
    BitFlags,
)
from .common import (
    ExitCode,
    index_code_to_id,
    get_index_code_position,
    process_backup_option,
)


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

    try:
        value_position = get_mapping_value_position(columns, crosswalk_name)
    except ValueError:
        raise ToronError(
            f"crosswalk {crosswalk_name!r} not found in columns: "
            f"{', '.join(repr(x) for x in columns)}"
        )

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

    # If column order is invalid, raise error.
    if (node1_index_pos is not None
        and node2_index_pos is not None
        and (max(node1_index_pos, node2_index_pos) < value_position
             or min(node1_index_pos, node2_index_pos) > value_position)):
        raise RuntimeError(
            f'Invalid column order in mapping data. The crosswalk column '
            f'must appear between the two groups of node columns.\n\n'
            f'Expected layout:\n'
            f'  <first node columns> <crosswalk name> <second node columns>\n\n'
            f'Found index code columns at positions {node1_index_pos} and '
            f'{node2_index_pos}, but the crosswalk column is at position '
            f'{value_position} -- it does not separate them.'
        )

    # Get slice positions for location columns.
    if node1_index_pos is None:
        node1_start, node1_stop = None, None
    elif node1_index_pos < value_position:
        node1_start, node1_stop = 0, value_position  # node1 on left
    else:
        node1_start, node1_stop = value_position + 1, len(columns)  # node1 on right

    if node2_index_pos is None:
        node2_start, node2_stop = None, None
    elif node2_index_pos < value_position:
        node2_start, node2_stop = 0, value_position  # node2 on left
    else:
        node2_start, node2_stop = value_position + 1, len(columns)  # node2 on right

    # If only one node is matched, try to match the other side by header.
    if node1_start is not None and node2_start is None:
        if node1_start > value_position:
            other_start, other_stop = 0, value_position  # left side
        else:
            other_start, other_stop = value_position + 1, len(columns)  # right side

        if node2.index_columns == columns[other_start:other_stop]:  # check header.
            node2_start, node2_stop = other_start, other_stop
        else:
            raise ToronError(
                f"unable to find FILE2 columns;\n"
                f"  Expected: {', '.join(repr(x) for x in node2.index_columns)}\n"
                f"     Found: {', '.join(repr(x) for x in columns[other_start:other_stop])}"
            )

    elif node2_start is not None and node1_start is None:
        if node2_start > value_position:
            other_start, other_stop = 0, value_position  # left side
        else:
            other_start, other_stop = value_position + 1, len(columns)  # right side

        if node1.index_columns == columns[other_start:other_stop]:  # check header.
            node1_start, node1_stop = other_start, other_stop
        else:
            raise ToronError(
                f"unable to find FILE1 columns;\n"
                f"  Expected: {', '.join(repr(x) for x in node1.index_columns)}\n"
                f"     Found: {', '.join(repr(x) for x in columns[other_start:other_stop])}"
            )

    elif node1_start is None and node2_start is None:
        # If no indexes, require node1 labels to match left side
        # and node2 labels to match right side.
        if (node1.index_columns == columns[:value_position]
                and node2.index_columns == columns[value_position+1:]):
            node1_start, node1_stop = 0, value_position
            node2_start, node2_stop = value_position+1, len(columns)
        else:
            msg_list = [
                f"no index codes found, unable to match by label columns;"
            ]
            if node1.index_columns != columns[:value_position]:
                msg_list.extend([
                    f"",
                    f"unable to find FILE1 columns;",
                    f"  Expected: {', '.join(repr(x) for x in node1.index_columns)}",
                    f"     Found: {', '.join(repr(x) for x in columns[:value_position])}",
                ])
            if node2.index_columns != columns[value_position+1:]:
                msg_list.extend([
                    f"",
                    f"unable to find FILE2 columns;",
                    f"  Expected: {', '.join(repr(x) for x in node2.index_columns)}",
                    f"     Found: {', '.join(repr(x) for x in columns[value_position+1:])}",
                ])

            raise ToronError('\n'.join(msg_list))

    # Prepare and return result values.
    positions = {
        'node1_index_pos': node1_index_pos,
        'node1_start': node1_start,
        'node1_stop': node1_stop,
        'node2_index_pos': node2_index_pos,
        'node2_start': node2_start,
        'node2_stop': node2_stop,
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


def make_getter_functions(
    node: TopoNode,
    index_code_pos: Optional[int],
    sample_header: Sequence[str],
    start: Optional[int],
    stop: Optional[int],
) -> Tuple[Callable[[Sequence], Optional[int]],
           Callable[[Sequence], List],
           Callable[[Optional[int], Sequence[str]], BitFlags]]:
    """Make and return a tuple of three getter functions.

    The three functions are:

    * get_index_id(): Takes a row with index code and returns index_id.
    * get_location(): Takes a row and slice positions, returns location.
    * get_level(): Takes an index_id and location, returns BitFlags.
    """
    if index_code_pos is not None:
        node_unique_id_bytes = uuid.UUID(node.unique_id).bytes
        def node_get_index_id(row: Sequence) -> Optional[int]:
            index_code = row[index_code_pos]
            if not index_code:
                return None
            return index_code_to_id(index_code, node_unique_id_bytes)
    else:
        def node_get_index_id(row: Sequence) -> Optional[int]:
            return None

    node_get_location = get_location_factory(
        header_row=sample_header,
        label_columns=node.index_columns,
        start=start,
        stop=stop,
    )

    node_entire_space = BitFlags([1] * len(node.index_columns))  # All ones.

    def node_get_level(index_id: Optional[int], location: Sequence[str]) -> BitFlags:
        if index_id is None:
            return BitFlags(location)
        return node_entire_space

    return (node_get_index_id, node_get_location, node_get_level)


@eagerly_initialize
def normalize_mapping_data(
    node1: TopoNode,
    node2: TopoNode,
    crosswalk_name: str,
    data: Union[Iterable[Sequence], Iterable[Dict]],
    columns: Optional[Sequence[str]] = None,
) -> Generator[Sequence, None, None]:
    """Normalize mapping data to yield lists to load into Mapper."""
    data, columns = normalize_tabular(data, columns)
    positions, data_iter = get_column_positions(node1, node2, crosswalk_name, data, columns)

    value_position = get_mapping_value_position(columns, crosswalk_name)
    get_mapping_value = lambda row: row[value_position]

    (node1_get_index_id,
     node1_get_location,
     node1_get_level) = make_getter_functions(node1,
                                              positions['node1_index_pos'],
                                              columns,
                                              positions['node1_start'],
                                              positions['node1_stop'])

    (node2_get_index_id,
     node2_get_location,
     node2_get_level) = make_getter_functions(node2,
                                              positions['node2_index_pos'],
                                              columns,
                                              positions['node2_start'],
                                              positions['node2_stop'])

    for row in data_iter:
        node1_index_id = node1_get_index_id(row)
        node1_location = node1_get_location(row)

        node2_index_id = node2_get_index_id(row)
        node2_location = node2_get_location(row)

        yield [
            node1_index_id,
            node1_location,
            node1_get_level(node1_index_id, node1_location),

            node2_index_id,
            node2_location,
            node2_get_level(node2_index_id, node2_location),

            get_mapping_value(row),
        ]


def read_from_stdin(args: argparse.Namespace) -> ExitCode:
    """Insert crosswalk relations read from stdin stream."""
    # Check that crosswalk is defined in nodes.
    left_crosswalk = args.node1.get_crosswalk(args.node2, args.crosswalk)
    right_crosswalk = args.node2.get_crosswalk(args.node1, args.crosswalk)
    if args.direction == 'both':
        if right_crosswalk and not left_crosswalk:
            applogger.warning(f'no {args.crosswalk!r} crosswalk in FILE1')
            args.direction = 'right'
        elif left_crosswalk and not right_crosswalk:
            applogger.warning(f'no {args.crosswalk!r} crosswalk in FILE2')
            args.direction = 'left'
        elif not left_crosswalk and not right_crosswalk:
            applogger.error(f'no {args.crosswalk!r} crosswalk in FILE1 or FILE2')
            return ExitCode.ERR  # <- EXIT!
    elif args.direction == 'left' and not left_crosswalk:
        applogger.error(f'no {args.crosswalk!r} crosswalk in FILE1')
        return ExitCode.ERR  # <- EXIT!
    elif args.direction == 'right' and not right_crosswalk:
        applogger.error(f'no {args.crosswalk!r} crosswalk in FILE2')
        return ExitCode.ERR  # <- EXIT!
    else:
        raise ValueError

    # Normalize and load mapping data.
    data = normalize_mapping_data(
        args.node1, args.node2, args.crosswalk, csv.reader(args.stdin)
    )
    mapper = Mapper(args.node1, args.node2, data)

    # Match mapping to node labels.
    applogger.info(f'matching FILE1 index records')
    mapper.match_node_records('node1', match_limit=1, allow_overlapping=False)
    applogger.info(f'matching FILE2 index records')
    mapper.match_node_records('node2', match_limit=1, allow_overlapping=False)

    # Insert relations into FILE2.
    if args.direction in {'both', 'right'}:
        applogger.info(f'loading relations: FILE1 -> FILE2')
        relations = mapper.get_relations('node2')
        args.node2.insert_relations2(
            args.node1,
            args.crosswalk,
            data=relations,
            columns=['other_index_id', 'index_id', 'mapping_level', 'relation_value'],
        )
        crosswalk = args.node2.get_crosswalk(args.node1, args.crosswalk)
        if crosswalk.is_locally_complete:
            applogger.info(f'crosswalk is complete')
        else:
            applogger.warning(f'crosswalk is incomplete')

    # Insert relations into FILE1.
    if args.direction in {'both', 'left'}:
        applogger.info(f'loading relations: FILE1 <- FILE2')
        relations = mapper.get_relations('node1')
        args.node1.insert_relations2(
            args.node2,
            args.crosswalk,
            data=relations,
            columns=['other_index_id', 'index_id', 'mapping_level', 'relation_value'],
        )
        crosswalk = args.node1.get_crosswalk(args.node2, args.crosswalk)
        if crosswalk.is_locally_complete:
            applogger.info(f'crosswalk is complete')
        else:
            applogger.warning(f'crosswalk is incomplete')

    return ExitCode.OK


def process_crosswalk_action(args: argparse.Namespace) -> ExitCode:
    """Write crosswalk to ``args.stdout`` or read from ``args.stdin``."""
    if args.stdin_is_streamed:
        process_backup_option(args, node_args=['node1', 'node2'])
        return read_from_stdin(args)
    else:
        applogger.error('not implemented')
        return ExitCode.ERR
