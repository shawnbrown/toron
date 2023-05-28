"""Tools for building correspondence mappings between label sets."""

import sqlite3
from itertools import (
    groupby,
)
from json import (
    dumps as _dumps,
    loads as _loads,
)
from ._typing import (
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Sequence,
    Tuple,
    TYPE_CHECKING,
)

from ._utils import (
    TabularData,
    make_readerlike,
    parse_edge_shorthand,
)

if TYPE_CHECKING:
    from .node import Node


class Mapper(object):
    """Object to build a correspondence mapping between label sets.

    This class create a small in-memory database. When the object is
    garbage collected, the temporary database is removed. It uses the
    following schema:

    .. code-block:: text

        +---------------+    +----------------+    +---------------+
        | left_matches  |    | source_mapping |    | right_matches |
        +---------------+    +----------------+    +---------------+
        | run_id        |<---| run_id         |--->| run_id        |
        | index_id      |    | left_labels    |    | index_id      |
        | weight_value  |    | right_labels   |    | weight_value  |
        | proportion    |    | weight         |    | proportion    |
        | mapping_level |    +----------------+    | mapping_level |
        +---------------+                          +---------------+
    """
    def __init__(self, data: TabularData, name: str):
        self.con = sqlite3.connect(':memory:')
        self.cur = self.con.executescript("""
            CREATE TEMP TABLE source_mapping(
                run_id INTEGER PRIMARY KEY,
                left_labels TEXT NOT NULL,
                right_labels TEXT NOT NULL,
                weight REAL NOT NULL
            );
            CREATE TEMP TABLE left_matches(
                run_id INTEGER NOT NULL REFERENCES source_mapping(run_id),
                index_id INTEGER,
                weight_value REAL CHECK (0.0 <= weight_value),
                proportion REAL CHECK (0.0 <= proportion AND proportion <= 1.0),
                mapping_level BLOB_BITFLAGS
            );
            CREATE TEMP TABLE right_matches(
                run_id INTEGER NOT NULL REFERENCES source_mapping(run_id),
                index_id INTEGER,
                weight_value REAL CHECK (0.0 <= weight_value),
                proportion REAL CHECK (0.0 <= proportion AND proportion <= 1.0),
                mapping_level BLOB_BITFLAGS
            );
        """)

        iterator = make_readerlike(data)
        fieldnames = [str(x).strip() for x in next(iterator)]
        name = name.strip()

        for i, x in enumerate(fieldnames):
            if name == x or name == parse_edge_shorthand(x).get('edge_name'):
                weight_pos = i  # Get index position of weight column
                break
        else:  # no break
            msg = f'{name!r} is not in data, got header: {fieldnames!r}'
            raise ValueError(msg)

        self.left_keys = fieldnames[:weight_pos]
        self.right_keys = fieldnames[weight_pos+1:]

        for row in iterator:
            left_labels = _dumps(row[:weight_pos])
            right_labels = _dumps(row[weight_pos+1:])
            weight = row[weight_pos]
            sql = 'INSERT INTO temp.source_mapping VALUES (NULL, ?, ?, ?)'
            self.cur.execute(sql, (left_labels, right_labels, weight))

    @staticmethod
    def _find_matches_format_data(
        node: 'Node',
        column_names: Sequence[str],
        iterable: Iterable[Tuple[str, int]],
    ) -> Iterator[Tuple[List[int], Dict[str, str], Iterator[Tuple]]]:
        """Takes a *node*, a sequence of label *keys*, and an *iterable*
        containing ``(label_values, run_id)`` records. Returns an
        iterator of ``(run_ids, where_dict, matches)`` records.

        .. code-block::

            >>> node = Node(...)
            >>> column_names = ['col1', 'col2']
            >>> iterable = [
            ...     ('["A", "x"]', 101),
            ...     ('["A", "y"]', 102),
            ...     ('["B", "x"]', 103),
            ...     ('["B", "y"]', 104),
            ...     ('["C", "x"]', 105),
            ...     ('["C", "y"]', 106),
            ... ]
            >>> formatted = dal._find_matches_format_data(node, column_names, iterable)
            >>> for run_ids, where_dict, matches in formatted:
            ...     print(f'{run_ids=}  {where_dict=}  {list(matches)=}')
            ...
            run_ids=[101]  where_dict={'col1': 'A', 'col2': 'x'}  list(matches)=[(1, 'A', 'x')]
            run_ids=[102]  where_dict={'col1': 'A', 'col2': 'y'}  list(matches)=[(2, 'A', 'y')]
            run_ids=[103]  where_dict={'col1': 'B', 'col2': 'x'}  list(matches)=[(3, 'B', 'x')]
            run_ids=[104]  where_dict={'col1': 'B', 'col2': 'y'}  list(matches)=[(4, 'B', 'y')]
            run_ids=[105]  where_dict={'col1': 'C', 'col2': 'x'}  list(matches)=[(5, 'C', 'x')]
            run_ids=[106]  where_dict={'col1': 'C', 'col2': 'y'}  list(matches)=[(6, 'C', 'y')]
        """
        # Group rows using `label_values` as the key.
        def get_label_values(row):
            label_values, _ = row  # Discards `run_id` value.
            return label_values

        grouped = groupby(iterable, key=get_label_values)

        # Helper function to format records as where_dicts.
        def get_where_dict(x):
            return dict((k, v) for k, v in zip(column_names, _loads(x)) if v)

        # Helper function to format groups as lists of `run_id` values.
        def get_run_ids(group):
            return [run_id for _, run_id in group]  # Discards `label_values` key.

        items = ((get_where_dict(k), get_run_ids(g)) for k, g in grouped)

        # Unzip items into separate where_dict and run_id containers.
        where_dicts, grouped_run_ids = zip(*items)

        # Get node matches (NOTE: accessing internal ``_dal`` directly).
        grouped_matches = node._dal.index_records_grouped(where_dicts)

        # Reformat records for output.
        zipped = zip(grouped_run_ids, grouped_matches)
        run_ids_where_dict_matches = ((x, y, z) for (x, (y, z)) in zipped)

        return run_ids_where_dict_matches

    def find_matches(
        self,
        node: 'Node',
        side: Literal['left', 'right'],
    ) -> None:
        if side == 'left':
            column_names = self.left_keys
        elif side == 'right':
            column_names = self.right_keys
        else:
            msg = f"side must be 'left' or 'right', got {side!r}"
            raise ValueError(msg)
