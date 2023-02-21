"""Graph implementation and functions for the Toron project."""
import json
import sqlite3
from itertools import (
    compress,
    groupby,
)
from ._typing import (
    Literal,
    Optional,
    TypeAlias,
)

from ._schema import BitList
from ._utils import (
    TabularData,
    make_readerlike,
)
from .node import Node


Direction: TypeAlias = Literal['->', '-->', '<->', '<-->', '<-', '<--']


class _EdgeMapper(object):
    """A class to match records and add edges to nodes.

    This class creates a small in-memory database to add edges to
    nodes. When the match is complete and the object is garbage
    cleaned, the temporary database is removed.

    .. code-block:: text

        +---------------+    +----------------+    +---------------+
        | left_matches  |    | source_mapping |    | right_matches |
        +---------------+    +----------------+    +---------------+
        | run_id        |<---| run_id         |--->| run_id        |
        | index_id      |    | left_labels    |    | index_id      |
        | mapping_level |    | right_labels   |    | mapping_level |
        +---------------+    | weight         |    +---------------+
                             +----------------+
    """
    def __init__(
        self,
        data : TabularData,
        name : str,
        left_node : Node,
        direction : Direction,
        right_node : Node,
        selector : Optional[str] = None,
    ) -> None:
        self.name = name
        self.left_node = left_node
        self.direction = direction
        self.right_node = right_node
        self.selector = selector
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
                mapping_level BLOB_BITLIST
            );
            CREATE TEMP TABLE right_matches(
                run_id INTEGER NOT NULL REFERENCES source_mapping(run_id),
                index_id INTEGER,
                mapping_level BLOB_BITLIST
            );
        """)

        iterator = make_readerlike(data)
        fieldnames = next(iterator)
        weight_pos = fieldnames.index(name)

        left_cols = fieldnames[:weight_pos]
        right_cols = fieldnames[weight_pos+1:]

        left_mask = tuple(int(col in left_cols) for col in fieldnames)
        right_mask = tuple(int(col in right_cols) for col in fieldnames)

        self.left_keys = list(compress(fieldnames, left_mask))
        self.right_keys = list(compress(fieldnames, right_mask))

        for row in iterator:
            left_labels = json.dumps(list(compress(row, left_mask)))
            right_labels = json.dumps(list(compress(row, right_mask)))
            weight = row[weight_pos]
            sql = 'INSERT INTO temp.source_mapping VALUES (NULL, ?, ?, ?)'
            self.cur.execute(sql, (left_labels, right_labels, weight))

    def find_matches(self, side: Literal['left', 'right']) -> None:
        if side == 'left':
            keys = self.left_keys
            node = self.left_node
        elif side == 'right':
            keys = self.right_keys
            node = self.right_node
        else:
            msg = f"side must be 'left' or 'right', got {side!r}"
            raise ValueError(msg)

        # Order by labels for itertools.groupby() later.
        self.cur.execute(f"""
            SELECT {side}_labels, run_id
            FROM temp.source_mapping
            ORDER BY {side}_labels
        """)

        # Group rows using lablels as key.
        grouped = groupby(self.cur, key=lambda row: row[0])

        # Format keys as dictionary, format groups as list of run_ids.
        format_key = lambda x: dict(zip(keys, json.loads(x)))
        format_group = lambda g: [x[1] for x in g]
        items = ((format_key(k), format_group(g)) for k, g in grouped)

        # Unzip items into separate where_dict and run_id containers.
        where_dicts, grouped_run_ids = zip(*items)

        # Get node matches (NOTE: accessing internal ``_dal`` directly).
        grouped_matches = node._dal.index_records_grouped(where_dicts)

        # Add exact matches.
        for run_ids, (key, matches) in zip(grouped_run_ids, grouped_matches):
            first_match = next(matches)
            num_of_matches = 1 + sum(1 for _ in matches)
            if num_of_matches > 1:  # If more than one index record, the
                continue            # match is ambiguous--skip to next!

            index_id, *_ = first_match  # Unpack index record (discards labels).
            parameters = ((run_id, index_id) for run_id in run_ids)
            sql = f'INSERT INTO temp.{side}_matches VALUES (?, ?, NULL)'
            self.cur.executemany(sql, parameters)

    def close(self) -> None:
        self.cur.close()
        self.con.close()

    def __del__(self) -> None:
        self.close()


def add_edge(
    data : TabularData,
    name : str,
    left_node : Node,
    direction : Direction,
    right_node : Node,
    selector : Optional[str] = None,
) -> None:
    raise NotImplementedError
