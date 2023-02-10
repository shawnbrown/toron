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
        +---------------+    | right_labels   |    +---------------+
                             | weight         |
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
                index_id INTEGER
            );
            CREATE TEMP TABLE right_matches(
                run_id INTEGER NOT NULL REFERENCES source_mapping(run_id),
                index_id INTEGER
            );
        """)

        iterator = make_readerlike(data)
        fieldnames = next(iterator)
        weight_pos = fieldnames.index(name)

        left_cols = fieldnames[:weight_pos]
        right_cols = fieldnames[weight_pos+1:]

        left_mask = tuple(int(col in left_cols) for col in fieldnames)
        right_mask = tuple(int(col in right_cols) for col in fieldnames)

        left_keys = list(compress(fieldnames, left_mask))
        right_keys = list(compress(fieldnames, right_mask))

        for row in iterator:
            left_labels = json.dumps(list(compress(row, left_mask)))
            right_labels = json.dumps(list(compress(row, right_mask)))
            weight = row[weight_pos]
            sql = 'INSERT INTO temp.source_mapping VALUES (NULL, ?, ?, ?)'
            self.cur.execute(sql, (left_labels, right_labels, weight))

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
