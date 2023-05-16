"""Tools for building correspondence mappings between label sets."""

import sqlite3
from itertools import (
    compress,
)
from json import (
    dumps as _dumps,
)

from ._utils import (
    TabularData,
    make_readerlike,
    parse_edge_shorthand,
)


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
        try:
            weight_pos = fieldnames.index(name)
        except ValueError:
            for i, x in enumerate(fieldnames):
                if name == parse_edge_shorthand(x).get('edge_name'):
                    weight_pos = i
                    break
            else:  # no break
                msg = f'{name!r} is not in data, got header: {fieldnames!r}'
                raise ValueError(msg)

        left_mask = tuple(i < weight_pos for i in range(len(fieldnames)))
        right_mask = tuple(i > weight_pos for i in range(len(fieldnames)))

        self.left_keys = list(compress(fieldnames, left_mask))
        self.right_keys = list(compress(fieldnames, right_mask))

        for row in iterator:
            left_labels = _dumps(list(compress(row, left_mask)))
            right_labels = _dumps(list(compress(row, right_mask)))
            weight = row[weight_pos]
            sql = 'INSERT INTO temp.source_mapping VALUES (NULL, ?, ?, ?)'
            self.cur.execute(sql, (left_labels, right_labels, weight))
