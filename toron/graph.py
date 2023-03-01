"""Graph implementation and functions for the Toron project."""
import sqlite3
from json import (
    dumps as _dumps,
    loads as _loads,
)
from itertools import (
    compress,
    groupby,
)
from ._typing import (
    Dict,
    Iterable,
    Literal,
    Optional,
    Tuple,
    TypeAlias,
    Union,
)

from ._schema import BitList
from ._utils import (
    TabularData,
    make_readerlike,
    NOVALUE,
)
from .node import Node


NoValueType: TypeAlias = NOVALUE.__class__


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
        right_node : Node,
    ) -> None:
        self.name = name
        self.left_node = left_node
        self.right_node = right_node
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
            left_labels = _dumps(list(compress(row, left_mask)))
            right_labels = _dumps(list(compress(row, right_mask)))
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
        format_key = lambda x: dict(zip(keys, _loads(x)))
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

    def get_relations(
        self, side: Literal['left', 'right']
    ) -> Iterable[Tuple[int, int, float]]:
        """Returns an iterable of relations going into the table on the
        given *side* (coming from the other side).

        The following example gets an iterable of incoming relations
        for the right-side table (coming from the left and going to
        the right)::

            >>> relations = mapper.get_relations('right')
        """
        if side == 'left':
            other_side = 'right'
        elif side == 'right':
            other_side = 'left'
        else:
            msg = f"side must be 'left' or 'right', got {side!r}"
            raise ValueError(msg)

        self.cur.execute(f"""
            SELECT
                t2.index_id AS other_index_id,
                t3.index_id AS index_id,
                SUM(weight) AS relation_value
            FROM temp.source_mapping t1
            JOIN temp.{other_side}_matches t2 USING (run_id)
            JOIN temp.{side}_matches t3 USING (run_id)
            GROUP BY t2.index_id, t3.index_id
        """)
        return self.cur

    def close(self) -> None:
        try:
            self.cur.close()  # Fails if Connection is not open.
        except sqlite3.ProgrammingError:
            pass

        self.con.close()


Direction: TypeAlias = Literal['->', '-->', '<->', '<-->', '<-', '<--']


def add_edge(
    data : TabularData,
    name : str,
    left_node : Node,
    direction : Direction,
    right_node : Node,
    selectors: Union[Iterable[str], None, NoValueType] = NOVALUE,
) -> None:
    mapper = _EdgeMapper(data, name, left_node, right_node)
    try:
        mapper.find_matches('left')
        mapper.find_matches('right')

        if '<' in direction:
            relations = mapper.get_relations('left')
            left_node._dal.add_incoming_edge(
                unique_id=right_node._dal.unique_id,
                name=name,
                relations=relations,
                selectors=selectors,
                filename_hint=right_node._dal.data_source or NOVALUE,
            )

        if '>' in direction:
            relations = mapper.get_relations('right')
            right_node._dal.add_incoming_edge(
                unique_id=left_node._dal.unique_id,
                name=name,
                relations=relations,
                selectors=selectors,
                filename_hint=left_node._dal.data_source or NOVALUE,
            )

    finally:
        mapper.close()


class _QuantityIterator(object):
    """An iterator to temporarily store disaggregated quantity data.

    This object is used to to store large amounts of quantity data
    on drive rather than in memory. Internally, the iterator opens a
    temporary file and stores its incoming data in a SQLite database.
    The temporary file remains open until the iterator is exhausted
    or until the close() method is called.
    """
    def __init__(self,
        unique_id: str,
        data: Iterable[Tuple[int, Dict[str, str], float]],
    ) -> None:
        self.unique_id = unique_id

        # Create a connection and a temporary table.
        connection = sqlite3.connect('')  # <- Using '' makes a temp file.

        cursor = connection.execute("""
            CREATE TEMP TABLE temp_quantities (
                index_id INTEGER NOT NULL,
                attributes TEXT NOT NULL,
                quantity_value REAL NOT NULL
            )
        """)

        # Populate the temporary table with the given data.
        cursor.executemany(
            'INSERT INTO temp.temp_quantities VALUES(?, ?, ?)',
            ((a, _dumps(b, sort_keys=True), c) for a, b, c in data),
        )

        # Define a generator to wrap the connection and cursor objects
        # and close them after iteration.
        def generator(connection, cursor):
            cursor.execute("""
                SELECT
                    index_id,
                    attributes,
                    quantity_value
                FROM temp_quantities
            """)
            try:
                for index_id, attributes, quantity_value in cursor:
                    yield index_id, _loads(attributes), quantity_value
            finally:
                # Space used by the temp table and temp file are reclaimed
                # after the connection is closed (the associated file gets
                # automatically removed).
                cursor.close()
                connection.close()

        self._generator = generator(connection, cursor)

    def close(self) -> None:
        self._generator.close()

    def __next__(self) -> Tuple[int, Dict[str, str], float]:
        return next(self._generator)

    def __iter__(self):
        return self
