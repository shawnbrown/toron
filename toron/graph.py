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
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
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
        | weight_value  |    | right_labels   |    | weight_value  |
        | proportion    |    | weight         |    | proportion    |
        | mapping_level |    +----------------+    | mapping_level |
        +---------------+                          +---------------+
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
                weight_value REAL CHECK (0.0 <= weight_value),
                proportion REAL CHECK (0.0 <= proportion AND proportion <= 1.0),
                mapping_level BLOB_BITLIST
            );
            CREATE TEMP TABLE right_matches(
                run_id INTEGER NOT NULL REFERENCES source_mapping(run_id),
                index_id INTEGER,
                weight_value REAL CHECK (0.0 <= weight_value),
                proportion REAL CHECK (0.0 <= proportion AND proportion <= 1.0),
                mapping_level BLOB_BITLIST
            );
        """)

        iterator = make_readerlike(data)
        fieldnames = next(iterator)
        weight_pos = fieldnames.index(name)

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

    @staticmethod
    def _find_matches_format_data(
        node: Node, keys: Sequence[str], iterable: Iterable[Tuple[str, int]]
    ) -> Iterator[Tuple[List[int], Dict[str, str], Iterator[Tuple]]]:
        """Takes a list of index keys, a node, and an iterable of
        source labels (values to use for matching) and run ids.
        Returns an iterable of run_ids, where_dicts, and index
        matches.

        .. code-block::

            >>> node = Node(...)
            >>> keys = ['col1', 'col2']
            >>> iterable = [
            ...     ('["A", "x"]', 101),
            ...     ('["A", "y"]', 102),
            ...     ('["B", "x"]', 103),
            ...     ('["B", "y"]', 104),
            ...     ('["C", "x"]', 105),
            ...     ('["C", "y"]', 106),
            ... ]
            >>> formatted = dal._find_matches_format_data(node, keys, iterable)
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
        # Group rows using source labels as the key.
        grouped = groupby(iterable, key=lambda row: row[0])

        # Helper function to format keys as dictionaries.
        def format_key(x):
            return dict((k, v) for k, v in zip(keys, _loads(x)) if v)

        # Helper function to format groups as lists of run_ids (discards key).
        def format_group(group):
            return [x[1] for x in group]

        items = ((format_key(k), format_group(g)) for k, g in grouped)

        # Unzip items into separate where_dict and run_id containers.
        where_dicts, grouped_run_ids = zip(*items)

        # Get node matches (NOTE: accessing internal ``_dal`` directly).
        grouped_matches = node._dal.index_records_grouped(where_dicts)

        # Reformat records for output.
        zipped = zip(grouped_run_ids, grouped_matches)
        run_ids_where_dict_matches = ((x, y, z) for (x, (y, z)) in zipped)

        return run_ids_where_dict_matches

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

        run_ids_key_matches = self._find_matches_format_data(node, keys, self.cur)

        # Add exact matches.
        for run_ids, key, matches in run_ids_key_matches:
            first_match = next(matches)
            num_of_matches = 1 + sum(1 for _ in matches)
            if num_of_matches > 1:  # If more than one index record, the
                continue            # match is ambiguous--skip to next!

            index_id, *_ = first_match  # Unpack index record (discards labels).
            parameters = ((run_id, index_id) for run_id in run_ids)
            sql = f'INSERT INTO temp.{side}_matches (run_id, index_id) VALUES (?, ?)'
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
