"""Graph implementation and functions for the Toron project."""
import sqlite3
from json import (
    dumps as _dumps,
    loads as _loads,
)
from itertools import (
    compress,
    groupby,
    product,
)
from ._typing import (
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeAlias,
    Union,
)

from ._schema import BitFlags
from ._utils import (
    TabularData,
    make_readerlike,
    NOVALUE,
    ToronWarning,
)
from ._mapper import Mapper
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

    def _refresh_proportions(self, side: Literal['left', 'right']) -> None:
        """Update 'proportion' values in left or right matches table."""
        sql = f"""
            WITH
                SummedValues AS (
                    SELECT
                        run_id AS summed_run_id,
                        SUM(weight_value) AS summed_weight_value
                    FROM temp.{side}_matches
                    GROUP BY run_id
                )
            UPDATE temp.{side}_matches
            SET proportion=COALESCE(
                (weight_value / (SELECT summed_weight_value
                                 FROM SummedValues
                                 WHERE run_id=summed_run_id)),
                1.0
            )
        """
        self.cur.execute(sql)

    @staticmethod
    def _find_matches_warn(
        *,
        unresolvable_count: int = 0,
        overlimit_count: int = 0,
        overlimit_max: int = 0,
        match_limit: Union[int, float] = 1,
        invalid_count: int = 0,
        invalid_categories: Set[Tuple] = set(),
        unweighted_count: int = 0,
    ) -> None:
        """If needed, emit ToronWarning with relevant information."""
        messages = []

        if unresolvable_count:
            messages.append(
                f'skipped {unresolvable_count} values that matched no records'
            )

        if overlimit_count:
            messages.append(
                f'skipped {overlimit_count} values that matched too many records'
            )
            messages.append(
                f'current match_limit is {match_limit} but data includes values '
                f'that match up to {overlimit_max} records'
            )

        if invalid_count:
            category_list = [', '.join(c) for c in sorted(invalid_categories)]
            category_string = '\n  '.join(category_list)
            messages.append(
                f'skipped {invalid_count} values that used invalid categories:\n'
                f'  {category_string}\n'
            )

        if unweighted_count:
            messages.append(
                f'skipped {unweighted_count} values that ambiguously matched '
                f'to one or more records that have no associated weight'
            )

        if messages:
            import warnings
            msg = ', '.join(messages)
            warnings.warn(msg, category=ToronWarning, stacklevel=3)

    def find_matches(
        self,
        side: Literal['left', 'right'],
        match_limit: Union[int, float] = 1,
        weight_name: Optional[str] = None,
        allow_overlapping: bool = False,
    ) -> None:
        if side == 'left':
            keys = self.left_keys
            node = self.left_node
        elif side == 'right':
            keys = self.right_keys
            node = self.right_node
        else:
            msg = f"side must be 'left' or 'right', got {side!r}"
            raise ValueError(msg)

        if not isinstance(match_limit, (int, float)):
            msg = f'match_limit must be int or float, got {match_limit!r}'
            raise TypeError(msg)
        elif match_limit < 1:
            msg = f'match_limit must be 1 or greater, got {match_limit!r}'
            raise ValueError(msg)

        parameters: Iterable[Tuple]

        # Order by labels for itertools.groupby() later.
        self.cur.execute(f"""
            SELECT {side}_labels, run_id
            FROM temp.source_mapping
            ORDER BY {side}_labels
        """)

        run_ids_key_matches = self._find_matches_format_data(node, keys, self.cur)

        index_columns = node.index_columns()
        structure_set = set(node.structure())

        # Add exact matches and log information for other records.
        ambiguous_matches = []
        invalid_count = 0
        invalid_categories = set()
        unresolvable_count = 0
        overlimit_count = 0
        overlimit_max = 0
        for run_ids, key, matches in run_ids_key_matches:
            first_match = next(matches, tuple())  # Empty tuple if no matches.
            num_of_matches = (1 if first_match else 0) + sum(1 for _ in matches)

            # Add exact matches to given matches table.
            if num_of_matches == 1:
                index_id, *_ = first_match  # Unpack index record (discards labels).
                parameters = ((run_id, index_id) for run_id in run_ids)
                sql = f'INSERT INTO temp.{side}_matches (run_id, index_id) VALUES (?, ?)'
                self.cur.executemany(sql, parameters)
                continue

            # If no match, add to count.
            if num_of_matches == 0:
                unresolvable_count += 1
                continue

            # If match is ambiguous, check for invalid category structure.
            key_cols = key.keys()
            bitmask = tuple(int(col in key_cols) for col in index_columns)
            if bitmask not in structure_set:
                invalid_count += 1
                bad_category = (x for x, y in zip(index_columns, bitmask) if y)
                invalid_categories.add(tuple(bad_category))
                continue

            # If ambiguous match is under allowed limit, save for later.
            if num_of_matches <= match_limit:
                ambiguous_matches.append((run_ids, key, num_of_matches))
                continue

            # Else, we're over match_limit, add to count.
            overlimit_count += 1
            overlimit_max = max(overlimit_max, num_of_matches)

        # Sort matches from least to most ambiguous.
        ambiguous_matches = sorted(ambiguous_matches, key=lambda x: x[2])

        # Add ambiguous matches to given matches table.
        unweighted_count = 0
        if ambiguous_matches:
            for run_ids, where_dict, count in ambiguous_matches:
                # Get records (NOTE: accessing internal ``_dal`` directly).
                records = list(
                    node._dal.weight_records(weight_name, **where_dict)
                )

                # Optionally, filter to records that have not already been
                # matched at a finer-grained/less-ambiguous level.
                if not allow_overlapping:
                    index_ids = (f'({index_id})' for (index_id, _) in records)
                    sql = f"""
                        WITH ambiguous_match (index_id) AS (
                            VALUES {', '.join(index_ids)}
                        )
                        SELECT index_id FROM ambiguous_match
                        EXCEPT
                        SELECT index_id FROM temp.{side}_matches
                    """
                    self.cur.execute(sql)
                    no_overlap = [row[0] for row in self.cur]
                    records = [(x, y) for (x, y) in records if x in no_overlap]

                # If any record is missing a weight value, skip to next match.
                if any(weight is None for (_, weight) in records):
                    unweighted_count += 1
                    continue

                # Build bit list to encode mapping level.
                key_cols = where_dict.keys()
                mapping_level = BitFlags(*((col in key_cols) for col in index_columns))

                # Build iterator of parameters for executemany().
                parameters = product(run_ids, records)
                parameters = ((a, b, c) for (a, (b, c)) in parameters)
                parameters = ((a, b, c, mapping_level) for (a, b, c) in parameters)
                sql = f"""
                    INSERT INTO temp.{side}_matches
                        (run_id, index_id, weight_value, mapping_level)
                    VALUES (?, ?, ?, ?)
                """
                self.cur.executemany(sql, parameters)

        self._refresh_proportions(side)

        self._find_matches_warn(
            unresolvable_count=unresolvable_count,
            invalid_count=invalid_count,
            invalid_categories=invalid_categories,
            overlimit_count=overlimit_count,
            overlimit_max=overlimit_max,
            match_limit=match_limit,
            unweighted_count=unweighted_count,
        )

    def get_relations(
        self, side: Literal['left', 'right']
    ) -> Iterable[Tuple[int, int, float, Union[BitFlags, None]]]:
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
            WITH
                joint_probability AS (
                    SELECT
                        run_id,
                        src.index_id AS other_index_id,
                        dst.index_id AS index_id,
                        src.proportion * dst.proportion AS proportion,
                        dst.mapping_level AS mapping_level
                    FROM temp.{other_side}_matches src
                    JOIN temp.{side}_matches dst USING (run_id)
                )
            SELECT
                other_index_id,
                index_id,
                SUM(weight * proportion) AS relation_value,
                mapping_level
            FROM temp.source_mapping
            JOIN joint_probability USING (run_id)
            GROUP BY other_index_id, index_id, mapping_level
            ORDER BY other_index_id, index_id, mapping_level
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
    match_limit: Union[int, float] = 1,
    weight_name: Optional[str] = None,
    allow_overlapping: bool = False,
) -> None:
    mapper = Mapper(data, name)
    try:
        mapper.find_matches(left_node, 'left', match_limit, weight_name, allow_overlapping)
        mapper.find_matches(right_node, 'right', match_limit, weight_name, allow_overlapping)

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
