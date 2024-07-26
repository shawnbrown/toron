"""Tools for building weighted crosswalks between sets of labels."""

import sqlite3
from contextlib import (
    closing,
)
from json import (
    dumps,
    loads,
)
from itertools import (
    compress,
)
from ._typing import (
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    TYPE_CHECKING,
)

from ._utils import (
    normalize_tabular,
    parse_edge_shorthand,
    BitFlags,
)

if TYPE_CHECKING:
    from .data_models import Structure
    from .node import Node


class Mapper(object):
    """Class to build a weighted crosswalk between sets of labels.

    This class create a temporary database--when an instance is garbage
    collected, its database is deleted. It uses the following schema:

    .. code-block:: text

        +---------------+    +----------------+    +---------------+
        | left_matches  |    | mapping_data   |    | right_matches |
        +---------------+    +----------------+    +---------------+
        | run_id        |<---| run_id         |--->| run_id        |
        | index_id      |    | left_location  |    | index_id      |
        | weight_value  |    | left_level     |    | weight_value  |
        | mapping_level |    | right_location |    | mapping_level |
        | proportion    |    | right_level    |    | proportion    |
        +---------------+    | mapping_value  |    +---------------+
                             +----------------+
    """
    def __init__(
        self,
        crosswalk_name: str,
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        self.con = sqlite3.connect('')  # Empty string creates temp file.
        self.cur = self.con.executescript("""
            CREATE TABLE mapping_data(
                run_id INTEGER PRIMARY KEY,
                left_location TEXT NOT NULL,
                left_level BLOB_BITFLAGS NOT NULL,
                right_location TEXT NOT NULL,
                right_level BLOB_BITFLAGS NOT NULL,
                mapping_value REAL NOT NULL
            );
            CREATE TABLE left_matches(
                run_id INTEGER NOT NULL REFERENCES mapping_data(run_id),
                index_id INTEGER,
                weight_value REAL CHECK (0.0 <= weight_value),
                mapping_level BLOB_BITFLAGS NOT NULL,
                proportion REAL CHECK (0.0 <= proportion AND proportion <= 1.0)
            );
            CREATE TABLE right_matches(
                run_id INTEGER NOT NULL REFERENCES mapping_data(run_id),
                index_id INTEGER,
                weight_value REAL CHECK (0.0 <= weight_value),
                mapping_level BLOB_BITFLAGS NOT NULL,
                proportion REAL CHECK (0.0 <= proportion AND proportion <= 1.0)
            );
        """)

        data, columns = normalize_tabular(data, columns)

        for i, col in enumerate(columns):
            if (
                crosswalk_name == col or
                crosswalk_name == parse_edge_shorthand(col).get('edge_name')
            ):
                value_pos = i  # Get index position of value column
                break
        else:  # no break
            msg = f'{crosswalk_name!r} is not in data, got header: {columns!r}'
            raise ValueError(msg)

        self.left_columns = columns[:value_pos]
        self.right_columns = columns[value_pos+1:]

        for row in data:
            if not row:
                continue  # If row is empty, skip to next.

            sql = """
                INSERT INTO mapping_data
                  (left_location, left_level, right_location, right_level, mapping_value)
                  VALUES (:left_location, :left_level, :right_location, :right_level, :mapping_value)
            """
            parameters = {
                'left_location': dumps(row[:value_pos]),
                'left_level': bytes(BitFlags(x != '' for x in row[:value_pos])),
                'right_location': dumps(row[value_pos+1:]),
                'right_level': bytes(BitFlags(x != '' for x in row[value_pos+1:])),
                'mapping_value': row[value_pos],
            }
            try:
                self.cur.execute(sql, parameters)
            except sqlite3.IntegrityError as err:
                msg = f'{err}\nfailed to insert:\n  {tuple(parameters.values())}'
                raise sqlite3.IntegrityError(msg) from err

    @staticmethod
    def _get_level_pairs(
        left_or_right_columns: Sequence[str],
        left_or_right_levels: Sequence[bytes],
        node_columns: Sequence[str],
        node_structures: Sequence['Structure'],
    ) -> List[Tuple[bytes, Optional[bytes]]]:
        """Return a list of level pairs--tuples containing two levels,
        ``input_bytes`` and ``node_bytes``. Level pairs are sorted in
        descending order of granularity (as defined in the node
        structures). If an input level has no corresponding node level,
        then the ``node_bytes`` will be ``None``.

        .. code-block:: python

            >>> mapper = Mapper(...)
            >>> level_pairs = mapper._get_level_pairs(
            ...     left_columns,
            ...     left_levels,
            ...     node_columns,
            ...     node_structures,
            ... )
            >>> level_pairs
            [(b'\xe0', b'\xe0'),
             (b'\xc0', b'\xc0'),
             (b'\x80', b'\x80'),
             (b'\x60', None),
             (b'\x20', None)]
        """
        # Build dictionary with bytes (keys) and granularity (values).
        make_item = lambda x: (bytes(BitFlags(x.bits)), x.granularity)
        granularity_items = (make_item(x) for x in node_structures)
        granularity_dict = {k: v for k, v in granularity_items if k != b''}

        # Build list of `(input_bytes, node_bytes)` items.
        levels = []
        for input_bytes in left_or_right_levels:
            input_bits = BitFlags(input_bytes)
            mapped_columns = tuple(compress(left_or_right_columns, input_bits))
            node_bits = BitFlags((x in mapped_columns) for x in node_columns)
            node_bytes: Optional[bytes] = bytes(node_bits)
            if node_bytes not in granularity_dict:
                node_bytes = None
            levels.append((input_bytes, node_bytes))

        # Sort levels from highest to lowest granularity.
        sort_key = lambda x: granularity_dict.get(x[1], -1)
        levels = sorted(levels, key=sort_key, reverse=True)

        return levels

    def match_records(
        self,
        node: 'Node',
        side: Literal['left', 'right'],
    ) -> None:
        """Match mapping rows to node index records."""
        if side == 'left':
            match_table = 'left_matches'
            match_columns = self.left_columns
            location_column = 'left_location'
            level_column = 'left_level'
        elif side == 'right':
            match_table = 'right_matches'
            match_columns = self.right_columns
            location_column = 'right_location'
            level_column = 'right_level'
        else:
            msg = f"side must be 'left' or 'right', got {side!r}"
            raise ValueError(msg)

        with node._managed_cursor() as node_cur, \
                closing(self.con.cursor()) as cur1, \
                closing(self.con.cursor()) as cur2:

            index_repo = node._dal.IndexRepository(node_cur)
            property_repo = node._dal.IndexRepository(node_cur)

            cur1.execute(f'SELECT DISTINCT {level_column} FROM mapping_data')
            all_match_levels = [x[0] for x in cur1]

            # Get level pairs in order of decreasing granularity.
            ordered_level_pairs = self._get_level_pairs(
                left_or_right_columns=match_columns,
                left_or_right_levels=all_match_levels,
                node_columns=node._dal.ColumnManager(node_cur).get_columns(),
                node_structures=node._dal.StructureRepository(node_cur).get_all(),
            )

            # Loop over levels from highest to lowest granularity.
            for match_bytes, node_bytes in ordered_level_pairs:
                if node_bytes is None:
                    continue  # Skip if no matching level in node.

                sql = f"""
                    SELECT run_id, {location_column}
                    FROM mapping_data
                    WHERE {level_column}=?
                """
                cur1.execute(sql, (match_bytes,))

                # Loop over mapping rows for current granularity level.
                for row in cur1:
                    run_id, location_labels = row
                    zipped = zip(match_columns, loads(location_labels))
                    criteria = {k: v for k, v in zipped if v != ''}

                    # Loop over index records that match current mapping row.
                    for index in index_repo.find_by_label(criteria):
                        weight_value = 100
                        sql = f"""
                            INSERT INTO {match_table}
                                (run_id, index_id, weight_value, mapping_level)
                            VALUES
                                (?, ?, ?, ?)
                        """
                        parameters = (run_id, index.id, weight_value, node_bytes)
                        cur2.execute(sql, parameters)

    def close(self) -> None:
        """Close internal connection to temporary database."""
        try:
            self.cur.close()  # Fails if Connection is not open.
        except sqlite3.ProgrammingError:
            pass

        self.con.close()

    def __del__(self) -> None:
        self.close()
