"""Tools for building weighted crosswalks between sets of labels."""

import sqlite3
from json import (
    dumps,
)
from itertools import (
    compress,
)
from ._typing import (
    Dict,
    Iterable,
    List,
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
        | proportion    |    | right_location |    | proportion    |
        +---------------+    | right_level    |    +---------------+
                             | mapping_value  |
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
                proportion REAL CHECK (0.0 <= proportion AND proportion <= 1.0)
            );
            CREATE TABLE right_matches(
                run_id INTEGER NOT NULL REFERENCES mapping_data(run_id),
                index_id INTEGER,
                weight_value REAL CHECK (0.0 <= weight_value),
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
    def _parse_mapping_levels(
        left_or_right_columns: Sequence[str],
        left_or_right_levels: Sequence[bytes],
        node_columns: Sequence[str],
        node_structures: Sequence['Structure'],
    ) -> Tuple[List[Tuple[bytes, Tuple[str, ...], BitFlags]],
               List[Tuple[bytes, Tuple[str, ...], BitFlags]]]:
        """Return a two lists (a tuple) of mapping level information.
        The first list contains valid records (in descending order of
        granularity) and the second list contains invalid records.

        .. code-block:: python

            >>> results = Mapper._parse_mapping_levels(
            ...     left_columns,
            ...     left_levels,
            ...     node_columns,
            ...     node_structures,
            ... )
            >>>valid_levels, invalid_levels = results
            >>> valid_levels
            [(b'\xe0', ('A', 'B', 'C'), BitFlags(1, 1, 1)),
             (b'\xc0', ('A', 'B'), BitFlags(1, 1, 0)),
             (b'\x80', ('A',), BitFlags(1, 0, 0))]
            >>> invalid_levels
            [(b'\x60', ('B', 'C'), BitFlags(0, 1, 1)),
             (b'\x20', ('C',), BitFlags(0, 0, 1))]
        """
        # Make a list of level-info tuples.
        def make_info(bytes_flag):
            mapping_columns = tuple(compress(left_or_right_columns, BitFlags(bytes_flag)))
            mapping_level = BitFlags((x in mapping_columns) for x in node_columns)
            return (bytes_flag, mapping_columns, mapping_level)
        level_info = [make_info(x) for x in left_or_right_levels]

        # Make dict with bit-flags keys and granularity values.
        all_valid_levels = {BitFlags(x.bits): x.granularity for x in node_structures}

        # Get valid levels and sort by greatest level of granularity.
        valid_level_info = [x for x in level_info if x[2] in all_valid_levels]
        def sort_key(x):
            val = all_valid_levels.get(x[2])
            return val if val is not None else -1.0
        valid_level_info = sorted(valid_level_info, key=sort_key, reverse=True)

        # Get invalid levels in given order.
        invalid_level_info = [x for x in level_info if x[2] not in all_valid_levels]

        return (valid_level_info, invalid_level_info)

    def close(self) -> None:
        """Close internal connection to temporary database."""
        try:
            self.cur.close()  # Fails if Connection is not open.
        except sqlite3.ProgrammingError:
            pass

        self.con.close()

    def __del__(self) -> None:
        self.close()
