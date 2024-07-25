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

    def close(self) -> None:
        """Close internal connection to temporary database."""
        try:
            self.cur.close()  # Fails if Connection is not open.
        except sqlite3.ProgrammingError:
            pass

        self.con.close()

    def __del__(self) -> None:
        self.close()
