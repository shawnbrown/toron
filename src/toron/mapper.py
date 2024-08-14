"""Tools for building weighted crosswalks between sets of labels."""

import logging
import sqlite3
from collections import Counter
from contextlib import (
    closing,
)
from json import (
    dumps,
    loads,
)
from itertools import (
    compress,
    islice,
)
from ._typing import (
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    TYPE_CHECKING,
)

from .data_service import (
    get_default_weight_group,
)
from ._utils import (
    eagerly_initialize,
    normalize_tabular,
    parse_edge_shorthand,
    BitFlags,
)

if TYPE_CHECKING:
    from .data_models import Structure
    from .node import Node


logger = logging.getLogger(__name__)


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

        with closing(self.con.cursor()) as cur:
            cur = self.con.executescript("""
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
                    cur.execute(sql, parameters)
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

    @staticmethod
    def _refresh_proportions(
        cursor: sqlite3.Cursor, side: Literal['left', 'right']
    ) -> None:
        """Update 'proportion' values in left or right matches table."""
        cursor.execute(f"""
            WITH
                AggregatedValues AS (
                    SELECT
                        run_id            AS run_id_aggregated,
                        SUM(weight_value) AS weight_value_sum,
                        COUNT(*)          AS weight_value_count
                    FROM {side}_matches
                    GROUP BY run_id
                )
            UPDATE {side}_matches
            SET proportion=COALESCE(
                (CAST(weight_value AS REAL) / (SELECT weight_value_sum
                                               FROM AggregatedValues
                                               WHERE run_id=run_id_aggregated)),
                (1.0 / (SELECT weight_value_count
                        FROM AggregatedValues
                        WHERE run_id=run_id_aggregated))
            )
        """)

    def match_records(
        self,
        node: 'Node',
        side: Literal['left', 'right'],
        match_limit: int = 1,
        allow_overlapping: bool = False,
    ) -> None:
        """Match mapping rows to node index records.

        Use *allow_overlapping* to specify exclusive or inclusive record
        linkage. When it's ``False``, records that have already been
        matched are excluded from later, more ambiguous mappings. When
        it's ``True``, records that have already been matched can still
        be matched again later by increasingly ambiguous mappings.
        """
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

        invalid_categories = []
        counter: Counter = Counter()
        with node._managed_cursor() as node_cur, \
                closing(self.con.cursor()) as cur1, \
                closing(self.con.cursor()) as cur2:

            index_repo = node._dal.IndexRepository(node_cur)
            weight_repo = node._dal.WeightRepository(node_cur)

            cur1.execute(f'SELECT DISTINCT {level_column} FROM mapping_data')
            all_match_levels = [x[0] for x in cur1]

            weight_group = get_default_weight_group(
                property_repo=node._dal.PropertyRepository(node_cur),
                weight_group_repo=node._dal.WeightGroupRepository(node_cur),
                required=True,
            )

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
                    # If no matching level in node, category is invalid.
                    invalid_categories.append(
                        tuple(compress(match_columns, BitFlags(match_bytes)))
                    )
                    cur1.execute(
                        f'SELECT COUNT(*) FROM mapping_data WHERE {level_column}=?',
                        (match_bytes,),
                    )
                    counter['invalid_rows'] += cur1.fetchone()[0]
                    continue  # Skip to next level in ordered pairs.

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
                    all_matches = index_repo.find_by_label(criteria)
                    matches = list(islice(all_matches, match_limit + 1))
                    len_matches = len(matches)
                    if len_matches > match_limit:
                        counter['overlimit_max'] = max(
                            counter['overlimit_max'],
                            len_matches + sum(1 for _ in all_matches),
                        )
                        counter['count_overlimit'] += 1
                        continue  # Skip to next row in mapping.

                    # If match is ambiguous, check for records that overlap
                    # with records that have already been matched at a finer
                    # level of granularity.
                    if len_matches > 1:
                        sql = f'SELECT EXISTS (SELECT 1 FROM {match_table} WHERE index_id=?)'
                        is_overlap = lambda x: cur2.execute(sql, (x.id,),).fetchone()[0]

                        if allow_overlapping:
                            counter['overlaps_included'] += \
                                sum(is_overlap(x) for x in matches)
                        else:
                            # When not allowed, filter to non-overlapping only.
                            matches = [x for x in matches if is_overlap(x) == 0]
                            counter['overlaps_excluded'] += len_matches - len(matches)

                    # Build tuple of `(index_id, weight_value)` for all matches.
                    index_id_and_weight_value = []
                    for index in matches:
                        weight = weight_repo.get_by_weight_group_id_and_index_id(
                            weight_group.id, index.id
                        )
                        index_id_and_weight_value.append(
                            (index.id, getattr(weight, 'value', None))
                        )

                    # If match is ambiguous and any weight is missing, skip it.
                    if len(index_id_and_weight_value) > 1 and \
                            any(x is None for _, x in index_id_and_weight_value):
                        counter['count_unweighted'] += 1
                        continue  # Skip to next row in mapping.

                    # Insert matches into appropriate table.
                    for index_id, weight_value in index_id_and_weight_value:
                        sql = f"""
                            INSERT INTO {match_table}
                                (run_id, index_id, weight_value, mapping_level)
                            VALUES
                                (?, ?, ?, ?)
                        """
                        parameters = (run_id, index_id, weight_value, node_bytes)
                        cur2.execute(sql, parameters)

            self._refresh_proportions(cur1, side)

        if counter['overlaps_included']:
            logger.info(
                f"included {counter['overlaps_included']} ambiguous matches "
                f"that overlap with records that were also matched at a "
                f"finer level of granularity"
            )
        elif counter['overlaps_excluded']:
            logger.warning(
                f"omitted {counter['overlaps_excluded']} ambiguous matches "
                f"that overlap with records that were already matched at a "
                f"finer level of granularity"
            )

        if counter['count_overlimit']:
            logger.warning(
                f"skipped {counter['count_overlimit']} values that matched too many records"
            )
            logger.warning(
                f"current match_limit is {match_limit} but data includes values "
                f"that match up to {counter['overlimit_max']} records"
            )

        if counter['count_unweighted']:
            logger.warning(
                f"skipped {counter['count_unweighted']} values that ambiguously "
                f"matched to one or more records that have no associated weight"
            )

        if counter['invalid_rows']:
            category_list = [', '.join(c) for c in sorted(invalid_categories)]
            category_string = '\n  '.join(category_list)
            logger.warning(
                f"skipped {counter['invalid_rows']} values that used invalid "
                f"categories:\n  {category_string}"
            )

    @eagerly_initialize
    def get_relations(
        self, direction: Literal['<-', '->']
    ) -> Generator[Tuple[int, int, float, Union[BitFlags, None]], None, None]:
        """Returns an iterator of relations for the direction given.
        The *direction* can be ``'->'`` (left-to-right) or ``'<-'``
        (right-to-left):

        .. code-block:: python

            >>> relations = mapper.get_relations('->')
        """
        if direction == '<-':
            side = 'left'
            other_side = 'right'
        elif direction == '->':
            side = 'right'
            other_side = 'left'
        else:
            msg = f"direction must be '<-' or '->', got {direction!r}"
            raise ValueError(msg)

        with closing(self.con.cursor()) as cur:
            cur.execute(f"""
                WITH
                    joint_probability AS (
                        SELECT
                            run_id,
                            src.index_id AS other_index_id,
                            dst.index_id AS index_id,
                            src.proportion * dst.proportion AS proportion,
                            dst.mapping_level AS mapping_level
                        FROM {other_side}_matches src
                        JOIN {side}_matches dst USING (run_id)
                    )
                SELECT
                    other_index_id,
                    index_id,
                    mapping_level,
                    SUM(mapping_value * proportion) AS relation_value
                FROM mapping_data
                JOIN joint_probability USING (run_id)
                GROUP BY other_index_id, index_id, mapping_level
                ORDER BY other_index_id, index_id, mapping_level
            """)
            for row in cur:
                yield row

    def close(self) -> None:
        """Close internal connection to temporary database."""
        self.con.close()

    def __del__(self) -> None:
        self.close()
