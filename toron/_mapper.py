"""Tools for building correspondence mappings between label sets."""

import sqlite3
from itertools import (
    groupby,
    product,
)
from json import (
    dumps as _dumps,
    loads as _loads,
)
from ._typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    TYPE_CHECKING,
)

from ._schema import BitFlags
from ._utils import (
    TabularData,
    ToronWarning,
    make_readerlike,
    parse_edge_shorthand,
)

if TYPE_CHECKING:
    from ._dal import DataAccessLayer
    from .node import Node


def _get_dal(
    dal_or_node: Union['DataAccessLayer', 'Node']
) -> 'DataAccessLayer':
    """Helper function to return DataAccessLayer."""
    if hasattr(dal_or_node, '_dal'):
        return dal_or_node._dal
    return dal_or_node


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

        for i, x in enumerate(fieldnames):
            if name == x or name == parse_edge_shorthand(x).get('edge_name'):
                weight_pos = i  # Get index position of weight column
                break
        else:  # no break
            msg = f'{name!r} is not in data, got header: {fieldnames!r}'
            raise ValueError(msg)

        self.left_keys = fieldnames[:weight_pos]
        self.right_keys = fieldnames[weight_pos+1:]

        for row in iterator:
            sql = """
                INSERT INTO temp.source_mapping
                  (left_labels, right_labels, weight)
                  VALUES (:left_labels, :right_labels, :weight)
            """
            parameters = {
                'left_labels': _dumps(row[:weight_pos]),
                'right_labels': _dumps(row[weight_pos+1:]),
                'weight': row[weight_pos],
            }
            try:
                self.cur.execute(sql, parameters)
            except sqlite3.IntegrityError as err:
                msg = f'{err}\nfailed to insert:\n  {tuple(parameters.values())}'
                raise sqlite3.IntegrityError(msg) from err

    @staticmethod
    def _find_matches_format_data(
        dal_or_node: Union['DataAccessLayer', 'Node'],
        column_names: Sequence[str],
        iterable: Iterable[Tuple[str, int]],
    ) -> Iterator[Tuple[List[int], Dict[str, str], Iterator[Tuple]]]:
        """Takes a *node*, a sequence of label *keys*, and an *iterable*
        containing ``(label_values, run_id)`` records. Returns an
        iterator of ``(run_ids, where_dict, matches)`` records.

        .. tip::
            If *iterable* records are sorted by label values, this
            operation is more efficient--internally it executes one
            query per group as generated by the itertools.groupby()
            function. Unordered records will result in some queries
            being repeated which makes the operation less efficient.

        .. code-block::

            >>> node = Node(...)
            >>> column_names = ['col1', 'col2']
            >>> iterable = [
            ...     ('["A", "x"]', 101),
            ...     ('["A", "y"]', 102),
            ...     ('["B", "x"]', 103),
            ...     ('["B", "y"]', 104),
            ...     ('["C", "x"]', 105),
            ...     ('["C", "y"]', 106),
            ... ]
            >>> formatted = dal._find_matches_format_data(node, column_names, iterable)
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
        # Group rows using `label_values` as the key.
        def get_label_values(row):
            label_values, _ = row  # Discards `run_id` value.
            return label_values

        grouped = groupby(iterable, key=get_label_values)

        # Helper function to format records as where_dicts.
        def get_where_dict(x):
            return dict((k, v) for k, v in zip(column_names, _loads(x)) if v)

        # Helper function to format groups as lists of `run_id` values.
        def get_run_ids(group):
            return [run_id for _, run_id in group]  # Discards `label_values` key.

        items = ((get_where_dict(k), get_run_ids(g)) for k, g in grouped)

        # Unzip items into separate where_dict and run_id containers.
        try:
            where_dicts, grouped_run_ids = zip(*items)
        except ValueError:  # If no items to unpack, assign empty tuples.
            where_dicts, grouped_run_ids = (), ()

        # Get node matches.
        dal = _get_dal(dal_or_node)
        grouped_matches = dal.index_records_grouped(where_dicts)

        # Reformat records for output.
        zipped = zip(grouped_run_ids, grouped_matches)
        run_ids_where_dict_matches = ((x, y, z) for (x, (y, z)) in zipped)

        return run_ids_where_dict_matches

    @staticmethod
    def _match_exact_or_get_info(
        cursor: sqlite3.Cursor,
        side: Literal['left', 'right'],
        index_columns: Sequence[str],
        structure_set: Set[Tuple[Literal[0, 1], ...]],
        run_ids: List[int],
        key: Dict[str, str],
        matches: Iterator[Tuple],
        match_limit: Union[int, float] = 1,
    ) -> Dict[str, Any]:
        """Add exact match or return match info."""
        first_match = next(matches, tuple())  # Empty tuple if no matches.
        num_of_matches = (1 if first_match else 0) + sum(1 for _ in matches)

        info_dict: Dict[str, Any] = {}

        if num_of_matches == 1:
            # Insert the record, leave info_dict empty (found exact match).
            index_id, *_ = first_match  # Unpack index record (discards labels).
            parameters = ((run_id, index_id) for run_id in run_ids)
            sql = f'INSERT INTO temp.{side}_matches (run_id, index_id) VALUES (?, ?)'
            cursor.executemany(sql, parameters)
        elif num_of_matches == 0:
            # Log count to info_dict (no matches found).
            info_dict['count_unmatchable'] = 1
        else:
            # Check for allowed category structure (match is ambiguous).
            where_cols = key.keys()
            bitmask = tuple(int(col in where_cols) for col in index_columns)
            if bitmask not in structure_set:
                # Log count and invalid category (bitmask not allowed).
                info_dict['count_invalid'] = 1
                bad_category = (x for x, y in zip(index_columns, bitmask) if y)
                info_dict['invalid_categories'] = {tuple(bad_category)}
            elif num_of_matches <= match_limit:
                # Log matches to info_dict for later (ambiguous but within limit).
                info_dict['list_ambiguous'] = [(run_ids, key, num_of_matches)]
            else:
                # Log counts to info_dict (ambiguous, too many matches).
                info_dict['count_overlimit'] = 1
                info_dict['num_of_matches'] = num_of_matches

        return info_dict

    @staticmethod
    def _match_ambiguous_or_get_info(
        dal_or_node: Union['DataAccessLayer', 'Node'],
        cursor: sqlite3.Cursor,
        side: Literal['left', 'right'],
        run_ids: List[int],
        where_dict: Dict[str, str],
        index_columns: Sequence[str],
        weight_name: Optional[str] = None,
        allow_overlapping: bool = False,
    ) -> Dict[str, Any]:
        """Add ambiguous match or return match info."""
        info_dict: Dict[str, Any] = {}

        # Get records.
        dal = _get_dal(dal_or_node)
        records = list(dal.weight_records(weight_name, **where_dict))

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
            cursor.execute(sql)
            no_overlap = [row[0] for row in cursor]
            records = [(x, y) for (x, y) in records if x in no_overlap]

        if any(weight is None for (_, weight) in records):
            # If any record is missing a weight value, log it in the
            # info_dict but don't insert any records.
            info_dict['count_unweighted'] = 1
        else:
            # Build bit list to encode mapping level.
            key_cols = where_dict.keys()
            mapping_level = BitFlags(*((col in key_cols) for col in index_columns))

            # Build iterator of parameters for executemany().
            parameters: Iterable[Tuple]
            parameters = product(run_ids, records)
            parameters = ((a, b, c) for (a, (b, c)) in parameters)
            parameters = ((a, b, c, mapping_level) for (a, b, c) in parameters)
            sql = f"""
                INSERT INTO temp.{side}_matches
                    (run_id, index_id, weight_value, mapping_level)
                VALUES (?, ?, ?, ?)
            """
            cursor.executemany(sql, parameters)

        return info_dict

    @staticmethod
    def _refresh_proportions(
        cursor: sqlite3.Cursor, side: Literal['left', 'right']
    ) -> None:
        """Update 'proportion' values in left or right matches table."""
        cursor.execute(f"""
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
        """)

    @staticmethod
    def _warn_match_stats(
        *,
        count_unmatchable: int = 0,
        count_overlimit: int = 0,
        overlimit_max: int = 0,
        match_limit: Union[int, float] = 1,
        count_invalid: int = 0,
        invalid_categories: Set[Tuple] = set(),
        count_unweighted: int = 0,
    ) -> None:
        """If needed, emit ToronWarning with relevant information."""
        messages = []

        if count_unmatchable:
            messages.append(
                f'skipped {count_unmatchable} values that matched no records'
            )

        if count_overlimit:
            messages.append(
                f'skipped {count_overlimit} values that matched too many records'
            )
            messages.append(
                f'current match_limit is {match_limit} but data includes values '
                f'that match up to {overlimit_max} records'
            )

        if count_invalid:
            category_list = [', '.join(c) for c in sorted(invalid_categories)]
            category_string = '\n  '.join(category_list)
            messages.append(
                f'skipped {count_invalid} values that used invalid categories:\n'
                f'  {category_string}\n'
            )

        if count_unweighted:
            messages.append(
                f'skipped {count_unweighted} values that ambiguously matched '
                f'to one or more records that have no associated weight'
            )

        if messages:
            import warnings
            msg = ', '.join(messages)
            warnings.warn(msg, category=ToronWarning, stacklevel=3)

    def find_matches(
        self,
        dal_or_node: Union['DataAccessLayer', 'Node'],
        side: Literal['left', 'right'],
        match_limit: Union[int, float] = 1,
        weight_name: Optional[str] = None,
        allow_overlapping: bool = False,
    ) -> None:
        dal = _get_dal(dal_or_node)

        if side == 'left':
            column_names = self.left_keys
        elif side == 'right':
            column_names = self.right_keys
        else:
            msg = f"side must be 'left' or 'right', got {side!r}"
            raise ValueError(msg)

        if not isinstance(match_limit, (int, float)):
            msg = f'match_limit must be int or float, got {match_limit!r}'
            raise TypeError(msg)
        elif match_limit < 1:
            msg = f'match_limit must be 1 or greater, got {match_limit!r}'
            raise ValueError(msg)

        # Use "ORDER BY" to sort labels for _find_matches_format_data().
        self.cur.execute(f"""
            SELECT {side}_labels, run_id
            FROM temp.source_mapping
            ORDER BY {side}_labels
        """)

        run_ids_key_matches = self._find_matches_format_data(
            dal_or_node=dal,
            column_names=column_names,
            iterable=self.cur,
        )

        index_columns = dal.index_columns()
        structure_set: Set[Tuple[Literal[0, 1], ...]] = set(dal.structure())

        list_ambiguous = []
        match_stats: Dict[str, Any] = {
            'count_unmatchable': 0,
            'count_unweighted': 0,
            'count_invalid': 0,
            'invalid_categories': set(),
            'count_overlimit': 0,
            'overlimit_max': 0,
        }

        for run_ids, key, matches in run_ids_key_matches:
            info_dict = self._match_exact_or_get_info(
                cursor=self.cur,
                side=side,
                index_columns=index_columns,
                structure_set=structure_set,
                run_ids=run_ids,
                key=key,
                matches=matches,
                match_limit=match_limit,
            )

            if not info_dict:
                continue

            list_ambiguous.extend(info_dict.get('list_ambiguous', []))

            count_invalid = info_dict.get('count_invalid', 0)
            match_stats['count_invalid'] += count_invalid

            invalid_categories = info_dict.get('invalid_categories', set())
            match_stats['invalid_categories'].update(invalid_categories)

            count_unmatchable = info_dict.get('count_unmatchable', 0)
            match_stats['count_unmatchable'] += count_unmatchable

            count_overlimit = info_dict.get('count_overlimit', 0)
            match_stats['count_overlimit'] += count_overlimit

            overlimit_max = match_stats['overlimit_max']
            num_of_matches = info_dict.get('num_of_matches', 0)
            match_stats['overlimit_max'] = max(overlimit_max, num_of_matches)

        if list_ambiguous:
            # Sort matches by count (from least to most ambiguous).
            list_ambiguous = sorted(list_ambiguous, key=lambda x: x[2])

            # Handle ambiguous matches.
            for run_ids, where_dict, _ in list_ambiguous:
                info_dict = Mapper._match_ambiguous_or_get_info(
                    dal_or_node=dal,
                    cursor=self.cur,
                    side=side,
                    run_ids=run_ids,
                    where_dict=where_dict,
                    index_columns=index_columns,
                    weight_name=weight_name,
                    allow_overlapping=allow_overlapping,
                )

                count_unweighted = info_dict.get('count_unweighted', 0)
                match_stats['count_unweighted'] += count_unweighted

        self._refresh_proportions(self.cur, side)

        self._warn_match_stats(**match_stats)

    def assign_matches_by_id(self, side: Literal['left', 'right']) -> None:
        """Assign matches from "other_index_id" or "index_id" values."""
        if side == 'left':
            column_names = self.left_keys
        elif side == 'right':
            column_names = self.right_keys
        else:
            msg = f"side must be 'left' or 'right', got {side!r}"
            raise ValueError(msg)

        # Verify that first column is 'other_index_id' or 'index_id.
        if column_names[0] not in ('other_index_id', 'index_id'):
            msg = f"expected 'other_index_id' or 'index_id', got {column_names[0]!r}"
            raise Exception(msg)

        # Prepare parameters for INSERT statement.
        self.cur.execute(f"""
            SELECT run_id, {side}_labels, weight
            FROM temp.source_mapping
        """)
        func = lambda row: {'run_id': row[0],
                            'index_id': _loads(row[1])[0],
                            'weight_value': row[2]}
        parameters = [func(row) for row in self.cur]

        # Build and execute INSERT statement.
        sql = f"""
            INSERT INTO temp.{side}_matches
                (run_id, index_id, weight_value, proportion, mapping_level)
            VALUES
                (:run_id, :index_id, :weight_value, 1.0, NULL)
        """
        self.cur.executemany(sql, parameters)

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
