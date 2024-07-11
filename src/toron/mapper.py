"""Tools for building weighted crosswalks between sets of labels."""

import sqlite3
from json import (
    dumps,
)

from ._typing import (
    Dict,
    Iterable,
    Optional,
    Sequence,
    Union,
)

from ._utils import (
    normalize_tabular,
    parse_edge_shorthand,
)


class Mapper(object):
    """Class to build a weighted crosswalk between sets of labels.

    This class create a temporary database--when an instance is garbage
    collected, its database is deleted. It uses the following schema:

    .. code-block:: text

        +---------------+    +---------------+    +---------------+
        | left_matches  |    | mapping_data  |    | right_matches |
        +---------------+    +---------------+    +---------------+
        | run_id        |<---| run_id        |--->| run_id        |
        | index_id      |    | left_labels   |    | index_id      |
        | weight_value  |    | right_labels  |    | weight_value  |
        | mapping_level |    | mapping_value |    | mapping_level |
        | proportion    |    +---------------+    | proportion    |
        +---------------+                         +---------------+
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
                left_labels TEXT NOT NULL,
                right_labels TEXT NOT NULL,
                mapping_value REAL NOT NULL
            );
            CREATE TABLE left_matches(
                run_id INTEGER NOT NULL REFERENCES mapping_data(run_id),
                index_id INTEGER,
                weight_value REAL CHECK (0.0 <= weight_value),
                mapping_level BLOB_BITFLAGS,
                proportion REAL CHECK (0.0 <= proportion AND proportion <= 1.0)
            );
            CREATE TABLE right_matches(
                run_id INTEGER NOT NULL REFERENCES mapping_data(run_id),
                index_id INTEGER,
                weight_value REAL CHECK (0.0 <= weight_value),
                mapping_level BLOB_BITFLAGS,
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

        self.left_keys = columns[:value_pos]
        self.right_keys = columns[value_pos+1:]

        for row in data:
            if not row:
                continue  # If row is empty, skip to next.

            sql = """
                INSERT INTO mapping_data
                  (left_labels, right_labels, mapping_value)
                  VALUES (:left_labels, :right_labels, :mapping_value)
            """
            parameters = {
                'left_labels': dumps(row[:value_pos]),
                'right_labels': dumps(row[value_pos+1:]),
                'mapping_value': row[value_pos],
            }
            try:
                self.cur.execute(sql, parameters)
            except sqlite3.IntegrityError as err:
                msg = f'{err}\nfailed to insert:\n  {tuple(parameters.values())}'
                raise sqlite3.IntegrityError(msg) from err

    def close(self) -> None:
        """Close internal connection to temporary database."""
        try:
            self.cur.close()  # Fails if Connection is not open.
        except sqlite3.ProgrammingError:
            pass

        self.con.close()

    def __del__(self) -> None:
        self.close()
