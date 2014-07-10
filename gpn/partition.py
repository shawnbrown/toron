# -*- coding: utf-8 -*-
import os
import re
import sqlite3
import tempfile

from decimal import Decimal

# For URI Filename handling.
try:
    from urllib.request import pathname2url  # New package structure in 3.0.
    from urllib.parse import urlencode
except ImportError:
    from urllib import pathname2url
    from urllib import urlencode

#
# Internal Partition structure:
#
#                           +----------------+     +=================+
#     +================+    | cell_label     |     | hierarchy       |
#     | cell           |    +----------------+     +=================+
#     +================+    | cell_label_id  |     | hierarchy_id    |--+
#  +--| cell_id        |--->| cell_id        |     | hierarchy_value |  |
#  |  | partial        |    | hierarchy_id   |<-+  | hierarchy_level |  |
#  |  +----------------+    | label_id       |<-+  +-----------------+  |
#  |                        +----------------+  |                       |
#  |   +----------------+                       |  +-----------------+  |
#  |   | property       |  +----------------+   |  | label           |  |
#  |   +----------------+  | partition      |   |  +-----------------+  |
#  |   | property_id    |  +----------------+   +--| label_id        |  |
#  |   | property_key   |  | partition_id   |   +--| hierarchy_id    |<-+
#  |   | property_value |  | partition_hash |      | label_value     |
#  |   | created_date   |  | created_date   |      +-----------------+
#  |   +----------------+  +----------------+
#  |                                      +----------------+
#  |          +======================+    | edge_weight    |
#  |          | edge                 |    +----------------+
#  |          +======================+    | edge_weight_id |--+
#  |       +--| edge_id              |--->| edge_id        |  |
#  |       |  | other_partition_hash |    | weight_type    |  |
#  |       |  | other_partition_file |    | weight_note    |  |
#  |       |  +----------------------+    | proportional   |  |
#  |       |                              +----------------+  |
#  |       |                                                  |
#  |       |  +-----------------+     +--------------------+  |
#  |       |  | relation        |     | relation_weight    |  |
#  |       |  +-----------------+     +--------------------+  |
#  |       |  | relation_id     |--+  | relation_weight_id |  |
#  |       +->| edge_id         |  |  | edge_weight_id     |<-+
#  |          | other_cell_id   |  +->| relation_id        |
#  +--------->| cell_id         |     | weight_value       |
#             +-----------------+     +--------------------+
#

_create_partition = """
    CREATE TABLE cell (
        cell_id INTEGER PRIMARY KEY AUTOINCREMENT,
        partial INTEGER DEFAULT 0 CHECK (partial IN (0, 1))
    );

    CREATE TABLE hierarchy (
        hierarchy_id INTEGER PRIMARY KEY AUTOINCREMENT,
        hierarchy_value TEXT UNIQUE NOT NULL,
        hierarchy_level INTEGER UNIQUE NOT NULL
    );

    CREATE TABLE label (
        label_id INTEGER DEFAULT NULL UNIQUE,
        hierarchy_id INTEGER NOT NULL,
        label_value TEXT,
        FOREIGN KEY (hierarchy_id) REFERENCES hierarchy(hierarchy_id),
        PRIMARY KEY (label_id, hierarchy_id),
        UNIQUE (hierarchy_id, label_value)
    );

    CREATE TRIGGER AutoIncrementLabelId AFTER INSERT ON label
    BEGIN
        UPDATE label
        SET label_id = (SELECT MAX(COALESCE(label_id, 0))+1 FROM label)
        WHERE label_id IS NULL;
    END;

    CREATE TABLE cell_label (
        cell_label_id INTEGER PRIMARY KEY,
        cell_id INTEGER,
        hierarchy_id INTEGER,
        label_id INTEGER,
        FOREIGN KEY (cell_id) REFERENCES cell(cell_id),
        FOREIGN KEY (label_id, hierarchy_id) REFERENCES label(label_id, hierarchy_id)
        UNIQUE (cell_id, hierarchy_id)
    );
    CREATE INDEX nonunique_celllabel_cellid ON cell_label (cell_id);
    CREATE INDEX nonunique_celllabel_hierarchyid ON cell_label (hierarchy_id);
    CREATE INDEX nonunique_celllabel_labelid ON cell_label (label_id);

    CREATE TRIGGER UniqueLabelCombination BEFORE INSERT ON cell_label
    BEGIN
        SELECT RAISE(ROLLBACK, 'insert on table "cell_label" violates unique label-combination constraint')
        FROM (
                SELECT GROUP_CONCAT(label_id)
                FROM (
                    SELECT cell_id, label_id
                    FROM cell_label
                    ORDER BY cell_id, label_id
                )
                GROUP BY cell_id

                INTERSECT

                SELECT GROUP_CONCAT(label_id)
                FROM (
                    SELECT cell_id, label_id
                    FROM cell_label
                    WHERE cell_id = NEW.cell_id
                        AND cell_label_id != NEW.cell_label_id

                    UNION

                    SELECT NEW.cell_id, NEW.label_id
                    ORDER BY cell_id, label_id
                )
                GROUP BY cell_id
        );
    END;

    CREATE TRIGGER UpdateUniqueLabels BEFORE UPDATE ON cell_label
    BEGIN
        SELECT RAISE(ROLLBACK, 'insert on table "cell_label" violates unique label-combination constraint')
        FROM (
                SELECT GROUP_CONCAT(label_id)
                FROM (
                    SELECT cell_id, label_id
                    FROM cell_label
                    ORDER BY cell_id, label_id
                )
                GROUP BY cell_id

                INTERSECT

                SELECT GROUP_CONCAT(label_id)
                FROM (
                    SELECT cell_id, label_id
                    FROM cell_label
                    WHERE cell_id = NEW.cell_id
                        AND cell_label_id != NEW.cell_label_id

                    UNION

                    SELECT NEW.cell_id, NEW.label_id
                    ORDER BY cell_id, label_id
                )
                GROUP BY cell_id
        );
    END;

    CREATE TABLE partition (
        partition_id INTEGER PRIMARY KEY,
        partition_hash TEXT UNIQUE ON CONFLICT REPLACE NOT NULL,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    );

    CREATE TABLE edge (
        edge_id INTEGER PRIMARY KEY AUTOINCREMENT,
        other_partition_hash TEXT NOT NULL UNIQUE,
        other_partition_file TEXT
    );

    CREATE TABLE edge_weight (
        edge_weight_id INTEGER PRIMARY KEY,
        edge_id INTEGER,
        type TEXT,
        note TEXT,
        proportional INTEGER DEFAULT 0 CHECK (proportional IN (0, 1)),
        FOREIGN KEY (edge_id) REFERENCES edge(edge_id),
        UNIQUE (edge_id, type)
    );

    CREATE TABLE relation (
        relation_id INTEGER PRIMARY KEY,
        edge_id INTEGER,
        other_cell_id INTEGER NOT NULL,
        cell_id INTEGER,
        FOREIGN KEY (edge_id) REFERENCES edge(edge_id),
        FOREIGN KEY (cell_id) REFERENCES cell(cell_id),
        UNIQUE (edge_id, other_cell_id, cell_id)
    );

    CREATE TABLE relation_weight (
        relation_weight_id INTEGER PRIMARY KEY,
        edge_weight_id INTEGER,
        relation_id INTEGER,
        weight TEXTNUM,  /* <- Custom type for Python Decimals. */
        FOREIGN KEY (edge_weight_id) REFERENCES edge_weight(edge_weight_id),
        FOREIGN KEY (relation_id) REFERENCES relation(relation_id)
    );

    CREATE TABLE property (
        property_id INTEGER PRIMARY KEY,
        property_key TEXT,
        property_val TEXT,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    );
"""

# Register SQLite adapter/converter for Decimal type.
sqlite3.register_adapter(Decimal, str)
sqlite3.register_converter('TEXTNUM', lambda x: Decimal(x.decode('utf-8')))


# Flags.
IN_MEMORY = 1  #: Create a temporary partition in RAM.
TEMP_FILE = 2  #: Write a temporary partition to disk instead of using RAM.
READ_ONLY = 4  #: Connect to an existing Partition in read-only mode.


class _Connector(object):
    """Opens a SQLite connection to a Partition database."""

    def __init__(self, database=None, mode=None):
        """Creates a callable `connect` object that can be used to
        establish connections to a Partition database.  Connecting to
        a Partition name that does not exist will create a new
        Partition of the given name.

        """
        global _create_partition
        self._memory_conn = None
        self._temp_path = None

        self._database = database

        if database and os.path.exists(database):
            self._database = database
            if not self._is_valid():
                raise Exception('File - %s - is not a valid partition.' % database)
        else:
            if database and mode == None:
                self._database = database
            elif mode & TEMP_FILE:
                fd, temp_path = tempfile.mkstemp(suffix='.partition')
                os.close(fd)
                self._database = temp_path
                self._temp_path = temp_path
            elif not database or mode & IN_MEMORY:
                self._memory_conn =  sqlite3.connect(':memory:',
                                                     detect_types=sqlite3.PARSE_DECLTYPES)
                self._memory_conn.cursor().execute('PRAGMA foreign_keys=ON')

            # Populate new partition.
            connection = self.__call__()
            cursor = connection.cursor()
            cursor.executescript(_create_partition)
            connection.close()

    def __call__(self):
        """Opens a SQLite connection to a Partition database."""
        # Docstring (above) should be same as docstring for class.
        if self._database:
            connection =  sqlite3.connect(self._database,
                                          detect_types=sqlite3.PARSE_DECLTYPES)
            connection.cursor().execute('PRAGMA foreign_keys=ON')

        elif self._memory_conn:

            class InMemoryConnection(object):
                def __init__(self, conn):
                    self._conn = conn

                def close(self):
                    self._conn.rollback()  # Uncommitted changes will be lost!

                def __getattr__(self, name):
                    return getattr(self._conn, name)

            connection = InMemoryConnection(self._memory_conn)

        return connection

    def __del__(self):
        """Clean-up connection objects."""
        if self._memory_conn:
            self._memory_conn.close()

        if self._temp_path:
            os.remove(self._temp_path)

    def _is_valid(self):
        """Return True if database is a valid Partition, else False."""
        connection = self.__call__()
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables_contained = set(x[0] for x in cursor)
            connection.close()
        except sqlite3.DatabaseError:
            tables_contained = set()
            connection.close()

        tables_required = set(['cell', 'hierarchy', 'label', 'cell_label',
                               'partition', 'edge', 'edge_weight',
                               'relation', 'relation_weight', 'property',
                               'sqlite_sequence'])
        return tables_required == tables_contained

    @staticmethod
    def _path_to_uri(path, **kwds):
        """Takes file path, returns URI filename. See documentation at
        <http://www.sqlite.org/uri.html> for details.

        """
        path = os.path.normpath(path)
        prefix = 'file:'
        if os.name == 'nt':
            match = re.match(r'/?([a-zA-Z]:)[/\\]?(.*)', path)
            if match:
                driveletter, drivepath = match.groups()
                prefix = prefix + '///' + driveletter + '/'
                path = drivepath
        path = pathname2url(path)
        query_params = [(k, v) for k, v in kwds.items() if v is not None]
        query_params = urlencode(sorted(query_params))
        return prefix + path + ('?' if query_params else '') + query_params


class Partition(object):
    def __init__(self, path=None, mode=0):
        """Get existing Partition or create a new one."""
        self._connect = _Connector(path, mode=mode)
