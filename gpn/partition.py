# -*- coding: utf-8 -*-
import itertools
import os
import re
import sqlite3
import tempfile

from decimal import Decimal

# For URI Filename handling.
from urllib.parse import urlencode
from urllib.request import pathname2url


#
# Internal Partition structure:
#
#     +===============+     +----------------+     +=================+
#     | cell          |     | cell_label     |     | hierarchy       |
#     +===============+     +----------------+     +=================+
#  +--| cell_id       |--+  | cell_label_id  |     | hierarchy_id    |--+
#  |  | cell_labels   |  +->| cell_id        |     | hierarchy_value |  |
#  |  | partial       |     | hierarchy_id   |<-+  | hierarchy_level |  |
#  |  +---------------+     | label_id       |<-+  +-----------------+  |
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
        cell_labels TEXT UNIQUE DEFAULT '',
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

    CREATE TRIGGER DenormalizeLabelIds AFTER UPDATE ON cell_label
    BEGIN
        UPDATE cell
        SET cell_labels=(
            SELECT GROUP_CONCAT(label_id)
            FROM (
                SELECT label_id
                FROM cell_label
                WHERE cell_id=NEW.cell_id
                ORDER BY label_id
            )
        )
        WHERE cell_id=NEW.cell_id;
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


# Counter used to create unique names for in-memory partitions.
_MEMORY_SEQUENCE = itertools.count(start=1)


class _Connector(object):
    """Opens a SQLite connection to a Partition database."""

    def __init__(self, path=None, mode=None):
        """Creates a callable `connect` object that can be used to
        establish connections to a Partition database.  Connecting to
        a Partition name that does not exist will create a new
        Partition of the given name.  Omitting the `path` argument
        will create an anonymous, temporary Partition.

        """
        global _create_partition
        assert mode in (None, 'ro', 'rw', 'rwc', 'memory')
        assert path != ':memory:', ("Illegal path.  Use mode='memory' "
                                    "to create an in-memory partition.")
        self._memory_conn = None
        self._temp_path = None

        if path and os.path.exists(path):
            # Existing partition.
            assert mode != 'memory', ('Cannot create in-memory partition--'
                                      'already exists on disk.')
            self._uri = self._path_to_uri(path, mode=mode)
            if not self._is_valid():
                raise Exception('File - %s - is not a valid partition.' % path) from None
        else:
            # New partition.
            if mode == 'memory':
                temp_path = 'memptn' + str(next(_MEMORY_SEQUENCE))
                self._uri = self._path_to_uri(temp_path, mode=mode, cache='shared')
                self._memory_conn = self._connect(self._uri)
            else:
                if path:
                    self._uri = self._path_to_uri(path, mode=mode)
                else:
                    fh, temp_path = tempfile.mkstemp(suffix='.partition')
                    os.close(fh)
                    self._temp_path = temp_path
                    self._uri = self._path_to_uri(temp_path, mode=mode)

            # Populate new partition.
            connection = self._connect(self._uri)
            cursor = connection.cursor()
            cursor.executescript(_create_partition)
            connection.close()

    def __call__(self):
        """Opens a SQLite connection to a Partition database."""
        # Docstring (above) should be same as docstring for class.
        return self._connect(self._uri)

    def __del__(self):
        """Clean-up connection objects."""
        if self._memory_conn:
            self._memory_conn.close()

        if self._temp_path:
            os.remove(self._temp_path)

    def _is_valid(self):
        """Return True if database is a valid Partition, else False."""
        connection = self._connect(self._uri)
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables_contained = {x[0] for x in cursor}
            connection.close()
        except sqlite3.DatabaseError:
            tables_contained = set()
            connection.close()

        tables_required = {'cell', 'hierarchy', 'label', 'cell_label',
                           'partition', 'edge', 'edge_weight', 'relation',
                           'relation_weight', 'property', 'sqlite_sequence'}
        return tables_required == tables_contained

    @staticmethod
    def _connect(uri_filename):
        """Connect to database specified by URI filename."""
        connection =  sqlite3.connect(uri_filename,
                                      uri=True,
                                      detect_types=sqlite3.PARSE_DECLTYPES)
        connection.cursor().execute('PRAGMA foreign_keys=ON')
        return connection


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


# Partition flags.
READ_ONLY = 1      #: Connect to an existing Partition in read-only mode.
OUT_OF_MEMORY = 2  #: Write a temporary partition to disk instead of using RAM.


class Partition(object):
    def __init__(self, path=None, flags=0):
        """Get existing Partition or create a new one."""
        if path:
            mode = 'ro' if flags & READ_ONLY else None
        else:
            mode = None if flags & OUT_OF_MEMORY else 'memory'
        self._connect = _Connector(path, mode=mode)
