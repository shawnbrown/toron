# -*- coding: utf-8 -*-
import os
import sqlite3
import tempfile

from decimal import Decimal


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


# Register SQLite adapter/converter for Decimal type.
sqlite3.register_adapter(Decimal, str)
sqlite3.register_converter('TEXTNUM', lambda x: Decimal(x.decode('utf-8')))


_create_partition = """
    CREATE TABLE cell (
        cell_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        partial INTEGER DEFAULT 0 CHECK (partial IN (0, 1))
    );

    CREATE TABLE hierarchy (
        hierarchy_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
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

    CREATE TRIGGER CheckUniqueLabels_ins BEFORE INSERT ON cell_label
    BEGIN
        SELECT RAISE(ABORT, 'CHECK constraint failed: cell_label (each cell must be associated with a unique set of labels)')
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

    CREATE TRIGGER CheckUniqueLabels_upd BEFORE UPDATE ON cell_label
    BEGIN
        SELECT RAISE(ABORT, 'CHECK constraint failed: cell_label (each cell must be associated with a unique set of labels)')
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

    CREATE TRIGGER CheckUniqueLabels_del BEFORE DELETE ON cell_label
    BEGIN
        SELECT RAISE(ABORT, 'CHECK constraint failed: cell_label (each cell must be associated with a unique set of labels)')
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
                    WHERE cell_id = OLD.cell_id
                        AND cell_label_id != OLD.cell_label_id

                    UNION

                    SELECT OLD.cell_id, OLD.label_id
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
        edge_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
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


# Mode flags.
IN_MEMORY = 1  #: Create a temporary partition in RAM.
TEMP_FILE = 2  #: Write a temporary partition to disk instead of using RAM.
READ_ONLY = 4  #: Connect to an existing Partition in read-only mode.


class _Connector(object):
    """Opens a SQLite connection to a Partition database.  If a named
    Partition does not exist, it is created.

    """
    def __init__(self, database=None, mode=0):
        """Creates a callable `connect` object that can be used to
        establish connections to a Partition database.  Connecting to
        a Partition name that does not exist will create a new
        Partition of the given name.

        """
        global _create_partition
        self._memory_conn = None
        self._temp_path = None
        self._is_read_only = bool(mode & READ_ONLY)

        self._database = database

        if database and os.path.exists(database):
            self._database = database
            try:
                connection = sqlite3.connect(database)
                is_valid = self._is_valid(connection)
                connection.close()
            except Exception:
                is_valid = False

            if not is_valid:
                raise Exception('File - %s - is not a valid partition.' % database)

        else:
            if database and (not mode or self._is_read_only):
                self._database = database
            elif mode & TEMP_FILE:
                fd, temp_path = tempfile.mkstemp(suffix='.partition')
                os.close(fd)
                self._database = temp_path
                self._temp_path = temp_path
            elif not database or mode & IN_MEMORY:
                self._memory_conn =  sqlite3.connect(':memory:',
                                                     detect_types=sqlite3.PARSE_DECLTYPES)

            # Populate new partition.
            if self._database:
                connection = self._connect(self._database)
            else:
                connection = self._connect(self._memory_conn)
            cursor = connection.cursor()
            cursor.execute('PRAGMA synchronous=OFF')
            cursor.executescript(_create_partition)
            cursor.execute('PRAGMA synchronous=FULL')
            connection.close()

    def __call__(self):
        """Opens a SQLite connection to a Partition database.  If a
        named Partition does not exist, it is created.

        """
        # Docstring (above) should be same as docstring for class.
        if self._database:
            connection = self._connect(self._database)
        elif self._memory_conn:
            connection = self._connect(self._memory_conn)

        # Enable foreign key constraints (uses triggers for older versions)
        # and set read-only PRAGMA if appropriate.
        cursor = connection.cursor()
        if sqlite3.sqlite_version_info >= (3, 6, 19):
            cursor.execute('PRAGMA foreign_keys=ON')
        else:
            sql_script = _all_foreign_key_triggers()
            cursor.executescript(sql_script)

        if self._is_read_only:
            cursor.execute('PRAGMA query_only=1')

        return connection

    def __del__(self):
        """Clean-up connection objects."""
        if self._memory_conn:
            self._memory_conn.close()

        if self._temp_path:
            os.remove(self._temp_path)

    @staticmethod
    def _connect(database):
        if isinstance(database, sqlite3.Connection):
            class ConnectionWrapper(object):
                def __init__(self, conn):
                    self._conn = conn

                def close(self):
                    try:
                        self._conn.rollback()  # Uncommitted changes will be lost!
                    except sqlite3.ProgrammingError:
                        pass  # Closing already closed connection should pass.

                def __del__(self):
                    self.close()

                def __getattr__(self, name):
                    return getattr(self._conn, name)

            connection = ConnectionWrapper(database)
        else:
            connection = sqlite3.connect(database,
                                         detect_types=sqlite3.PARSE_DECLTYPES)
        return connection

    @staticmethod
    def _is_valid(connection):
        """Return True if database is a valid Partition, else False."""
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables_contained = set(x[0] for x in cursor)
        except sqlite3.DatabaseError:
            tables_contained = set()

        tables_required = set(['cell', 'hierarchy', 'label', 'cell_label',
                               'partition', 'edge', 'edge_weight',
                               'relation', 'relation_weight', 'property',
                               'sqlite_sequence'])
        return tables_required == tables_contained


# Foreign key constraints on older versions of SQLite can be enforced
# with triggers.  The following functions construct appropriate,
# temporary triggers for tables in a default/main database.
#
# See <http://www.sqlite.org/cvstrac/wiki?p=ForeignKeyTriggers> for
# more information.
#
def _all_foreign_key_triggers():
    all_triggers = [
        _foreign_key_triggers('lbl_harchy', 'label', 'hierarchy_id', 'hierarchy', 'hierarchy_id'),
        _foreign_key_triggers('cellbl_lbl', 'cell_label', ['hierarchy_id', 'label_id'], 'label', ['hierarchy_id', 'label_id']),
        _foreign_key_triggers('cellbl_cel', 'cell_label', 'cell_id', 'cell', 'cell_id'),
        _foreign_key_triggers('edgwt_edg', 'edge_weight', 'edge_id', 'edge', 'edge_id'),
        _foreign_key_triggers('rel_edg', 'relation', 'edge_id', 'edge', 'edge_id'),
        _foreign_key_triggers('rel_cel', 'relation', 'cell_id', 'cell', 'cell_id'),
        _foreign_key_triggers('relwt_rel', 'relation_weight', 'relation_id', 'relation', 'relation_id'),
        _foreign_key_triggers('relwt_edgwt', 'relation_weight', 'edge_weight_id', 'edge_weight', 'edge_weight_id'),
    ]
    return '\n\n\n'.join(all_triggers)


def _foreign_key_triggers(name, table, column, f_table, f_column, not_null=True):
    args = _normalize_args_for_trigger(column, f_column, not_null)
    column, f_column, not_null = args  # Unpack args.

    null_clause = _null_clause_for_trigger(column, not_null, 'NEW')
    where_clause = _where_clause_for_trigger(f_column, column, prefix='NEW')
    before_insert = _insert_trigger('fki_'+name, table, null_clause, f_table, where_clause)
    before_update = _update_trigger('fku_'+name, table, null_clause, f_table, where_clause)

    null_clause = _null_clause_for_trigger(column, not_null, 'OLD')
    where_clause = _where_clause_for_trigger(column, f_column, prefix='OLD')
    before_delete = _delete_trigger('fkd_'+name, table, null_clause, f_table, where_clause)

    return '\n\n'.join([before_insert, before_update, before_delete])


def _normalize_args_for_trigger(child_key, parent_key, not_null):
    if isinstance(child_key, str):
        child_key = [child_key]
    if isinstance(parent_key, str):
        parent_key = [parent_key]
    if isinstance(not_null, bool):
        not_null = [not_null] * len(child_key)
    assert len(child_key) == len(parent_key) == len(not_null)
    return child_key, parent_key, not_null


def _null_clause_for_trigger(column, not_null, prefix):
    def fn(col, notnl):
        return '' if notnl else '%s.%s IS NOT NULL' % (prefix, col)
    null_clause = zip(column, not_null)
    null_clause = [fn(x, y) for x, y in null_clause]
    null_clause = [x for x in null_clause if x]
    null_clause = ' AND '.join(null_clause)
    null_clause += '\n             AND ' if null_clause else ''
    return null_clause


def _where_clause_for_trigger(left_cols, right_cols, prefix):
    where_clause = zip(left_cols, right_cols)
    where_clause = ['%s=%s.%s' % (x, prefix, y) for x, y in where_clause]
    where_clause = ' AND '.join(where_clause)
    return where_clause


def _insert_trigger(name, child, null_clause, parent, where_clause):
    return ('CREATE TEMPORARY TRIGGER IF NOT EXISTS {name}\n'
            'BEFORE INSERT ON {database}.{child_table} FOR EACH ROW\n'
            'WHEN {child_null_clause}(SELECT 1 FROM {database}.{parent_table} WHERE {parent_where_clause}) IS NULL\n'
            'BEGIN\n'
            '    SELECT RAISE(ABORT, \'FOREIGN KEY constraint failed\');\n'
            'END;').format(database='main',
                           name=name,
                           child_table=child,
                           child_null_clause=null_clause,
                           parent_table=parent,
                           parent_where_clause=where_clause)


def _update_trigger(name, child, null_clause, parent, where_clause):
    return ('CREATE TEMPORARY TRIGGER IF NOT EXISTS {name}\n'
            'BEFORE UPDATE ON {database}.{child_table} FOR EACH ROW\n'
            'WHEN {child_null_clause}(SELECT 1 FROM {database}.{parent_table} WHERE {parent_where_clause}) IS NULL\n'
            'BEGIN\n'
            '    SELECT RAISE(ABORT, \'FOREIGN KEY constraint failed\');\n'
            'END;').format(database='main',
                           name=name,
                           child_table=child,
                           child_null_clause=null_clause,
                           parent_table=parent,
                           parent_where_clause=where_clause)


def _delete_trigger(name, child, null_clause, parent, where_clause):
    return ('CREATE TEMPORARY TRIGGER IF NOT EXISTS {name}\n'
            'BEFORE DELETE ON {database}.{parent_table} FOR EACH ROW\n'
            'WHEN {parent_null_clause}(SELECT 1 FROM {database}.{child_table} WHERE {child_where_clause}) IS NOT NULL\n'
            'BEGIN\n'
            '    SELECT RAISE(ABORT, \'FOREIGN KEY constraint failed\');\n'
            'END;').format(database='main',
                           name=name,
                           child_table=child,
                           parent_null_clause=null_clause,
                           parent_table=parent,
                           child_where_clause=where_clause)
