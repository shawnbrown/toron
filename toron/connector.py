# -*- coding: utf-8 -*-
import os
import re
import sqlite3
import tempfile

from decimal import Decimal


# Schema:
#                           +----------------+     +=================+
#     +================+    | cell_label     |     | hierarchy       |
#     | cell           |    +----------------+     +=================+
#     +================+    | cell_label_id  |     | hierarchy_id    |--+
#  +--| cell_id        |--->| cell_id        |     | hierarchy_value |  |
#  |  | partial        |    | hierarchy_id   |<-+  | hierarchy_level |  |
#  |  +----------------+    | label_id       |<-+  +-----------------+  |
#  |                        +----------------+  |                       |
#  |   +----------------+                       |  +-----------------+  |
#  |   | property       |    +--------------+   |  | label           |  |
#  |   +----------------+    | node         |   |  +-----------------+  |
#  |   | property_id    |    +--------------+   +--| label_id        |  |
#  |   | property_key   |    | node_id      |   +--| hierarchy_id    |<-+
#  |   | property_value |    | node_hash    |      | label_value     |
#  |   | created_date   |    | created_date |      +-----------------+
#  |   +----------------+    +--------------+
#  |
#  |         +==================+     +--------------------+
#  |         | edge             |     | weight             |
#  |         +==================+     +--------------------+
#  |      +--| edge_id          |--+  | weight_id          |--+
#  |      |  | edge_name        |  +->| edge_id            |  |
#  |      |  | edge_description |     | weight_name        |  |
#  |      |  | edge_order       |     | weight_description |  |
#  |      |  | other_node_hash  |     | weight_order       |  |
#  |      |  | other_node_name  |     | proportional       |  |
#  |      |  +------------------+     +--------------------+  |
#  |      |                                                   |
#  |      |     +---------------+     +--------------------+  |
#  |      |     | relation      |     | relation_weight    |  |
#  |      |     +---------------+     +--------------------+  |
#  |      |     | relation_id   |--+  | relation_weight_id |  |
#  |      +---->| edge_id       |  |  | weight_id          |<-+
#  |            | other_cell_id |  +->| relation_id        |
#  +----------> | cell_id       |     | weight_value       |
#               +---------------+     +--------------------+


# Register SQLite adapter/converter for Decimal type.
sqlite3.register_adapter(Decimal, str)
sqlite3.register_converter('TEXTNUM', lambda x: Decimal(x.decode('utf-8')))

_invalid_root_hierarchy = """
    SELECT hierarchy_value AS invalid_root
    FROM label
    NATURAL JOIN (SELECT hierarchy_id, hierarchy_value
                  FROM hierarchy
                  ORDER BY hierarchy_level
                  LIMIT 1)
    WHERE label_value != 'UNMAPPED'
    GROUP BY hierarchy_value
    HAVING COUNT(*) != 1
"""

_duplicate_label_sets = """
    SELECT *
    FROM (SELECT GROUP_CONCAT(label_id) AS label_set
          FROM (SELECT cell_id, label_id
                FROM cell_label
                ORDER BY cell_id, label_id)
          GROUP BY cell_id)
    GROUP BY label_set
    HAVING COUNT(*) > 1
"""

_invalid_unmapped_levels = """
    SELECT GROUP_CONCAT(hierarchy_level)
    FROM (SELECT cell_id,
                 CASE
                     WHEN label_id IN (SELECT label_id
                                       FROM label
                                       WHERE label_value='UNMAPPED')
                     THEN 1
                     ELSE 0
                 END AS unmapped_code,
                 hierarchy_level
          FROM cell_label
          NATURAL JOIN label
          NATURAL JOIN hierarchy
          WHERE cell_id IN (SELECT DISTINCT cell_id
                            FROM cell_label
                            WHERE label_id IN (SELECT label_id
                                               FROM label
                                               WHERE label_value='UNMAPPED'))
          ORDER BY cell_id, unmapped_code, hierarchy_level)
    GROUP BY cell_id

    EXCEPT

    SELECT GROUP_CONCAT(hierarchy_level)
    FROM (SELECT hierarchy_level
          FROM hierarchy
          ORDER BY hierarchy_level)
"""


_schema = [
    """
    CREATE TABLE cell (
       cell_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        partial INTEGER DEFAULT 0 CHECK (partial IN (0, 1))
    )
    """,
    """
    CREATE TABLE hierarchy (
        hierarchy_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        hierarchy_value TEXT UNIQUE NOT NULL CHECK(hierarchy_value!='cell_id'
                                                   AND hierarchy_value NOT LIKE '%.%'),
        hierarchy_level INTEGER UNIQUE NOT NULL
    )
    """,
    """
    CREATE TABLE label (
        label_id INTEGER DEFAULT NULL UNIQUE,
        hierarchy_id INTEGER NOT NULL,
        label_value TEXT,
        FOREIGN KEY (hierarchy_id) REFERENCES hierarchy(hierarchy_id),
        PRIMARY KEY (label_id, hierarchy_id),
        UNIQUE (hierarchy_id, label_value)
    )
    """,
    """
    CREATE INDEX idx_Label_HierarchyId ON label (hierarchy_id);
    """,
    """
    CREATE TRIGGER trg_AutoIncrementLabelId_InsertLabel AFTER INSERT ON label
    BEGIN
        UPDATE label
        SET label_id = (SELECT MAX(COALESCE(label_id, 0))+1 FROM label)
        WHERE label_id IS NULL;
    END
    """,
    """
    CREATE TRIGGER trg_CheckRootHierarchy_InsertLabel AFTER INSERT ON label
    WHEN NEW.hierarchy_id=(SELECT hierarchy_id
                           FROM hierarchy
                           ORDER BY hierarchy_level
                           LIMIT 1)
         AND (%s) IS NOT NULL
    BEGIN
        SELECT RAISE(ABORT, 'CHECK constraint failed: label (root hierarchy cannot have multiple values)');
    END
    """ % _invalid_root_hierarchy,
    """
    CREATE TRIGGER trg_CheckRootHierarchy_UpdateLabel AFTER UPDATE ON label
    WHEN NEW.hierarchy_id=(SELECT hierarchy_id
                           FROM hierarchy
                           ORDER BY hierarchy_level
                           LIMIT 1)
         AND (%s) IS NOT NULL
    BEGIN
        SELECT RAISE(ABORT, 'CHECK constraint failed: label (root hierarchy cannot have multiple values)');
    END
    """ % _invalid_root_hierarchy,
    """
    CREATE TRIGGER trg_CheckRootHierarchy_UpdateHierarchy AFTER UPDATE ON hierarchy
    WHEN (%s) IS NOT NULL
    BEGIN
        SELECT RAISE(ABORT, 'CHECK constraint failed: label (root hierarchy cannot have multiple values)');
    END
    """ % _invalid_root_hierarchy,
    """
    CREATE TRIGGER trg_CheckRootHierarchy_DeleteHierarchy AFTER DELETE ON hierarchy
    WHEN (%s) IS NOT NULL
    BEGIN
        SELECT RAISE(ABORT, 'CHECK constraint failed: label (root hierarchy cannot have multiple values)');
    END
    """ % _invalid_root_hierarchy,
    """
    CREATE TABLE cell_label (
        cell_label_id INTEGER PRIMARY KEY,
        cell_id INTEGER,
        hierarchy_id INTEGER,
        label_id INTEGER,
        FOREIGN KEY (cell_id) REFERENCES cell(cell_id),
        FOREIGN KEY (label_id, hierarchy_id) REFERENCES label(label_id, hierarchy_id),
        UNIQUE (cell_id, hierarchy_id)
    )
    """,
    """
    CREATE INDEX idx_CellLabel_CellId ON cell_label (cell_id)
    """,
    """
    CREATE INDEX idx_CellLabel_HierarchyId ON cell_label (hierarchy_id)
    """,
    """
    CREATE INDEX idx_CellLabel_LabelId ON cell_label (label_id)
    """,
    """
    CREATE TRIGGER trg_CheckUniqueLabels_InsertCellLabel AFTER INSERT ON cell_label
    WHEN (%s)
    BEGIN
        SELECT RAISE(ABORT, 'CHECK constraint failed: cell_label (duplicate label set)');
    END
    """ % _duplicate_label_sets,
    """
    CREATE TRIGGER trg_CheckUniqueLabels_UpdateCellLabel AFTER UPDATE ON cell_label
    WHEN (%s)
    BEGIN
        SELECT RAISE(ABORT, 'CHECK constraint failed: cell_label (duplicate label set)');
    END
    """ % _duplicate_label_sets,
    """
    CREATE TRIGGER trg_CheckUniqueLabels_DeleteCellLabel AFTER DELETE ON cell_label
    WHEN (%s)
    BEGIN
        SELECT RAISE(ABORT, 'CHECK constraint failed: cell_label (duplicate label set)');
    END
    """ % _duplicate_label_sets,
    """
    CREATE TRIGGER trg_CheckUnmappedHierarchy_InsertCellLabel AFTER INSERT ON cell_label
    WHEN (%s)
    BEGIN
        SELECT RAISE(ABORT, 'CHECK constraint failed: cell_label (invalid unmapped level)');
    END
    """ % _invalid_unmapped_levels,
    """
    CREATE TRIGGER trg_CheckUnmappedHierarchy_UpdateCellLabel AFTER UPDATE ON cell_label
    WHEN (%s)
    BEGIN
        SELECT RAISE(ABORT, 'CHECK constraint failed: cell_label (invalid unmapped level)');
    END
    """ % _invalid_unmapped_levels,
    """
    CREATE TRIGGER trg_CheckUnmappedHierarchy_UpdateHierarchy AFTER UPDATE ON hierarchy
    WHEN (%s)
    BEGIN
        SELECT RAISE(ABORT, 'CHECK constraint failed: cell_label (invalid unmapped level)');
    END
    """ % _invalid_unmapped_levels,
    """
    CREATE TABLE node (
        node_id INTEGER PRIMARY KEY,
        node_hash TEXT UNIQUE ON CONFLICT REPLACE NOT NULL,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE edge (
        edge_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        edge_name TEXT DEFAULT 'unnamed' NOT NULL,
        edge_description TEXT,
        edge_order INTEGER DEFAULT NULL,
        other_node_hash TEXT NOT NULL,
        other_node_name TEXT,
        UNIQUE (other_node_hash, edge_name),
        UNIQUE (other_node_hash, edge_order)
    )
    """,
    """
    CREATE TRIGGER trg_AutoIncrementEdgeOrder_InsertEdge AFTER INSERT ON edge
    BEGIN
        UPDATE edge
        SET edge_order = (SELECT MAX(COALESCE(edge_order, 0))+1
                          FROM edge
                          WHERE other_node_hash=NEW.other_node_hash)
        WHERE edge_order IS NULL;
    END
    """,
    """
    CREATE TABLE weight (
        weight_id INTEGER PRIMARY KEY,
        edge_id INTEGER,
        weight_name TEXT DEFAULT 'unnamed' NOT NULL,
        weight_description TEXT,
        weight_order INTEGER DEFAULT NULL,
        proportional INTEGER DEFAULT 0 CHECK (proportional IN (0, 1)),
        FOREIGN KEY (edge_id) REFERENCES edge(edge_id),
        UNIQUE (edge_id, weight_name),
        UNIQUE (edge_id, weight_order)
    )
    """,
    """
    CREATE TRIGGER trg_AutoIncrementWeightOrder_InsertEdge AFTER INSERT ON edge
    BEGIN
        UPDATE weight
        SET weight_order = (SELECT MAX(COALESCE(weight_order, 0))+1
                            FROM weight
                            WHERE edge_id=NEW.edge_id)
        WHERE weight_order IS NULL;
    END
    """,
    """
    CREATE TABLE relation (
        relation_id INTEGER PRIMARY KEY,
        edge_id INTEGER,
        other_cell_id INTEGER NOT NULL,
        cell_id INTEGER,
        FOREIGN KEY (edge_id) REFERENCES edge(edge_id),
        FOREIGN KEY (cell_id) REFERENCES cell(cell_id),
        UNIQUE (edge_id, other_cell_id, cell_id)
    )
    """,
    """
    CREATE TABLE relation_weight (
        relation_weight_id INTEGER PRIMARY KEY,
        weight_id INTEGER,
        relation_id INTEGER,
        weight TEXTNUM,  /* <- Custom type for Python Decimals. */
        FOREIGN KEY (weight_id) REFERENCES weight(weight_id),
        FOREIGN KEY (relation_id) REFERENCES relation(relation_id)
    )
    """,
    """
    CREATE TABLE property (
        property_id INTEGER PRIMARY KEY,
        property_key TEXT,
        property_val TEXT,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    )
    """,
]


def _get_schema_dict(sql_type=None):
    """Return schema dictionary with table/index/trigger name as key.

    Value of `sql_type` can be TABLE, INDEX, or TRIGGER.  If ommitted,
    all types are returned.
    """
    global _schema

    if sql_type:
        msg = "sql_type must be 'TABLE', 'INDEX', 'TRIGGER', or None."
        assert sql_type in ('TABLE', 'INDEX', 'TRIGGER'), msg
    else:
        sql_type = '(?:TABLE|INDEX|TRIGGER)'
    regex = re.compile('CREATE %s (\w+)' % sql_type)

    sql_objects = {}
    for operation in _schema:
        match = regex.search(operation)
        if match:
            sql_objects[match.group(1)] = operation

    return sql_objects


_expensive_constraints = ['trg_CheckUniqueLabels_InsertCellLabel',
                          'trg_CheckUniqueLabels_UpdateCellLabel',
                          'trg_CheckUniqueLabels_DeleteCellLabel',
                          'trg_CheckUnmappedHierarchy_InsertCellLabel',
                          'trg_CheckUnmappedHierarchy_UpdateCellLabel',
                          'trg_CheckUnmappedHierarchy_UpdateHierarchy']


# Mode flags.
IN_MEMORY = 1  #: Create a temporary node in RAM.
TEMP_FILE = 2  #: Write a temporary node to disk instead of using RAM.
READ_ONLY = 4  #: Connect to an existing node in read-only mode.


class _Connector(object):
    """Opens a SQLite connection to a Node database.  If a named
    Node does not exist, it is created.

    """
    def __init__(self, filepath=None, mode=0):
        """Creates a callable `connect` object that can be used to
        establish connections to a Node database.  Connecting to a Node
        that does not exist will create a new Node using the given name.

        When using IN_MEMORY or TEMP_FILE modes, `filepath` is ignored.

        """
        global _schema
        self._mode = mode
        self._init_as_temp = bool(TEMP_FILE & mode)

        if filepath and os.path.isfile(filepath):
            # Connect to existing database and assert validity.
            try:
                with sqlite3.connect(filepath) as connection:
                    assert self._is_valid(connection)
            except Exception:
                raise Exception('File - %s - is not a valid node.' % filepath)
            self._dbsrc = filepath
        else:
            # Prepare new _dbsrc (either filepath or in-memory connection).
            if filepath and (not mode):
                self._dbsrc = filepath
            elif TEMP_FILE & mode:
                fd, temp_path = tempfile.mkstemp(suffix='.node')
                os.close(fd)
                self._dbsrc = temp_path
            elif (IN_MEMORY & mode) or (not filepath):
                self._dbsrc =  sqlite3.connect(':memory:',
                                               detect_types=sqlite3.PARSE_DECLTYPES,
                                               factory=_SharedConnection)
            else:
                msg = 'Unexpected parameter values: filepath=%r, mode=%r'
                raise ValueError(msg % (filepath, mode))

            # Establish connection and populate new database.
            with self._connect(self._dbsrc) as connection:
                cursor = connection.cursor()
                cursor.execute('PRAGMA synchronous=OFF')
                for operation in _schema:
                    cursor.execute(operation)
                cursor.execute('PRAGMA synchronous=FULL')

    def __call__(self):
        """Opens a SQLite connection to a Node database.  If a named Node
        does not exist, it is created.

        """
        # Docstring (above) should be same as docstring for class.

        connection = self._connect(self._dbsrc)
        cursor = connection.cursor()

        # Enable foreign keys (use triggers with older SQLite).
        if sqlite3.sqlite_version_info >= (3, 6, 19):
            cursor.execute('PRAGMA foreign_keys=ON')
        else:
            sql_script = _all_foreign_key_triggers()
            cursor.executescript(sql_script)

        # Set to read-only if appropriate.
        if READ_ONLY & self._mode:
            if sqlite3.sqlite_version_info >= (3, 8, 0):
                cursor.execute('PRAGMA query_only=1')
            else:
                sql_script = _all_read_only_triggers()
                cursor.executescript(sql_script)

        return connection

    def __del__(self):
        """Clean-up connection objects."""
        try:
            self._dbsrc.close_parent()  # Permanently close in-memory db!
        except AttributeError:
            pass

        if (TEMP_FILE & self._mode) and self._init_as_temp:
            os.remove(self._dbsrc)

    @staticmethod
    def _connect(database_source):
        if isinstance(database_source, sqlite3.Connection):
            return database_source
        return sqlite3.connect(database_source, detect_types=sqlite3.PARSE_DECLTYPES)

    @staticmethod
    def _is_valid(connection):
        """Return True if database is a valid Node, else False."""
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables_contained = set(x[0] for x in cursor)
        except sqlite3.DatabaseError:
            tables_contained = set()

        tables_required = set(['cell', 'hierarchy', 'label', 'cell_label',
                               'node', 'edge', 'weight',
                               'relation', 'relation_weight', 'property',
                               'sqlite_sequence'])
        return tables_required == tables_contained


class _SharedConnection(sqlite3.Connection):
    """Subclass for in-memory, shared connection."""
    def __init__(self, *args, **kwds):
        sqlite3.Connection.__init__(self, *args, **kwds)
        self._isolation_level = self.isolation_level

    def close(self):
        """Close child connection object (remains usable)."""
        try:
            self.rollback()  # Uncommitted changes will be lost!
        except sqlite3.ProgrammingError:
            pass  # Closing already closed connection should pass.

        self.isolation_level = self._isolation_level  # Reset isolation level.

    def close_parent(self):
        """Close parent object (unusable from this point forward)."""
        return super(self.__class__, self).close()

    def cursor(self):
        return super(self.__class__, self).cursor(_ChildCursor)


class _ChildCursor(sqlite3.Cursor):
    """Child cursor for shared, in-memory connection object."""
    def close(self):
        """Close child cursor object (remains usable)."""
        conn = self.connection
        try:
            conn.rollback()  # Uncommitted changes will be lost!
        except sqlite3.ProgrammingError:
            pass  # Closing already closed connection should pass.


########################################################################
# Since version 3.6.19, SQLite supports foreign key constraints.  Older
# versions can emulate these constraints with triggers.  The following
# functions construct appropriate, temporary triggers for use with these
# older versions.
#
# See <http://www.sqlite.org/cvstrac/wiki?p=ForeignKeyTriggers> for more
# information.
########################################################################
def _all_foreign_key_triggers():
    all_triggers = [
        # FOREIGN KEY (hierarchy_id) REFERENCES hierarchy(hierarchy_id)
        _foreign_key_triggers(name='lbl_harchy',
                              child_table='label',
                              child_key='hierarchy_id',
                              parent_table='hierarchy',
                              parent_key='hierarchy_id'),

        # FOREIGN KEY (label_id, hierarchy_id)
        #     REFERENCES label(label_id, hierarchy_id)
        _foreign_key_triggers(name='cellbl_lbl',
                              child_table='cell_label',
                              child_key=['hierarchy_id', 'label_id'],
                              parent_table='label',
                              parent_key=['hierarchy_id', 'label_id']),

        # FOREIGN KEY (cell_id) REFERENCES cell(cell_id)
        _foreign_key_triggers(name='cellbl_cel',
                              child_table='cell_label',
                              child_key='cell_id',
                              parent_table='cell',
                              parent_key='cell_id'),

        # FOREIGN KEY (edge_id) REFERENCES edge(edge_id)
        _foreign_key_triggers(name='wt_edg',
                              child_table='weight',
                              child_key='edge_id',
                              parent_table='edge',
                              parent_key='edge_id'),

        # FOREIGN KEY (edge_id) REFERENCES edge(edge_id)
        _foreign_key_triggers(name='rel_edg',
                              child_table='relation',
                              child_key='edge_id',
                              parent_table='edge',
                              parent_key='edge_id'),

        # FOREIGN KEY (cell_id) REFERENCES cell(cell_id)
        _foreign_key_triggers(name='rel_cel',
                              child_table='relation',
                              child_key='cell_id',
                              parent_table='cell',
                              parent_key='cell_id'),

        # FOREIGN KEY (relation_id) REFERENCES relation(relation_id)
        _foreign_key_triggers(name='relwt_rel',
                              child_table='relation_weight',
                              child_key='relation_id',
                              parent_table='relation',
                              parent_key='relation_id'),

        # FOREIGN KEY (weight_id) REFERENCES weight(weight_id)
        _foreign_key_triggers(name='relwt_wt',
                              child_table='relation_weight',
                              child_key='weight_id',
                              parent_table='weight',
                              parent_key='weight_id'),
    ]
    return '\n\n\n'.join(all_triggers)


def _foreign_key_triggers(name, child_table, child_key, parent_table,
                          parent_key, not_null=True):
    args = _normalize_args_for_trigger(child_key, parent_key, not_null)
    child_key, parent_key, not_null = args  # Unpack args.

    # Get INSERT and UPDATE triggers.
    null_clause = _null_clause_for_trigger(child_key, not_null, prefix='NEW')
    where_clause = _where_clause_for_trigger(parent_key, child_key, prefix='NEW')
    args = (child_table, null_clause, parent_table, where_clause)
    before_insert = _insert_trigger('fki_'+name, *args)
    before_update = _update_trigger('fku_'+name, *args)

    # Get DELETE trigger.
    null_clause = _null_clause_for_trigger(child_key, not_null, prefix='OLD')
    where_clause = _where_clause_for_trigger(child_key, parent_key, prefix='OLD')
    args = (child_table, null_clause, parent_table, where_clause)
    before_delete = _delete_trigger('fkd_'+name, *args)

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


########################################################################
# Since version 3.8.0, SQLite supports the query_only PRAGMA (which this
# package uses to implement read-only connections).  Older versions can
# partially enforce a read-only mode using triggers.  The following
# functions construct appropriate, temporary triggers for use with these
# older versions.
#
# While a read-only mechanism has been available since 3.7.7 (via the
# URI Filename parameter `?mode=ro`), it is not supported in versions of
# Python before 3.4.
########################################################################
def _all_read_only_triggers():
    all_triggers = [
        _read_only_triggers('cel', 'cell'),
        _read_only_triggers('harchy', 'hierarchy'),
        _read_only_triggers('lbl', 'label'),
        _read_only_triggers('cellbl', 'cell_label'),
        _read_only_triggers('edg', 'edge'),
        _read_only_triggers('wt', 'weight'),
        _read_only_triggers('rel', 'relation'),
        _read_only_triggers('relwt', 'relation_weight'),
        _read_only_triggers('prop', 'property'),
        _read_only_triggers('nde', 'node'),
    ]
    return '\n\n\n'.join(all_triggers)


def _read_only_triggers(name, table):
    return ('CREATE TEMPORARY TRIGGER IF NOT EXISTS roi_{name}\n'
            'BEFORE INSERT ON main.{table} FOR EACH ROW\n'
            'BEGIN\n'
            '    SELECT RAISE(ABORT, \'attempt to write a readonly database\');\n'
            'END;\n'
            '\n'
            'CREATE TEMPORARY TRIGGER IF NOT EXISTS rou_{name}\n'
            'BEFORE UPDATE ON main.{table} FOR EACH ROW\n'
            'BEGIN\n'
            '    SELECT RAISE(ABORT, \'attempt to write a readonly database\');\n'
            'END;\n'
            '\n'
            'CREATE TEMPORARY TRIGGER IF NOT EXISTS rod_{name}\n'
            'BEFORE DELETE ON main.{table} FOR EACH ROW\n'
            'BEGIN\n'
            '    SELECT RAISE(ABORT, \'attempt to write a readonly database\');\n'
            'END;').format(name=name, table=table)
