# -*- coding: utf-8 -*-
import os
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
#  |                                  +--------------------+
#  |          +==================+    | weight             |
#  |          | edge             |    +--------------------+
#  |          +==================+    | weight_id          |--+
#  |       +--| edge_id          |--->| edge_id            |  |
#  |       |  | edge_name        |    | weight_name        |  |
#  |       |  | edge_description |    | weight_description |  |
#  |       |  | other_node_hash  |    | weight_order       |  |
#  |       |  | other_node_name  |    | proportional       |  |
#  |       |  +------------------+    +--------------------+  |
#  |       |                                                  |
#  |       |   +---------------+     +--------------------+   |
#  |       |   | relation      |     | relation_weight    |   |
#  |       |   +---------------+     +--------------------+   |
#  |       |   | relation_id   |--+  | relation_weight_id |   |
#  |       +-->| edge_id       |  |  | weight_id          |<--+
#  |           | other_cell_id |  +->| relation_id        |
#  +---------->| cell_id       |     | weight_value       |
#              +---------------+     +--------------------+


# Register SQLite adapter/converter for Decimal type.
sqlite3.register_adapter(Decimal, str)
sqlite3.register_converter('TEXTNUM', lambda x: Decimal(x.decode('utf-8')))

_duplicate_label_sets = """
    SELECT *
    FROM (SELECT GROUP_CONCAT(label_id) AS label_combo
          FROM (SELECT cell_id, label_id
                FROM cell_label
                ORDER BY cell_id, label_id)
          GROUP BY cell_id)
    GROUP BY label_combo
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

_schema_items = [
    ('cell',
     """
     CREATE TABLE cell (
         cell_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
         partial INTEGER DEFAULT 0 CHECK (partial IN (0, 1))
     )
     """),
    ('hierarchy',
     """
     CREATE TABLE hierarchy (
         hierarchy_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
         hierarchy_value TEXT UNIQUE NOT NULL CHECK(hierarchy_value!='cell_id'
                                                    AND hierarchy_value NOT LIKE '%.%'),
         hierarchy_level INTEGER UNIQUE NOT NULL
     )
     """),
    ('label',
     """
     CREATE TABLE label (
         label_id INTEGER DEFAULT NULL UNIQUE,
         hierarchy_id INTEGER NOT NULL,
         label_value TEXT,
         FOREIGN KEY (hierarchy_id) REFERENCES hierarchy(hierarchy_id),
         PRIMARY KEY (label_id, hierarchy_id),
         UNIQUE (hierarchy_id, label_value)
     )
     """),
    ('nonunique_label_hierarchyid',
     """
     CREATE INDEX nonunique_label_hierarchyid ON label (hierarchy_id)
     """),
    ('AutoIncrementLabelId',
     """
     CREATE TRIGGER AutoIncrementLabelId AFTER INSERT ON label
     BEGIN
         UPDATE label
         SET label_id = (SELECT MAX(COALESCE(label_id, 0))+1 FROM label)
         WHERE label_id IS NULL;
     END
     """),
    ('MaximalCellLabel_ins',
     """
     CREATE TRIGGER MaximalCellLabel_ins BEFORE INSERT ON label
     WHEN (NEW.hierarchy_id=(SELECT hierarchy_id
                             FROM hierarchy
                             ORDER BY hierarchy_level
                             LIMIT 1)
           AND (SELECT 2 < COUNT(*)
                FROM (SELECT label_value
                      FROM label
                      WHERE NEW.hierarchy_id=hierarchy_id
                      UNION
                      SELECT 'UNMAPPED'
                      UNION
                      SELECT NEW.label_value)))
     BEGIN
         SELECT RAISE(ABORT, 'CHECK constraint failed: label (root hierarchy cannot have multiple values)');
     END
     """),
    ('MaximalCellLabel_upd',
     """
     CREATE TRIGGER MaximalCellLabel_upd BEFORE UPDATE ON label
     WHEN (NEW.hierarchy_id=(SELECT hierarchy_id
                             FROM hierarchy
                             ORDER BY hierarchy_level
                             LIMIT 1)
           AND (SELECT 2 < COUNT(*)
                FROM (SELECT label_value
                      FROM label
                      WHERE NEW.hierarchy_id=hierarchy_id
                      UNION
                      SELECT 'UNMAPPED'
                      UNION
                      SELECT NEW.label_value)))
     BEGIN
         SELECT RAISE(ABORT, 'CHECK constraint failed: label (root hierarchy cannot have multiple values)');
     END
     """),
    ('MaximalCellHierarchy_upd',
     """
     CREATE TRIGGER MaximalCellHierarchy_upd AFTER UPDATE ON hierarchy
     WHEN (SELECT 2 < COUNT(*)
           FROM (SELECT label_value
                 FROM label
                 WHERE label.hierarchy_id IN (SELECT hierarchy_id
                                              FROM hierarchy
                                              ORDER BY hierarchy_level
                                              LIMIT 1)
                 UNION
                 SELECT 'UNMAPPED'))
     BEGIN
         SELECT RAISE(ABORT, 'CHECK constraint failed: label (root hierarchy cannot have multiple values)');
     END
     """),
    ('MaximalCellHierarchy_del',
     """
     CREATE TRIGGER MaximalCellHierarchy_del AFTER DELETE ON hierarchy
     WHEN (SELECT 2 < COUNT(*)
           FROM (SELECT label_value
                 FROM label
                 WHERE label.hierarchy_id IN (SELECT hierarchy_id
                                              FROM hierarchy
                                              ORDER BY hierarchy_level
                                              LIMIT 1)
                 UNION
                 SELECT 'UNMAPPED'))
     BEGIN
         SELECT RAISE(ABORT, 'CHECK constraint failed: label (root hierarchy cannot have multiple values)');
     END
     """),
    ('cell_label',
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
     """),
    ('nonunique_celllabel_cellid',
     """
     CREATE INDEX nonunique_celllabel_cellid ON cell_label (cell_id)
     """),
    ('nonunique_celllabel_hierarchyid',
     """
     CREATE INDEX nonunique_celllabel_hierarchyid ON cell_label (hierarchy_id)
     """),
    ('nonunique_celllabel_labelid',
     """
     CREATE INDEX nonunique_celllabel_labelid ON cell_label (label_id)
     """),
    ('CheckUniqueLabels_ins',
     """
     CREATE TRIGGER CheckUniqueLabels_ins AFTER INSERT ON cell_label
     WHEN (%s)
     BEGIN
         SELECT RAISE(ABORT, 'CHECK constraint failed: cell_label (duplicate label set)');
     END
     """ % _duplicate_label_sets),
    ('CheckUniqueLabels_upd',
     """
     CREATE TRIGGER CheckUniqueLabels_upd AFTER UPDATE ON cell_label
     WHEN (%s)
     BEGIN
         SELECT RAISE(ABORT, 'CHECK constraint failed: cell_label (duplicate label set)');
     END
     """ % _duplicate_label_sets),
    ('CheckUniqueLabels_del',
     """
     CREATE TRIGGER CheckUniqueLabels_del AFTER DELETE ON cell_label
     WHEN (%s)
     BEGIN
         SELECT RAISE(ABORT, 'CHECK constraint failed: cell_label (duplicate label set)');
     END
     """ % _duplicate_label_sets),
    ('CheckUnmappedLevels_cellbl_ins',
     """
     CREATE TRIGGER CheckUnmappedLevels_cellbl_ins AFTER INSERT ON cell_label
     WHEN (%s)
     BEGIN
         SELECT RAISE(ABORT, 'CHECK constraint failed: cell_label (invalid unmapped level)');
     END
     """ % _invalid_unmapped_levels),
    ('CheckUnmappedLevels_cellbl_upd',
     """
     CREATE TRIGGER CheckUnmappedLevels_cellbl_upd AFTER UPDATE ON cell_label
     WHEN (%s)
     BEGIN
         SELECT RAISE(ABORT, 'CHECK constraint failed: cell_label (invalid unmapped level)');
     END
     """ % _invalid_unmapped_levels),
    ('CheckUnmappedLevels_hier_upd',
     """
     CREATE TRIGGER CheckUnmappedLevels_hier_upd AFTER UPDATE ON hierarchy
     WHEN (%s)
     BEGIN
         SELECT RAISE(ABORT, 'CHECK constraint failed: cell_label (invalid unmapped level)');
     END
     """ % _invalid_unmapped_levels),
    ('node',
     """
     CREATE TABLE node (
         node_id INTEGER PRIMARY KEY,
         node_hash TEXT UNIQUE ON CONFLICT REPLACE NOT NULL,
         created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
     )
     """),
    ('edge',
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
     """),
    ('AutoIncrementEdgeOrder',
     """
     CREATE TRIGGER AutoIncrementEdgeOrder AFTER INSERT ON edge
     BEGIN
         UPDATE edge
         SET edge_order = (SELECT MAX(COALESCE(edge_order, 0))+1
                           FROM edge
                           WHERE other_node_hash=NEW.other_node_hash)
         WHERE edge_order IS NULL;
     END
     """),
    ('weight',
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
     """),
    ('AutoIncrementWeightOrder',
     """
     CREATE TRIGGER AutoIncrementWeightOrder AFTER INSERT ON edge
     BEGIN
         UPDATE weight
         SET weight_order = (SELECT MAX(COALESCE(weight_order, 0))+1
                             FROM weight
                             WHERE edge_id=NEW.edge_id)
         WHERE weight_order IS NULL;
     END
     """),
    ('relation',
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
     """),
    ('relation_weight',
     """
     CREATE TABLE relation_weight (
         relation_weight_id INTEGER PRIMARY KEY,
         weight_id INTEGER,
         relation_id INTEGER,
         weight TEXTNUM,  /* <- Custom type for Python Decimals. */
         FOREIGN KEY (weight_id) REFERENCES weight(weight_id),
         FOREIGN KEY (relation_id) REFERENCES relation(relation_id)
     )
     """),
    ('property',
     """
     CREATE TABLE property (
         property_id INTEGER PRIMARY KEY,
         property_key TEXT,
         property_val TEXT,
         created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
     )
     """),
]

_expensive_constraints = ['CheckUniqueLabels_ins',
                          'CheckUniqueLabels_upd',
                          'CheckUniqueLabels_del',
                          'CheckUnmappedLevels_cellbl_ins',
                          'CheckUnmappedLevels_cellbl_upd',
                          'CheckUnmappedLevels_hier_upd']


# Mode flags.
IN_MEMORY = 1  #: Create a temporary node in RAM.
TEMP_FILE = 2  #: Write a temporary node to disk instead of using RAM.
READ_ONLY = 4  #: Connect to an existing node in read-only mode.


class _Connector(object):
    """Opens a SQLite connection to a Node database.  If a named
    Node does not exist, it is created.

    """
    def __init__(self, database=None, mode=0):
        """Creates a callable `connect` object that can be used to
        establish connections to a Node database.  Connecting to a Node
        name that does not exist will create a new Node of the given
        name.

        """
        global _schema_items
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
                raise Exception('File - %s - is not a valid node.' % database)

        else:
            if database and (not mode or self._is_read_only):
                self._database = database
            elif mode & TEMP_FILE:
                fd, temp_path = tempfile.mkstemp(suffix='.node')
                os.close(fd)
                self._database = temp_path
                self._temp_path = temp_path
            elif not database or mode & IN_MEMORY:
                self._memory_conn =  sqlite3.connect(':memory:',
                                                     detect_types=sqlite3.PARSE_DECLTYPES,
                                                     factory=_SharedConnection)

            # Populate new node.
            if self._database:
                connection = self._connect(self._database)
            else:
                connection = self._connect(self._memory_conn)
            cursor = connection.cursor()
            cursor.execute('PRAGMA synchronous=OFF')
            for _, operation in _schema_items:
                cursor.execute(operation)
            cursor.execute('PRAGMA synchronous=FULL')
            connection.close()

    def __call__(self):
        """Opens a SQLite connection to a Node database.  If a
        named Node does not exist, it is created.

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
            if sqlite3.sqlite_version_info >= (3, 8, 0):
                cursor.execute('PRAGMA query_only=1')
            else:
                sql_script = _all_read_only_triggers()
                cursor.executescript(sql_script)

        return connection

    def __del__(self):
        """Clean-up connection objects."""
        if self._memory_conn:
            self._memory_conn.close_parent()  # Permanently close!

        if self._temp_path:
            os.remove(self._temp_path)

    @staticmethod
    def _connect(database):
        if isinstance(database, sqlite3.Connection):
            return database
        return sqlite3.connect(database, detect_types=sqlite3.PARSE_DECLTYPES)

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
