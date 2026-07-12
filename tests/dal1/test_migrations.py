"""Tests for migrations module."""
import sqlite3
import unittest

from toron.dal1.migrations import (
    v020_to_v030_step01_link_table,
    v020_to_v030_step02_relation_table,
    v020_to_v030_step03_quantity_table,
    v020_to_v030_step04_rename_label_tables,
    v020_to_v030_step05_properties,
    v030_to_v031_step01_properties,
    apply_migrations,
)


FULL_NODE_SCHEMA_V_020 = """
    BEGIN TRANSACTION;

    CREATE TABLE node_index(
        index_id INTEGER PRIMARY KEY AUTOINCREMENT,
        "label_a" TEXT NOT NULL CHECK ("label_a" != '') DEFAULT '-',
        "label_b" TEXT NOT NULL CHECK ("label_b" != '') DEFAULT '-',
        "label_c" TEXT NOT NULL CHECK ("label_c" != '') DEFAULT '-'
    );
    INSERT INTO "node_index" VALUES(0,  '-',  '-',  '-');
    INSERT INTO "node_index" VALUES(1, '1A', '1B', '1C');
    INSERT INTO "node_index" VALUES(2, '2A', '2B', '2C');
    INSERT INTO "node_index" VALUES(3, '3A', '3B', '3C');

    CREATE TABLE location(
        _location_id INTEGER PRIMARY KEY,
        "label_a" TEXT NOT NULL DEFAULT '',
        "label_b" TEXT NOT NULL DEFAULT '',
        "label_c" TEXT NOT NULL DEFAULT ''
    );
    INSERT INTO "location" VALUES(1, '1A', '1B', '1C');
    INSERT INTO "location" VALUES(2, '2A', '2B', '2C');
    INSERT INTO "location" VALUES(3, '3A', '3B', '3C');

    CREATE TABLE structure(
        _structure_id INTEGER PRIMARY KEY,
        _granularity REAL,
        "label_a" INTEGER NOT NULL CHECK ("label_a" IN (0, 1)) DEFAULT 0,
        "label_b" INTEGER NOT NULL CHECK ("label_b" IN (0, 1)) DEFAULT 0,
        "label_c" INTEGER NOT NULL CHECK ("label_c" IN (0, 1)) DEFAULT 0
    );
    INSERT INTO "structure" VALUES(1,NULL,0,0,0);
    INSERT INTO "structure" VALUES(2,1.584962500721156,1,1,1);

    CREATE TABLE weight_group(
        weight_group_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        selectors TEXT_SELECTORS,
        is_complete INTEGER NOT NULL CHECK (is_complete IN (0, 1)) DEFAULT 0,
        UNIQUE (name)
    );
    INSERT INTO "weight_group" VALUES(1,'weight',NULL,NULL,1);

    CREATE TABLE weight(
        weight_id INTEGER PRIMARY KEY,
        weight_group_id INTEGER,
        index_id INTEGER CHECK (index_id > 0),
        weight_value REAL NOT NULL,
        FOREIGN KEY(weight_group_id) REFERENCES weight_group(weight_group_id) ON DELETE CASCADE,
        FOREIGN KEY(index_id) REFERENCES node_index(index_id) DEFERRABLE INITIALLY DEFERRED,
        UNIQUE (index_id, weight_group_id)
    );
    INSERT INTO "weight" VALUES(1,1,1,10.0);
    INSERT INTO "weight" VALUES(2,1,2,20.0);
    INSERT INTO "weight" VALUES(3,1,3,15.0);

    CREATE TABLE attribute_group(
        attribute_group_id INTEGER PRIMARY KEY,
        attributes TEXT_ATTRIBUTES NOT NULL,
        UNIQUE (attributes)
    );
    INSERT INTO "attribute_group" VALUES(1, '{"category": "one"}');
    INSERT INTO "attribute_group" VALUES(2, '{"category": "two"}');

    CREATE TABLE quantity(
        quantity_id INTEGER PRIMARY KEY,
        _location_id INTEGER,
        attribute_group_id INTEGER,
        quantity_value NUMERIC NOT NULL,
        FOREIGN KEY(_location_id) REFERENCES location(_location_id) ON DELETE CASCADE,
        FOREIGN KEY(attribute_group_id) REFERENCES attribute_group(attribute_group_id) ON DELETE CASCADE
    );
    INSERT INTO "quantity" VALUES(1, 1, 1, 15);
    INSERT INTO "quantity" VALUES(2, 2, 1, 10);  /* <- Same location and attrs as quantity_id 4 */
    INSERT INTO "quantity" VALUES(3, 3, 2, 20);
    INSERT INTO "quantity" VALUES(4, 2, 1, 35);  /* <- Same location and attrs as quantity_id 2 */
    INSERT INTO "quantity" VALUES(5, 1, 2, 10);

    CREATE TABLE crosswalk(
        crosswalk_id INTEGER PRIMARY KEY,
        other_unique_id TEXT NOT NULL,
        other_filename_hint TEXT,
        name TEXT NOT NULL,
        description TEXT,
        selectors TEXT_SELECTORS,
        is_default INTEGER CHECK (is_default IS NULL OR is_default=1) DEFAULT NULL,
        user_properties TEXT_USERPROPERTIES,
        other_index_hash TEXT,
        is_locally_complete INTEGER NOT NULL CHECK (is_locally_complete IN (0, 1)) DEFAULT 0,
        UNIQUE (name, other_unique_id),
        UNIQUE (is_default, other_unique_id)
    );
    INSERT INTO "crosswalk" VALUES(
        1,
        '22222222-2222-2222-22222222222222222',
        'node2.toron',
        'weight',
        NULL,
        NULL,
        1,
        NULL,
        'b78d268304863017119b485a6f58007a5df9c1368a85e460cc3d86480c4a58eb',
        1
    );

    CREATE TABLE relation(
        relation_id INTEGER PRIMARY KEY,
        crosswalk_id INTEGER,
        other_index_id INTEGER NOT NULL CHECK (TYPEOF(other_index_id) = "integer"),
        index_id INTEGER,
        mapping_level BLOB_BITFLAGS,
        relation_value REAL NOT NULL CHECK (TYPEOF(relation_value) = "real" AND 0.0 <= relation_value),
        proportion REAL CHECK (0.0 <= proportion AND proportion <= 1.0),
        FOREIGN KEY(crosswalk_id) REFERENCES crosswalk(crosswalk_id) ON DELETE CASCADE,
        FOREIGN KEY(index_id) REFERENCES node_index(index_id) DEFERRABLE INITIALLY DEFERRED,
        UNIQUE (crosswalk_id, other_index_id, index_id)
    );
    INSERT INTO main.relation VALUES(1, 1, 0, 0,  NULL,  0.0, 1.0);
    INSERT INTO main.relation VALUES(2, 1, 1, 1, X'E0', 10.0, 1.0);
    INSERT INTO main.relation VALUES(3, 1, 2, 1,  NULL, 70.0, 1.0);
    INSERT INTO main.relation VALUES(4, 1, 3, 2,  NULL, 20.0, 1.0);
    INSERT INTO main.relation VALUES(5, 1, 4, 2, X'C0', 60.0, 1.0);
    INSERT INTO main.relation VALUES(6, 1, 5, 3, X'C0', 30.0, 1.0);
    INSERT INTO main.relation VALUES(7, 1, 6, 3, X'80', 50.0, 1.0);

    CREATE TABLE property(
        key TEXT PRIMARY KEY NOT NULL,
        value TEXT_JSON
    );
    INSERT INTO "property" VALUES('toron_schema_version','"0.2.0"');
    INSERT INTO "property" VALUES('toron_app_version','"0.1.0"');
    INSERT INTO "property" VALUES('unique_id','"11111111-1111-1111-11111111111111111"');
    INSERT INTO "property" VALUES('index_hash','"c4c96cd71102046c61ec8326b2566d9e48ef2ba26d4252ba84db28ba352a0079"');
    INSERT INTO "property" VALUES('default_weight_group_id','1');
    INSERT INTO "property" VALUES('discrete_categories','[["label_b", "label_c", "label_a"]]');
    INSERT INTO "property" VALUES('domain','{"foo": "bar", "baz": "qux"}');

    CREATE UNIQUE INDEX unique_index_label_columns ON node_index("label_a", "label_b", "label_c");
    CREATE UNIQUE INDEX unique_location_label_columns ON location("label_a", "label_b", "label_c");
    CREATE UNIQUE INDEX unique_structure_label_columns ON structure("label_a", "label_b", "label_c");
    CREATE TRIGGER trigger_on_update_for_undefined
            BEFORE UPDATE ON main.node_index FOR EACH ROW WHEN OLD.index_id = 0
            BEGIN
                SELECT RAISE(FAIL, 'cannot modify undefined record (index_id 0)');
            END;
    CREATE TRIGGER trigger_on_delete_for_undefined
            BEFORE DELETE ON main.node_index FOR EACH ROW WHEN OLD.index_id = 0
            BEGIN
                SELECT RAISE(FAIL, 'cannot delete undefined record (index_id 0)');
            END;

    DELETE FROM "sqlite_sequence";
    INSERT INTO "sqlite_sequence" VALUES('node_index',3);
    COMMIT;
"""


class TestApplyMigrations(unittest.TestCase):
    def setUp(self):
        self.con = sqlite3.connect(':memory:')
        self.addCleanup(self.con.close)

        self.cur = self.con.cursor()
        self.addCleanup(self.cur.close)

        self.cur.execute('PRAGMA foreign_keys=OFF')
        self.addCleanup(self.cur.execute, 'PRAGMA foreign_keys=ON')

    def test_v020_to_v030_step01_link_table(self):
        self.cur.executescript("""
            /* Create old style (version 0.2.0) 'crosswalk' table. */
            CREATE TABLE crosswalk(
                crosswalk_id INTEGER PRIMARY KEY,
                other_unique_id TEXT NOT NULL,
                other_filename_hint TEXT,
                name TEXT NOT NULL,
                description TEXT,
                selectors TEXT_SELECTORS,
                is_default INTEGER CHECK (is_default IS NULL OR is_default=1) DEFAULT NULL,
                user_properties TEXT_USERPROPERTIES,
                other_index_hash TEXT,
                is_locally_complete INTEGER NOT NULL CHECK (is_locally_complete IN (0, 1)) DEFAULT 0,
                UNIQUE (name, other_unique_id),
                UNIQUE (is_default, other_unique_id)
            );
            INSERT INTO "crosswalk" VALUES(
                1, '1111-11-11-11111111', 'node1.toron', 'weight', NULL, NULL, 1, NULL,
                '6fc1368a85e830f9d285a64863017119b458007a5d460cc3d86480c4a58ebb78',
                1
            );
            INSERT INTO "crosswalk" VALUES(
                2, '2222-22-22-22222222', 'node2.toron', 'weight', NULL, NULL, 1, NULL,
                'b78d268304863017119b485a6f58007a5df9c1368a85e460cc3d86480c4a58eb',
                1
            );
        """)

        v020_to_v030_step01_link_table(self.cur)  # <- Function under test.

        self.cur.execute("""
            SELECT
                link_id,  /* <- New column name (was crosswalk_id). */
                other_unique_id,
                other_filename_hint,
                name,
                description,
                selectors,
                is_default,
                user_properties,
                other_index_hash,
                is_locally_complete
            FROM main.link  /* <- New table name (was main.crosswalk). */
        """)
        self.assertEqual(
            set(self.cur.fetchall()),
            {
                (1, '1111-11-11-11111111', 'node1.toron', 'weight', None, None, 1, None,
                     '6fc1368a85e830f9d285a64863017119b458007a5d460cc3d86480c4a58ebb78', 1),
                (2, '2222-22-22-22222222', 'node2.toron', 'weight', None, None, 1, None,
                     'b78d268304863017119b485a6f58007a5df9c1368a85e460cc3d86480c4a58eb', 1),
            },
        )

    def test_v020_to_v030_step02_relation_table(self):
        self.cur.executescript("""
            /* Create old style (version 0.2.0) 'relation' table. */
            CREATE TABLE relation(
                relation_id INTEGER PRIMARY KEY,
                crosswalk_id INTEGER,
                other_index_id INTEGER NOT NULL CHECK (TYPEOF(other_index_id) = "integer"),
                index_id INTEGER,
                mapping_level BLOB_BITFLAGS,
                relation_value REAL NOT NULL CHECK (TYPEOF(relation_value) = "real" AND 0.0 <= relation_value),
                proportion REAL CHECK (0.0 <= proportion AND proportion <= 1.0),
                FOREIGN KEY(crosswalk_id) REFERENCES crosswalk(crosswalk_id) ON DELETE CASCADE,
                FOREIGN KEY(index_id) REFERENCES node_index(index_id) DEFERRABLE INITIALLY DEFERRED,
                UNIQUE (crosswalk_id, other_index_id, index_id)
            );
            INSERT INTO main.relation VALUES(1, 1, 0, 0,  NULL,  0.0, 1.0);
            INSERT INTO main.relation VALUES(2, 1, 1, 1, X'E0', 10.0, 1.0);
            INSERT INTO main.relation VALUES(3, 1, 2, 1,  NULL, 70.0, 1.0);
            INSERT INTO main.relation VALUES(4, 1, 3, 2,  NULL, 20.0, 1.0);
            INSERT INTO main.relation VALUES(5, 1, 4, 2, X'C0', 60.0, 1.0);
            INSERT INTO main.relation VALUES(6, 1, 5, 3, X'C0', 30.0, 1.0);
            INSERT INTO main.relation VALUES(7, 1, 6, 3, X'80', 50.0, 1.0);
        """)

        v020_to_v030_step02_relation_table(self.cur, whole_space_level=b'\xe0')  # <- Function under test.

        self.cur.execute("""
            SELECT
                mapping_id,  /* <- New column name (was relation_id). */
                link_id,  /* <- New column name (was crosswalk_id). */
                other_index_id,
                index_id,
                mapping_level,
                mapping_value,  /* <- New column name (was relation_value). */
                proportion REAL
            FROM main.mapping  /* <- New table name (was main.relation). */
        """)
        self.assertEqual(
            set(self.cur.fetchall()),
            {                                      # <- removed undefined-to-undefined.
                (2, 1, 1, 1, b'\xe0', 10.0, 1.0),
                (3, 1, 2, 1, b'\xe0', 70.0, 1.0),  # <- mapping_level filled-in
                (4, 1, 3, 2, b'\xe0', 20.0, 1.0),  # <- mapping_level filled-in
                (5, 1, 4, 2, b'\xc0', 60.0, 1.0),
                (6, 1, 5, 3, b'\xc0', 30.0, 1.0),
                (7, 1, 6, 3, b'\x80', 50.0, 1.0),
            },
        )

    def test_v020_to_v030_step03_quantity_table(self):
        self.cur.executescript("""
            /* Create old style (version 0.2.0) 'quantity' table. */
            CREATE TABLE quantity(
                quantity_id INTEGER PRIMARY KEY,
                _location_id INTEGER,
                attribute_group_id INTEGER,
                quantity_value NUMERIC NOT NULL,
                FOREIGN KEY(_location_id) REFERENCES location(_location_id) ON DELETE CASCADE,
                FOREIGN KEY(attribute_group_id) REFERENCES attribute_group(attribute_group_id) ON DELETE CASCADE
            );
            INSERT INTO "quantity" VALUES(1, 1, 1, 15);
            INSERT INTO "quantity" VALUES(2, 2, 1, 10);  /* <- Same location and attrs as quantity_id 4 */
            INSERT INTO "quantity" VALUES(3, 3, 2, 20);
            INSERT INTO "quantity" VALUES(4, 2, 1, 35);  /* <- Same location and attrs as quantity_id 2 */
            INSERT INTO "quantity" VALUES(5, 1, 2, 10);
        """)

        v020_to_v030_step03_quantity_table(self.cur)  # <- Function under test.

        self.cur.execute('SELECT * FROM quantity')
        self.assertEqual(
            set(self.cur.fetchall()),
            {
                (1, 1, 1, 15),
                (2, 2, 1, 45),  # <- Summed value from old 2 and old 4 (10 + 35).
                (3, 3, 2, 20),
                                # <- Quantity with `id=4` no longer exists.
                (5, 1, 2, 10),
            },
        )

    def test_v020_to_v030_step04_rename_label_tables(self):
        self.cur.executescript("""
            /* Create old style (version 0.2.0) label tables. */

            CREATE TABLE node_index(
                index_id INTEGER PRIMARY KEY AUTOINCREMENT,
                "label_a" TEXT NOT NULL CHECK ("label_a" != '') DEFAULT '-',
                "label_b" TEXT NOT NULL CHECK ("label_b" != '') DEFAULT '-',
                "label_c" TEXT NOT NULL CHECK ("label_c" != '') DEFAULT '-'
            );
            INSERT INTO "node_index" VALUES(0,  '-',  '-',  '-');
            INSERT INTO "node_index" VALUES(1, '1A', '1B', '1C');
            INSERT INTO "node_index" VALUES(2, '2A', '2B', '2C');
            INSERT INTO "node_index" VALUES(3, '3A', '3B', '3C');

            CREATE TABLE location(
                _location_id INTEGER PRIMARY KEY,
                "label_a" TEXT NOT NULL DEFAULT '',
                "label_b" TEXT NOT NULL DEFAULT '',
                "label_c" TEXT NOT NULL DEFAULT ''
            );
            INSERT INTO "location" VALUES(1, '1A', '1B', '1C');
            INSERT INTO "location" VALUES(2, '2A', '2B', '2C');
            INSERT INTO "location" VALUES(3, '3A', '3B', '3C');

            CREATE TABLE structure(
                _structure_id INTEGER PRIMARY KEY,
                _granularity REAL,
                "label_a" INTEGER NOT NULL CHECK ("label_a" IN (0, 1)) DEFAULT 0,
                "label_b" INTEGER NOT NULL CHECK ("label_b" IN (0, 1)) DEFAULT 0,
                "label_c" INTEGER NOT NULL CHECK ("label_c" IN (0, 1)) DEFAULT 0
            );
            INSERT INTO "structure" VALUES(1,NULL,0,0,0);
            INSERT INTO "structure" VALUES(2,1.584962500721156,1,1,1);
        """)

        v020_to_v030_step04_rename_label_tables(self.cur)  # <- Function under test.

        self.cur.execute('SELECT * FROM label_index')
        self.assertEqual(
            set(self.cur.fetchall()),
            {(0, '-', '-', '-'),
             (1, '1A', '1B', '1C'),
             (2, '2A', '2B', '2C'),
             (3, '3A', '3B', '3C')},
        )

        self.cur.execute('SELECT * FROM label_location')
        self.assertEqual(
            set(self.cur.fetchall()),
            {(1, '1A', '1B', '1C'),
             (2, '2A', '2B', '2C'),
             (3, '3A', '3B', '3C')},
        )

        self.cur.execute('SELECT * FROM label_structure')
        self.assertEqual(
            set(self.cur.fetchall()),
            {(1, None, 0, 0, 0),
             (2, 1.584962500721156, 1, 1, 1)},
        )

    def test_v020_to_v030_step05_properties(self):
        self.cur.executescript("""
            CREATE TABLE attribute_group(
                attribute_group_id INTEGER PRIMARY KEY,
                attributes TEXT_ATTRIBUTES NOT NULL,
                UNIQUE (attributes)
            );
            INSERT INTO "attribute_group" VALUES(1, '{"A": "aaa"}');
            INSERT INTO "attribute_group" VALUES(2, '{"B": "bbb", "C": "ccc"}');

            CREATE TABLE property(
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT_JSON
            );
            INSERT INTO "property" VALUES('toron_schema_version', '"0.2.0"');
            INSERT INTO "property" VALUES('discrete_categories','[["label_b", "label_c", "label_a"]]');
            INSERT INTO "property" VALUES('domain', '{"domain": "foo_bar"}');
        """)

        v020_to_v030_step05_properties(self.cur)  # <- Function under test.

        self.cur.execute("SELECT value from property where key='toron_schema_version'")
        self.assertEqual(self.cur.fetchone()[0], '"0.3.0"')

        self.cur.execute("SELECT value from property where key='domain'")
        self.assertEqual(self.cur.fetchone()[0], '"foo_bar"')

        self.cur.execute("SELECT value from property where key='registered_attributes'")
        self.assertEqual(self.cur.fetchone()[0], '["A", "B", "C"]')

        self.cur.execute("SELECT value from property where key='discrete_categories'")
        self.assertEqual(self.cur.fetchall(), [], msg='should not be present')

        self.cur.execute("SELECT value from property where key='partition_definitions'")
        self.assertEqual(self.cur.fetchone()[0], '[["label_b", "label_c", "label_a"]]')

    def test_v030_to_v031_step01_properties(self):
        self.cur.executescript("""
            /* Create version 0.3.0 'property' and 'attribute_group' tables. */
            CREATE TABLE attribute_group(
                attribute_group_id INTEGER PRIMARY KEY,
                attributes TEXT_ATTRIBUTES NOT NULL,
                UNIQUE (attributes)
            );
            INSERT INTO "attribute_group" VALUES(1, '{"A": "aaa"}');
            INSERT INTO "attribute_group" VALUES(2, '{"B": "bbb", "C": "ccc"}');

            CREATE TABLE property(
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT_JSON
            );
            INSERT INTO "property" VALUES('toron_schema_version', '"0.3.0"');
            INSERT INTO "property" VALUES('domain', '"foo_bar"');
            INSERT INTO "property" VALUES('registered_attributes', '["A", "B", "C"]');
            INSERT INTO "property" VALUES('partition_definitions','[["label_b", "label_c", "label_a"]]');
        """)

        with self.assertLogs('app-toron', level='INFO'):
            v030_to_v031_step01_properties(self.cur)  # <- Function under test.

        self.cur.execute("SELECT value from property where key='toron_schema_version'")
        self.assertEqual(self.cur.fetchone()[0], '"0.3.1"')

        self.cur.execute("SELECT value from property where key='created_date'")
        self.assertRegex(self.cur.fetchone()[0], r'^"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00"$')

        self.cur.execute("SELECT value from property where key='user_properties'")
        self.assertEqual(self.cur.fetchone()[0], '{}')

    def test_apply_migrations(self):
        self.cur.executescript(FULL_NODE_SCHEMA_V_020)

        with self.assertLogs('app-toron', level='INFO'):
            apply_migrations(self.cur)  # <- Function under test.

        self.cur.execute("SELECT value from property where key='toron_schema_version'")
        self.assertEqual(self.cur.fetchone()[0], '"0.3.1"')

        self.cur.execute("SELECT value from property where key='domain'")
        self.assertEqual(self.cur.fetchone()[0], '"baz_qux_foo_bar"')

        self.cur.execute("SELECT value from property where key='registered_attributes'")
        self.assertEqual(self.cur.fetchone()[0], '["category"]')

        self.cur.execute('SELECT * FROM mapping')
        self.assertEqual(
            set(self.cur.fetchall()),
            {                                      # <- removed undefined-to-undefined.
                (2, 1, 1, 1, b'\xe0', 10.0, 1.0),
                (3, 1, 2, 1, b'\xe0', 70.0, 1.0),  # <- mapping_level filled-in
                (4, 1, 3, 2, b'\xe0', 20.0, 1.0),  # <- mapping_level filled-in
                (5, 1, 4, 2, b'\xc0', 60.0, 1.0),
                (6, 1, 5, 3, b'\xc0', 30.0, 1.0),
                (7, 1, 6, 3, b'\x80', 50.0, 1.0),
            },
        )

        self.cur.execute('SELECT * FROM quantity')
        self.assertEqual(
            set(self.cur.fetchall()),
            {
                (1, 1, 1, 15),
                (2, 2, 1, 45),  # <- Summed value from old 2 and old 4 (10 + 35).
                (3, 3, 2, 20),
                                # <- Quantity with `id=4` no longer exists.
                (5, 1, 2, 10),
            },
        )
