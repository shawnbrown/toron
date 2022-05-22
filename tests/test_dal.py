"""Tests for toron/node.py module."""

import gc
import os
import sqlite3
import unittest
from textwrap import dedent

from .common import TempDirTestCase

from toron._node_schema import connect
from toron._node_schema import _schema_script
from toron._node_schema import _add_functions_and_triggers
from toron._node_schema import DataAccessLayer


class TestQuoteIdentifier(unittest.TestCase):
    def test_passing_behavior(self):
        values = [
            ('abc',        '"abc"'),
            ('a b c',      '"a b c"'),      # whitepsace
            ('   abc   ',  '"abc"'),        # leading/trailing whitespace
            ('a   b\tc',   '"a b c"'),      # irregular whitepsace
            ('a\n b\r\nc', '"a b c"'),      # linebreaks
            ("a 'b' c",    '"a \'b\' c"'),  # single quotes
            ('a "b" c',    '"a ""b"" c"'),  # double quotes
        ]
        for s_in, s_out in values:
            with self.subTest(input_string=s_in, output_string=s_out):
                self.assertEqual(DataAccessLayer._quote_identifier(s_in), s_out)

    def test_surrogate_codes(self):
        """Should only allow clean UTF-8 (no surrogate codes)."""
        column_bytes = b'tama\xf1o'  # "tamaño" is Spanish for "size"
        string_with_surrogate = column_bytes.decode('utf-8', 'surrogateescape')

        with self.assertRaises(UnicodeEncodeError):
            DataAccessLayer._quote_identifier(string_with_surrogate)

    def test_nul_byte(self):
        contains_nul = 'zip\x00 code'

        with self.assertRaises(UnicodeEncodeError):
            DataAccessLayer._quote_identifier(contains_nul)


class TestAddColumnsMakeSql(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.con = connect('mynode.toron', mode='memory')
        self.cur = self.con.cursor()
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def test_add_columns_to_new(self):
        """Add columns to new/empty node database."""
        statements = DataAccessLayer._add_columns_make_sql(self.cur, ['state', 'county'])
        expected = [
            'DROP INDEX IF EXISTS unique_element_index',
            'DROP INDEX IF EXISTS unique_structure_index',
            'ALTER TABLE element ADD COLUMN "state" TEXT DEFAULT \'-\' NOT NULL',
            'ALTER TABLE location ADD COLUMN "state" TEXT',
            'ALTER TABLE structure ADD COLUMN "state" INTEGER CHECK ("state" IN (0, 1)) DEFAULT 0',
            'ALTER TABLE element ADD COLUMN "county" TEXT DEFAULT \'-\' NOT NULL',
            'ALTER TABLE location ADD COLUMN "county" TEXT',
            'ALTER TABLE structure ADD COLUMN "county" INTEGER CHECK ("county" IN (0, 1)) DEFAULT 0',
            'CREATE UNIQUE INDEX unique_element_index ON element("state", "county")',
            'CREATE UNIQUE INDEX unique_structure_index ON structure("state", "county")'
        ]
        self.assertEqual(statements, expected)

    def test_add_columns_to_exsting(self):
        """Add columns to database with existing label columns."""
        # Add initial label columns.
        statements = DataAccessLayer._add_columns_make_sql(self.cur, ['state', 'county'])
        for stmnt in statements:
            self.cur.execute(stmnt)

        # Add attitional label columns.
        statements = DataAccessLayer._add_columns_make_sql(self.cur, ['tract', 'block'])
        expected = [
            'DROP INDEX IF EXISTS unique_element_index',
            'DROP INDEX IF EXISTS unique_structure_index',
            'ALTER TABLE element ADD COLUMN "tract" TEXT DEFAULT \'-\' NOT NULL',
            'ALTER TABLE location ADD COLUMN "tract" TEXT',
            'ALTER TABLE structure ADD COLUMN "tract" INTEGER CHECK ("tract" IN (0, 1)) DEFAULT 0',
            'ALTER TABLE element ADD COLUMN "block" TEXT DEFAULT \'-\' NOT NULL',
            'ALTER TABLE location ADD COLUMN "block" TEXT',
            'ALTER TABLE structure ADD COLUMN "block" INTEGER CHECK ("block" IN (0, 1)) DEFAULT 0',
            'CREATE UNIQUE INDEX unique_element_index ON element("state", "county", "tract", "block")',
            'CREATE UNIQUE INDEX unique_structure_index ON structure("state", "county", "tract", "block")'
        ]
        self.assertEqual(statements, expected)

    def test_no_columns_to_add(self):
        """When there are no new columns to add, should return empty list."""
        # Add initial label columns.
        statements = DataAccessLayer._add_columns_make_sql(self.cur, ['state', 'county'])
        for stmnt in statements:
            self.cur.execute(stmnt)

        # When there are no new columns to add, should return empty list.
        statements = DataAccessLayer._add_columns_make_sql(self.cur, ['state', 'county'])  # <- Columns already exist.
        self.assertEqual(statements, [])

    def test_duplicate_column_input(self):
        regex = 'duplicate column name: "county"'
        with self.assertRaisesRegex(ValueError, regex):
            DataAccessLayer._add_columns_make_sql(self.cur, ['state', 'county', 'county'])

    def test_normalization_duplicate_column_input(self):
        regex = 'duplicate column name: "county"'
        with self.assertRaisesRegex(ValueError, regex):
            columns = [
                'state',
                'county    ',  # <- Normalized to "county", collides with duplicate.
                'county',
            ]
            DataAccessLayer._add_columns_make_sql(self.cur, columns)

    def test_normalization_collision_with_existing(self):
        """Columns should be checked for collisions after normalizing."""
        # Add initial label columns.
        for stmnt in DataAccessLayer._add_columns_make_sql(self.cur, ['state', 'county']):
            self.cur.execute(stmnt)

        # Prepare attitional label columns.
        columns = [
            'state     ',  # <- Normalized to "state", which then gets skipped.
            'county    ',  # <- Normalized to "county", which then gets skipped.
            'tract     ',
        ]
        statements = DataAccessLayer._add_columns_make_sql(self.cur, columns)

        expected = [
            'DROP INDEX IF EXISTS unique_element_index',
            'DROP INDEX IF EXISTS unique_structure_index',
            'ALTER TABLE element ADD COLUMN "tract" TEXT DEFAULT \'-\' NOT NULL',
            'ALTER TABLE location ADD COLUMN "tract" TEXT',
            'ALTER TABLE structure ADD COLUMN "tract" INTEGER CHECK ("tract" IN (0, 1)) DEFAULT 0',
            'CREATE UNIQUE INDEX unique_element_index ON element("state", "county", "tract")',
            'CREATE UNIQUE INDEX unique_structure_index ON structure("state", "county", "tract")'
        ]
        msg = 'should only add "tract" because "state" and "county" already exist'
        self.assertEqual(statements, expected, msg=msg)

    def test_column_id_collision(self):
        regex = 'label name not allowed: "_location_id"'
        with self.assertRaisesRegex(ValueError, regex):
            DataAccessLayer._add_columns_make_sql(self.cur, ['state', '_location_id'])


class TestMakeSqlInsertElements(unittest.TestCase):
    def setUp(self):
        self.con = connect('mynode.toron', mode='memory')
        self.cur = self.con.cursor()

        for stmnt in DataAccessLayer._add_columns_make_sql(self.cur, ['state', 'county', 'town']):
            self.cur.execute(stmnt)

        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def test_simple_case(self):
        """Insert columns that match element table."""
        columns = ['state', 'county', 'town']
        sql = DataAccessLayer._add_elements_make_sql(self.cur, columns)
        expected = 'INSERT INTO element ("state", "county", "town") VALUES (?, ?, ?)'
        self.assertEqual(sql, expected)

    def test_differently_ordered_columns(self):
        """Order should reflect given *columns* not table order."""
        columns = ['town', 'county', 'state']  # <- Reverse order from table cols.
        sql = DataAccessLayer._add_elements_make_sql(self.cur, columns)
        expected = 'INSERT INTO element ("town", "county", "state") VALUES (?, ?, ?)'
        self.assertEqual(sql, expected)

    def test_subset_of_columns(self):
        """Insert fewer column that exist in the element table."""
        columns = ['state', 'county']  # <- Does not include "town", and that's OK.
        sql = DataAccessLayer._add_elements_make_sql(self.cur, columns)
        expected = 'INSERT INTO element ("state", "county") VALUES (?, ?)'
        self.assertEqual(sql, expected)

    def test_bad_column_value(self):
        regex = 'invalid column name: "region"'
        with self.assertRaisesRegex(sqlite3.OperationalError, regex):
            DataAccessLayer._add_elements_make_sql(self.cur, ['state', 'region'])


class TestInsertWeightGetId(unittest.TestCase):
    def setUp(self):
        self.con = connect('mynode.toron', mode='memory')
        self.cur = self.con.cursor()
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def test_simple_case(self):
        name = 'myname'
        type_info = {'category': 'stuff'}
        description = 'My description.'
        weight_id = DataAccessLayer._add_weights_get_new_id(self.cur, name, type_info, description)

        actual = self.cur.execute('SELECT * FROM weight').fetchall()
        expected = [(1, 'myname', {'category': 'stuff'}, 'My description.', None)]
        self.assertEqual(actual, expected)

        msg = 'retrieved weight_id should be same as returned from function'
        retrieved_weight_id = actual[0][0]
        self.assertEqual(retrieved_weight_id, weight_id, msg=msg)


class TestAddWeightsMakeSql(unittest.TestCase):
    def setUp(self):
        self.con = connect('mynode.toron', mode='memory')
        self.cur = self.con.cursor()

        for stmnt in DataAccessLayer._add_columns_make_sql(self.cur, ['state', 'county', 'town']):
            self.cur.execute(stmnt)

        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def test_all_columns(self):
        columns = ['state', 'county', 'town']
        sql = DataAccessLayer._add_weights_make_sql(self.cur, columns)
        expected = """
            INSERT INTO element_weight (weight_id, element_id, value)
            SELECT ? AS weight_id, element_id, ? AS value
            FROM element
            WHERE "state"=? AND "county"=? AND "town"=?
            GROUP BY "state", "county", "town"
            HAVING COUNT(*)=1
        """
        self.assertEqual(
            dedent(sql).strip(),
            dedent(expected).strip(),
        )

    def test_subset_of_columns(self):
        columns = ['state', 'county']
        sql = DataAccessLayer._add_weights_make_sql(self.cur, columns)
        expected = """
            INSERT INTO element_weight (weight_id, element_id, value)
            SELECT ? AS weight_id, element_id, ? AS value
            FROM element
            WHERE "state"=? AND "county"=?
            GROUP BY "state", "county"
            HAVING COUNT(*)=1
        """
        self.assertEqual(
            dedent(sql).strip(),
            dedent(expected).strip(),
        )

    def test_invalid_column(self):
        regex = 'invalid column name: "region"'
        with self.assertRaisesRegex(sqlite3.OperationalError, regex):
            columns = ['state', 'county', 'region']
            sql = DataAccessLayer._add_weights_make_sql(self.cur, columns)


class TestUpdateWeightIsComplete(unittest.TestCase):
    def setUp(self):
        self.con = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES, isolation_level=None)
        self.con.executescript(_schema_script)  # Create database schema.
        _add_functions_and_triggers(self.con)
        self.cur = self.con.cursor()

        self.columns = ['label_a', 'label_b']
        for stmnt in DataAccessLayer._add_columns_make_sql(self.cur, self.columns):
            self.cur.execute(stmnt)
        sql = DataAccessLayer._add_elements_make_sql(self.cur, self.columns)
        iterator = [
            ('X', '001'),
            ('Y', '001'),
            ('Z', '002'),
        ]
        self.cur.executemany(sql, iterator)

        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def test_complete(self):
        weight_id = DataAccessLayer._add_weights_get_new_id(self.cur, 'tot10', {'category': 'census'})

        # Insert element_weight records.
        iterator = [
            (weight_id, 12, 'X', '001'),
            (weight_id, 35, 'Y', '001'),
            (weight_id, 20, 'Z', '002'),
        ]
        sql = DataAccessLayer._add_weights_make_sql(self.cur, self.columns)
        self.cur.executemany(sql, iterator)

        DataAccessLayer._add_weights_set_is_complete(self.cur, weight_id)  # <- Update is_complete!

        # Check is_complete flag.
        self.cur.execute('SELECT is_complete FROM weight WHERE weight_id=?', (weight_id,))
        result = self.cur.fetchone()
        self.assertEqual(result, (1,), msg='weight is complete, should be 1')

    def test_incomplete(self):
        weight_id = DataAccessLayer._add_weights_get_new_id(self.cur, 'tot10', {'category': 'census'})

        # Insert element_weight records.
        iterator = [
            (weight_id, 12, 'X', '001'),
            (weight_id, 35, 'Y', '001'),
        ]
        sql = DataAccessLayer._add_weights_make_sql(self.cur, self.columns)
        self.cur.executemany(sql, iterator)

        DataAccessLayer._add_weights_set_is_complete(self.cur, weight_id)  # <- Update is_complete!

        # Check is_complete flag.
        self.cur.execute('SELECT is_complete FROM weight WHERE weight_id=?', (weight_id,))
        result = self.cur.fetchone()
        self.assertEqual(result, (0,), msg='weight is incomplete, should be 0')


class TestDataAccessLayerOnDisk(TempDirTestCase):
    def setUp(self):
        self.addCleanup(self.cleanup_temp_files)

    def test_on_disk(self):
        path = 'mynode.toron'
        self.assertFalse(os.path.isfile(path))
        dal = DataAccessLayer(path)

        del dal
        gc.collect()  # Explicitly trigger full garbage collection.

        msg = 'data should persist as a file on disk'
        self.assertTrue(os.path.isfile(path), msg=msg)


class TestDataAccessLayer(unittest.TestCase):
    def test_in_memory(self):
        path = 'mem1'
        self.assertFalse(os.path.isfile(path), msg='file should not already exist')
        dal = DataAccessLayer(path, mode='memory')

        msg = 'should not be saved as file, should by in-memory only'
        self.assertFalse(os.path.isfile(path), msg=msg)

        connection = dal._connection

        dummy_query = 'SELECT 42'  # To check connection status.
        cur = connection.execute(dummy_query)
        msg = 'in-memory connections should remain open after instantiation'
        self.assertEqual(cur.fetchone(), (42,), msg=msg)

        del dal
        gc.collect()  # Explicitly trigger full garbage collection.

        regex = 'closed database'
        msg = 'connection should be closed when DAL is garbage collected'
        with self.assertRaisesRegex(sqlite3.ProgrammingError, regex, msg=msg):
            connection.execute(dummy_query)

    @staticmethod
    def get_column_names(connection_or_cursor, table):
        cur = connection_or_cursor.execute(f'PRAGMA table_info({table})')
        return [row[1] for row in cur.fetchall()]

    def test_add_columns(self):
        path = 'mynode.toron'
        dal = DataAccessLayer(path, mode='memory')
        dal.add_columns(['state', 'county'])  # <- Add columns.

        con = dal._connection

        columns = self.get_column_names(con, 'element')
        self.assertEqual(columns, ['element_id', 'state', 'county'])

        columns = self.get_column_names(con, 'location')
        self.assertEqual(columns, ['_location_id', 'state', 'county'])

        columns = self.get_column_names(con, 'structure')
        self.assertEqual(columns, ['_structure_id', 'state', 'county'])

    def test_add_elements(self):
        path = 'mynode.toron'
        dal = DataAccessLayer(path, mode='memory')
        dal.add_columns(['state', 'county'])  # <- Add columns.

        elements = [
            ('IA', 'POLK'),
            ('IN', 'LA PORTE'),
            ('MN', 'HENNEPIN '),
        ]
        dal.add_elements(elements, columns=['state', 'county'])

        con = dal._connection
        result = con.execute('SELECT * FROM element').fetchall()
        expected = [
            (1, 'IA', 'POLK'),
            (2, 'IN', 'LA PORTE'),
            (3, 'MN', 'HENNEPIN '),
        ]
        self.assertEqual(result, expected)

    def test_add_elements_no_column_arg(self):
        path = 'mynode.toron'
        dal = DataAccessLayer(path, mode='memory')
        dal.add_columns(['state', 'county'])  # <- Add columns.

        elements = [
            ('state', 'county'),  # <- Header row.
            ('IA', 'POLK'),
            ('IN', 'LA PORTE'),
            ('MN', 'HENNEPIN '),
        ]
        dal.add_elements(elements) # <- No *columns* argument given.

        con = dal._connection
        result = con.execute('SELECT * FROM element').fetchall()
        expected = [
            (1, 'IA', 'POLK'),
            (2, 'IN', 'LA PORTE'),
            (3, 'MN', 'HENNEPIN '),
        ]
        self.assertEqual(result, expected)

    def test_add_elements_column_subset(self):
        """Omitted columns should get default value ('-')."""
        path = 'mynode.toron'
        dal = DataAccessLayer(path, mode='memory')
        dal.add_columns(['state', 'county'])  # <- Add columns.

        # Element rows include "state" but not "county".
        elements = [
            ('state',),  # <- Header row.
            ('IA',),
            ('IN',),
            ('MN',),
        ]
        dal.add_elements(elements) # <- No *columns* argument given.

        con = dal._connection
        result = con.execute('SELECT * FROM element').fetchall()
        expected = [
            (1, 'IA', '-'),  # <- "county" gets default '-'
            (2, 'IN', '-'),  # <- "county" gets default '-'
            (3, 'MN', '-'),  # <- "county" gets default '-'
        ]
        self.assertEqual(result, expected)

    def test_add_elements_column_superset(self):
        """Surplus columns should be filtered-out before loading."""
        path = 'mynode.toron'
        dal = DataAccessLayer(path, mode='memory')
        dal.add_columns(['state', 'county'])  # <- Add columns.

        # Element rows include unknown columns "region" and "group".
        elements = [
            ('region', 'state', 'group',  'county'),  # <- Header row.
            ('WNC',    'IA',    'GROUP2', 'POLK'),
            ('ENC',    'IN',    'GROUP7', 'LA PORTE'),
            ('WNC',    'MN',    'GROUP1', 'HENNEPIN '),
        ]
        dal.add_elements(elements) # <- No *columns* argument given.

        con = dal._connection
        result = con.execute('SELECT * FROM element').fetchall()
        expected = [
            (1, 'IA', 'POLK'),
            (2, 'IN', 'LA PORTE'),
            (3, 'MN', 'HENNEPIN '),
        ]
        self.assertEqual(result, expected)


class TestDataAccessLayerAddWeights(unittest.TestCase):
    """Tests for dal.add_weights() method."""
    def setUp(self):
        self.path = 'mynode.toron'
        self.dal = DataAccessLayer(self.path, mode='memory')
        self.dal.add_columns(['state', 'county', 'tract'])
        self.dal.add_elements([
            ('state', 'county', 'tract'),
            ('12', '001', '000200'),
            ('12', '003', '040101'),
            ('12', '003', '040102'),
            ('12', '005', '000300'),
            ('12', '007', '000200'),
            ('12', '011', '010401'),
            ('12', '011', '010601'),
            ('12', '017', '450302'),
            ('12', '019', '030202'),
        ])

        con = self.dal._connection
        self.cursor = con.cursor()
        self.addCleanup(con.close)
        self.addCleanup(self.cursor.close)

    def test_full_column_match(self):
        columns = ('state', 'county', 'tract', 'pop10')
        weights = [
            ('12', '001', '000200', 110),
            ('12', '003', '040101', 212),
            ('12', '003', '040102', 17),
            ('12', '005', '000300', 10),
            ('12', '007', '000200', 414),
            ('12', '011', '010401', 223),
            ('12', '011', '010601', 141),
            ('12', '017', '450302', 183),
            ('12', '019', '030202', 62),
        ]
        self.dal.add_weights(weights, columns, name='pop10', type_info={'category': 'census'})

        self.cursor.execute('SELECT * FROM weight')
        self.assertEqual(
            self.cursor.fetchall(),
            [(1, 'pop10', {'category': 'census'}, None, 1)],  # <- is_complete is 1
        )

        self.cursor.execute("""
            SELECT state, county, tract, value
            FROM element
            NATURAL JOIN element_weight
            WHERE weight_id=1
        """)
        self.assertEqual(set(self.cursor.fetchall()), set(weights))

    def test_skip_non_unique_matches(self):
        """Should only insert weights that match to a single element."""
        weights = [
            ('state', 'county', 'pop10'),
            ('12', '001', 110),
            ('12', '003', 229),  # <- Matches multiple elements.
            ('12', '005', 10),
            ('12', '007', 414),
            ('12', '011', 364),  # <- Matches multiple elements.
            ('12', '017', 183),
            ('12', '019', 62),
        ]
        self.dal.add_weights(weights, name='pop10', type_info={'category': 'census'})

        self.cursor.execute('SELECT * FROM weight')
        self.assertEqual(
            self.cursor.fetchall(),
            [(1, 'pop10', {'category': 'census'}, None, 0)],  # <- is_complete is 0
        )

        # Get loaded weights.
        self.cursor.execute("""
            SELECT state, county, value
            FROM element
            JOIN element_weight USING (element_id)
            WHERE weight_id=1
        """)
        result = self.cursor.fetchall()

        expected = [
            ('12', '001', 110),
            #('12', '003', 229),  <- Not included because no unique match.
            ('12', '005', 10),
            ('12', '007', 414),
            #('12', '011', 364),  <- Not included because no unique match.
            ('12', '017', 183),
            ('12', '019', 62),
        ]
        self.assertEqual(set(result), set(expected))

    @unittest.expectedFailure
    def test_match_by_element_id(self):
        raise NotImplementedError

    @unittest.expectedFailure
    def test_mismatched_labels_and_element_id(self):
        raise NotImplementedError

