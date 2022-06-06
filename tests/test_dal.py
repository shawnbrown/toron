"""Tests for toron/node.py module."""

import gc
import os
import sqlite3
import unittest
from collections import OrderedDict
from textwrap import dedent

from .common import get_column_names
from .common import TempDirTestCase

from toron._node_schema import connect
from toron._node_schema import _schema_script
from toron._node_schema import _add_functions_and_triggers
from toron._dal import DataAccessLayer
from toron._dal import DataAccessLayerPre24
from toron._dal import DataAccessLayerPre25
from toron._dal import DataAccessLayerPre35
from toron._dal import dal_class
from toron._exceptions import ToronError


SQLITE_VERSION_INFO = sqlite3.sqlite_version_info


class TestDataAccessLayerOnDisk(TempDirTestCase):
    def setUp(self):
        self.addCleanup(self.cleanup_temp_files)

    def test_init_on_disk(self):
        path = 'mynode.toron'
        self.assertFalse(os.path.isfile(path))
        dal = dal_class(path)

        del dal
        gc.collect()  # Explicitly trigger full garbage collection.

        msg = 'data should persist as a file on disk'
        self.assertTrue(os.path.isfile(path), msg=msg)


class TestDataAccessLayerInMemory(unittest.TestCase):
    def test_init_in_memory(self):
        path = 'mem1'
        self.assertFalse(os.path.isfile(path), msg='file should not already exist')
        dal = dal_class(path, mode='memory')

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


class TestAddColumns(unittest.TestCase):
    def test_add_columns(self):
        """Check that columns are added to appropriate tables."""
        path = 'mynode.toron'
        dal = DataAccessLayer(path, mode='memory')
        dal.set_data({'add_columns': ['state', 'county']})  # <- Add columns.

        con = dal._connection

        columns = get_column_names(con, 'element')
        self.assertEqual(columns, ['element_id', 'state', 'county'])

        columns = get_column_names(con, 'location')
        self.assertEqual(columns, ['_location_id', 'state', 'county'])

        columns = get_column_names(con, 'structure')
        self.assertEqual(columns, ['_structure_id', 'state', 'county'])

    def test_set_data_order(self):
        """The set_data() method should run 'add_columns' items first."""
        dal = DataAccessLayer('mynode.toron', mode='memory')

        mapping = OrderedDict([
            ('structure', [{'state'}, {'county'}, {'state', 'county'}]),
            ('add_columns', ['state', 'county']),
        ])

        try:
            dal.set_data(mapping)  # <- Should pass without error.
        except ToronError as err:
            if 'must first add columns' not in str(err):
                raise
            msg = "should run 'add_columns' first, regardless of mapping order"
            self.fail(msg)


class TestRenameColumnsApplyMapper(unittest.TestCase):
    def setUp(self):
        self.dal = DataAccessLayer('mynode.toron', mode='memory')
        self.dal.set_data({'add_columns': ['state', 'county', 'town']})
        self.con = self.dal._connection
        self.cur = self.con.cursor()
        self.addCleanup(self.cur.close)

    def test_mapper_callable(self):
        mapper = str.upper  # <- Callable mapper.
        result = self.dal._rename_columns_apply_mapper(self.cur, mapper)
        column_names, new_column_names = result  # Unpack result tuple
        self.assertEqual(column_names, ['"state"', '"county"', '"town"'])
        self.assertEqual(new_column_names, ['"STATE"', '"COUNTY"', '"TOWN"'])

    def test_mapper_dict(self):
        mapper = {'state': 'stusab', 'town': 'place'}  # <- Dict mapper.
        result = self.dal._rename_columns_apply_mapper(self.cur, mapper)
        column_names, new_column_names = result  # Unpack result tuple
        self.assertEqual(column_names, ['"state"', '"county"', '"town"'])
        self.assertEqual(new_column_names, ['"stusab"', '"county"', '"place"'])

    def test_mapper_bad_type(self):
        mapper = ['state', 'stusab']  # <- Bad mapper type.
        with self.assertRaises(ValueError):
            result = self.dal._rename_columns_apply_mapper(self.cur, mapper)

    def test_name_collision(self):
        regex = 'column name collisions: "(state|town)"->"XXXX", "(town|state)"->"XXXX"'
        with self.assertRaisesRegex(ValueError, regex):
            mapper = {'state': 'XXXX', 'county': 'COUNTY', 'town': 'XXXX'}
            result = self.dal._rename_columns_apply_mapper(self.cur, mapper)

    def test_name_collision_from_normalization(self):
        regex = 'column name collisions: "(state|town)"->"A B", "(town|state)"->"A B"'
        with self.assertRaisesRegex(ValueError, regex):
            mapper = {'state': 'A\t\tB', 'town': 'A    B    '}  # <- Gets normalized.
            result = self.dal._rename_columns_apply_mapper(self.cur, mapper)


class TestRenameColumnsMakeSql(unittest.TestCase):
    def setUp(self):
        self.column_names = ['"state"', '"county"', '"town"']
        self.new_column_names = ['"stusab"', '"county"', '"place"']

    @unittest.skipIf(SQLITE_VERSION_INFO < (3, 25, 0), 'requires 3.25.0 or newer')
    def test_native_rename_column_support(self):
        """Test native RENAME COLUMN statements."""
        sql = DataAccessLayer._rename_columns_make_sql(self.column_names, self.new_column_names)
        expected = [
            'ALTER TABLE element RENAME COLUMN "state" TO "stusab"',
            'ALTER TABLE location RENAME COLUMN "state" TO "stusab"',
            'ALTER TABLE structure RENAME COLUMN "state" TO "stusab"',
            'ALTER TABLE element RENAME COLUMN "town" TO "place"',
            'ALTER TABLE location RENAME COLUMN "town" TO "place"',
            'ALTER TABLE structure RENAME COLUMN "town" TO "place"',
        ]
        self.assertEqual(sql, expected)

    def test_pre25_without_native_rename(self):
        """Test legacy column-rename statements for workaround procedure."""
        sql = DataAccessLayerPre25._rename_columns_make_sql(self.column_names, self.new_column_names)
        expected = [
            'CREATE TABLE new_element(element_id INTEGER PRIMARY KEY AUTOINCREMENT, "stusab" TEXT DEFAULT \'-\' NOT NULL, "county" TEXT DEFAULT \'-\' NOT NULL, "place" TEXT DEFAULT \'-\' NOT NULL)',
            'INSERT INTO new_element SELECT element_id, "state", "county", "town" FROM element',
            'DROP TABLE element',
            'ALTER TABLE new_element RENAME TO element',
            'CREATE TABLE new_location(_location_id INTEGER PRIMARY KEY, "stusab" TEXT, "county" TEXT, "place" TEXT)',
            'INSERT INTO new_location SELECT _location_id, "state", "county", "town" FROM location',
            'DROP TABLE location',
            'ALTER TABLE new_location RENAME TO location',
            'CREATE TABLE new_structure(_structure_id INTEGER PRIMARY KEY, "stusab" INTEGER CHECK ("stusab" IN (0, 1)) DEFAULT 0, "county" INTEGER CHECK ("county" IN (0, 1)) DEFAULT 0, "place" INTEGER CHECK ("place" IN (0, 1)) DEFAULT 0)',
            'INSERT INTO new_structure SELECT _structure_id, "state", "county", "town" FROM structure',
            'DROP TABLE structure',
            'ALTER TABLE new_structure RENAME TO structure',
            'CREATE UNIQUE INDEX unique_element_index ON element("stusab", "county", "place")',
            'CREATE UNIQUE INDEX unique_structure_index ON structure("stusab", "county", "place")',
        ]
        self.assertEqual(sql, expected)


class TestRenameColumns(unittest.TestCase):
    def setUp(self):
        self.path = 'mynode.toron'
        self.dal = dal_class(self.path, mode='memory')
        self.dal.set_data({'add_columns': ['state', 'county', 'town']})
        self.dal.add_elements([
            ('state', 'county', 'town'),
            ('CA', 'SAN DIEGO', 'CORONADO'),
            ('IN', 'GRANT', 'MARION'),
            ('CA', 'MARIN', 'SAN RAFAEL'),
            ('CA', 'MARIN', 'SAUSALITO'),
            ('AR', 'MILLER', 'TEXARKANA'),
            ('TX', 'BOWIE', 'TEXARKANA'),
        ])
        self.con = self.dal._connection
        self.cur = self.con.cursor()
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def run_rename_test(self, rename_columns_func):
        columns_before_rename = get_column_names(self.cur, 'element')
        self.assertEqual(columns_before_rename, ['element_id', 'state', 'county', 'town'])

        data_before_rename = \
            self.cur.execute('SELECT state, county, town from element').fetchall()

        mapper = {'state': 'stusab', 'town': 'place'}
        rename_columns_func(self.dal, mapper)  # <- Rename columns!

        columns_after_rename = get_column_names(self.cur, 'element')
        self.assertEqual(columns_after_rename, ['element_id', 'stusab', 'county', 'place'])

        data_after_rename = \
            self.cur.execute('SELECT stusab, county, place from element').fetchall()

        self.assertEqual(data_before_rename, data_after_rename)

    @unittest.skipIf(SQLITE_VERSION_INFO < (3, 25, 0), 'requires 3.25.0 or newer')
    def test_rename_columns(self):
        """Test the native RENAME COLUMN implementation."""
        self.run_rename_test(DataAccessLayer.rename_columns)

    def test_legacy_rename_columns(self):
        """Test the alternate legacy implementation."""
        self.run_rename_test(DataAccessLayerPre25.rename_columns)

    def test_data_access_layer_rename_columns(self):
        """Test the assigned 'dal_class' class."""
        self.run_rename_test(dal_class.rename_columns)


class TestRemoveColumnsMakeSql(unittest.TestCase):
    def setUp(self):
        self.column_names = ['"state"', '"county"', '"mcd"', '"place"']
        self.columns_to_remove = ['"mcd"', '"place"']

    @unittest.skipIf(SQLITE_VERSION_INFO < (3, 35, 0), 'requires 3.35.0 or newer')
    def test_native_delete_column_support(self):
        sql_stmnts = DataAccessLayer._remove_columns_make_sql(self.column_names, self.columns_to_remove)
        expected = [
            'DROP INDEX IF EXISTS unique_element_index',
            'DROP INDEX IF EXISTS unique_structure_index',
            'ALTER TABLE main.element DROP COLUMN "mcd"',
            'ALTER TABLE main.location DROP COLUMN "mcd"',
            'ALTER TABLE main.structure DROP COLUMN "mcd"',
            'ALTER TABLE main.element DROP COLUMN "place"',
            'ALTER TABLE main.location DROP COLUMN "place"',
            'ALTER TABLE main.structure DROP COLUMN "place"',
            'CREATE UNIQUE INDEX unique_element_index ON element("state", "county")',
            'CREATE UNIQUE INDEX unique_structure_index ON structure("state", "county")',
        ]
        self.assertEqual(sql_stmnts, expected)

    def test_pre35_without_native_drop(self):
        """Check SQL of column removal procedure for legacy SQLite."""
        sql_stmnts = DataAccessLayerPre35._remove_columns_make_sql(self.column_names, self.columns_to_remove)
        expected = [
            'CREATE TABLE new_element(element_id INTEGER PRIMARY KEY AUTOINCREMENT, "state" TEXT DEFAULT \'-\' NOT NULL, "county" TEXT DEFAULT \'-\' NOT NULL)',
            'INSERT INTO new_element SELECT element_id, "state", "county" FROM element',
            'DROP TABLE element',
            'ALTER TABLE new_element RENAME TO element',
            'CREATE TABLE new_location(_location_id INTEGER PRIMARY KEY, "state" TEXT, "county" TEXT)',
            'INSERT INTO new_location SELECT _location_id, "state", "county" FROM location',
            'DROP TABLE location',
            'ALTER TABLE new_location RENAME TO location',
            'CREATE TABLE new_structure(_structure_id INTEGER PRIMARY KEY, "state" INTEGER CHECK ("state" IN (0, 1)) DEFAULT 0, "county" INTEGER CHECK ("county" IN (0, 1)) DEFAULT 0)',
            'INSERT INTO new_structure SELECT _structure_id, "state", "county" FROM structure',
            'DROP TABLE structure',
            'ALTER TABLE new_structure RENAME TO structure',
            'CREATE UNIQUE INDEX unique_element_index ON element("state", "county")',
            'CREATE UNIQUE INDEX unique_structure_index ON structure("state", "county")',
        ]
        self.assertEqual(sql_stmnts, expected)


class TestRemoveColumnsMixin(object):
    class_under_test = None  # When subclassing, assign DAL class to test.

    def setUp(self):
        self.dal = self.class_under_test('mynode.toron', mode='memory')

        con = self.dal._get_connection()
        self.addCleanup(con.close)

        self.cur = con.cursor()
        self.addCleanup(self.cur.close)

        self.dal.set_data({
            'add_columns': ['state', 'county', 'mcd', 'place'],
            'discrete_categories': [
                {'state'},
                {'state', 'county'},
                {'state', 'county', 'mcd'},
            ],
        })
        data = [
            ('state', 'county', 'mcd', 'place', 'population'),
            ('AZ', 'Graham', 'Safford', 'Cactus Flats', 1524),
            ('CA', 'Los Angeles', 'Newhall', 'Val Verde', 2399),
            ('CA', 'Riverside', 'Corona', 'Coronita', 2639),
            ('CA', 'San Benito', 'Hollister', 'Ridgemark', 3212),
            ('IN', 'LaPorte', 'Kankakee', 'Rolling Prairie', 562),
            ('MO', 'Cass', 'Raymore', 'Belton', 6259),
            ('OH', 'Franklin', 'Washington', 'Dublin', 40734),
            ('PA', 'Somerset', 'Somerset', 'Somerset', 6048),
            ('TX', 'Denton', 'Denton', 'Denton', 102631),
            ('TX', 'Cass', 'Atlanta', 'Queen City', 1397),
        ]
        self.dal.add_elements(data)
        self.dal.add_weights(data, name='population', type_info={'category': 'population'})

    def test_remove_columns(self):
        self.dal.remove_columns(['mcd', 'place'])  # <- Method under test.

        # Check rebuilt categories.
        data = self.dal.get_data(['discrete_categories'])
        self.assertEqual(data['discrete_categories'], [{'state'}, {'county', 'state'}])

        # Check elements and weights.
        actual = self.cur.execute('''
            SELECT a.*, b.value
            FROM element a
            JOIN element_weight b USING (element_id)
            JOIN weight c USING (weight_id)
            WHERE c.name='population'
        ''').fetchall()

        expected = [
            (1, 'AZ', 'Graham', 1524),
            (2, 'CA', 'Los Angeles', 2399),
            (3, 'CA', 'Riverside', 2639),
            (4, 'CA', 'San Benito', 3212),
            (5, 'IN', 'LaPorte', 562),
            (6, 'MO', 'Cass', 6259),
            (7, 'OH', 'Franklin', 40734),
            (8, 'PA', 'Somerset', 6048),
            (9, 'TX', 'Denton', 102631),
            (10, 'TX', 'Cass', 1397),
        ]

        self.assertEqual(actual, expected)

    def test_nonmatching_names(self):
        """Non-matching column names should be ignored."""
        self.dal.remove_columns(['nomatch1', 'nomatch2'])  # <- Method under test.

    def test_category_violation(self):
        regex = "cannot remove, categories are undefined for remaining columns: 'place'"
        with self.assertRaisesRegex(ToronError, regex):
            self.dal.remove_columns(['mcd'])  # <- Method under test.

    def test_category_override(self):
        self.dal.remove_columns(['mcd'], strategy='restructure')  # <- Method under test.

        # Check rebuilt categories.
        data = self.dal.get_data(['discrete_categories'])
        self.assertEqual(data['discrete_categories'], [{'state'}, {'county', 'state'}])

        # Check elements and weights.
        actual = self.cur.execute('''
            SELECT a.*, b.value
            FROM element a
            JOIN element_weight b USING (element_id)
            JOIN weight c USING (weight_id)
            WHERE c.name='population'
        ''').fetchall()
        expected = [
            (1, 'AZ', 'Graham', 'Cactus Flats', 1524),
            (2, 'CA', 'Los Angeles', 'Val Verde', 2399),
            (3, 'CA', 'Riverside', 'Coronita', 2639),
            (4, 'CA', 'San Benito', 'Ridgemark', 3212),
            (5, 'IN', 'LaPorte', 'Rolling Prairie', 562),
            (6, 'MO', 'Cass', 'Belton', 6259),
            (7, 'OH', 'Franklin', 'Dublin', 40734),
            (8, 'PA', 'Somerset', 'Somerset', 6048),
            (9, 'TX', 'Denton', 'Denton', 102631),
            (10, 'TX', 'Cass', 'Queen City', 1397),
        ]
        self.assertEqual(actual, expected)

    def test_granularity_error(self):
        regex = 'cannot remove, columns are needed to preserve granularity'
        with self.assertRaisesRegex(ToronError, regex):
            self.dal.remove_columns(['county', 'mcd', 'place'])  # <- Method under test.


@unittest.skipIf(SQLITE_VERSION_INFO < (3, 35, 0), 'requires 3.35.0 or newer')
class TestRemoveColumns(TestRemoveColumnsMixin, unittest.TestCase):
    class_under_test = DataAccessLayer


class TestRemoveColumnsLegacy(TestRemoveColumnsMixin, unittest.TestCase):
    class_under_test = DataAccessLayerPre24


class TestAddElementsMakeSql(unittest.TestCase):
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


class TestAddElements(unittest.TestCase):
    def test_add_elements(self):
        path = 'mynode.toron'
        dal = DataAccessLayer(path, mode='memory')
        dal.set_data({'add_columns': ['state', 'county']})  # <- Add columns.

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
        dal.set_data({'add_columns': ['state', 'county']})  # <- Add columns.

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
        dal.set_data({'add_columns': ['state', 'county']})  # <- Add columns.

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
        dal.set_data({'add_columns': ['state', 'county']})  # <- Add columns.

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


class TestAddWeightsGetNewId(unittest.TestCase):
    def setUp(self):
        self.con = connect('mynode.toron', mode='memory')
        self.cur = self.con.cursor()
        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def run_func_test(self, func):
        name = 'myname'
        type_info = {'category': 'stuff'}
        description = 'My description.'

        weight_id = func(self.cur, name, type_info, description)  # <- Test the function.

        actual = self.cur.execute('SELECT * FROM weight').fetchall()
        expected = [(1, name, type_info, description, None)]
        self.assertEqual(actual, expected)

        msg = 'retrieved weight_id should be same as returned from function'
        retrieved_weight_id = actual[0][0]
        self.assertEqual(retrieved_weight_id, weight_id, msg=msg)

    @unittest.skipIf(SQLITE_VERSION_INFO < (3, 35, 0), 'requires 3.35.0 or newer')
    def test_with_returning_clause(self):
        self.run_func_test(DataAccessLayer._add_weights_get_new_id)

    def test_pre35_without_returning_clause(self):
        self.run_func_test(DataAccessLayerPre35._add_weights_get_new_id)


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


class TestAddWeightsSetIsComplete(unittest.TestCase):
    def setUp(self):
        self.con = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES, isolation_level=None)
        self.con.executescript(_schema_script)  # Create database schema.
        _add_functions_and_triggers(self.con)
        self.cur = self.con.cursor()

        self.columns = ['label_a', 'label_b']
        for stmnt in dal_class._add_columns_make_sql(self.cur, self.columns):
            self.cur.execute(stmnt)
        sql = dal_class._add_elements_make_sql(self.cur, self.columns)
        iterator = [
            ('X', '001'),
            ('Y', '001'),
            ('Z', '002'),
        ]
        self.cur.executemany(sql, iterator)

        self.addCleanup(self.con.close)
        self.addCleanup(self.cur.close)

    def test_complete(self):
        weight_id = dal_class._add_weights_get_new_id(self.cur, 'tot10', {'category': 'census'})

        # Insert element_weight records.
        iterator = [
            (weight_id, 12, 'X', '001'),
            (weight_id, 35, 'Y', '001'),
            (weight_id, 20, 'Z', '002'),
        ]
        sql = dal_class._add_weights_make_sql(self.cur, self.columns)
        self.cur.executemany(sql, iterator)

        dal_class._add_weights_set_is_complete(self.cur, weight_id)  # <- Update is_complete!

        # Check is_complete flag.
        self.cur.execute('SELECT is_complete FROM weight WHERE weight_id=?', (weight_id,))
        result = self.cur.fetchone()
        self.assertEqual(result, (1,), msg='weight is complete, should be 1')

    def test_incomplete(self):
        weight_id = dal_class._add_weights_get_new_id(self.cur, 'tot10', {'category': 'census'})

        # Insert element_weight records.
        iterator = [
            (weight_id, 12, 'X', '001'),
            (weight_id, 35, 'Y', '001'),
        ]
        sql = dal_class._add_weights_make_sql(self.cur, self.columns)
        self.cur.executemany(sql, iterator)

        dal_class._add_weights_set_is_complete(self.cur, weight_id)  # <- Update is_complete!

        # Check is_complete flag.
        self.cur.execute('SELECT is_complete FROM weight WHERE weight_id=?', (weight_id,))
        result = self.cur.fetchone()
        self.assertEqual(result, (0,), msg='weight is incomplete, should be 0')


class TestAddWeights(unittest.TestCase):
    """Tests for dal.add_weights() method."""
    def setUp(self):
        self.path = 'mynode.toron'
        self.dal = dal_class(self.path, mode='memory')
        self.dal.set_data({'add_columns': ['state', 'county', 'tract']})
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


class TestGetAndSetDataProperty(unittest.TestCase):
    class_under_test = dal_class  # Use auto-assigned DAL class.

    def setUp(self):
        self.dal = self.class_under_test('mynode.toron', mode='memory')

        connection = self.dal._get_connection()
        self.addCleanup(connection.close)

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

        self.cursor.execute('''
            INSERT INTO property
            VALUES
                ('a', '{"x": 1, "y": 2}'),
                ('b', '"xyz"'),
                ('c', '0.1875')
        ''')

    def test_get_property_parse_json(self):
        """JSON values should be parsed into objects."""
        value = self.dal._get_data_property(self.cursor, 'a')  # <- Method under test.
        self.assertEqual(value, {'x': 1, 'y': 2})

        value = self.dal._get_data_property(self.cursor, 'b')  # <- Method under test.
        self.assertEqual(value, 'xyz')

        value = self.dal._get_data_property(self.cursor, 'c')  # <- Method under test.
        self.assertEqual(value, 0.1875)

    def test_get_property_missing_key(self):
        """Value should be None when key does not exist."""
        value = self.dal._get_data_property(self.cursor, 'd')  # <- Method under test.
        self.assertIsNone(value)

    def test_set_property(self):
        """Objects should be serialized as JSON formatted strings."""
        self.dal._set_data_property(self.cursor, 'e', [1, 'two', 3.1875])  # <- Method under test.
        self.cursor.execute("SELECT value FROM property WHERE key='e'")
        self.assertEqual(self.cursor.fetchall(), [([1, 'two', 3.1875],)])

    def test_set_property_update_existing(self):
        """Objects that already exist should get updated."""
        self.dal._set_data_property(self.cursor, 'a', [1, 2])  # <- Method under test.

        self.cursor.execute("SELECT value FROM property WHERE key='a'")
        self.assertEqual(self.cursor.fetchall(), [([1, 2],)])

    def test_set_property_value_is_none(self):
        """When value is None, record should be deleted."""
        get_results_sql = "SELECT * FROM property WHERE key IN ('a', 'b', 'c')"

        self.dal._set_data_property(self.cursor, 'a', None)  # <- Method under test.

        self.cursor.execute(get_results_sql)
        self.assertEqual(self.cursor.fetchall(), [('b', 'xyz'), ('c', 0.1875)])

        self.dal._set_data_property(self.cursor, 'b', None)  # <- Method under test.

        self.cursor.execute(get_results_sql)
        self.assertEqual(self.cursor.fetchall(), [('c', 0.1875),])

    def test_set_property_key_is_new_value_is_none(self):
        """Should not insert record when value is None."""
        self.dal._set_data_property(self.cursor, 'f', None)  # <- Method under test.

        self.cursor.execute("SELECT * FROM property WHERE key='f'")
        self.assertEqual(self.cursor.fetchall(), [])


@unittest.skipIf(SQLITE_VERSION_INFO < (3, 24, 0), 'requires 3.24.0 or newer')
class TestGetAndSetDataPropertyLatest(TestGetAndSetDataProperty):
    class_under_test = DataAccessLayer  # Use latest DAL class.


class TestGetAndSetDataPropertyPre24(TestGetAndSetDataProperty):
    class_under_test = DataAccessLayerPre24  # Use legacy DAL class.


class TestGetColumnNames(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class('mynode.toron', mode='memory')

    def test_get_names(self):
        self.dal.set_data({'add_columns': ['A', 'B', 'C']})
        data = self.dal.get_data(['column_names'])  # <- Method under test.
        self.assertEqual(data, {'column_names': ['A', 'B', 'C']})

    def test_no_columns_added(self):
        """Should return empty list when no columns have been added."""
        data = self.dal.get_data(['column_names'])  # <- Method under test.
        self.assertEqual(data, {'column_names': []})


class TestGetAndSetDiscreteCategories(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class('mynode.toron', mode='memory')

        connection = self.dal._get_connection()
        self.addCleanup(connection.close)

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

    def test_get_categories(self):
        self.cursor.execute('''
            INSERT INTO property
            VALUES ('discrete_categories', '[["A"], ["A", "B"], ["A", "B", "C"]]')
        ''')
        data = self.dal.get_data(['discrete_categories'])  # <- Method under test.
        expected = {'discrete_categories': [{"A"}, {"A", "B"}, {"A", "B", "C"}]}
        self.assertEqual(data, expected, msg='should get a list of sets')

    def test_get_categories_none_defined(self):
        """If no discrete categories, should return empty list."""
        self.cursor.execute("DELETE FROM property WHERE key='discrete_categories'")
        data = self.dal.get_data(['discrete_categories'])  # <- Method under test.
        self.assertEqual(data, {'discrete_categories': []})

    def test_set_categories(self):
        self.dal.set_data({'add_columns': ['A', 'B', 'C']})

        categories = [{'A'}, {'B'}, {'C'}]
        self.dal.set_data({'discrete_categories': categories})  # <- Method under test.

        self.cursor.execute("SELECT value FROM property WHERE key='discrete_categories'")
        result = self.cursor.fetchone()[0]
        self.assertEqual(result, [['A'], ['B'], ['C']])

    def test_get_and_set_categories(self):
        self.dal.set_data({'add_columns': ['A', 'B', 'C']})

        categories = [{"A"}, {"A", "B"}, {"A", "B", "C"}]

        self.dal.set_data({'discrete_categories': categories})  # <- Set!!!
        data = self.dal.get_data(['discrete_categories'])  # <- Get!!!
        self.assertEqual(data['discrete_categories'], categories)


class TestGetProperties(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class('mynode.toron', mode='memory')

        connection = self.dal._get_connection()
        self.addCleanup(connection.close)

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

        self.cursor.execute('''
            INSERT INTO property
            VALUES
                ('a', '{"x": 1, "y": 2}'),
                ('b', '"xyz"'),
                ('c', '0.1875')
        ''')

    def test_get_properties(self):
        data = self.dal.get_data(['a', 'b'])  # <- Method under test.
        self.assertEqual(data, {'a': {'x': 1, 'y': 2}, 'b': 'xyz'})

    def test_unknown_key(self):
        """Unknown keys should get None values."""
        data = self.dal.get_data(['c', 'd'])  # <- Method under test.
        self.assertEqual(data, {'c': 0.1875, 'd': None})


class TestSetStructure(unittest.TestCase):
    def setUp(self):
        self.dal = dal_class('mynode.toron', mode='memory')
        self.connection = self.dal._get_connection()
        self.cursor = self.connection.cursor()
        self.addCleanup(self.connection.close)
        self.addCleanup(self.cursor.close)

    def test_insert_structure(self):
        self.dal.set_data({'add_columns': ['state', 'county', 'town']})
        structure = [set(),
                     {'state'},
                     {'state', 'county'},
                     {'state', 'county', 'town'}]

        DataAccessLayer._set_data_structure(self.cursor, structure)  # <- Method under test.

        self.cursor.execute('SELECT state, county, town FROM main.structure')
        actual = self.cursor.fetchall()
        expected = [(0, 0, 0),  # <- set()
                    (1, 0, 0),  # <- {'state'}
                    (1, 1, 0),  # <- {'state', 'county'}
                    (1, 1, 1)]  # <- {'state', 'county', 'town'}
        self.assertEqual(actual, expected)

    def test_replace_existing(self):
        self.dal.set_data({'add_columns': ['A', 'B', 'C']})
        structure = [set(), {'A', 'B'}, {'A', 'B', 'C'}]
        DataAccessLayer._set_data_structure(self.cursor, structure)

        structure = [set(), {'A'}, {'B'}, {'A', 'B'}, {'A', 'B', 'C'}]
        DataAccessLayer._set_data_structure(self.cursor, structure)  # <- Method under test.

        self.cursor.execute('SELECT A, B, C FROM main.structure')
        actual = self.cursor.fetchall()
        expected = [(0, 0, 0),  # <- set()
                    (1, 0, 0),  # <- {'A'}
                    (0, 1, 0),  # <- {'B'}
                    (1, 1, 0),  # <- {'A', 'B'}
                    (1, 1, 1)]  # <- {'A', 'B', 'C'}
        self.assertEqual(actual, expected)

