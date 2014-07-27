# -*- coding: utf-8 -*-
import os
import sqlite3
import sys
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO  # New stdlib location in 3.0


from gpn.tests import _unittest as unittest
from gpn.tests.common import MkdtempTestCase

from gpn.partition import Partition
from gpn.connector import _create_partition
from gpn.connector import _create_triggers
from gpn import IN_MEMORY
from gpn import TEMP_FILE
from gpn import READ_ONLY


class TestInstantiation(MkdtempTestCase):
    def _make_partition(self, filename):
        global _create_partition
        self._existing_partition = filename
        connection = sqlite3.connect(self._existing_partition)
        cursor = connection.cursor()
        cursor.execute('PRAGMA synchronous=OFF')
        for operation in (_create_partition + _create_triggers):
            cursor.execute(operation)
        cursor.execute('PRAGMA synchronous=FULL')
        connection.close()

    def test_existing_partition(self):
        """Existing partition should load without errors."""
        filename = 'temppartition.node'
        self._make_partition(filename)
        ptn = Partition(self._existing_partition)  # Use existing file.
        self.assertEqual(ptn.name, 'temppartition')

    @unittest.skipIf(sqlite3.sqlite_version_info < (3, 8, 0),
        'The query_only PRAGMA was added to SQLite in version 3.8.0')
    def test_read_only_partition(self):
        """The READ_ONLY flag should open a Partition in read-only mode."""
        self._make_partition('existing_partition')

        ptn = Partition(self._existing_partition, mode=READ_ONLY)
        connection = ptn._connect()
        cursor = connection.cursor()

        regex = 'attempt to write a readonly database'
        with self.assertRaisesRegex((sqlite3.OperationalError,
                                     sqlite3.IntegrityError), regex):
            cursor.execute('INSERT INTO cell DEFAULT VALUES')

    def test_new_partition(self):
        """Named Partitions that do not exist should be created."""
        filepath = 'new_partition.node'
        self.assertFalse(os.path.exists(filepath))
        ptn = Partition(filepath)  # Create new file.
        del ptn
        self.assertTrue(os.path.exists(filepath))

    def test_subdirectory(self):
        """Subdirectory reference should also be supported."""
        os.mkdir('subdir')
        filepath = 'subdir/new_partition.node'
        self.assertFalse(os.path.exists(filepath))
        ptn = Partition(filepath)  # Create new file.
        self.assertEqual(ptn.name, 'subdir/new_partition')
        del ptn
        self.assertTrue(os.path.exists(filepath))

    def test_path_name_error(self):
        """If a path is specified, it should be used to set the partition name.
        If a `name` attribute is also provided, it must not be accepted.

        """
        regex = 'Cannot specify both path and name.'
        with self.assertRaisesRegex(AssertionError, regex):
            Partition('some_path.node', name='some_name')

    def test_temporary_partition(self):
        """Unnamed Partitions should be temporary (in memory or tempfile)."""
        # In memory.
        ptn = Partition()
        self.assertIsNone(ptn._connect._temp_path)
        self.assertIsNotNone(ptn._connect._memory_conn)
        self.assertEqual(ptn.name, '<unspecified>')

        # On disk.
        ptn = Partition(mode=TEMP_FILE)
        self.assertIsNotNone(ptn._connect._temp_path)
        self.assertIsNone(ptn._connect._memory_conn)
        self.assertEqual(ptn.name, '<unspecified>')

    def test_named_temporary_partitions(self):
        # In memory.
        partition_name = 'temp_with_name'
        ptn = Partition(name=partition_name)
        self.assertIsNone(ptn._connect._temp_path)
        self.assertIsNotNone(ptn._connect._memory_conn)
        self.assertEqual(ptn.name, partition_name)

        # On disk.
        ptn = Partition(name=partition_name, mode=TEMP_FILE)
        self.assertIsNotNone(ptn._connect._temp_path)
        self.assertIsNone(ptn._connect._memory_conn)
        self.assertEqual(ptn.name, partition_name)


class TestHash(unittest.TestCase):
    def test_get_hash(self):
        partition = Partition(mode=IN_MEMORY)
        connection = partition._connect()
        cursor = connection.cursor()

        # Hash of empty partition should be None.
        result = partition._get_hash(cursor)
        self.assertIsNone(result)

        # Build partition.
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'state', 0)")
        cursor.execute("INSERT INTO hierarchy VALUES (2, 'county', 1)")
        cursor.execute("INSERT INTO cell VALUES (1, 0)")
        cursor.execute("INSERT INTO label VALUES (1, 1, 'Indiana')")
        cursor.execute("INSERT INTO label VALUES (2, 2, 'LaPorte')")
        cursor.execute("INSERT INTO cell_label VALUES (1, 1, 1, 1)")
        cursor.execute("INSERT INTO cell_label VALUES (2, 1, 2, 2)")

        # Expected hash of "11Indiana12LaPorte" (independently verified).
        expected = 'a0eadc7b0547b9405dae9e3c50e038a550d9a718af10b53e567995a9378c22d7'
        result = partition._get_hash(cursor)
        self.assertEqual(expected, result)


class TestTransactionHandling(unittest.TestCase):
    def setUp(self):
        self._partition = Partition(mode=IN_MEMORY)
        connection = self._partition._connect()
        cursor = connection.cursor()
        cursor.executescript("""
            INSERT INTO hierarchy VALUES (1, 'country', 0);
            INSERT INTO hierarchy VALUES (2, 'region', 1);
            INSERT INTO cell VALUES (1, 0);
            INSERT INTO label VALUES (1, 1, 'USA');
            INSERT INTO label VALUES (2, 2, 'Northeast');
            INSERT INTO cell_label VALUES (1, 1, 1, 1);
            INSERT INTO cell_label VALUES (2, 1, 2, 2);
            INSERT INTO cell VALUES (2, 0);
            INSERT INTO label VALUES (3, 2, 'Midwest');
            INSERT INTO cell_label VALUES (3, 2, 1, 1);
            INSERT INTO cell_label VALUES (4, 2, 2, 3);
        """)

    def test_commit(self):
        with self._partition._connect() as connection:
            connection.isolation_level = None
            cursor = connection.cursor()
            cursor.execute('BEGIN TRANSACTION')

            cursor.execute('INSERT INTO cell VALUES (3, 0)')  # <- Change.

        cursor.execute('SELECT COUNT(*) FROM cell')
        msg = 'Changes should be committed.'
        self.assertEqual([(3,)], cursor.fetchall(), msg)

    def test_rollback(self):
        try:
            with self._partition._connect() as connection:
                connection.isolation_level = None    # <- REQUIRED!
                cursor = connection.cursor()         # <- REQUIRED!
                cursor.execute('BEGIN TRANSACTION')  # <- REQUIRED!

                cursor.execute('DROP TABLE cell_label')           # <- Change.
                cursor.execute('INSERT INTO cell VALUES (3, 0)')  # <- Change.
                cursor.execute('This is not valid SQL -- operational error!')  # <- Error!
        except sqlite3.OperationalError:
            pass

        connection = self._partition._connect()
        cursor = connection.cursor()

        msg = 'Changes should be rolled back.'

        cursor.execute('SELECT COUNT(*) FROM cell')
        self.assertEqual([(2,)], cursor.fetchall(), msg)

        cursor.execute('SELECT COUNT(*) FROM cell_label')
        self.assertEqual([(4,)], cursor.fetchall(), msg)


class TestInsert(unittest.TestCase):
    def test_insert_one_cell(self):
        partition = Partition(mode=IN_MEMORY)
        connection = partition._connect()
        cursor = connection.cursor()
        cursor.execute("INSERT INTO hierarchy VALUES (1, 'state', 0)")
        cursor.execute("INSERT INTO hierarchy VALUES (2, 'county', 1)")
        cursor.execute("INSERT INTO hierarchy VALUES (3, 'town', 2)")

        items = [('state', 'OH'), ('county', 'Franklin'), ('town', 'Columbus')]
        partition._insert_one_cell(cursor, items)  # <- Inserting here!

        # Cell table.
        cursor.execute('SELECT * FROM cell ORDER BY cell_id')
        expected = [(1, 0)]
        self.assertEqual(expected, cursor.fetchall())

        # Label table.
        cursor.execute('SELECT * FROM label ORDER BY label_id')
        expected = [(1,  1, 'OH'),
                    (2,  2, 'Franklin'),
                    (3,  3, 'Columbus')]
        self.assertEqual(expected, cursor.fetchall())

        # Cell_label table,
        expected = [(1, 1, 1, 1), (2, 1, 2, 2),  (3, 1, 3, 3)]
        cursor.execute('SELECT * FROM cell_label ORDER BY cell_label_id')
        self.assertEqual(expected, cursor.fetchall())

    def test_insert_cells(self):
        self.maxDiff = None

        fh = StringIO('state,county,town\n'
                      'OH,Allen,Lima\n'
                      'OH,Cuyahoga,Cleveland\n'
                      'OH,Franklin,Columbus\n'
                      'OH,Hamilton,Cincinnati\n'
                      'OH,Montgomery,Dayton\n')
        partition = Partition(mode=IN_MEMORY)
        partition._insert_cells(fh)  # <- Inserting here!

        connection = partition._connect()
        cursor = connection.cursor()

        # Hierarchy table.
        cursor.execute('SELECT * FROM hierarchy ORDER BY hierarchy_level')
        expected = [(1, 'state', 0), (2, 'county', 1), (3, 'town', 2)]
        self.assertEqual(expected, cursor.fetchall())

        # Cell table.
        cursor.execute('SELECT * FROM cell ORDER BY cell_id')
        expected = [(1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0)]
        self.assertEqual(expected, cursor.fetchall())

        # Label table.
        cursor.execute('SELECT * FROM label ORDER BY label_id')
        expected = [(1,  1, 'OH'),         (2,  2, 'Allen'),
                    (3,  3, 'Lima'),       (4,  2, 'Cuyahoga'),
                    (5,  3, 'Cleveland'),  (6,  2, 'Franklin'),
                    (7,  3, 'Columbus'),   (8,  2, 'Hamilton'),
                    (9,  3, 'Cincinnati'), (10, 2, 'Montgomery'),
                    (11, 3, 'Dayton'),     (12, 1, 'UNMAPPED'),
                    (13, 2, 'UNMAPPED'),   (14, 3, 'UNMAPPED')]
        self.assertEqual(expected, cursor.fetchall())

        # Cell_label table,
        cursor.execute('SELECT * FROM cell_label ORDER BY cell_label_id')
        expected = [(1,  1, 1, 1),  (2,  1, 2, 2),  (3,  1, 3, 3),
                    (4,  2, 1, 1),  (5,  2, 2, 4),  (6,  2, 3, 5),
                    (7,  3, 1, 1),  (8,  3, 2, 6),  (9,  3, 3, 7),
                    (10, 4, 1, 1),  (11, 4, 2, 8),  (12, 4, 3, 9),
                    (13, 5, 1, 1),  (14, 5, 2, 10), (15, 5, 3, 11),
                    (16, 6, 1, 12), (17, 6, 2, 13), (18, 6, 3, 14)]
        self.assertEqual(expected, cursor.fetchall())

        # Partition table (hash should be set).
        cursor.execute('SELECT partition_id, partition_hash FROM partition')
        hashval = '71eeab7a5b4609a1978bd5c19e7d490556c5e42c503b39480c504bbaf99efe30'
        self.assertEqual([(1, hashval)], cursor.fetchall())

    def test_insert_cells_multiple_files(self):
        """Insert should accept multiple files."""
        partition = Partition(mode=IN_MEMORY)

        fh = StringIO('state,county,town\n'
                      'OH,Allen,Lima\n')
        partition._insert_cells(fh)  # <- Inserting.

        fh = StringIO('state,county,town\n'
                      'OH,Cuyahoga,Cleveland\n')
        partition._insert_cells(fh)  # <- Inserting second file.

        connection = partition._connect()
        cursor = connection.cursor()

        # Hierarchy table.
        cursor.execute('SELECT * FROM hierarchy ORDER BY hierarchy_level')
        expected = [(1, 'state', 0), (2, 'county', 1), (3, 'town', 2)]
        self.assertEqual(expected, cursor.fetchall())

        # Cell table.
        cursor.execute('SELECT * FROM cell ORDER BY cell_id')
        expected = [(1, 0), (2, 0), (3, 0)]
        self.assertEqual(expected, cursor.fetchall())

        # Label table.
        cursor.execute('SELECT * FROM label ORDER BY label_id')
        expected = [(1, 1, 'OH'), (2, 2, 'Allen'),
                    (3, 3, 'Lima'), (4, 1, 'UNMAPPED'),
                    (5, 2, 'UNMAPPED'), (6, 3, 'UNMAPPED'),
                    (7, 2, 'Cuyahoga'), (8, 3, 'Cleveland')]
        self.assertEqual(expected, cursor.fetchall())

        # Partition table should have two hashes.
        cursor.execute('SELECT partition_id, partition_hash FROM partition')
        expected =  [(1, '5011d6c33da25f6a98422461595f275f'
                           '289a7a745a9e89ab6b4d36675efd944b'),
                     (2, '9184abbd5461828e01fe82209463221a'
                           '65d4c21b40287d633cf7e324a27475f5')]
        self.assertEqual(expected, cursor.fetchall())

    def test_insert_cells_bad_header(self):
        """Files must have the same header"""
        partition = Partition(mode=IN_MEMORY)
        fh = StringIO('state,county,town\n'
                      'OH,Hamilton,Cincinnati\n')
        partition._insert_cells(fh)

        regex = 'Fieldnames must match hierarchy values.'
        with self.assertRaisesRegex(AssertionError, regex):
            fh = StringIO('state,county\n'
                          'OH,Montgomery\n')
            partition._insert_cells(fh)

    def test_insert_cells_duplicate(self):
        """Duplicate rows should fail and rollback to previous state."""
        fh = StringIO('state,county,town\n'
                      'OH,Cuyahoga,Cleveland\n')
        partition = Partition(mode=IN_MEMORY)
        partition._insert_cells(fh)  # <- First insert!

        regex = 'CHECK constraint failed: cell_label'
        with self.assertRaisesRegex(sqlite3.IntegrityError, regex):
            fh = StringIO('state,county,town\n'
                          'OH,Franklin,Columbus\n'
                          'OH,Hamilton,Cincinnati\n'
                          'OH,Hamilton,Cincinnati\n')
            partition._insert_cells(fh)  # <- Second insert!

        connection = partition._connect()
        cursor = connection.cursor()

        # Cell table should include only values from first insert.
        cursor.execute('SELECT * FROM cell ORDER BY cell_id')
        expected = [(1, 0), (2, 0)]
        self.assertEqual(expected, cursor.fetchall())

        # Label table should include only values from first insert.
        cursor.execute('SELECT * FROM label ORDER BY label_id')
        expected = [(1, 1, 'OH'),       (2, 2, 'Cuyahoga'), (3, 3, 'Cleveland'),
                    (4, 1, 'UNMAPPED'), (5, 2, 'UNMAPPED'), (6, 3, 'UNMAPPED')]
        self.assertEqual(expected, cursor.fetchall())


class TestSelect(unittest.TestCase):
    def setUp(self):
        fh = StringIO('country,region,state,city\n'      # cell_ids
                      'USA,Midwest,IL,Chicago\n'         # 1
                      'USA,Northeast,NY,New York\n'      # 2
                      'USA,Northeast,PA,Philadelphia\n'  # 3
                      'USA,South,TX,Dallas\n'            # 4
                      'USA,South,TX,Houston\n'           # 5
                      'USA,South,TX,San Antonio\n'       # 6
                      'USA,West,AZ,Phoenix\n'            # 7
                      'USA,West,CA,Los Angeles\n'        # 8
                      'USA,West,CA,San Diego\n'          # 9
                      'USA,West,CA,San Jose\n')          # 10
        self.partition = Partition(mode=IN_MEMORY)
        self.partition._insert_cells(fh)

    def test_select_cell_id(self):
        """ """
        connection = self.partition._connect()
        cursor = connection.cursor()

        result = self.partition._select_cell_id(cursor, region='Northeast')
        self.assertEqual([2, 3], list(result))

        result = self.partition._select_cell_id(cursor, region='West', state='CA')
        self.assertEqual([8, 9, 10], list(result))

        kwds = {'region': 'West', 'state': 'CA'}
        result = self.partition._select_cell_id(cursor, **kwds)
        self.assertEqual([8, 9, 10], list(result))

        result = self.partition._select_cell_id(cursor, state='XX')
        self.assertEqual([], list(result))

        #result = partition._select_cell_id()
        #self.assertEqual([], list(result))

    def test_select_cell(self):
        result = self.partition.select_cell(region='West', state='CA')
        expected = [
            {'country': 'USA', 'region': 'West', 'state': 'CA', 'city': 'Los Angeles'},
            {'country': 'USA', 'region': 'West', 'state': 'CA', 'city': 'San Diego'},
            {'country': 'USA', 'region': 'West', 'state': 'CA', 'city': 'San Jose'},
        ]
        self.assertEqual(expected, list(result))


class TestFileImportExport(MkdtempTestCase):
    def setUp(self):
        super(self.__class__, self).setUp()
        fh = StringIO('country,region,state,city\n'
                      'USA,Midwest,IL,Chicago\n'
                      'USA,Northeast,NY,New York\n'
                      'USA,Northeast,PA,Philadelphia\n')
        partition = Partition(mode=IN_MEMORY)
        partition._insert_cells(fh)
        self.partition = partition

    def test_export(self):
        filename = 'tempexport.csv'
        self.partition.export_cells(filename)

        with open(filename) as fh:
            file_contents = fh.read()
            expected_contents = ('cell_id,country,region,state,city\n'
                                 '1,USA,Midwest,IL,Chicago\n'
                                 '2,USA,Northeast,NY,New York\n'
                                 '3,USA,Northeast,PA,Philadelphia\n'
                                 '4,UNMAPPED,UNMAPPED,UNMAPPED,UNMAPPED\n')
            self.assertEqual(expected_contents, file_contents)

    def test_already_exists(self):
        filename = 'tempexport.csv'
        with open(filename, 'w') as fh:
            fh.write('foo\n1\n2\n3')

        regex = filename + ' already exists'
        with self.assertRaisesRegex(AssertionError, regex):
            self.partition.export_cells(filename)


class TestRepr(unittest.TestCase):
    def test_empty(self):
        partition = Partition()
        expected = ("<class 'gpn.partition.Partition'>\n"
                    "Name: <unspecified>\n"
                    "Cells: None\n"
                    "Hierarchy: None\n"
                    "Edges: None")
        self.assertEqual(expected, repr(partition))

    def test_basic(self):
        fh = StringIO('country,region,state,city\n'
                      'USA,Midwest,IL,Chicago\n'
                      'USA,Northeast,NY,New York\n'
                      'USA,Northeast,PA,Philadelphia\n')
        partition = Partition(mode=IN_MEMORY, name='newptn')
        partition._insert_cells(fh)

        expected = ("<class 'gpn.partition.Partition'>\n"
                    "Name: newptn (a1c5c6a)\n"
                    "Cells: 4\n"
                    "Hierarchy: country (USA), region, state, city\n"
                    "Edges: None")
        self.assertEqual(expected, repr(partition))


if __name__ == '__main__':
    unittest.main()
