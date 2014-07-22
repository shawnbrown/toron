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
        self._make_partition('existing_partition')
        ptn = Partition(self._existing_partition)  # Use existing file.

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
        filename = 'new_partition'

        self.assertFalse(os.path.exists(filename))
        ptn = Partition(filename)  # Create new file.
        del ptn
        self.assertTrue(os.path.exists(filename))

    def test_temporary_partition(self):
        """Unnamed Partitions should be temporary (in memory or tempfile)."""
        # In memory.
        ptn = Partition()
        self.assertIsNone(ptn._connect._temp_path)
        self.assertIsNotNone(ptn._connect._memory_conn)

        # On disk.
        ptn = Partition(mode=TEMP_FILE)
        self.assertIsNotNone(ptn._connect._temp_path)
        self.assertIsNone(ptn._connect._memory_conn)


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
        expected = [(1,  1, 'UNMAPPED'),   (2,  2, 'UNMAPPED'),
                    (3,  3, 'UNMAPPED'),   (4,  1, 'OH'),
                    (5,  2, 'Allen'),      (6,  3, 'Lima'),
                    (7,  2, 'Cuyahoga'),   (8,  3, 'Cleveland'),
                    (9,  2, 'Franklin'),   (10, 3, 'Columbus'),
                    (11, 2, 'Hamilton'),   (12, 3, 'Cincinnati'),
                    (13, 2, 'Montgomery'), (14, 3, 'Dayton')]
        self.assertEqual(expected, cursor.fetchall())

        # Cell_label table,
        expected = [(1,  1, 1, 1), (2,  1, 2, 2),  (3,  1, 3, 3),
                    (4,  2, 1, 4), (5,  2, 2, 5),  (6,  2, 3, 6),
                    (7,  3, 1, 4), (8,  3, 2, 7),  (9,  3, 3, 8),
                    (10, 4, 1, 4), (11, 4, 2, 9),  (12, 4, 3, 10),
                    (13, 5, 1, 4), (14, 5, 2, 11), (15, 5, 3, 12),
                    (16, 6, 1, 4), (17, 6, 2, 13), (18, 6, 3, 14)]
        cursor.execute('SELECT * FROM cell_label ORDER BY cell_label_id')
        self.assertEqual(expected, cursor.fetchall())

        # Partition table.
        cursor.execute('SELECT partition_id, partition_hash '
                       'FROM partition ORDER BY partition_id')
        self.assertEqual([], cursor.fetchall())

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
        expected = [(1,  1, 'UNMAPPED'),   (2,  2, 'UNMAPPED'),
                    (3,  3, 'UNMAPPED'),   (4,  1, 'OH'),
                    (5,  2, 'Allen'),      (6,  3, 'Lima'),
                    (7,  2, 'Cuyahoga'),   (8,  3, 'Cleveland')]
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
        expected = [(1, 1, 'UNMAPPED'), (2, 2, 'UNMAPPED'), (3, 3, 'UNMAPPED'),
                    (4, 1, 'OH'), (5, 2, 'Cuyahoga'), (6, 3, 'Cleveland')]
        self.assertEqual(expected, cursor.fetchall())


class TestSelect(unittest.TestCase):
    def test_select_cell_id(self):
        """ """
        fh = StringIO('country,region,state,city\n'      # cell_ids
                      'USA,Midwest,IL,Chicago\n'         # 2 (1 is UNMAPPED)
                      'USA,Northeast,NY,New York\n'      # 3
                      'USA,Northeast,PA,Philadelphia\n'  # 4
                      'USA,South,TX,Dallas\n'            # 5
                      'USA,South,TX,Houston\n'           # 6
                      'USA,South,TX,San Antonio\n'       # 7
                      'USA,West,AZ,Phoenix\n'            # 8
                      'USA,West,CA,Los Angeles\n'        # 9
                      'USA,West,CA,San Diego\n'          # 10
                      'USA,West,CA,San Jose\n')          # 11
        partition = Partition(mode=IN_MEMORY)
        partition._insert_cells(fh)

        connection = partition._connect()
        cursor = connection.cursor()

        result = partition._select_cell_id(cursor, region='Northeast')
        self.assertEqual([3, 4], list(result))

        result = partition._select_cell_id(cursor, region='West', state='CA')
        self.assertEqual([9, 10, 11], list(result))

        kwds = {'region': 'West', 'state': 'CA'}
        result = partition._select_cell_id(cursor, **kwds)
        self.assertEqual([9, 10, 11], list(result))

        result = partition._select_cell_id(cursor, state='XX')
        self.assertEqual([], list(result))

        #result = partition._select_cell_id()
        #self.assertEqual([], list(result))

    def test_select_cell(self):
        fh = StringIO('country,region,state,city\n'      # cell_ids
                      'USA,Midwest,IL,Chicago\n'         # 2 (1 is UNMAPPED)
                      'USA,Northeast,NY,New York\n'      # 3
                      'USA,Northeast,PA,Philadelphia\n'  # 4
                      'USA,South,TX,Dallas\n'            # 5
                      'USA,South,TX,Houston\n'           # 6
                      'USA,South,TX,San Antonio\n'       # 7
                      'USA,West,AZ,Phoenix\n'            # 8
                      'USA,West,CA,Los Angeles\n'        # 9
                      'USA,West,CA,San Diego\n'          # 10
                      'USA,West,CA,San Jose\n')          # 11
        partition = Partition(mode=IN_MEMORY)
        partition._insert_cells(fh)

        result = partition.select_cell(region='West', state='CA')
        expected = [
            {'country': 'USA', 'region': 'West', 'state': 'CA', 'city': 'Los Angeles'},
            {'country': 'USA', 'region': 'West', 'state': 'CA', 'city': 'San Diego'},
            {'country': 'USA', 'region': 'West', 'state': 'CA', 'city': 'San Jose'},
        ]
        self.assertEqual(expected, list(result))


if __name__ == '__main__':
    unittest.main()
