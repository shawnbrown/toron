# -*- coding: utf-8 -*-
#import decimal
#import glob
import os
import sqlite3
import sys

from gpn.tests import _unittest as unittest
from gpn.tests.common import MkdtempTestCase

from gpn.partition import Partition
from gpn.connector import _create_partition
from gpn import IN_MEMORY
from gpn import TEMP_FILE
from gpn import READ_ONLY


class TestPartition(MkdtempTestCase):
    def test_existing_partition(self):
        """Existing partition should load without errors."""
        global _create_partition

        filename = 'existing_partition'
        connection = sqlite3.connect(filename)
        cursor = connection.cursor()
        cursor.execute('PRAGMA synchronous=OFF')
        cursor.executescript(_create_partition)  # Creating existing partition.
        cursor.execute('PRAGMA synchronous=FULL')
        connection.close()

        ptn = Partition(filename)  # Use existing file.

    @unittest.skip('Temporarily while removing URI Filename requirement.')
    @unittest.skipUnless(sys.version_info >= (3, 4), 'Only supported on 3.4.')
    def test_read_only_partition(self):
        """Existing partition should load without errors."""
        global _create_partition

        filename = 'existing_partition'
        connection = sqlite3.connect(filename)
        cursor = connection.cursor()
        cursor.executescript(_create_partition)  # Creating existing partition.
        connection.close()

        def read_only():
            ptn = Partition(filename, mode=READ_ONLY)
            connection = ptn._connect()
            cursor = connection.cursor()
            cursor.execute('INSERT INTO cell DEFAULT VALUES')
        self.assertRaises(sqlite3.OperationalError, read_only)

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


if __name__ == '__main__':
    unittest.main()
