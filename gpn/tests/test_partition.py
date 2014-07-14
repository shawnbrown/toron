# -*- coding: utf-8 -*-
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
    def _make_partition(self, filename):
        global _create_partition
        self._existing_partition = 'existing_partition'
        connection = sqlite3.connect(filename)
        cursor = connection.cursor()
        cursor.execute('PRAGMA synchronous=OFF')
        cursor.executescript(_create_partition)
        cursor.execute('PRAGMA synchronous=FULL')
        connection.close()

    def test_existing_partition(self):
        """Existing partition should load without errors."""
        self._make_partition('existing_partition')
        ptn = Partition(self._existing_partition)  # Use existing file.

    def test_read_only_partition(self):
        """The READ_ONLY flag should open a Partition in read-only mode."""
        self._make_partition('existing_partition')

        ptn = Partition(self._existing_partition, mode=READ_ONLY)
        connection = ptn._connect()
        cursor = connection.cursor()

        with self.assertRaises(sqlite3.OperationalError):
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


if __name__ == '__main__':
    unittest.main()
