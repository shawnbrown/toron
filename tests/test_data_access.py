"""Tests for toron/data_access.py module."""

import os
import sqlite3
import unittest
from contextlib import closing
from dataclasses import FrozenInstanceError
from tempfile import NamedTemporaryFile

from toron import dal1
from toron.data_access import (
    DataAccessLayer,
    get_data_access_layer,
    get_backend_from_path,
)


class TestGetDataAccessLayer(unittest.TestCase):
    def test_initialize(self):
        dal = get_data_access_layer()  # <- Factory function.
        self.assertIsInstance(dal, DataAccessLayer)

    def test_unknown_backend(self):
        regex = "could not find data backend 'xyz'"
        with self.assertRaisesRegex(RuntimeError, regex):
            dal = get_data_access_layer(backend='xyz')

    def test_missing_backend(self):
        regex = "could not find data backend 'XYZ'"
        with self.assertRaisesRegex(RuntimeError, regex):
            dal = get_data_access_layer(backend='XYZ')


class TestDataAccessLayer(unittest.TestCase):
    def setUp(self):
        self.dal = get_data_access_layer()

    def test_default_backend(self):
        msg = 'DAL1 should be current default'
        self.assertEqual(self.dal.backend, 'DAL1', msg=msg)

    def test_immutable(self):
        msg = 'should be immutable'
        with self.assertRaises(FrozenInstanceError, msg=msg):
            self.dal.backend = 'some-other-value'


class TestGetBackendFromPath(unittest.TestCase):
    def setUp(self):
        f = NamedTemporaryFile(prefix='toron-', delete=False)
        f.close()
        self.addCleanup(lambda: os.unlink(f.name))
        self.fname = f.name  # <- Start with empty file.

    def test_dal1(self):
        dal1.DataConnector().to_file(self.fname, fsync=False)
        self.assertEqual(get_backend_from_path(self.fname), 'DAL1')

    def test_sqlite_file(self):
        with closing(sqlite3.connect(self.fname)) as con:
            con.executescript("""
                CREATE TABLE mytable (A, B);
                INSERT INTO mytable VALUES (1, 1), (2, 2);
            """)

        regex = 'does not appear to be a Toron file'
        with self.assertRaisesRegex(ValueError, regex):
            get_backend_from_path(self.fname)

    def test_other_file(self):
        with open(self.fname, 'wb') as f:
            f.write(b'\xff' * 64)  # Write 64 bytes of 1s.

        regex = 'does not appear to be a Toron file'
        with self.assertRaisesRegex(ValueError, regex):
            get_backend_from_path(self.fname)

    def test_empty_file(self):
        regex = 'does not appear to be a Toron file'
        with self.assertRaisesRegex(ValueError, regex):
            get_backend_from_path(self.fname)

    def test_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            get_backend_from_path('missing-file-path.toron')
