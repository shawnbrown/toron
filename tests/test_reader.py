"""Tests for toron/reader.py module."""

import os
import sqlite3
import weakref
import unittest
from contextlib import closing

from toron.reader import NodeReader


class TestInstantiation(unittest.TestCase):
    def test_simple_case(self):
        reader = NodeReader([])
        self.assertEqual(list(reader), [])

    def test_close_finalizer(self):
        reader = NodeReader([])

        filepath = reader._filepath  # Get database file path.
        self.assertTrue(os.path.isfile(filepath))

        self.assertIsInstance(reader.close, weakref.finalize)

        reader.close()  # Call finalizer immediately.
        self.assertFalse(os.path.isfile(filepath))

    def test_loading_data(self):
        reader = NodeReader(
            data=[
                (10, {'a': 'foo'}, 25.0),
                (11, {'a': 'foo'}, 75.0),
                (12, {'a': 'bar'}, 50.0),
            ],
        )

        with closing(sqlite3.connect(reader._filepath)) as con:
            with closing(con.cursor()) as cur:
                cur.execute('SELECT * FROM attr_data')
                attr_data = [
                    (1, '{"a": "foo"}', None),
                    (2, '{"a": "bar"}', None),
                ]
                self.assertEqual(cur.fetchall(), attr_data)

                cur.execute('SELECT * FROM quant_data')
                quant_data = [
                    (10, 1, 25.0),
                    (11, 1, 75.0),
                    (12, 2, 50.0),
                ]
                self.assertEqual(cur.fetchall(), quant_data)
