"""Tests for toron/reader.py module."""

import os
import weakref
import unittest

from toron.reader import NodeReader


class TestInstantiation(unittest.TestCase):
    def test_simple_case(self):
        reader = NodeReader()
        self.assertEqual(list(reader), [])

    def test_close_finalizer(self):
        reader = NodeReader()

        filepath = reader._filepath  # Get database file path.
        self.assertTrue(os.path.isfile(filepath))

        self.assertIsInstance(reader.close, weakref.finalize)

        reader.close()  # Call finalizer immediately.
        self.assertFalse(os.path.isfile(filepath))
