"""Tests for toron/reader.py module."""

import unittest

from toron.reader import NodeReader


class TestInstantiation(unittest.TestCase):
    def test_simple_case(self):
        reader = NodeReader()
        self.assertEqual(list(reader), [])
