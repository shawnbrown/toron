"""Tests for toron/node.py module."""

import unittest
from toron.node import Node


class TestNode(unittest.TestCase):
    def test_instantiation(self):
        with self.assertRaises(NotImplementedError):
            node = Node()

