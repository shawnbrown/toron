"""Tests for toron.node module."""

import unittest

from toron.node import Node


class TestNodeMakeStructure(unittest.TestCase):
    def test_discrete_topology(self):
        discrete_categories = [{'A'}, {'B'}, {'A', 'C'}]
        result = Node._make_structure(discrete_categories)

        expected = [set(), {'A'}, {'B'}, {'A', 'C'}, {'A', 'B'}, {'A', 'B', 'C'}]
        self.assertEqual(result, expected)

    def test_discrete_nontopology(self):
        discrete_categories = [{'A', 'C'}, {'B', 'C'}]
        result = Node._make_structure(discrete_categories)

        expected = [set(), {'A', 'C'}, {'B', 'C'}, {'A', 'B', 'C'}]
        self.assertEqual(result, expected)

    def test_duplicate_input(self):
        discrete_categories = [{'A'}, {'A'}, {'A'}, {'A'}, {'A'}, {'A'}, {'B'}]
        result = Node._make_structure(discrete_categories)

        expected = [set(), {'A'}, {'B'}, {'A', 'B'}]
        self.assertEqual(result, expected)

