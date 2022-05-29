"""Tests for toron.node module."""

import unittest

from toron.node import Node
from toron._exceptions import ToronWarning


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


class TestNodeMinimizeDiscreteCategories(unittest.TestCase):
    def test_a_and_b_are_distinct(self):
        base_a = [{'A'}, {'A', 'B'}]
        base_b = [{'A', 'B', 'C'}]
        result = Node._minimize_discrete_categories(base_a, base_b)
        self.assertEqual(result, [{'A'}, {'A', 'B'}, {'A', 'B', 'C'}])

    def test_a_covers_b(self):
        base_a = [{'A'}, {'B'}, {'A', 'C'}]
        base_b = [{'A', 'B', 'C'}]
        result = Node._minimize_discrete_categories(base_a, base_b)
        self.assertEqual(result, [{'A'}, {'B'}, {'A', 'C'}])

    def test_b_covers_a(self):
        base_a = [{'A', 'B', 'C'}]
        base_b = [{'A'}, {'B'}, {'A', 'C'}]
        result = Node._minimize_discrete_categories(base_a, base_b)
        self.assertEqual(result, [{'A'}, {'B'}, {'A', 'C'}])

    def test_a_covers_part_of_b(self):
        base_a = [{'A'}]
        base_b = [{'A', 'B'}, {'B'}]
        result = Node._minimize_discrete_categories(base_a, base_b)
        self.assertEqual(result, [{'A'}, {'B'}])

    def test_b_covers_part_of_a(self):
        base_a = [{'A', 'B'}, {'B'}]
        base_b = [{'A'}]
        result = Node._minimize_discrete_categories(base_a, base_b)
        self.assertEqual(result, [{'B'}, {'A'}])

    def test_symmetric_difference(self):
        """Check when A covers part of B and B covers part of A."""
        base_a = [{'A'}, {'B'}, {'B', 'C'}]
        base_b = [{'A', 'C'}, {'C'}, {'C', 'D'}]
        result = Node._minimize_discrete_categories(base_a, base_b)
        self.assertEqual(result, [{'A'}, {'B'}, {'C'}, {'C', 'D'}])

    def test_a_and_b_covers_c(self):
        base_a = [{'A'}, {'A', 'B'}]
        base_b = [{'B', 'C'}, {'D'}]
        base_c = [{'A', 'B', 'C', 'D'}]
        result = Node._minimize_discrete_categories(base_a, base_b, base_c)
        self.assertEqual(result, [{'A'}, {'D'}, {'A', 'B'}, {'B', 'C'}])


class TestNodeAddDiscreteCategories(unittest.TestCase):
    def setUp(self):
        self.node = Node('mynode.toron', mode='memory')
        self.dal = self.node._dal

    def test_add_categories_when_none_exist(self):
        self.dal.set_discrete_categories([])  # <- Erase any existing categories.

        categories = [{'A'}, {'B'}, {'C'}]
        self.node.add_discrete_categories(categories)  # <- Method under test.

        actual = self.dal.get_discrete_categories()
        self.assertEqual(actual, categories, msg='should match given categories')

    def test_add_to_existing_categories(self):
        self.dal.set_discrete_categories([{'A'}, {'A', 'B'}])

        self.node.add_discrete_categories([{'B'}, {'A', 'B', 'C'}])  # <- Method under test.

        actual = self.dal.get_discrete_categories()
        expected = [{'A'}, {'B'}, {'A', 'B', 'C'}]
        self.assertEqual(actual, expected)

    def test_warning_for_omitted(self):
        self.dal.set_discrete_categories([{'A'}, {'B'}, {'A', 'B', 'C'}])

        regex = "omitting categories already covered: {('A', 'B'|'B', 'A')}"
        with self.assertWarnsRegex(ToronWarning, regex):
            self.node.add_discrete_categories([{'A', 'B'}, {'A', 'C'}])  # <- Method under test.

        actual = self.dal.get_discrete_categories()
        self.assertEqual(actual, [{'A'}, {'B'}, {'A', 'C'}])

