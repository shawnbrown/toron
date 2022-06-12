"""Tests for toron.node module."""

import unittest

from .common import get_column_names

from toron.node import Node
from toron._exceptions import ToronWarning


class TestNodeAddColumns(unittest.TestCase):
    def setUp(self):
        self.node = Node('mynode.toron', mode='memory')
        self.dal = self.node._dal
        self.cursor = self.dal._get_connection().cursor()

    def test_add_columns(self):
        self.node.add_columns(['A', 'B', 'C'])

        columns = get_column_names(self.cursor, 'element')
        self.assertEqual(columns, ['element_id', 'A', 'B', 'C'])

        self.cursor.execute('SELECT A, B, C FROM main.structure')
        actual = self.cursor.fetchall()
        trivial_topology = [(0, 0, 0), (1, 1, 1)]
        self.assertEqual(actual, trivial_topology)

    def test_add_columns_in_two_parts(self):
        self.node.add_columns(['A', 'B'])  # <- Method under test.

        columns = get_column_names(self.cursor, 'element')
        self.assertEqual(columns, ['element_id', 'A', 'B'])

        self.cursor.execute('SELECT A, B FROM main.structure')
        actual = self.cursor.fetchall()
        trivial_topology = [(0, 0), (1, 1)]
        self.assertEqual(actual, trivial_topology)

        self.node.add_columns(['C', 'D'])  # <- Method under test.

        columns = get_column_names(self.cursor, 'element')
        self.assertEqual(columns, ['element_id', 'A', 'B', 'C', 'D'])

        self.cursor.execute('SELECT A, B, C, D FROM main.structure')
        actual = self.cursor.fetchall()
        trivial_topology = [(0, 0, 0, 0), (1, 1, 0, 0), (1, 1, 1, 1)]
        self.assertEqual(actual, trivial_topology)


class TestNodeAddDiscreteCategories(unittest.TestCase):
    def setUp(self):
        self.node = Node('mynode.toron', mode='memory')
        self.dal = self.node._dal
        self.cursor = self.dal._get_connection().cursor()

    def test_add_categories_when_none_exist(self):
        self.dal.set_data({'add_columns': ['A', 'B', 'C']})
        self.dal.set_data({'discrete_categories': []})  # <- Erase any existing categories.

        categories = [{'A'}, {'B'}, {'C'}]
        self.node.add_discrete_categories(categories)  # <- Method under test.

        data = self.dal.get_data(['discrete_categories'])
        self.assertEqual(
            data['discrete_categories'],
            categories,
            msg='should match given categories',
        )

        self.cursor.execute('SELECT A, B, C FROM main.structure')
        actual = self.cursor.fetchall()
        expected = [(0, 0, 0), (1, 0, 0), (0, 1, 0), (0, 0, 1),
                    (1, 1, 0), (1, 0, 1), (0, 1, 1), (1, 1, 1)]
        self.assertEqual(actual, expected)

    def test_add_to_existing_categories(self):
        columns = ['A', 'B', 'C']
        categories = [{'A'}, {'A', 'B'}]
        structure = [set(), {'A'}, {'B', 'A'}]
        self.dal.set_data({'add_columns': columns})
        self.dal.set_data({'discrete_categories': categories})

        self.node.add_discrete_categories([{'B'}, {'A', 'B', 'C'}])  # <- Method under test.

        data = self.dal.get_data(['discrete_categories'])
        self.assertEqual(
            data['discrete_categories'],
            [{'A'}, {'B'}, {'A', 'B', 'C'}],
        )

        self.cursor.execute('SELECT A, B, C FROM main.structure')
        actual = self.cursor.fetchall()
        expected = [(0, 0, 0), (1, 0, 0), (0, 1, 0), (1, 1, 1), (1, 1, 0)]
        self.assertEqual(actual, expected)

    def test_warning_for_omitted(self):
        columns = ['A', 'B', 'C']
        categories = [{'A'}, {'B'}, {'A', 'B', 'C'}]
        structure = [set(), {'A'}, {'B'}, {'A', 'B'}, {'A', 'B', 'C'}]
        self.dal.set_data({'add_columns': columns})
        self.dal.set_data({'discrete_categories': categories})

        regex = "omitting categories already covered: {('A', 'B'|'B', 'A')}"
        with self.assertWarnsRegex(ToronWarning, regex):
            self.node.add_discrete_categories([{'A', 'B'}, {'A', 'C'}])  # <- Method under test.

        data = self.dal.get_data(['discrete_categories'])
        self.assertEqual(
            data['discrete_categories'],
            [{'A'}, {'B'}, {'A', 'C'}],
        )

        self.cursor.execute('SELECT A, B, C FROM main.structure')
        actual = self.cursor.fetchall()
        expected = [(0, 0, 0), (1, 0, 0), (0, 1, 0),
                    (1, 0, 1), (1, 1, 0), (1, 1, 1)]
        self.assertEqual(actual, expected)

    @unittest.skip('not fully implemented')
    def test_structure_without_discrete_categories(self):
        """When no discrete categories are defined, the 'structure'
        table should contain an "indiscrete topology".

        An indiscrete topology (also called a trivial topology) is one
        where the only open sets are the empty set (all zeros) and the
        entire space (all ones).
        """
        self.dal.set_data({'add_columns': ['A', 'B', 'C']})

        self.node.add_discrete_categories([])  # <- Method under test.

        self.cursor.execute("SELECT value FROM main.property WHERE key='discrete_categories'")
        actual = [set(x) for x in self.cursor.fetchone()[0]]
        indiscrete_category = [{'A', 'B', 'C'}]
        self.assertEqual(actual, indiscrete_category)

        self.cursor.execute('SELECT A, B, C FROM main.structure')
        actual = self.cursor.fetchall()
        indiscrete_topology = [(0, 0, 0), (1, 1, 1)]
        self.assertEqual(actual, indiscrete_topology)


class TestNodeRemoveDiscreteCategories(unittest.TestCase):
    def setUp(self):
        self.node = Node('mynode.toron', mode='memory')
        self.dal = self.node._dal
        self.cursor = self.dal._get_connection().cursor()

    def test_remove_categories(self):
        self.node.add_columns(['A', 'B', 'C'])
        categories = [{'A'}, {'B'}, {'C'}]
        self.node.add_discrete_categories(categories)

        self.node.remove_discrete_categories([{'C'}])  # <- Method under test.

        data = self.dal.get_data(['discrete_categories'])
        self.assertEqual(
            data['discrete_categories'],
            [{'A'}, {'B'}, {'A', 'B', 'C'}],
        )

        self.cursor.execute('SELECT A, B, C FROM main.structure')
        actual = self.cursor.fetchall()
        structure = [(0, 0, 0), (1, 0, 0), (0, 1, 0), (1, 1, 1), (1, 1, 0)]
        self.assertEqual(actual, structure)

    @unittest.skip('not yet implemented')
    def test_mandatory_category_warning(self):
        raise NotImplementedError

    @unittest.skip('not yet implemented')
    def test_no_match_warning(self):
        raise NotImplementedError

