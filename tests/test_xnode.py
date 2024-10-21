"""Tests for toron/node.py module."""

import unittest

from .common import get_column_names
from toron.selectors import SimpleSelector

from toron.xnode import xNode
from toron._utils import ToronWarning


class TestNodeAddIndexColumns(unittest.TestCase):
    def setUp(self):
        self.node = xNode()
        self.dal = self.node._dal
        self.cursor = self.dal._get_connection().cursor()

    def test_add_index_columns(self):
        self.node.add_index_columns(['A', 'B', 'C'])

        columns = get_column_names(self.cursor, 'node_index')
        self.assertEqual(columns, ['index_id', 'A', 'B', 'C'])

        self.cursor.execute('SELECT * FROM main.structure')
        actual = {row[2:] for row in self.cursor.fetchall()}
        expected = {(0, 0, 0), (1, 1, 1)}  # The trivial topology.
        self.assertEqual(actual, expected)

    def test_add_index_columns_in_two_parts(self):
        self.node.add_index_columns(['A', 'B'])  # <- Method under test.

        columns = get_column_names(self.cursor, 'node_index')
        self.assertEqual(columns, ['index_id', 'A', 'B'])

        self.cursor.execute('SELECT * FROM main.structure')
        actual = {row[2:] for row in self.cursor.fetchall()}
        expected = {(0, 0), (1, 1)}  # The trivial topology.
        self.assertEqual(actual, expected)

        self.node.add_index_columns(['C', 'D'])  # <- Method under test.

        columns = get_column_names(self.cursor, 'node_index')
        self.assertEqual(columns, ['index_id', 'A', 'B', 'C', 'D'])

        self.cursor.execute('SELECT * FROM main.structure')
        actual = {row[2:] for row in self.cursor.fetchall()}
        expected = {(0, 0, 0, 0), (1, 1, 0, 0), (1, 1, 1, 1)}
        self.assertEqual(actual, expected)


class TestNodeAddDiscreteCategories(unittest.TestCase):
    def setUp(self):
        self.node = xNode()
        self.dal = self.node._dal
        self.cursor = self.dal._get_connection().cursor()

    def test_add_categories_when_none_exist(self):
        self.dal.set_data({'add_index_columns': ['A', 'B', 'C']})
        self.dal.set_data({'discrete_categories': []})  # <- Erase any existing categories.

        categories = [{'A'}, {'B'}, {'C'}]
        self.node.add_discrete_categories(categories)  # <- Method under test.

        data = self.dal.get_data(['discrete_categories'])
        self.assertEqual(
            data['discrete_categories'],
            categories,
            msg='should match given categories',
        )

        self.cursor.execute('SELECT * FROM main.structure')
        actual = {row[2:] for row in self.cursor.fetchall()}
        expected = {(0, 0, 0), (1, 0, 0), (0, 1, 0), (0, 0, 1),
                    (1, 1, 0), (1, 0, 1), (0, 1, 1), (1, 1, 1)}
        self.assertEqual(actual, expected)

    def test_add_to_existing_categories(self):
        columns = ['A', 'B', 'C']
        categories = [{'A'}, {'A', 'B'}]
        structure = [set(), {'A'}, {'B', 'A'}]
        self.dal.set_data({'add_index_columns': columns})
        self.dal.set_data({'discrete_categories': categories})

        self.node.add_discrete_categories([{'B'}, {'A', 'B', 'C'}])  # <- Method under test.

        data = self.dal.get_data(['discrete_categories'])
        self.assertEqual(
            data['discrete_categories'],
            [{'A'}, {'B'}, {'A', 'B', 'C'}],
        )

        self.cursor.execute('SELECT * FROM main.structure')
        actual = {row[2:] for row in self.cursor.fetchall()}
        expected = {(0, 0, 0), (1, 0, 0), (0, 1, 0), (1, 1, 1), (1, 1, 0)}
        self.assertEqual(actual, expected)

    def test_warning_for_omitted(self):
        columns = ['A', 'B', 'C']
        categories = [{'A'}, {'B'}, {'A', 'B', 'C'}]
        structure = [set(), {'A'}, {'B'}, {'A', 'B'}, {'A', 'B', 'C'}]
        self.dal.set_data({'add_index_columns': columns})
        self.dal.set_data({'discrete_categories': categories})

        regex = "omitting categories already covered: {('A', 'B'|'B', 'A')}"
        with self.assertWarnsRegex(ToronWarning, regex):
            self.node.add_discrete_categories([{'A', 'B'}, {'A', 'C'}])  # <- Method under test.

        data = self.dal.get_data(['discrete_categories'])
        self.assertEqual(
            data['discrete_categories'],
            [{'A'}, {'B'}, {'A', 'C'}],
        )

        self.cursor.execute('SELECT * FROM main.structure')
        actual = {row[2:] for row in self.cursor.fetchall()}
        expected = {(0, 0, 0), (1, 0, 0), (0, 1, 0),
                    (1, 0, 1), (1, 1, 0), (1, 1, 1)}
        self.assertEqual(actual, expected)

    def test_structure_without_discrete_categories(self):
        """When no discrete categories are defined, the `structure`
        table should contain the "indiscrete topology".

        The indiscrete topology (also called the trivial topology) is
        one where the only open sets are the empty set (all zeros) and
        the entire space (all ones).
        """
        self.dal.set_data({'add_index_columns': ['A', 'B', 'C']})

        self.node.add_discrete_categories([])  # <- Method under test.

        self.cursor.execute("SELECT value FROM main.property WHERE key='discrete_categories'")
        actual = [set(x) for x in self.cursor.fetchone()[0]]
        indiscrete_category = [{'A', 'B', 'C'}]
        self.assertEqual(actual, indiscrete_category)

        self.cursor.execute('SELECT * FROM main.structure')
        actual = {row[2:] for row in self.cursor.fetchall()}
        indiscrete_topology = {(0, 0, 0), (1, 1, 1)}
        self.assertEqual(actual, indiscrete_topology)


class TestNodeRemoveDiscreteCategories(unittest.TestCase):
    def setUp(self):
        self.node = xNode()
        self.dal = self.node._dal
        self.cursor = self.dal._get_connection().cursor()

    def test_remove_categories(self):
        self.node.add_index_columns(['A', 'B', 'C'])
        categories = [{'A'}, {'B'}, {'C'}]
        self.node.add_discrete_categories(categories)

        self.node.remove_discrete_categories([{'C'}])  # <- Method under test.

        data = self.dal.get_data(['discrete_categories'])
        self.assertEqual(
            data['discrete_categories'],
            [{'A'}, {'B'}, {'A', 'B', 'C'}],
        )

        self.cursor.execute('SELECT * FROM main.structure')
        actual = {row[2:] for row in self.cursor.fetchall()}
        structure = {(0, 0, 0), (1, 0, 0), (0, 1, 0), (1, 1, 1), (1, 1, 0)}
        self.assertEqual(actual, structure)

    #@unittest.skip('not yet implemented')
    #def test_mandatory_category_warning(self):
    #   raise NotImplementedError

    #@unittest.skip('not yet implemented')
    #def test_no_match_warning(self):
    #    raise NotImplementedError


class TestNodeWrapperMethods(unittest.TestCase):
    def setUp(self):
        self.node = xNode()
        self.dal = self.node._dal
        self.cursor = self.dal._get_connection().cursor()

    def test_adding_data(self):
        """Test wrapper methods for adding data to a node.

        This test checks the following methods:

        * Node.add_index_columns()
        * Node.add_index_records()
        * Node.add_weights()
        * Node.add_quantities()
        """
        data = [
            ['idx1', 'idx2', 'attr1', 'attr2', 'wght1', 'counts'],
            ['A', 'x', 'foo', 'corge', 14, 12],
            ['B', 'y', 'bar', 'qux', 11, 10],
            ['C', 'z', 'baz', 'quux', 16, 15],
        ]
        self.node.add_index_columns(['idx1', 'idx2'])
        self.node.add_index_records(data)
        self.node.add_weights(data, 'wght1', selectors=['[attr1]'])
        self.node.add_quantities(data, 'counts', ['attr1', 'attr2'])

        self.cursor.execute('SELECT * FROM node_index')
        expected = [
            (0, '-', '-'),
            (1, 'A', 'x'),
            (2, 'B', 'y'),
            (3, 'C', 'z'),
        ]
        self.assertEqual(self.cursor.fetchall(), expected)

        self.cursor.execute('SELECT * FROM weighting')
        expected = [
            (1, 'wght1', None, ['[attr1]'], 1),
        ]
        self.assertEqual(self.cursor.fetchall(), expected)

        self.cursor.execute('SELECT * FROM weight')
        expected = [
            (1, 1, 1, 14.0),
            (2, 1, 2, 11.0),
            (3, 1, 3, 16.0),
        ]
        self.assertEqual(self.cursor.fetchall(), expected)

        self.cursor.execute('SELECT * FROM location')
        expected = [
            (1, 'A', 'x'),
            (2, 'B', 'y'),
            (3, 'C', 'z'),
        ]
        self.assertEqual(self.cursor.fetchall(), expected)

        self.cursor.execute('SELECT * FROM attribute')
        expected = [
            (1, {'attr1': 'foo', 'attr2': 'corge'}),
            (2, {'attr1': 'bar', 'attr2': 'qux'}),
            (3, {'attr1': 'baz', 'attr2': 'quux'}),
        ]
        self.assertEqual(self.cursor.fetchall(), expected)

        self.cursor.execute('SELECT * FROM quantity')
        expected = [
            (1, 1, 1, 12),
            (2, 2, 2, 10),
            (3, 3, 3, 15),
        ]
        self.assertEqual(self.cursor.fetchall(), expected)

    def test_reading_data(self):
        """Test wrapper methods for reading data from a node.

        This test checks the following methods:

        * Node.index_columns()
        * Node.index_records()
        * TODO: Node.weights()
        * TODO: Node.quantities()
        """
        data = [
            ['idx1', 'idx2', 'attr1', 'attr2', 'wght1', 'counts'],
            ['A', 'x', 'foo', 'corge', 14, 12],
            ['B', 'y', 'bar', 'qux', 11, 10],
            ['C', 'z', 'baz', 'quux', 16, 15],
        ]
        self.node.add_index_columns(['idx1', 'idx2'])
        self.node.add_index_records(data)
        self.node.add_weights(data, 'wght1', selectors=['[attr1]'])
        self.node.add_quantities(data, 'counts', ['attr1', 'attr2'])

        columns = self.node.index_columns()
        expected = ['idx1', 'idx2']
        self.assertEqual(columns, expected)

        records = self.node.index_records()
        expected = [
            (0, '-', '-'),
            (1, 'A', 'x'),
            (2, 'B', 'y'),
            (3, 'C', 'z'),
        ]
        self.assertEqual(list(records), expected)
