"""Tests for toron/graph.py module."""

import unittest

from toron.node import Node
from toron.graph import add_edge


class TestAddEdge(unittest.TestCase):
    def setUp(self):
        self.node1 = Node()
        self.node2 = Node()
        #self.dal = self.node._dal
        #self.cursor = self.dal._get_connection().cursor()

    def test_add_edge(self):
        pass
        #self.node.add_index_columns(['A', 'B', 'C'])
        #
        #columns = get_column_names(self.cursor, 'label_index')
        #self.assertEqual(columns, ['index_id', 'A', 'B', 'C'])
        #
        #self.cursor.execute('SELECT * FROM main.structure')
        #actual = {row[1:] for row in self.cursor.fetchall()}
        #expected = {(0, 0, 0), (1, 1, 1)}  # The trivial topology.
        #self.assertEqual(actual, expected)

