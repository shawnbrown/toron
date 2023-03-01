"""Tests for toron/graph.py module."""

import unittest
from collections.abc import Iterator

from toron.node import Node
from toron.graph import (
    add_edge,
    _EdgeMapper,
    _QuantityIterator,
)


class TwoNodesTestCase(unittest.TestCase):
    def setUp(self):
        self.node1 = Node()
        data1 = [
            ['idx', 'wght'],
            ['A', 16],
            ['B', 8],
            ['C', 32],
        ]
        self.node1.add_index_columns(['idx'])
        self.node1.add_index_records(data1)
        self.node1.add_weights(data1, 'wght', selectors=['[attr1]'])

        self.node2 = Node()
        data2 = [
            ['idx1', 'idx2', 'wght'],
            ['A', 'x', 3],
            ['A', 'y', 15],
            ['B', 'x', 3],
            ['B', 'y', 7],
            ['C', 'x', 13],
            ['C', 'y', 22],
        ]
        self.node2.add_index_columns(['idx1', 'idx2'])
        self.node2.add_index_records(data2)
        self.node2.add_weights(data2, 'wght', selectors=['[attr1]'])


class TestEdgeMapper(TwoNodesTestCase):
    def setUp(self):
        super().setUp()
        self.data = [
            ['idx', 'population', 'idx1', 'idx2'],
            ['A', 10, 'A', 'x'],
            ['A', 70, 'A', 'y'],
            ['B', 20, 'B', 'x'],
            ['B', 60, 'B', 'y'],
            ['C', 30, 'C', 'x'],
            ['C', 50, 'C', 'y'],
        ]

    def test_init(self):
        mapper = _EdgeMapper(self.data, 'population', self.node1, self.node2)

        mapper.cur.execute('SELECT * FROM temp.source_mapping')
        expected = [
            (1, '["A"]', '["A", "x"]', 10.0),
            (2, '["A"]', '["A", "y"]', 70.0),
            (3, '["B"]', '["B", "x"]', 20.0),
            (4, '["B"]', '["B", "y"]', 60.0),
            (5, '["C"]', '["C", "x"]', 30.0),
            (6, '["C"]', '["C", "y"]', 50.0),
        ]
        self.assertEqual(mapper.cur.fetchall(), expected)

    def test_find_matches(self):
        mapper = _EdgeMapper(self.data, 'population', self.node1, self.node2)

        mapper.find_matches('left')  # <- Method under test.
        mapper.cur.execute('SELECT * FROM temp.left_matches')
        expected = [
            (1, 1, None),
            (2, 1, None),
            (3, 2, None),
            (4, 2, None),
            (5, 3, None),
            (6, 3, None),
        ]
        self.assertEqual(mapper.cur.fetchall(), expected)

        mapper.find_matches('right')  # <- Method under test.
        mapper.cur.execute('SELECT * FROM temp.right_matches')
        expected = [
            (1, 1, None),
            (2, 2, None),
            (3, 3, None),
            (4, 4, None),
            (5, 5, None),
            (6, 6, None),
        ]
        self.assertEqual(mapper.cur.fetchall(), expected)

        with self.assertRaises(ValueError):
            mapper.find_matches('blerg')  # <- Method under test.

    def test_get_relations(self):
        mapper = _EdgeMapper(self.data, 'population', self.node1, self.node2)
        mapper.find_matches('left')
        mapper.find_matches('right')

        relations = mapper.get_relations('right')  # <- Method under test.

        expected = [
            (1, 1, 10.0),
            (1, 2, 70.0),
            (2, 3, 20.0),
            (2, 4, 60.0),
            (3, 5, 30.0),
            (3, 6, 50.0),
        ]
        self.assertEqual(list(relations), expected)


class TestAddEdge(TwoNodesTestCase):
    def test_basics(self):
        mapping_data = [
            ['idx', 'population', 'idx1', 'idx2'],
            ['A', 10, 'A', 'x'],
            ['A', 70, 'A', 'y'],
            ['B', 20, 'B', 'x'],
            ['B', 60, 'B', 'y'],
            ['C', 30, 'C', 'x'],
            ['C', 50, 'C', 'y'],
        ]

        add_edge(                 # <- The method under test.
            data=mapping_data,
            name='population',
            left_node=self.node1,
            direction='-->',
            right_node=self.node2,
        )

        con = self.node2._dal._get_connection()
        results = con.execute('SELECT * FROM relation').fetchall()

        expected = [
            (1, 1, 1, 1, 10.0, 0.125, None),
            (2, 1, 1, 2, 70.0, 0.875, None),
            (3, 1, 2, 3, 20.0, 0.25,  None),
            (4, 1, 2, 4, 60.0, 0.75,  None),
            (5, 1, 3, 5, 30.0, 0.375, None),
            (6, 1, 3, 6, 50.0, 0.625, None),
            (7, 1, 0, 0,  0.0, 1.0,   None),
        ]
        self.assertEqual(results, expected)


class TestQuantityIterator(unittest.TestCase):
    def test_iterator_protocol(self):
        iterator = _QuantityIterator('0000-00-00-00-000000', [
            (1, {'a': 'foo'}, 4.5),
            (2, {'a': 'foo'}, 2.5),
            (3, {'a': 'foo'}, 3.0),
            (4, {'a': 'foo'}, 9.0),
        ])
        self.assertIs(iter(iterator), iter(iterator))
        self.assertIsInstance(iterator, Iterator)

    def test_unchanged_data(self):
        data = [
            (1, {'a': 'foo'}, 4.5),
            (2, {'a': 'foo'}, 2.5),
            (3, {'a': 'foo'}, 3.0),
            (4, {'a': 'foo'}, 9.0),
        ]
        iterator = _QuantityIterator('0000-00-00-00-000000', data)
        self.assertEqual(list(iterator), data)

    def test_aggregated_output(self):
        iterator = _QuantityIterator('0000-00-00-00-000000', [
            (1, {'a': 'foo'}, 4.5),
            (2, {'a': 'foo'}, 2.5),
            (3, {'a': 'foo'}, 3.0),
            (4, {'a': 'foo'}, 3.0),  # <- Gets aggregated.
            (4, {'a': 'foo'}, 2.0),  # <- Gets aggregated.
            (4, {'a': 'foo'}, 4.0),  # <- Gets aggregated.
        ])

        expected = [
            (1, {'a': 'foo'}, 4.5),
            (2, {'a': 'foo'}, 2.5),
            (3, {'a': 'foo'}, 3.0),
            (4, {'a': 'foo'}, 9.0),  # <- Aggregated from 3.0 + 2.0 + 4.0
        ]
        self.assertEqual(list(iterator), expected)
