"""Tests for toron/node.py module."""

import sqlite3
import unittest

from .common import TempDirTestCase

from toron.node import Node


class TestNode(TempDirTestCase):
    def setUp(self):
        self.addCleanup(self.cleanup_temp_files)

    @staticmethod
    def get_column_names(connection_or_cursor, table):
        cur = connection_or_cursor.execute(f'PRAGMA table_info({table})')
        return [row[1] for row in cur.fetchall()]

    def test_initialize(self):
        node = Node('mynode.toron')
        self.assertEqual(node.path, 'mynode.toron')
        self.assertEqual(node.mode, 'rwc')

    def test_add_columns(self):
        path = 'mynode.toron'
        node = Node(path)
        node.add_columns(['state', 'county'])  # <- Add columns.

        con = sqlite3.connect(path)

        columns = self.get_column_names(con, 'element')
        self.assertEqual(columns, ['element_id', 'state', 'county'])

        columns = self.get_column_names(con, 'location')
        self.assertEqual(columns, ['location_id', 'state', 'county'])

        columns = self.get_column_names(con, 'structure')
        self.assertEqual(columns, ['structure_id', 'state', 'county'])

    def test_add_elements(self):
        path = 'mynode.toron'
        node = Node(path)
        node.add_columns(['state', 'county'])  # <- Add columns.

        elements = [
            ('IA', 'POLK'),
            ('IN', 'LA PORTE'),
            ('MN', 'HENNEPIN '),
        ]
        node.add_elements(elements, columns=['state', 'county'])

        con = sqlite3.connect(path)
        result = con.execute('SELECT * FROM element').fetchall()
        expected = [
            (1, 'IA', 'POLK'),
            (2, 'IN', 'LA PORTE'),
            (3, 'MN', 'HENNEPIN '),
        ]
        self.assertEqual(result, expected)

    def test_add_elements_no_column_arg(self):
        path = 'mynode.toron'
        node = Node(path)
        node.add_columns(['state', 'county'])  # <- Add columns.

        elements = [
            ('state', 'county'),  # <- Header row.
            ('IA', 'POLK'),
            ('IN', 'LA PORTE'),
            ('MN', 'HENNEPIN '),
        ]
        node.add_elements(elements) # <- No *columns* argument given.

        con = sqlite3.connect(path)
        result = con.execute('SELECT * FROM element').fetchall()
        expected = [
            (1, 'IA', 'POLK'),
            (2, 'IN', 'LA PORTE'),
            (3, 'MN', 'HENNEPIN '),
        ]
        self.assertEqual(result, expected)

    def test_add_elements_column_subset(self):
        """Omitted columns should get default value ('-')."""
        path = 'mynode.toron'
        node = Node(path)
        node.add_columns(['state', 'county'])  # <- Add columns.

        # Element rows include "state" but not "county".
        elements = [
            ('state',),  # <- Header row.
            ('IA',),
            ('IN',),
            ('MN',),
        ]
        node.add_elements(elements) # <- No *columns* argument given.

        con = sqlite3.connect(path)
        result = con.execute('SELECT * FROM element').fetchall()
        expected = [
            (1, 'IA', '-'),  # <- "county" gets default '-'
            (2, 'IN', '-'),  # <- "county" gets default '-'
            (3, 'MN', '-'),  # <- "county" gets default '-'
        ]
        self.assertEqual(result, expected)

    def test_add_elements_column_superset(self):
        """Surplus columns should be filtered-out before loading."""
        path = 'mynode.toron'
        node = Node(path)
        node.add_columns(['state', 'county'])  # <- Add columns.

        # Element rows include unknown columns "region" and "group".
        elements = [
            ('region', 'state', 'group',  'county'),  # <- Header row.
            ('WNC',    'IA',    'GROUP2', 'POLK'),
            ('ENC',    'IN',    'GROUP7', 'LA PORTE'),
            ('WNC',    'MN',    'GROUP1', 'HENNEPIN '),
        ]
        node.add_elements(elements) # <- No *columns* argument given.

        con = sqlite3.connect(path)
        result = con.execute('SELECT * FROM element').fetchall()
        expected = [
            (1, 'IA', 'POLK'),
            (2, 'IN', 'LA PORTE'),
            (3, 'MN', 'HENNEPIN '),
        ]
        self.assertEqual(result, expected)


class TestNodeAddWeights(TempDirTestCase):
    """Tests for node.add_weights() method."""
    def setUp(self):
        self.path = 'mynode.toron'
        self.node = Node(self.path)
        self.node.add_columns(['state', 'county', 'tract'])
        self.node.add_elements([
            ('state', 'county', 'tract'),
            ('12', '001', '000200'),
            ('12', '003', '040101'),
            ('12', '003', '040102'),
            ('12', '005', '000300'),
            ('12', '007', '000200'),
            ('12', '011', '010401'),
            ('12', '011', '010601'),
            ('12', '017', '450302'),
            ('12', '019', '030202'),
        ])

        con = sqlite3.connect(self.path)
        self.cursor = con.cursor()
        self.addCleanup(self.cleanup_temp_files)
        self.addCleanup(con.close)
        self.addCleanup(self.cursor.close)

    def test_full_column_match(self):
        columns = ('state', 'county', 'tract', 'pop10')
        weights = [
            ('12', '001', '000200', 110),
            ('12', '003', '040101', 212),
            ('12', '003', '040102', 17),
            ('12', '005', '000300', 10),
            ('12', '007', '000200', 414),
            ('12', '011', '010401', 223),
            ('12', '011', '010601', 141),
            ('12', '017', '450302', 183),
            ('12', '019', '030202', 62),
        ]
        self.node.add_weights(weights, columns, name='pop10', type_info={'category': 'census'})

        self.cursor.execute('SELECT * FROM weight')
        self.assertEqual(
            self.cursor.fetchall(),
            [(1, 'pop10', '{"category": "census"}', None, 1)],  # <- is_complete is 1
        )

        self.cursor.execute("""
            SELECT state, county, tract, value
            FROM element
            NATURAL JOIN element_weight
            WHERE weight_id=1
        """)
        self.assertEqual(set(self.cursor.fetchall()), set(weights))

    def test_skip_non_unique_matches(self):
        """Should only insert weights that match to a single element."""
        weights = [
            ('state', 'county', 'pop10'),
            ('12', '001', 110),
            ('12', '003', 229),  # <- Matches multiple elements.
            ('12', '005', 10),
            ('12', '007', 414),
            ('12', '011', 364),  # <- Matches multiple elements.
            ('12', '017', 183),
            ('12', '019', 62),
        ]
        self.node.add_weights(weights, name='pop10', type_info={'category': 'census'})

        self.cursor.execute('SELECT * FROM weight')
        self.assertEqual(
            self.cursor.fetchall(),
            [(1, 'pop10', '{"category": "census"}', None, 0)],  # <- is_complete is 0
        )

        # Get loaded weights/
        self.cursor.execute("""
            SELECT state, county, value
            FROM element
            JOIN element_weight USING (element_id)
            WHERE weight_id=1
        """)
        result = self.cursor.fetchall()

        expected = [
            ('12', '001', 110),
            #('12', '003', 229),  <- Not included because no unique match.
            ('12', '005', 10),
            ('12', '007', 414),
            #('12', '011', 364),  <- Not included because no unique match.
            ('12', '017', 183),
            ('12', '019', 62),
        ]
        self.assertEqual(set(result), set(expected))

    @unittest.expectedFailure
    def test_match_by_element_id(self):
        raise NotImplementedError

    @unittest.expectedFailure
    def test_mismatched_labels_and_element_id(self):
        raise NotImplementedError

