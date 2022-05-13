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

