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

