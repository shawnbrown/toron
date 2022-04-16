"""Tests for toron/node.py module."""

import os
import sqlite3
import unittest
from .common import MkdtempTestCase
from toron.node import Node


class TestNode(MkdtempTestCase):
    def test_new_node(self):
        """If a node file doesn't exist it should be created."""
        path = 'mynode.node'
        node = Node(path)  # Creates node file if none exists.

        con = sqlite3.connect(path)
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur}
        tables.discard('sqlite_sequence')  # <- Table added by SQLite.

        expected = {
            'edge',
            'element',
            'location',
            'property',
            'quantity',
            'relation',
            'structure',
            'weight',
            'weight_info',
        }
        self.assertSetEqual(tables, expected)

    def test_nonfile_path(self):
        """Non-file resources should fail immediately."""
        path = 'mydirectory'
        os.mkdir(path)  # <- Create a directory with the given `path` name.

        msg = 'should fail if path is a directory instead of a file'
        with self.assertRaisesRegex(Exception, 'not a Toron Node', msg=msg):
            node = Node(path)

