"""Tests for toron/_data_access/schema.py module."""
import sqlite3
import unittest
from contextlib import closing

from toron._data_access.schema import (
    create_node_schema,
)


class TestCreateNodeSchema(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(
            database=':memory:',
            detect_types=sqlite3.PARSE_DECLTYPES,
            isolation_level=None,
        )
        self.addCleanup(self.connection.close)

    @staticmethod
    def get_tables(connection):
        """Helper function to return tables present in SQLite database."""
        with closing(connection.cursor()) as cur:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return {row[0] for row in cur}

    def test_empty_schema(self):
        """Should create new schema when database is empty."""
        create_node_schema(self.connection)

        tables = self.get_tables(self.connection)
        expected = {
            'attribute',
            'edge',
            'location',
            'node_index',
            'property',
            'quantity',
            'relation',
            'structure',
            'weight',
            'weighting',
            'sqlite_sequence',  # <- Table added by SQLite.
        }
        self.assertSetEqual(tables, expected)

    def test_nonempty_schema(self):
        """Should raise an error when database already has other tables."""
        self.connection.execute("""
            CREATE TABLE dummy_table (
                dummy_id INTEGER PRIMARY KEY,
                dummy_value TEXT
            )
        """)

        regex = "database must be empty; found tables: 'dummy_table'"
        with self.assertRaisesRegex(RuntimeError, regex):
            create_node_schema(self.connection)
