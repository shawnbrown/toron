"""Tests for RelationRepository class."""

import sqlite3
import unittest

from toron.dal1.data_connector import DataConnector
from toron.data_models import Relation
from toron.dal1.repositories import RelationRepository


class TestRelationRepository(unittest.TestCase):
    def setUp(self):
        connector = DataConnector()
        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

        # Disable foreign keys for testing only.
        self.cursor.execute('PRAGMA foreign_keys=OFF')

    def assertRecords(self, expected_records, msg=None):
        self.cursor.execute(f'SELECT * FROM relation')
        actual_records = self.cursor.fetchall()
        self.assertEqual(actual_records, expected_records, msg=msg)

    def test_add(self):
        repository = RelationRepository(self.cursor)

        repository.add(9, 1, 1, None,     5.0)
        repository.add(9, 1, 2, None,     3.0, None)
        repository.add(9, 2, 3, None,    11.0, 1.0)
        repository.add(9, 2, 4, b'\x10',  7.0, None)

        self.assertRecords([
            (1, 9, 1, 1, None,    5.0, None),
            (2, 9, 1, 2, None,    3.0, None),
            (3, 9, 2, 3, None,   11.0, 1.0),
            (4, 9, 2, 4, b'\x10', 7.0, None),
        ])

        msg = 'should fail, `other_index_id` and `index_id` pairs must be unique per edge'
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add(9, 1, 2, None, 17.0)  # <- Pair id `1, 2` already exists for edge 9.

        # Add relations for a second edge.
        repository.add(10, 1, 1, None, 4.0)
        repository.add(10, 1, 2, None, 6.0)
        repository.add(10, 2, 3, None, 5.0)
        repository.add(10, 2, 4, None, 8.0)

        self.assertRecords([
            (1,  9, 1, 1, None,    5.0, None),
            (2,  9, 1, 2, None,    3.0, None),
            (3,  9, 2, 3, None,   11.0, 1.0),
            (4,  9, 2, 4, b'\x10', 7.0, None),
            (5, 10, 1, 1, None,    4.0, None),
            (6, 10, 1, 2, None,    6.0, None),
            (7, 10, 2, 3, None,    5.0, None),
            (8, 10, 2, 4, None,    8.0, None),
        ])

    def test_get(self):
        self.cursor.executescript("""
            INSERT INTO relation VALUES (1, 9, 1, 1, NULL,  5.0, NULL);
            INSERT INTO relation VALUES (2, 9, 2, 3, NULL,  3.0, 1.0);
            INSERT INTO relation VALUES (3, 9, 3, 5, X'10', 7.0, NULL);
        """)
        repository = RelationRepository(self.cursor)

        self.assertEqual(repository.get(1), Relation(1, 9, 1, 1, None,    5.0))
        self.assertEqual(repository.get(2), Relation(2, 9, 2, 3, None,    3.0, 1.0))
        self.assertEqual(repository.get(3), Relation(3, 9, 3, 5, b'\x10', 7.0))
        with self.assertRaisesRegex(KeyError, 'no relation with id of 4'):
            repository.get(4)

    def test_update(self):
        self.cursor.executescript("""
            INSERT INTO relation VALUES (1, 5, 1, 1, NULL,  125.0, NULL);
            INSERT INTO relation VALUES (2, 5, 1, 2, NULL,  375.0, NULL);
            INSERT INTO relation VALUES (3, 5, 2, 3, X'10', 620.0, NULL);
        """)
        repository = RelationRepository(self.cursor)

        repository.update(Relation(1, 5, 1, 1, None,    125.0, 0.25))
        repository.update(Relation(2, 5, 1, 2, None,    375.0, 0.75))
        repository.update(Relation(3, 5, 2, 3, b'\x10', 620.0, 1.00))

        expected = [
            (1, 5, 1, 1, None,    125.0, 0.25),
            (2, 5, 1, 2, None,    375.0, 0.75),
            (3, 5, 2, 3, b'\x10', 620.0, 1.00),
        ]
        self.assertRecords(expected)

        repository.update(Relation(4, 5, 3, 4, None, 570.0, 1.0))
        self.assertRecords(expected, msg='should be unchanged, no relation_id=4')

    def test_delete(self):
        self.cursor.executescript("""
            INSERT INTO relation VALUES (1, 5, 1, 1, NULL, 125.0, NULL);
            INSERT INTO relation VALUES (2, 5, 1, 2, NULL, 375.0, NULL);
        """)
        repository = RelationRepository(self.cursor)

        repository.delete(1)
        self.assertRecords([(2, 5, 1, 2, None, 375.0, None)])

        repository.delete(2)
        self.assertRecords([])

        repository.delete(3)  # No relation_id=3, should pass without error.
        self.assertRecords([])

    def test_crosswalk_is_complete(self):
        self.cursor.executescript("""
            ALTER TABLE main.node_index ADD COLUMN
                A TEXT NOT NULL CHECK (A != '') DEFAULT '-';

            INSERT INTO node_index VALUES (1, 'foo');
            INSERT INTO node_index VALUES (2, 'bar');

            INSERT INTO relation VALUES (1, 5, 1, 1, NULL, 125.0, NULL);
        """)
        repository = RelationRepository(self.cursor)

        self.assertFalse(
            repository.crosswalk_is_complete(crosswalk_id=5),
            msg='Crosswalk is not complete, no relation matches index_id 2.'
        )

        # Add a relation that matches to index_id 2.
        self.cursor.execute('INSERT INTO relation VALUES (2, 5, 1, 2, NULL, 375.0, NULL)')
        self.assertTrue(
            repository.crosswalk_is_complete(crosswalk_id=5),
            msg='Crosswalk is complete, should return True.'
        )
