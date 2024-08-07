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

        repository.add(9, 1, 1,  5.0)
        repository.add(9, 1, 2,  3.0, None,    None)
        repository.add(9, 2, 3, 11.0, None,    1.0)
        repository.add(9, 2, 4,  7.0, b'\x10', None)

        self.assertRecords([
            (1, 9, 1, 1,  5.0, None,    None),
            (2, 9, 1, 2,  3.0, None,    None),
            (3, 9, 2, 3, 11.0, None,    1.0),
            (4, 9, 2, 4,  7.0, b'\x10', None),
        ])

        msg = 'should fail, `other_index_id` and `index_id` pairs must be unique per edge'
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add(9, 1, 2, 17.0)  # <- Pair id 1/2 already exists for edge 9.

        # Add a second edge.
        repository.add(10, 1, 1, 4.0)
        repository.add(10, 1, 2, 6.0)
        repository.add(10, 2, 3, 5.0)
        repository.add(10, 2, 4, 8.0)

        self.assertRecords([
            (1,  9, 1, 1,  5.0, None,    None),
            (2,  9, 1, 2,  3.0, None,    None),
            (3,  9, 2, 3, 11.0, None,    1.0),
            (4,  9, 2, 4,  7.0, b'\x10', None),
            (5, 10, 1, 1,  4.0, None,    None),
            (6, 10, 1, 2,  6.0, None,    None),
            (7, 10, 2, 3,  5.0, None,    None),
            (8, 10, 2, 4,  8.0, None,    None),
        ])

    def test_get(self):
        self.cursor.executescript("""
            INSERT INTO relation VALUES (1, 9, 1, 1, 5.0, NULL,  NULL);
            INSERT INTO relation VALUES (2, 9, 2, 3, 3.0, NULL,  1.0);
            INSERT INTO relation VALUES (3, 9, 3, 5, 7.0, X'10', NULL);
        """)
        repository = RelationRepository(self.cursor)

        self.assertEqual(repository.get(1), Relation(1, 9, 1, 1, 5.0))
        self.assertEqual(repository.get(2), Relation(2, 9, 2, 3, 3.0, None, 1.0))
        self.assertEqual(repository.get(3), Relation(3, 9, 3, 5, 7.0, b'\x10'))
        self.assertIsNone(repository.get(4))

    def test_update(self):
        self.cursor.executescript("""
            INSERT INTO relation VALUES (1, 5, 1, 1, 125.0, NULL, NULL);
            INSERT INTO relation VALUES (2, 5, 1, 2, 375.0, NULL, NULL);
            INSERT INTO relation VALUES (3, 5, 2, 3, 620.0, X'10', NULL);
        """)
        repository = RelationRepository(self.cursor)

        repository.update(Relation(1, 5, 1, 1, 125.0, None,    0.25))
        repository.update(Relation(2, 5, 1, 2, 375.0, None,    0.75))
        repository.update(Relation(3, 5, 2, 3, 620.0, b'\x10', 1.0))

        expected = [
            (1, 5, 1, 1, 125.0, None,    0.25),
            (2, 5, 1, 2, 375.0, None,    0.75),
            (3, 5, 2, 3, 620.0, b'\x10', 1.0),
        ]
        self.assertRecords(expected)

        repository.update(Relation(4, 5, 3, 4, 570.0, None, 1.0))
        self.assertRecords(expected, msg='should be unchanged, no relation_id=4')

    def test_delete(self):
        self.cursor.executescript("""
            INSERT INTO relation VALUES (1, 5, 1, 1, 125.0, NULL, NULL);
            INSERT INTO relation VALUES (2, 5, 1, 2, 375.0, NULL, NULL);
        """)
        repository = RelationRepository(self.cursor)

        repository.delete(1)
        self.assertRecords([(2, 5, 1, 2, 375.0, None, None)])

        repository.delete(2)
        self.assertRecords([])

        repository.delete(3)  # No relation_id=3, should pass without error.
        self.assertRecords([])

    @unittest.skip('not implemented')
    def test_find_by_crosswalk_id(self):
        raise NotImplementedError

    def test_crosswalk_is_complete(self):
        self.cursor.executescript("""
            ALTER TABLE main.node_index ADD COLUMN
                A TEXT NOT NULL CHECK (A != '') DEFAULT '-';

            INSERT INTO node_index VALUES (1, 'foo');
            INSERT INTO node_index VALUES (2, 'bar');

            INSERT INTO relation VALUES (1, 5, 1, 1, 125.0, NULL, NULL);
        """)
        repository = RelationRepository(self.cursor)

        self.assertFalse(
            repository.crosswalk_is_complete(crosswalk_id=5),
            msg='Crosswalk is not complete, no relation matches index_id 2.'
        )

        # Add a relation that matches to index_id 2.
        self.cursor.execute('INSERT INTO relation VALUES (2, 5, 1, 2, 375.0, NULL, NULL)')
        self.assertTrue(
            repository.crosswalk_is_complete(crosswalk_id=5),
            msg='Crosswalk is complete, should return True.'
        )
