"""Tests for WeightRepository class."""

import sqlite3
import unittest

from toron.dal1.data_connector import DataConnector
from toron.data_models import Weight, BaseWeightRepository
from toron.dal1.repositories import WeightRepository


class TestWeightRepository(unittest.TestCase):
    def setUp(self):
        connector = DataConnector()
        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

        # Disable foreign keys for testing only.
        self.cursor.execute('PRAGMA foreign_keys=OFF')

    def assertRecords(self, expected_records, msg=None):
        self.cursor.execute(f'SELECT * FROM weight')
        actual_records = self.cursor.fetchall()
        self.assertEqual(actual_records, expected_records, msg=msg)

    def test_inheritance(self):
        """Should subclass from appropriate abstract base class."""
        self.assertTrue(issubclass(WeightRepository, BaseWeightRepository))

    def test_add(self):
        repository = WeightRepository(self.cursor)

        repository.add(1, 1, 3.0)  # Test positional.
        repository.add(weight_group_id=1, index_id=2, value=7.0)  # Test keyword.

        self.assertRecords([(1, 1, 1, 3.0), (2, 1, 2, 7.0)])

        msg='should fail, index_id values must be unique per weight group'
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add(1, 2, 5.0)  # <- WeightGroup 1 already has index_id 2.

        # Add second weight group (weight_group_id=2).
        repository.add(weight_group_id=2, index_id=1, value=6.0)
        repository.add(weight_group_id=2, index_id=2, value=8.0)

        self.assertRecords([(1, 1, 1, 3.0), (2, 1, 2, 7.0),
                            (3, 2, 1, 6.0), (4, 2, 2, 8.0)])

    def test_get(self):
        self.cursor.executescript("""
            INSERT INTO weight VALUES (1, 1, 1, 3.0);
            INSERT INTO weight VALUES (2, 1, 2, 7.0);
        """)
        repository = WeightRepository(self.cursor)

        self.assertEqual(repository.get(1), Weight(1, 1, 1, 3.0))
        self.assertEqual(repository.get(2), Weight(2, 1, 2, 7.0))
        self.assertIsNone(repository.get(3))

    def test_update(self):
        self.cursor.executescript("""
            INSERT INTO weight VALUES (1, 1, 1, 3.0);
            INSERT INTO weight VALUES (2, 1, 2, 7.0);
        """)
        repository = WeightRepository(self.cursor)

        repository.update(Weight(1, 1, 1, 25.0))
        repository.update(Weight(2, 1, 2, 55.0))

        self.assertRecords([(1, 1, 1, 25.0), (2, 1, 2, 55.0)])

        repository.update(Weight(3, 1, 2, 55.0))  # No weight_id=3, should pass without error.
        self.assertRecords([(1, 1, 1, 25.0), (2, 1, 2, 55.0)], msg='should be unchanged')

    def test_delete(self):
        self.cursor.executescript("""
            INSERT INTO weight VALUES (1, 1, 1, 3.0);
            INSERT INTO weight VALUES (2, 1, 2, 7.0);
        """)
        repository = WeightRepository(self.cursor)

        repository.delete(1)
        self.assertRecords([(2, 1, 2, 7.0)])

        repository.delete(2)
        self.assertRecords([])

        repository.delete(3)  # No weight_id=3, should pass without error.
        self.assertRecords([])

    @unittest.skip('not implemented')
    def test_find_by_weight_group_id(self):
        raise NotImplementedError
