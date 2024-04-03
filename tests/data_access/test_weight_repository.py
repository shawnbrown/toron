"""Tests for toron/_data_access/weight_repository.py module."""

import sqlite3
import unittest

from toron._data_access.data_connector import DataConnector
from toron._data_access.base_classes import Weight, BaseWeightRepository
from toron._data_access.repositories import WeightRepository


class TestWeightRepository(unittest.TestCase):
    def setUp(self):
        connector = DataConnector()
        resource = connector.acquire_resource()
        self.addCleanup(lambda: connector.release_resource(resource))

        self.cursor = resource.cursor()
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
        repository.add(weighting_id=1, index_id=2, value=7.0)  # Test keyword.

        self.assertRecords([(1, 1, 1, 3.0), (2, 1, 2, 7.0)])

        msg='should fail, index_id values must be unique per weighting'
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add(1, 2, 5.0)  # <- Weighting 1 already has index_id 2.

        # Add second weighting (weighting_id=2).
        repository.add(weighting_id=2, index_id=1, value=6.0)
        repository.add(weighting_id=2, index_id=2, value=8.0)

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
    def test_find_by_weighting_id(self):
        raise NotImplementedError
