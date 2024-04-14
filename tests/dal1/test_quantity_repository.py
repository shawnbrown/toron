"""Tests for QuantityRepository class."""

import sqlite3
import unittest

from toron.dal1.data_connector import DataConnector
from toron.data_models import Quantity
from toron.dal1.repositories import QuantityRepository


class TestQuantityRepository(unittest.TestCase):
    def setUp(self):
        connector = DataConnector()
        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

        # Disable foreign keys for testing only.
        self.cursor.execute('PRAGMA foreign_keys=OFF')

    def assertRecords(self, expected_records, msg=None):
        self.cursor.execute(f'SELECT * FROM quantity')
        actual_records = self.cursor.fetchall()
        self.assertEqual(actual_records, expected_records, msg=msg)

    def test_add(self):
        repository = QuantityRepository(self.cursor)

        repository.add(1, 1, 131.0)  # Test positional.
        repository.add(location_id=1, attribute_id=2, value=109.0)  # Test keyword.

        self.assertRecords([(1, 1, 1, 131.0), (2, 1, 2, 109.0)])

        # NOTE: Currently the `quantity` table does not prevent multiple
        # attributes per location (this is different from how the `weight`
        # table restricts multiple index ids per weight_group).

        # Add second location (location_id=2).
        repository.add(location_id=2, attribute_id=1, value=151.0)
        repository.add(location_id=2, attribute_id=2, value=157.0)

        self.assertRecords([
            (1, 1, 1, 131),
            (2, 1, 2, 109),
            (3, 2, 1, 151),
            (4, 2, 2, 157),
        ])

    def test_get(self):
        self.cursor.executescript("""
            INSERT INTO quantity VALUES (1, 1, 1, 131.0);
            INSERT INTO quantity VALUES (2, 1, 2, 109.0);
        """)
        repository = QuantityRepository(self.cursor)

        self.assertEqual(repository.get(1), Quantity(1, 1, 1, 131.0))
        self.assertEqual(repository.get(2), Quantity(2, 1, 2, 109.0))
        self.assertIsNone(repository.get(3))

    def test_update(self):
        self.cursor.executescript("""
            INSERT INTO quantity VALUES (1, 1, 1, 131.0);
            INSERT INTO quantity VALUES (2, 1, 2, 109.0);
        """)
        repository = QuantityRepository(self.cursor)

        repository.update(Quantity(1, 1, 1, 173.0))
        repository.update(Quantity(2, 1, 3, 109.0))

        self.assertRecords([(1, 1, 1, 173.0), (2, 1, 3, 109.0)])

        repository.update(Quantity(3, 2, 4, 181.0))  # No quantity_id=3, should pass without error.
        self.assertRecords([(1, 1, 1, 173.0), (2, 1, 3, 109.0)], msg='should be unchanged')

    def test_delete(self):
        self.cursor.executescript("""
            INSERT INTO quantity VALUES (1, 1, 1, 131.0);
            INSERT INTO quantity VALUES (2, 1, 2, 109.0);
        """)
        repository = QuantityRepository(self.cursor)

        repository.delete(1)
        self.assertRecords([(2, 1, 2, 109.0)])

        repository.delete(2)
        self.assertRecords([])

        repository.delete(3)  # No quantity_id=3, should pass without error.
        self.assertRecords([])

    @unittest.skip('not implemented')
    def find_by_attribute_id(self):
        raise NotImplementedError
