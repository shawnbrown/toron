"""Tests for WeightingRepository class."""

import sqlite3
import unittest

from toron._data_access.data_connector import DataConnector
from toron._data_models import Weighting
from toron._data_access.repositories import WeightingRepository


class TestWeightingRepository(unittest.TestCase):
    def setUp(self):
        connector = DataConnector()
        resource = connector.acquire_resource()
        self.addCleanup(lambda: connector.release_resource(resource))

        self.cursor = resource.cursor()
        self.addCleanup(self.cursor.close)

    def assertRecords(self, expected_records, msg=None):
        self.cursor.execute(f'SELECT * FROM weighting')
        actual_records = self.cursor.fetchall()
        self.assertEqual(actual_records, expected_records, msg=msg)

    def test_add(self):
        repository = WeightingRepository(self.cursor)

        # Test various default values.
        repository.add('name1')
        repository.add('name2', 'Name Two')
        repository.add('name3', 'Name Three', ['[foo]', '[bar]'])
        repository.add('name4', 'Name Four', '[baz]')
        repository.add('name5', 'Name Five', ['[qux]', '[quux]'], True)

        self.assertRecords([
            (1, 'name1', None, None, 0),
            (2, 'name2', 'Name Two', None, 0),
            (3, 'name3', 'Name Three', ['[foo]', '[bar]'], 0),
            (4, 'name4', 'Name Four', ['[baz]'], 0),
            (5, 'name5', 'Name Five', ['[qux]', '[quux]'], 1),
        ])

        msg = "should fail, 'name' values must be unique per weighting"
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add('name5')  # <- The name "name5" already exists.

        msg = 'should fail, selectors must be strings'
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add('name6', 'Name Six', [111, 222])  # <- Selectors are integers.

    def test_get(self):
        self.cursor.executescript("""
            INSERT INTO weighting VALUES (1, 'name1', NULL, NULL, 1);
            INSERT INTO weighting VALUES (2, 'name2', NULL, '["[foo]", "[bar]"]', 0);
        """)
        repository = WeightingRepository(self.cursor)

        self.assertEqual(repository.get(1), Weighting(1, 'name1', None, None, 1))
        self.assertEqual(repository.get(2), Weighting(2, 'name2', None, ['[foo]', '[bar]'], 0))
        self.assertIsNone(repository.get(3))

    def test_update(self):
        self.cursor.executescript("""
            INSERT INTO weighting VALUES (1, 'name1', NULL, NULL, 1);
            INSERT INTO weighting VALUES (2, 'name2', NULL, '["[bar]"]', 0);
        """)
        repository = WeightingRepository(self.cursor)

        repository.update(Weighting(1, 'name1', 'Name One', ['[foo]'], 1))

        self.assertRecords([
            (1, 'name1', 'Name One', ['[foo]'], 1),
            (2, 'name2', None, ['[bar]'], 0),
        ])

        repository.update(Weighting(3, 'name3', None, None, 1))  # No weighting_id=3, should pass without error.

        self.assertRecords(
            [
                (1, 'name1', 'Name One', ['[foo]'], 1),
                (2, 'name2', None, ['[bar]'], 0),
            ],
            msg='No weighting_id=3, should remain unchanged',
        )

    def test_delete(self):
        self.cursor.executescript("""
            INSERT INTO weighting VALUES (1, 'name1', 'Name One', '["[foo]"]', 1);
            INSERT INTO weighting VALUES (2, 'name2', NULL, '["[bar]"]', 0);
        """)
        repository = WeightingRepository(self.cursor)

        repository.delete(1)
        self.assertRecords([(2, 'name2', None, ['[bar]'], 0)])

        repository.delete(2)
        self.assertRecords([])

        repository.delete(3)  # No weighting_id=3, should pass without error.
        self.assertRecords([])
