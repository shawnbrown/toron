"""Tests for AttributeGroupRepository class."""

import sqlite3
import unittest
from collections import OrderedDict

from toron.dal1.data_connector import DataConnector
from toron.data_models import AttributeGroup
from toron.dal1.repositories import AttributeGroupRepository


class TestAttributeGroupRepository(unittest.TestCase):
    def setUp(self):
        connector = DataConnector()
        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

    def assertRecords(self, expected_records, msg=None):
        self.cursor.execute(f'SELECT * FROM attribute_group')
        actual_records = self.cursor.fetchall()
        self.assertEqual(actual_records, expected_records, msg=msg)

    def test_add(self):
        repository = AttributeGroupRepository(self.cursor)

        repository.add({'aaa': 'A', 'bbb': 'B'})
        repository.add({'aaa': 'A', 'ccc': 'C'})

        self.assertRecords([
            (1, {'aaa': 'A', 'bbb': 'B'}),
            (2, {'aaa': 'A', 'ccc': 'C'}),
        ])

        msg = "should fail, {'aaa': 'A', 'bbb': 'B'} already exists"
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add(OrderedDict([('aaa', 'A'), ('bbb', 'B')]))

        msg = "should fail, attr already exists, key order should be normalized"
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add(OrderedDict([('bbb', 'B'), ('aaa', 'A')]))

        msg = "should be dict with str keys and str values"
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add({'aaa': 1})

        # NOTE!: When a Python dict is converted to a JSON object,
        # all of its keys are coerced to strings.
        repository.add({444: 'D', 555: 'E'})  # <- Keys are integers.
        self.assertRecords([
            (1, {'aaa': 'A', 'bbb': 'B'}),
            (2, {'aaa': 'A', 'ccc': 'C'}),
            (3, {'444': 'D', '555': 'E'}),  # <- Keys are now strings!
        ])

    def test_get(self):
        self.cursor.executescript("""
            INSERT INTO attribute_group VALUES (1, '{"aaa": "A", "bbb": "B"}');
            INSERT INTO attribute_group VALUES (2, '{"aaa": "A", "ccc": "C"}');
        """)
        repository = AttributeGroupRepository(self.cursor)

        self.assertEqual(repository.get(1), AttributeGroup(1, {'aaa': 'A', 'bbb': 'B'}))
        self.assertEqual(repository.get(2), AttributeGroup(2, {'aaa': 'A', 'ccc': 'C'}))
        with self.assertRaisesRegex(KeyError, 'no attribute group with id of 3'):
            repository.get(3)

    def test_update(self):
        self.cursor.executescript("""
            INSERT INTO attribute_group VALUES (1, '{"aaa": "A", "bbb": "B"}');
            INSERT INTO attribute_group VALUES (2, '{"aaa": "A", "ccc": "C"}');
        """)
        repository = AttributeGroupRepository(self.cursor)

        repository.update(AttributeGroup(1, {'xxx': 'X', 'zzz': 'Z'}))

        expected = [
            (1, {'xxx': 'X', 'zzz': 'Z'}),  # <- Value modified.
            (2, {'aaa': 'A', 'ccc': 'C'}),
        ]
        self.assertRecords(expected)

        repository.update(AttributeGroup(3, {'yyy': 'Y'}))  # No attribute_group_id=3.

        self.assertRecords(
            expected,
            msg='should be unchanged, there is no attribute_group_id=3',
        )

    def test_delete_and_cascade(self):
        self.cursor.executescript("""
            INSERT INTO attribute_group VALUES (1, '{"aaa": "A", "bbb": "B"}');
            INSERT INTO attribute_group VALUES (2, '{"aaa": "A", "ccc": "C"}');
        """)
        repository = AttributeGroupRepository(self.cursor)

        repository.delete_and_cascade(1)
        self.assertRecords([(2, {'aaa': 'A', 'ccc': 'C'})])

        repository.delete_and_cascade(2)
        self.assertRecords([])

        repository.delete_and_cascade(3)  # No attribute_group_id=3, should pass without error.
        self.assertRecords([])
