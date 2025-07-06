"""Tests for CrosswalkRepository class."""

import sqlite3
import unittest

from toron.dal1.data_connector import DataConnector
from toron.data_models import Crosswalk
from toron.dal1.repositories import CrosswalkRepository


class TestCrosswalkRepository(unittest.TestCase):
    def setUp(self):
        connector = DataConnector()
        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        self.cursor = connection.cursor()
        self.addCleanup(self.cursor.close)

    def assertRecords(self, expected_records, msg=None):
        self.cursor.execute(f'SELECT * FROM crosswalk')
        actual_records = self.cursor.fetchall()
        self.assertEqual(actual_records, expected_records, msg=msg)

    def test_add(self):
        repository = CrosswalkRepository(self.cursor)

        repository.add('111-unique-id-1111', None, 'name1', is_default=True)
        repository.add('111-unique-id-1111', None, 'name2')  # <- Same `other_unique_id`, different name.
        repository.add(
            '222-unique-id-2222',  # <- Different `other_unique_id`.
            'somefile.toron',
            'name1',
            description='A crosswalk to some other node.',
            selectors=['[foo]', '[bar]'],
            is_default=True,
            user_properties={'prop1': 111},
            other_index_hash='78b320d6dbbb48c8',
            is_locally_complete=True,
        )

        # Note: The item forth from the end (`is_default`) is True/False in
        # the user-facing object (the Crosswalk record class) but it's 1/None
        # on the database side to facilitate the SQLite constraint that
        # enforces one default crosswalk per `other_unique_id`.
        self.assertRecords([
            (1, '111-unique-id-1111', None, 'name1', None, None, 1, None, None, 0),
            (2, '111-unique-id-1111', None, 'name2', None, None, None, None, None, 0),
            (3, '222-unique-id-2222',
                'somefile.toron',
                'name1',
                'A crosswalk to some other node.',
                ['[foo]', '[bar]'],
                1,
                {'prop1': 111},
                '78b320d6dbbb48c8',
                1)
        ])

        msg = "should fail, 'name' values must be unique per other_index_id"
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add('111-unique-id-1111', None, 'name1')  # <- The name "name1" already exists for this other_unique_id.

        msg = 'should fail, selectors must be strings'
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add('111-unique-id-1111', None, 'name2', selectors=[111, 222])  # <- Selectors are integers.

        msg = 'should fail, user_properties must be dict (JSON object)'
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add('111-unique-id-1111', None, 'name3', user_properties=['AAA', 'BBB'])

    def test_get(self):
        self.cursor.executescript("""
            INSERT INTO crosswalk VALUES (1, '111-unique-id-1111', NULL, 'name1', NULL, NULL, 1, NULL, NULL, 0);
            INSERT INTO crosswalk VALUES (2, '111-unique-id-1111', NULL, 'name2', NULL, NULL, NULL, NULL, NULL, 0);
            INSERT INTO crosswalk VALUES (3, '222-unique-id-2222', 'somefile.toron', 'name1',
                                          'A crosswalk to some other node.', '["[foo]", "[bar]"]',
                                          1, '{"prop1": 111}', '78b320d6dbbb48c8', 1);
        """)
        repository = CrosswalkRepository(self.cursor)

        self.assertEqual(
            repository.get(1),
            Crosswalk(
                id=1,
                other_unique_id='111-unique-id-1111',
                other_filename_hint=None,
                name='name1',
                description=None,
                selectors=None,
                is_default=True,  # <- Crosswalk value True, database value 1.
                user_properties=None,
                other_index_hash=None,
                is_locally_complete=False,  # <- Crosswalk value False, database value 0.
            ),
        )

        self.assertEqual(
            repository.get(2),
            Crosswalk(
                id=2,
                other_unique_id='111-unique-id-1111',
                other_filename_hint=None,
                name='name2',
                description=None,
                selectors=None,
                is_default=False,  # <- Crosswalk value False, database value NULL.
                user_properties=None,
                other_index_hash=None,
                is_locally_complete=False,  # <- Crosswalk value False, database value 0.
            ),
        )

        self.assertEqual(
            repository.get(3),
            Crosswalk(
                id=3,
                other_unique_id='222-unique-id-2222',
                other_filename_hint='somefile.toron',
                name='name1',
                description='A crosswalk to some other node.',
                selectors=['[foo]', '[bar]'],
                is_default=True,  # <- Crosswalk value True, database value 1.
                user_properties={'prop1': 111},
                other_index_hash='78b320d6dbbb48c8',
                is_locally_complete=True,  # <- Crosswalk value True, database value 1.
            ),
        )

        with self.assertRaisesRegex(KeyError, 'no crosswalk with id of 4'):
            repository.get(4)

    def test_get_all(self):
        self.cursor.executescript("""
            INSERT INTO crosswalk VALUES (1, '111-unique-id-1111', NULL, 'name1', NULL, NULL, 1, NULL, NULL, 0);
            INSERT INTO crosswalk VALUES (2, '111-unique-id-1111', NULL, 'name2', NULL, NULL, NULL, NULL, NULL, 0);
            INSERT INTO crosswalk VALUES (3, '222-unique-id-2222', 'somefile.toron', 'name1',
                                          'A crosswalk to some other node.', '["[foo]", "[bar]"]',
                                          1, '{"prop1": 111}', '78b320d6dbbb48c8', 1);
        """)
        repository = CrosswalkRepository(self.cursor)

        actual = repository.get_all()
        expected = [
            Crosswalk(
                id=1,
                other_unique_id='111-unique-id-1111',
                other_filename_hint=None,
                name='name1',
                description=None,
                selectors=None,
                is_default=True,
                user_properties=None,
                other_index_hash=None,
                is_locally_complete=False,
            ),
            Crosswalk(
                id=2,
                other_unique_id='111-unique-id-1111',
                other_filename_hint=None,
                name='name2',
                description=None,
                selectors=None,
                is_default=False,
                user_properties=None,
                other_index_hash=None,
                is_locally_complete=False,
            ),
            Crosswalk(
                id=3,
                other_unique_id='222-unique-id-2222',
                other_filename_hint='somefile.toron',
                name='name1',
                description='A crosswalk to some other node.',
                selectors=['[foo]', '[bar]'],
                is_default=True,
                user_properties={'prop1': 111},
                other_index_hash='78b320d6dbbb48c8',
                is_locally_complete=True,
            ),
        ]
        self.assertEqual(actual, expected)

    def test_update(self):
        self.cursor.executescript("""
            INSERT INTO crosswalk VALUES (1, '111-unique-id-1111', NULL, 'name1', NULL, NULL, 1, NULL, NULL, 0);
            INSERT INTO crosswalk VALUES (2, '111-unique-id-1111', NULL, 'name2', NULL, NULL, NULL, NULL, NULL, 0);
            INSERT INTO crosswalk VALUES (3, '222-unique-id-2222', 'somefile.toron', 'name1',
                                          'A crosswalk to some other node.', '["[foo]", "[bar]"]',
                                          1, '{"prop1": 111}', '78b320d6dbbb48c8', 1);
        """)
        repository = CrosswalkRepository(self.cursor)

        # Change name (matched WHERE crosswalk_id=2, all other values are SET).
        repository.update(Crosswalk(2, '111-unique-id-1111', None, 'name-two'))
        self.assertRecords([
            (1, '111-unique-id-1111', None, 'name1', None, None, 1, None, None, 0),
            (2, '111-unique-id-1111', None, 'name-two', None, None, None, None, None, 0),  # <- Name changed!
            (3, '222-unique-id-2222', 'somefile.toron', 'name1', 'A crosswalk to some other node.',
             ['[foo]', '[bar]'], 1, {'prop1': 111}, '78b320d6dbbb48c8', 1),
        ])

        # Check coersion from False to None for `is_default` column.
        repository.update(Crosswalk(1, '111-unique-id-1111', None, 'name1', is_default=False))
        self.assertRecords([
            (1, '111-unique-id-1111', None, 'name1', None, None, None, None, None, 0),  # <- 4th from end should be None!
            (2, '111-unique-id-1111', None, 'name-two', None, None, None, None, None, 0),
            (3, '222-unique-id-2222', 'somefile.toron', 'name1', 'A crosswalk to some other node.',
             ['[foo]', '[bar]'], 1, {'prop1': 111}, '78b320d6dbbb48c8', 1),
        ])

        # Check selectors JSON.
        repository.update(Crosswalk(3, '222-unique-id-2222', None, 'name1', selectors=['[baz]']))  # <- Set selector.
        self.assertRecords([
            (1, '111-unique-id-1111', None, 'name1', None, None, None, None, None, 0),
            (2, '111-unique-id-1111', None, 'name-two', None, None, None, None, None, 0),
            (3, '222-unique-id-2222', None, 'name1', None, ['[baz]'], None, None, None, 0),  # <- JSON should round-trip!
        ])

        # Check user_properties JSON.
        repository.update(Crosswalk(3, '222-unique-id-2222', None, 'name1', user_properties={'alt-prop': 42}))  # <- Set user_properties.
        self.assertRecords([
            (1, '111-unique-id-1111', None, 'name1', None, None, None, None, None, 0),
            (2, '111-unique-id-1111', None, 'name-two', None, None, None, None, None, 0),
            (3, '222-unique-id-2222', None, 'name1', None, None, None, {'alt-prop': 42}, None, 0),  # <- JSON should round-trip!
        ])

        msg = "should fail, 'name' values must be unique per other_index_id"
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.update(Crosswalk(2, '111-unique-id-1111', None, 'name1'))

        # No record exists with crosswalk_id=4.
        try:
            repository.update(Crosswalk(4, '444-unique-id-4444', None, 'name1'))
        except Exception as err:
            self.fail(f'updating non-existant records should not raise error, got {err!r}')

    def test_delete_and_cascade(self):
        self.cursor.executescript("""
            INSERT INTO crosswalk VALUES (1, '111-unique-id-1111', NULL, 'name1', NULL, NULL, NULL, NULL, NULL, 0);
            INSERT INTO crosswalk VALUES (2, '111-unique-id-1111', NULL, 'name2', NULL, NULL, NULL, NULL, NULL, 0);
        """)
        repository = CrosswalkRepository(self.cursor)

        repository.delete_and_cascade(1)
        self.assertRecords([(2, '111-unique-id-1111', None, 'name2', None, None, None, None, None, 0)])

        repository.delete_and_cascade(2)
        self.assertRecords([])

        try:
            repository.delete_and_cascade(3)  # No weight_group_id=3, should pass without error.
        except Exception as err:
            self.fail(f'should not raise error, got {err!r}')
        self.assertRecords([])

    def test_find_by_other_unique_id(self):
        self.cursor.executescript("""
            INSERT INTO crosswalk VALUES (1, '111-unique-id-1111', NULL, 'name1', NULL, NULL, NULL, NULL, NULL, 0);
            INSERT INTO crosswalk VALUES (2, '111-unique-id-1111', NULL, 'name2', NULL, NULL, NULL, NULL, NULL, 0);
            INSERT INTO crosswalk VALUES (3, '222-unique-id-2222', NULL, 'name1', NULL, NULL, NULL, NULL, NULL, 0);
        """)
        repository = CrosswalkRepository(self.cursor)

        actual = repository.find_by_other_unique_id('111-unique-id-1111')
        expected = [
            Crosswalk(1, '111-unique-id-1111', None, 'name1'),
            Crosswalk(2, '111-unique-id-1111', None, 'name2'),
        ]
        self.assertEqual(list(actual), expected)

        actual = repository.find_by_other_unique_id('222-unique-id-2222')
        expected = [
            Crosswalk(3, '222-unique-id-2222', None, 'name1'),
        ]
        self.assertEqual(list(actual), expected)

        actual = repository.find_by_other_unique_id('444-unique-id-4444')
        self.assertEqual(list(actual), [])

    def test_find_by_other_filename_hint(self):
        self.cursor.executescript("""
            INSERT INTO crosswalk VALUES (1, '111-unique-id-1111', 'fileone.toron', 'name1', NULL, NULL, NULL, NULL, NULL, 0);
            INSERT INTO crosswalk VALUES (2, '111-unique-id-1111', 'fileone.toron', 'name2', NULL, NULL, NULL, NULL, NULL, 0);
            INSERT INTO crosswalk VALUES (3, '222-unique-id-2222', NULL, 'name1', NULL, NULL, NULL, NULL, NULL, 0);
        """)
        repository = CrosswalkRepository(self.cursor)

        actual = repository.find_by_other_filename_hint('fileone.toron')
        expected = [
            Crosswalk(1, '111-unique-id-1111', 'fileone.toron', 'name1'),
            Crosswalk(2, '111-unique-id-1111', 'fileone.toron', 'name2'),
        ]
        self.assertEqual(list(actual), expected)

        actual = repository.find_by_other_filename_hint(None)
        self.assertEqual(list(actual), [], msg='should return empty iter when given None')

        actual = repository.find_by_other_filename_hint('unknownfile.toron')
        self.assertEqual(list(actual), [], msg='should return empty iter when given an unknown filename.')
