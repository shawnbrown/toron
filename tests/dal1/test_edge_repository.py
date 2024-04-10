"""Tests for EdgeRepository class."""

import sqlite3
import unittest

from toron.dal1.data_connector import DataConnector
from toron.data_models import Edge
from toron.dal1.repositories import EdgeRepository


class TestEdgeRepository(unittest.TestCase):
    def setUp(self):
        connector = DataConnector()
        resource = connector.acquire_resource()
        self.addCleanup(lambda: connector.release_resource(resource))

        self.cursor = resource.cursor()
        self.addCleanup(self.cursor.close)

    def assertRecords(self, expected_records, msg=None):
        self.cursor.execute(f'SELECT * FROM edge')
        actual_records = self.cursor.fetchall()
        self.assertEqual(actual_records, expected_records, msg=msg)

    def test_add(self):
        repository = EdgeRepository(self.cursor)

        repository.add('name1', '111-unique-id-1111', is_default=True)
        repository.add('name2', '111-unique-id-1111')  # <- Same `other_unique_id`, different name.
        repository.add(
            'name1',
            '222-unique-id-2222',  # <- Different `other_unique_id`.
            other_filename_hint='somefile.toron',
            other_index_hash='78b320d6dbbb48c8',
            description='An edge to some other node.',
            selectors=['[foo]', '[bar]'],
            user_properties={'prop1': 111},
            is_locally_complete=True,
            is_default=True,
        )

        # Note: The last item (`is_default`) is True/False on the user-facing
        # object (the Edge record class) but it's 1/None on the database side
        # to facilitate the SQLite constraint that enforces one default edge
        # per `other_unique_id`.
        self.assertRecords([
            (1, 'name1', '111-unique-id-1111', None, None, None, None, None, 0, 1),
            (2, 'name2', '111-unique-id-1111', None, None, None, None, None, 0, None),
            (3, 'name1', '222-unique-id-2222', 'somefile.toron', '78b320d6dbbb48c8',
             'An edge to some other node.', ['[foo]', '[bar]'], {'prop1': 111}, 1, 1),
        ])

        msg = "should fail, 'name' values must be unique per other_index_id"
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add('name1', '111-unique-id-1111')  # <- The name "name1" already exists for this other_unique_id.

        msg = 'should fail, selectors must be strings'
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add('name2', '111-unique-id-1111', selectors=[111, 222])  # <- Selectors are integers.

        msg = 'should fail, user_properties must be dict (JSON object)'
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.add('name3', '111-unique-id-1111', user_properties=['AAA', 'BBB'])

    def test_get(self):
        self.cursor.executescript("""
            INSERT INTO edge VALUES (1, 'name1', '111-unique-id-1111', NULL, NULL, NULL, NULL, NULL, 0, 1);
            INSERT INTO edge VALUES (2, 'name2', '111-unique-id-1111', NULL, NULL, NULL, NULL, NULL, 0, NULL);
            INSERT INTO edge VALUES (3, 'name1', '222-unique-id-2222', 'somefile.toron', '78b320d6dbbb48c8',
                                     'An edge to some other node.', '["[foo]", "[bar]"]', '{"prop1": 111}', 1, 1);
        """)
        repository = EdgeRepository(self.cursor)

        self.assertEqual(
            repository.get(1),
            Edge(
                id=1,
                name='name1',
                other_unique_id='111-unique-id-1111',
                other_filename_hint=None,
                other_index_hash=None,
                description=None,
                selectors=None,
                user_properties=None,
                is_locally_complete=False,  # <- Edge value False, database value 0.
                is_default=True,  # <- Edge value True, database value 1.
            ),
        )

        self.assertEqual(
            repository.get(2),
            Edge(
                id=2,
                name='name2',
                other_unique_id='111-unique-id-1111',
                other_filename_hint=None,
                other_index_hash=None,
                description=None,
                selectors=None,
                user_properties=None,
                is_locally_complete=False,  # <- Edge value False, database value 0.
                is_default=False,  # <- Edge value False, database value NULL.
            ),
        )

        self.assertEqual(
            repository.get(3),
            Edge(
                id=3,
                name='name1',
                other_unique_id='222-unique-id-2222',
                other_filename_hint='somefile.toron',
                other_index_hash='78b320d6dbbb48c8',
                description='An edge to some other node.',
                selectors=['[foo]', '[bar]'],
                user_properties={'prop1': 111},
                is_locally_complete=True,  # <- Edge value True, database value 1.
                is_default=True,  # <- Edge value True, database value 1.
            ),
        )

        self.assertIsNone(repository.get(4))  # <- No edge_id=4.

    def test_update(self):
        self.cursor.executescript("""
            INSERT INTO edge VALUES (1, 'name1', '111-unique-id-1111', NULL, NULL, NULL, NULL, NULL, 0, 1);
            INSERT INTO edge VALUES (2, 'name2', '111-unique-id-1111', NULL, NULL, NULL, NULL, NULL, 0, NULL);
            INSERT INTO edge VALUES (3, 'name1', '222-unique-id-2222', 'somefile.toron', '78b320d6dbbb48c8',
                                     'An edge to some other node.', '["[foo]", "[bar]"]', '{"prop1": 111}', 1, 1);
        """)
        repository = EdgeRepository(self.cursor)

        # Change name (matched WHERE edge_id=2, all other values are SET).
        repository.update(Edge(2, 'name-two', '111-unique-id-1111'))
        self.assertRecords([
            (1, 'name1', '111-unique-id-1111', None, None, None, None, None, 0, 1),
            (2, 'name-two', '111-unique-id-1111', None, None, None, None, None, 0, None),  # <- Name changed!
            (3, 'name1', '222-unique-id-2222', 'somefile.toron', '78b320d6dbbb48c8',
             'An edge to some other node.', ['[foo]', '[bar]'], {'prop1': 111}, 1, 1),
        ])

        # Check coersion from False to None for `is_default` column.
        repository.update(Edge(1, 'name1', '111-unique-id-1111', is_default=False))
        self.assertRecords([
            (1, 'name1', '111-unique-id-1111', None, None, None, None, None, 0, None),  # <- Should end with None!
            (2, 'name-two', '111-unique-id-1111', None, None, None, None, None, 0, None),
            (3, 'name1', '222-unique-id-2222', 'somefile.toron', '78b320d6dbbb48c8',
             'An edge to some other node.', ['[foo]', '[bar]'], {'prop1': 111}, 1, 1),
        ])

        # Check selectors JSON.
        repository.update(Edge(3, 'name1', '222-unique-id-2222', selectors=['[baz]']))  # <- Set selector.
        self.assertRecords([
            (1, 'name1', '111-unique-id-1111', None, None, None, None, None, 0, None),
            (2, 'name-two', '111-unique-id-1111', None, None, None, None, None, 0, None),
            (3, 'name1', '222-unique-id-2222', None, None, None, ['[baz]'], None, 0, None),  # <- JSON should round-trip!
        ])

        # Check user_properties JSON.
        repository.update(Edge(3, 'name1', '222-unique-id-2222', user_properties={'alt-prop': 42}))  # <- Set user_properties.
        self.assertRecords([
            (1, 'name1', '111-unique-id-1111', None, None, None, None, None, 0, None),
            (2, 'name-two', '111-unique-id-1111', None, None, None, None, None, 0, None),
            (3, 'name1', '222-unique-id-2222', None, None, None, None, {'alt-prop': 42}, 0, None),  # <- JSON should round-trip!
        ])

        msg = "should fail, 'name' values must be unique per other_index_id"
        with self.assertRaises(sqlite3.IntegrityError, msg=msg):
            repository.update(Edge(2, 'name1', '111-unique-id-1111'))

        # No record exists with edge_id=4.
        try:
            repository.update(Edge(4, 'name1', '444-unique-id-4444'))
        except Exception as err:
            self.fail(f'updating non-existant records should not raise error, got {err!r}')

    def test_delete(self):
        self.cursor.executescript("""
            INSERT INTO edge VALUES (1, 'name1', '111-unique-id-1111', NULL, NULL, NULL, NULL, NULL, 0, 1);
            INSERT INTO edge VALUES (2, 'name2', '111-unique-id-1111', NULL, NULL, NULL, NULL, NULL, 0, NULL);
        """)
        repository = EdgeRepository(self.cursor)

        repository.delete(1)
        self.assertRecords([(2, 'name2', '111-unique-id-1111', None, None, None, None, None, 0, None)])

        repository.delete(2)
        self.assertRecords([])

        try:
            repository.delete(3)  # No weighting_id=3, should pass without error.
        except Exception as err:
            self.fail(f'should not raise error, got {err!r}')
        self.assertRecords([])
