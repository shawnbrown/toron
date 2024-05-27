"""Tests for toron/node.py module."""

import sqlite3
import sys
import unittest
from contextlib import suppress
from unittest.mock import (
    Mock,
    call,
    sentinel,
)
if sys.version_info >= (3, 8):
    from typing import get_args
else:
    from typing_extensions import get_args

from toron._utils import ToronWarning
from toron.data_models import (
    Crosswalk,
    Index,
    Structure,
    WeightGroup,
)
from toron.node import Node


class TestInstantiation(unittest.TestCase):
    def test_backend_implicit(self):
        """When no arguments are given, should create empty node."""
        node = Node()
        self.assertEqual(node._dal.backend, 'DAL1')

    def test_backend_explicit(self):
        """The ``backend`` can be given explicitly."""
        node = Node(backend='DAL1')
        self.assertEqual(node._dal.backend, 'DAL1')

    def test_backend_keyword_only(self):
        """The ``backend`` argument is keyword-only (not positional)."""
        with self.assertRaises(TypeError):
            node = Node('DAL1')  # Using positional argument.

    def test_backend_unknown(self):
        """Invalid ``backend`` values should raise an error."""
        with self.assertRaises(RuntimeError):
            node = Node(backend=None)

        with self.assertRaises(RuntimeError):
            node = Node(backend='DAL#')

    def test_kwds(self):
        """The ``**kwds`` are used to create a DataConnector."""
        node = Node(cache_to_drive=True)


class TestManagedConnectionCursorAndTransaction(unittest.TestCase):
    def test_managed_connection_type(self):
        """Connection manager should return appropriate type."""
        node = Node()  # Create node and get connection type (generic T1).
        connection_type = get_args(node._dal.DataConnector.__orig_bases__[0])[0]

        with node._managed_connection() as connection:
            pass

        self.assertIsInstance(connection, connection_type)

    def test_managed_connection_calls(self):
        """Connection manager should interact with connection methods."""
        node = Node()
        node._connector = Mock()

        with node._managed_connection() as connection:
            node._connector.assert_has_calls([
                call.acquire_connection(),  # <- Connection acquired.
            ])

        node._connector.assert_has_calls([
            call.acquire_connection(),
            call.release_connection(connection),  # <- Connection released.
        ])

    def test_managed_cursor_type(self):
        """Data cursor manager should return appropriate type."""
        node = Node()  # Create node and get cursor type (generic T2).
        cursor_type = get_args(node._dal.DataConnector.__orig_bases__[0])[1]

        with node._managed_connection() as connection:
            with node._managed_cursor(connection) as cursor:
                pass

        self.assertIsInstance(cursor, cursor_type)

    def test_managed_cursor_calls(self):
        """Cursor manager should interact with cursor methods."""
        node = Node()
        node._connector = Mock()

        # The acquire_connection() mock must return unique objects.
        node._connector.acquire_connection.side_effect = lambda: object()

        with node._managed_connection() as connection:
            with node._managed_cursor(connection) as cursor:
                node._connector.assert_has_calls([
                    call.acquire_cursor(connection),  # <- Cursor acquired.
                ])

            node._connector.assert_has_calls([
                call.acquire_cursor(connection),
                call.release_cursor(cursor),  # <- Cursor released.
            ])

    def test_managed_cursor_calls_implicit_connection(self):
        """Test ``_managed_cursor`` called without ``connection`` argument
        (should automatically create a connection internally).
        """
        node = Node()
        node._connector = Mock()
        dummy_connections = [sentinel.con1, sentinel.con2]
        node._connector.acquire_connection.side_effect = dummy_connections

        with node._managed_cursor() as cursor:  # <- No `connection` passed.
            node._connector.assert_has_calls([
                call.acquire_connection(),  # <- Connection acquired automatically.
                call.acquire_cursor(sentinel.con1),  # <- Cursor acquired.
            ])

        node._connector.assert_has_calls([
            call.release_cursor(cursor),  # <- Cursor released.
            call.release_connection(sentinel.con1),  # <- Connection released.
        ])

    def test_managed_transaction(self):
        """Should commit changes when no errors occur."""
        node = Node()
        node._connector = Mock()
        node._connector.acquire_connection.return_value = sentinel.con
        node._connector.acquire_cursor.return_value = sentinel.cur

        with node._managed_connection() as connection:
            with node._managed_cursor(connection) as cursor:
                with node._managed_transaction(cursor) as cursor:
                    node._connector.assert_has_calls([
                        call.acquire_connection(),
                        call.acquire_cursor(sentinel.con),
                        call.transaction_begin(sentinel.cur),  # <- BEGIN
                    ])

        node._connector.assert_has_calls([
            call.transaction_commit(sentinel.cur),  # <- COMMIT
            call.release_cursor(sentinel.cur),
            call.release_connection(sentinel.con),
        ])

    def test_managed_transaction_rollback(self):
        """Should roll-back changes when an error occurs."""
        node = Node()
        node._connector = Mock()
        node._connector.acquire_connection.return_value = sentinel.con
        node._connector.acquire_cursor.return_value = sentinel.cur

        with suppress(RuntimeError):
            with node._managed_connection() as connection:
                with node._managed_cursor(connection) as cursor:
                    with node._managed_transaction(cursor) as cursor:
                        raise RuntimeError  # <- Error inside the transaction.

        node._connector.assert_has_calls([
            call.acquire_connection(),
            call.acquire_cursor(sentinel.con),
            call.transaction_begin(sentinel.cur),
            call.transaction_rollback(sentinel.cur),  # <- ROLLBACK
            call.release_cursor(sentinel.cur),
            call.release_connection(sentinel.con),
        ])

    def test_managed_transaction_implicit_resources(self):
        """When called without args, should auto-acquire resources."""
        node = Node()
        node._connector = Mock()
        node._connector.acquire_connection.return_value = sentinel.con
        node._connector.acquire_cursor.return_value = sentinel.cur

        with node._managed_transaction() as cursor:
            pass

        node._connector.assert_has_calls([
            call.acquire_connection(),
            call.acquire_cursor(sentinel.con),
            call.transaction_begin(sentinel.cur),
            call.transaction_commit(sentinel.cur),  # <- COMMIT
            call.release_cursor(sentinel.cur),
            call.release_connection(sentinel.con),
        ])

    def test_managed_transaction_implicit_resources(self):
        node = Node()
        node._connector = Mock()
        node._connector.acquire_connection.return_value = sentinel.con
        node._connector.acquire_cursor.return_value = sentinel.cur

        with suppress(RuntimeError):
            with node._managed_transaction() as cursor:
                raise RuntimeError  # <- Error inside the transaction.

        node._connector.assert_has_calls([
            call.acquire_connection(),
            call.acquire_cursor(sentinel.con),
            call.transaction_begin(sentinel.cur),
            call.transaction_rollback(sentinel.cur),  # <- ROLLBACK
            call.release_cursor(sentinel.cur),
            call.release_connection(sentinel.con),
        ])


class TestDiscreteCategoriesMethods(unittest.TestCase):
    def setUp(self):
        self.node = Node()
        with self.node._managed_cursor() as cur:
            column_manager = self.node._dal.ColumnManager(cur)
            index_repo = self.node._dal.IndexRepository(cur)

            column_manager.add_columns('A', 'B', 'C')
            index_repo.add('a1', 'b1', 'c1')
            index_repo.add('a1', 'b1', 'c2')
            index_repo.add('a1', 'b2', 'c3')
            index_repo.add('a1', 'b2', 'c4')
            index_repo.add('a2', 'b3', 'c5')
            index_repo.add('a2', 'b3', 'c6')
            index_repo.add('a2', 'b4', 'c7')
            index_repo.add('a2', 'b4', 'c8')

    def get_structure_helper(self):  # <- Helper function.
        """Return structure in order of structure_id."""
        with self.node._managed_cursor() as cursor:
            resutls = self.node._dal.StructureRepository(cursor).get_all()
            return sorted(resutls, key=lambda structure: structure.id)

    def test_discrete_categories_property(self):
        node = self.node
        with node._managed_cursor() as cursor:
            prop_repo = self.node._dal.PropertyRepository(cursor)
            prop_repo.add('discrete_categories', [['A'], ['B'], ['A', 'C']])

        self.assertEqual(node.discrete_categories, [{'A'}, {'B'}, {'A', 'C'}])

    def test_add_discrete_categories(self):
        node = self.node

        # Creates the property if it doesn't exist.
        node.add_discrete_categories({'A'}, {'B'})
        self.assertEqual(node.discrete_categories, [{'A'}, {'B'}, {'A', 'B', 'C'}])
        expected = [
            Structure(id=1, granularity=None, bits=(0, 0, 0)),
            Structure(id=2, granularity=1.0,  bits=(1, 0, 0)),
            Structure(id=3, granularity=2.0,  bits=(0, 1, 0)),
            Structure(id=4, granularity=3.0,  bits=(1, 1, 1)),
            Structure(id=5, granularity=2.0,  bits=(1, 1, 0)),
        ]
        self.assertEqual(self.get_structure_helper(), expected)

        # Updates the property if it does exist.
        node.add_discrete_categories({'A', 'C'})
        self.assertEqual(node.discrete_categories, [{'A'}, {'B'}, {'A', 'C'}])
        expected = [
            Structure(id=1, granularity=None, bits=(0, 0, 0)),
            Structure(id=2, granularity=1.0,  bits=(1, 0, 0)),
            Structure(id=3, granularity=2.0,  bits=(0, 1, 0)),
            Structure(id=4, granularity=3.0,  bits=(1, 0, 1)),
            Structure(id=5, granularity=2.0,  bits=(1, 1, 0)),
            Structure(id=6, granularity=3.0,  bits=(1, 1, 1))
        ]
        self.assertEqual(self.get_structure_helper(), expected)

        # Raises error if category does not match existing index column.
        regex = r"invalid category value 'D'"
        with self.assertRaisesRegex(ValueError, regex):
            node.add_discrete_categories({'C', 'D'})

        # Check that a warning is raised on redundant categories.
        with self.assertWarns(ToronWarning) as cm:
            node.add_discrete_categories({'A', 'B'})

        # Check warning message.
        regex = r"omitting redundant categories: \{'[AB]', '[AB]'\}"
        self.assertRegex(str(cm.warning), regex)

        # Check that existing categories were not changed by error or warning.
        self.assertEqual(node.discrete_categories, [{'A'}, {'B'}, {'A', 'C'}], msg='should be unchanged')

    def test_drop_discrete_categories(self):
        node = self.node
        with node._managed_cursor() as cursor:
            prop_repo = node._dal.PropertyRepository(cursor)
            prop_repo.add('discrete_categories', [['A'], ['B'], ['A', 'C']])

        node.drop_discrete_categories({'A'}, {'B'})
        self.assertEqual(node.discrete_categories, [{'A', 'C'}, {'A', 'B', 'C'}])
        expected = [
            Structure(id=1, granularity=None, bits=(0, 0, 0)),
            Structure(id=2, granularity=3.0,  bits=(1, 0, 1)),
            Structure(id=3, granularity=3.0,  bits=(1, 1, 1)),
        ]
        self.assertEqual(self.get_structure_helper(), expected)

        node.drop_discrete_categories({'A', 'B'})  # <- Not present (no change).
        self.assertEqual(node.discrete_categories, [{'A', 'C'}, {'A', 'B', 'C'}])
        expected = [
            Structure(id=1, granularity=None, bits=(0, 0, 0)),
            Structure(id=2, granularity=3.0,  bits=(1, 0, 1)),
            Structure(id=3, granularity=3.0,  bits=(1, 1, 1)),
        ]
        self.assertEqual(self.get_structure_helper(), expected)

        node.drop_discrete_categories({'A', 'C'})
        self.assertEqual(node.discrete_categories, [{'A', 'B', 'C'}])
        expected = [
            Structure(id=1, granularity=None, bits=(0, 0, 0)),
            Structure(id=2, granularity=3.0,  bits=(1, 1, 1)),
        ]
        self.assertEqual(self.get_structure_helper(), expected)

    def test_drop_discrete_categories_error(self):
        """Should raise error if user tries to remove whole space."""
        node = self.node
        with node._managed_cursor() as cursor:
            prop_repo = node._dal.PropertyRepository(cursor)
            prop_repo.add('discrete_categories', [['A'], ['B'], ['A', 'C']])

        regex = r"cannot drop whole space: \{'[ABC]', '[ABC]', '[ABC]'\}"

        # Test implicit whole space element (it's  covered by other elements).
        with self.assertRaisesRegex(ValueError, regex):
            node.drop_discrete_categories({'A', 'B', 'C'})

        # Change categories so that the whole space element appears explicitly.
        node.drop_discrete_categories({'A'}, {'A', 'C'})
        self.assertEqual(node.discrete_categories, [{'B'}, {'A', 'B', 'C'}])

        # Test with explicit whole space.
        with self.assertRaisesRegex(ValueError, regex):
            node.drop_discrete_categories({'A', 'B', 'C'})


class TestIndexColumnMethods(unittest.TestCase):
    @staticmethod
    def get_cols_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            return node._dal.ColumnManager(cursor).get_columns()

    @staticmethod
    def add_cols_helper(node, *columns):  # <- Helper function.
        with node._managed_cursor() as cursor:
            manager = node._dal.ColumnManager(cursor)
            manager.add_columns(*columns)

    @staticmethod
    def get_categories_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            prop_repo = node._dal.PropertyRepository(cursor)
            return [set(x) for x in prop_repo.get('discrete_categories')]

    @staticmethod
    def add_categories_helper(node, categories):  # <- Helper function.
        with node._managed_cursor() as cursor:
            prop_repo = node._dal.PropertyRepository(cursor)
            categories = [list(x) for x in categories]
            prop_repo.add('discrete_categories', categories)

    def test_add_index_columns(self):
        node = Node()

        node.add_index_columns('A', 'B')

        self.assertEqual(self.get_cols_helper(node), ('A', 'B'))

    def test_add_index_columns_atomic(self):
        """Adding columns should be an atomic operation (either all
        columns should be added or none should be added).
        """
        node = Node()

        with suppress(Exception):
            # Second 'baz' causes an error (cannot have duplicate names).
            node.add_index_columns('foo', 'bar', 'baz', 'baz')

        msg = 'should be empty tuple, no column names'
        self.assertEqual(self.get_cols_helper(node), (), msg=msg)

    def test_index_columns_property(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B')

        columns = node.index_columns  # Accessed as property attribute.

        self.assertEqual(columns, ('A', 'B'))

    def test_rename_index_columns(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B', 'C', 'D')

        if sqlite3.sqlite_version_info >= (3, 25, 0) or node._dal.backend != 'DAL1':
            node.rename_index_columns({'B': 'G', 'D': 'T'})
        else:
            import toron.dal1
            toron.dal1.legacy_rename_columns(node, {'B': 'G', 'D': 'T'})

        self.assertEqual(self.get_cols_helper(node), ('A', 'G', 'C', 'T'))

    def test_rename_index_columns_and_categories(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B', 'C', 'D')
        self.add_categories_helper(node, [{'A'}, {'A', 'B'}, {'A', 'B', 'C', 'D'}])

        if sqlite3.sqlite_version_info >= (3, 25, 0) or node._dal.backend != 'DAL1':
            node.rename_index_columns({'B': 'G', 'D': 'T'})
        else:
            import toron.dal1
            toron.dal1.legacy_rename_columns(node, {'B': 'G', 'D': 'T'})

        self.assertEqual(self.get_cols_helper(node), ('A', 'G', 'C', 'T'))
        self.assertEqual(
            self.get_categories_helper(node),
            [{'A'}, {'A', 'G'}, {'A', 'G', 'C', 'T'}],
        )

    def test_drop_index_columns(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B', 'C', 'D')

        if sqlite3.sqlite_version_info >= (3, 35, 5) or node._dal.backend != 'DAL1':
            node.drop_index_columns('B', 'D')
        else:
            import toron.dal1
            toron.dal1.legacy_drop_columns(node, 'B', 'D')

        self.assertEqual(self.get_cols_helper(node), ('A', 'C'))

    def test_drop_index_columns_all(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B', 'C')

        if node._dal.backend == 'DAL1' and sqlite3.sqlite_version_info < (3, 35, 5):
            self.skipTest('requires SQLite 3.35.5 or newer')

        regex = 'cannot remove all index columns'
        with self.assertRaisesRegex(RuntimeError, regex):
            node.drop_index_columns('A', 'B', 'C')


class TestIndexMethods(unittest.TestCase):
    @staticmethod
    def add_cols_helper(node, *columns):  # <- Helper function.
        with node._managed_cursor() as cursor:
            manager = node._dal.ColumnManager(cursor)
            manager.add_columns(*columns)

    @staticmethod
    def add_index_helper(node, data):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.IndexRepository(cursor)
            for row in data:
                repository.add(*row)

    @staticmethod
    def get_index_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.IndexRepository(cursor)
            return list(repository.get_all())

    @staticmethod
    def add_structure_helper(node, data):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.StructureRepository(cursor)
            for granularity, *bits in data:
                repository.add(granularity, *bits)

    @staticmethod
    def get_structure_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.StructureRepository(cursor)
            return sorted(repository.get_all(), key=lambda x: x.id)

    def test_insert(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B')
        self.add_structure_helper(node, [(None, 0, 0), (None, 1, 1)])

        data = [('A', 'B'), ('foo', 'x'), ('bar', 'y')]
        node.insert_index(data)

        expected = [
            Index(0, '-', '-'),
            Index(1, 'foo', 'x'),
            Index(2, 'bar', 'y'),
        ]
        self.assertEqual(self.get_index_helper(node), expected)

        expected = [
            Structure(id=1, granularity=None, bits=(0, 0)),
            Structure(id=2, granularity=1.0,  bits=(1, 1)),
        ]
        self.assertEqual(self.get_structure_helper(node), expected)

    def test_insert_different_order(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B')

        data = [('B', 'A'), ('x', 'foo'), ('y', 'bar')]  # <- Different order.
        node.insert_index(data)

        expected = [
            Index(0, '-', '-'),
            Index(1, 'foo', 'x'),
            Index(2, 'bar', 'y'),
        ]
        self.assertEqual(self.get_index_helper(node), expected)

    def test_insert_invalid_columns(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B', 'C', 'D')

        regex = r"missing required columns: 'C', 'D'"
        with self.assertRaisesRegex(ValueError, regex):
            node.insert_index([('A', 'B'), ('foo', 'x'), ('bar', 'y')])

    def test_insert_duplicate_or_empty_strings(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B')

        data = [
            ('A', 'B'),
            ('foo', 'x'),
            ('foo', 'x'),  # <- Duplicate of previous record.
            ('bar', ''),   # <- Contains empty string.
            ('bar', 'y'),
            ('baz', 'z'),
        ]

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            node.insert_index(data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            'skipped 2 rows with duplicate labels or empty strings, loaded 3 rows',
        )

        # Check the loaded data.
        expected = [
            Index(0, '-', '-'),
            Index(1, 'foo', 'x'),
            Index(2, 'bar', 'y'),
            Index(3, 'baz', 'z'),
        ]
        self.assertEqual(self.get_index_helper(node), expected)

    def test_insert_index_group_is_complete(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B')
        data = [('foo', 'x'), ('bar', 'y')]
        self.add_index_helper(node, data)

        with node._managed_cursor() as cursor:
            group_repo = node._dal.WeightGroupRepository(cursor)
            weight_repo = node._dal.WeightRepository(cursor)

            # Add weight_group_id 1 and weight records.
            group_repo.add('group1', is_complete=True)
            weight_repo.add(1, 1, 6000)
            weight_repo.add(1, 2, 4000)

            # Add weight_group_id 2 and weight records.
            group_repo.add('group2', is_complete=False)
            weight_repo.add(2, 1, 6000)

            # Insert new index record!
            node.insert_index([('A', 'B'), ('baz', 'z')])

            # Check that group1's is_complete status is changed to False.
            group = group_repo.get_by_name('group1')
            self.assertFalse(group.is_complete)

            # Check that group2's is_complete status remains False (unchanged).
            group = group_repo.get_by_name('group2')
            self.assertFalse(group.is_complete)

    def test_insert_index_crosswalk_is_complete(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B')
        data = [('foo', 'x'), ('bar', 'y')]
        self.add_index_helper(node, data)

        with node._managed_cursor() as cursor:
            crosswalk_repo = node._dal.CrosswalkRepository(cursor)
            relation_repo = node._dal.RelationRepository(cursor)

            # Add crosswalk_id 1 and weight records.
            crosswalk_repo.add('111-111-1111', 'somenode.toron', 'edge1', is_locally_complete=True)
            relation_repo.add(1, 1, 1, 6000)
            relation_repo.add(1, 2, 2, 4000)

            # Add crosswalk_id 2 and weight records.
            crosswalk_repo.add('222-222-2222', 'anothernode.toron', 'edge2', is_locally_complete=False)
            relation_repo.add(2, 1, 1, 4000)
            relation_repo.add(2, 2, 1, 2000)  # <- Maps to local index_id 1 (no relation goes to index_id 2)

            # Insert new index record!
            node.insert_index([('A', 'B'), ('baz', 'z')])

            # Check that edge1's is_locally_complete is changed to False.
            crosswalk = crosswalk_repo.get(1)
            self.assertFalse(crosswalk.is_locally_complete)

            # Check that edge2's is_locally_complete remains False (unchanged).
            crosswalk = crosswalk_repo.get(2)
            self.assertFalse(crosswalk.is_locally_complete)

    def test_insert_index_modifies_index_hash(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B')

        with node._managed_cursor() as cursor:
            prop_repo = node._dal.PropertyRepository(cursor)

            node.insert_index([('A', 'B'), ('foo', 'a'), ('bar', 'b')])
            self.assertEqual(
                prop_repo.get('index_hash'),
                '5dfadd0e50910f561636c47335ecf8316251cbd85964eadb5c00103502edf177',
                msg='hash for index_ids 0, 1, and 2',
            )

            node.insert_index([('A', 'B'), ('baz', 'z')])
            self.assertEqual(
                prop_repo.get('index_hash'),
                'c4c96cd71102046c61ec8326b2566d9e48ef2ba26d4252ba84db28ba352a0079',
                msg='hash for index_ids 0, 1, 2, and 3',
            )

    def test_select(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B')
        data = [('foo', 'x'), ('foo', 'y'), ('bar', 'x'), ('bar', 'y')]
        self.add_index_helper(node, data)

        self.assertEqual(
            list(node.select_index(A='foo')),  # <- Filter on one column.
            [(1, 'foo', 'x'), (2, 'foo', 'y')],
        )

        self.assertEqual(
            list(node.select_index(header=True, B='x')),  # <- Include header.
            [('index_id', 'A', 'B'), (1, 'foo', 'x'), (3, 'bar', 'x')],
        )

        self.assertEqual(
            list(node.select_index(A='bar', B='x')),  # <- Filter on multiple columns.
            [(3, 'bar', 'x')],
        )

        self.assertEqual(
            list(node.select_index(index_id=4, A='bar')),  # <- Criteria includes `index_id`.
            [(4, 'bar', 'y')],
        )

        self.assertEqual(
            list(node.select_index(A='baz')),  # <- No matching value 'baz'.
            [],
        )

        self.assertEqual(
            list(node.select_index()),  # <- No criteria (returns all).
            [(0, '-', '-'),
             (1, 'foo', 'x'),
             (2, 'foo', 'y'),
             (3, 'bar', 'x'),
             (4, 'bar', 'y')],
        )


class TestNodeUpdateIndex(unittest.TestCase):
    @staticmethod
    def get_index_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.IndexRepository(cursor)
            return list(repository.get_all())

    @staticmethod
    def get_weight_helper(node):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM weight')
            return cursor.fetchall()

    @staticmethod
    def get_relation_helper(node):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM relation')
            return cursor.fetchall()

    def setUp(self):
        node = Node()
        with node._managed_cursor() as cursor:
            manager = node._dal.ColumnManager(cursor)
            manager.add_columns('A', 'B')

            repository = node._dal.IndexRepository(cursor)
            repository.add('foo', 'x')
            repository.add('bar', 'y')

            prop_repo = node._dal.PropertyRepository(cursor)
            prop_repo.add('index_hash', '5dfadd0e50910f561636c47335ecf8316251cbd85964eadb5c00103502edf177')

            weight_group_repo = node._dal.WeightGroupRepository(cursor)
            weight_group_repo.add('group1')  # Adds weight_group_id 1.
            weight_repo = node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 175000)
            weight_repo.add(1, 2,  25000)

            crosswalk_repo = node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add('111-11-1111', None, 'other1')  # Adds crosswalk_id 1.
            relation_repo = node._dal.RelationRepository(cursor)
            relation_repo.add(1, 1, 1, 16350, 0.75, None)
            relation_repo.add(1, 1, 2, 5450,  0.25, None)
            relation_repo.add(1, 2, 2, 13050, 1.00, None)

        self.node = node

    def test_update_all_values(self):
        data = [('index_id', 'A', 'B'), (1, 'baz', 'z')]  # <- Updating columns A & B.
        self.node.update_index(data)
        expected = [Index(0, '-', '-'), Index(1, 'baz', 'z'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_update_different_order(self):
        """Columns should be matched on name, not positional order."""
        data = [('index_id', 'B', 'A'), (1, 'z', 'baz')]  # <- Different order (B then A)
        self.node.update_index(data)
        expected = [Index(0, '-', '-'), Index(1, 'baz', 'z'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_update_partial_values(self):
        """Update requires all label columns, raise error if missing."""
        data = [('index_id', 'B'), (2, 'xyz')]  # <- Missing column A.

        regex = "missing required columns: 'A'"
        with self.assertRaisesRegex(ValueError, regex):
            self.node.update_index(data)

        # Check values (unchanged).
        expected = [Index(0, '-', '-'), Index(1, 'foo', 'x'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_update_ignore_extra_cols(self):
        """When given extra columns, they are ignored when loading."""
        data = [('index_id', 'A', 'B', 'C'), (1, 'baz', 'z', 'zzz')]  # <- Column C not in index.
        self.node.update_index(data)
        expected = [Index(0, '-', '-'), Index(1, 'baz', 'z'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_update_non_matching_id(self):
        """If index_id did not exist, raise warning and skip to next.

        .. note::
            If the index_id *did* exist at the beginning of the update
            but it was merged by a previous row, then an exception
            should be raised and the transaction should be rolled back
            (this is checked later by the test case
            `test_merge_resulting_in_missing_index_id()`).
        """
        data = [
            ('index_id', 'A', 'B'),
            (4, 'baz', 'z'),  # <- No index_id 4!
            (2, 'bar', 'YYY'),
        ]

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.update_index(data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            'skipped 1 rows with non-matching index_id values, updated 1 rows',
        )

        # Check values (index 2 should be updated).
        expected = [Index(0, '-', '-'), Index(1, 'foo', 'x'), Index(2, 'bar', 'YYY')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_empty_string(self):
        """Should raise warning and skip to next for empty strings."""
        data = [
            ('index_id', 'A', 'B'),
            (1, 'bar', ''),  # <- Has empty string.
            (2, 'bar', 'YYY'),
        ]

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.update_index(data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            'skipped 1 rows with empty string values, updated 1 rows',
        )

        # Check values (index_id 2 updated).
        expected = [Index(0, '-', '-'), Index(1, 'foo', 'x'), Index(2, 'bar', 'YYY')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_no_index_id_column(self):
        """Must have 'index_id' to identify records when updating."""
        data = [('A', 'B'), ('baz', 'z')]  # <- No 'index_id' column.

        regex = "column 'index_id' required to update records"
        with self.assertRaisesRegex(ValueError, regex):
            self.node.update_index(data)

    def test_update_resulting_in_duplicate(self):
        """If updated labels are not unique, should merge records."""
        # The following data updates the labels of index_id 1 to `bar, y`.
        # But these are the same labels used for index_id 2. Because index
        # labels must be unique, this update should merge index_id 1 and
        # 2 into the same record. The final record should used the index_id
        # of the record being updated (in this case, 1).
        data = [('index_id', 'A', 'B'), (1, 'bar', 'y')]

        # Check that merge does not happen implicitly.
        with self.assertRaises(ValueError):
            self.node.update_index(data)  # <- Update creates duplicate labels.

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.update_index(data, merge_on_conflict=True)  # <- Update causes records to merge.

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            'merged 1 existing records with duplicate label values, updated 1 rows',
        )

        msg = 'Record index_id 2 should be merged with index_id 1.'
        expected = [Index(0, '-', '-'), Index(1, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected, msg=msg)

        msg = 'Weight records and values should be merged into one record.'
        expected = [(1, 1, 1, 200000.0)]
        self.assertEqual(self.get_weight_helper(self.node), expected, msg=msg)

        msg = 'Three relations merged into two, remaining relations have index_id 1.'
        expected = [(1, 1, 1, 1, 21800.0, 1.0, None), (2, 1, 2, 1, 13050.0, 1.0, None)]
        self.assertEqual(self.get_relation_helper(self.node), expected, msg=msg)

    def test_merge_resulting_in_missing_index_id(self):
        """Should raise error if attempting to update a record that was merged."""
        # When applying the first update, index_id 1 gets the same labels
        # as index_id 2 (bar, y) which triggers a merge of these two records.
        # Then when applying the second update, there is no index_id 2 (it
        # was just merged with 1). If a merge removes records that the user
        # was attempting to update, the results can be very confusing. Users
        # would be right to wonder why `baz, z` is not included in the newly
        # updated index. To prevent this confusing situation, attempting to
        # update a record that was previously merged should raise an exception
        # and any changes should be rolled-back.
        data = [('index_id', 'A', 'B'), (1, 'bar', 'y'), (2, 'baz', 'z')]

        regex = 'cannot update index_id 2, it was merged with another record on a previous row'
        with self.assertRaisesRegex(ValueError, regex):
            self.node.update_index(data, merge_on_conflict=True)

        # Check values (unchanged).
        expected = [Index(0, '-', '-'), Index(1, 'foo', 'x'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_refresh_structure_granularity(self):
        """Check that update_index() updates granularity."""
        # Set up structure.
        with self.node._managed_cursor() as cursor:
            repository = self.node._dal.StructureRepository(cursor)
            for granularity, *bits in [(None, 0, 0), (None, 1, 1)]:
                repository.add(granularity, *bits)

        # Call update and verify values.
        data = [('index_id', 'A', 'B'), (1, 'baz', 'z')]  # <- Updating columns A & B.
        self.node.update_index(data)
        expected = [Index(0, '-', '-'), Index(1, 'baz', 'z'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)

        # Get structure and check for updated values.
        with self.node._managed_cursor() as cursor:
            repository = self.node._dal.StructureRepository(cursor)
            actual = sorted(repository.get_all(), key=lambda x: x.id)

        expected = [
            Structure(id=1, granularity=None, bits=(0, 0)),
            Structure(id=2, granularity=1.0,  bits=(1, 1))
        ]
        self.assertEqual(actual, expected)

    def test_merging_and_is_complete_status(self):
        with self.node._managed_cursor() as cursor:
            group_repo = self.node._dal.WeightGroupRepository(cursor)
            weight_repo = self.node._dal.WeightRepository(cursor)

            # Add weight_group_id 2 and weight record.
            group_repo.add('group2', is_complete=False)
            weight_repo.add(2, 1, 6000)

            # Apply update which triggers a merge of existing records.
            data = [('index_id', 'A', 'B'), (1, 'bar', 'y')]
            with self.assertWarns(ToronWarning) as cm:
                self.node.update_index(data, merge_on_conflict=True)

            # Check that is_incomplete has been changed to True.
            group = group_repo.get_by_name('group2')
            self.assertTrue(group.is_complete)

    def test_merging_and_is_locally_complete_status(self):
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)
            relation_repo = self.node._dal.RelationRepository(cursor)

            # Add crosswalk_id 1 and weight records.
            crosswalk_repo.add('111-111-1111', 'somenode.toron', 'edge1', is_locally_complete=False)
            relation_repo.add(2, 1, 1, 4000)
            relation_repo.add(2, 2, 1, 2000)  # <- Maps to local index_id 1 (no relation goes to index_id 2)

            # Apply update which triggers a merge of existing records.
            data = [('index_id', 'A', 'B'), (1, 'bar', 'y')]
            with self.assertWarns(ToronWarning) as cm:
                self.node.update_index(data, merge_on_conflict=True)

            # Check that is_locally_complete has been changed to True.
            crosswalk = crosswalk_repo.get(1)
            self.assertTrue(crosswalk.is_locally_complete)

    def test_merging_and_index_hash_updates(self):
        with self.node._managed_cursor() as cursor:
            prop_repo = self.node._dal.PropertyRepository(cursor)

            # Check starting 'index_hash' property.
            self.assertEqual(
                prop_repo.get('index_hash'),
                '5dfadd0e50910f561636c47335ecf8316251cbd85964eadb5c00103502edf177',
                msg='hash for index_ids 0, 1, and 2',
            )

            # Apply update which triggers a merge of existing records.
            data = [('index_id', 'A', 'B'), (1, 'bar', 'y')]
            with self.assertWarns(ToronWarning):
                self.node.update_index(data, merge_on_conflict=True)

            # Check modified 'index_hash' property.
            self.assertEqual(
                prop_repo.get('index_hash'),
                '7c3ccd10bb7ec37b46d37926ae6274267f007a34aeaf15c882a715a7f3300529',
                msg='hash for index_ids 0 and 1 (index_id 2 was merged into 1)',
            )


class TestNodeDeleteIndex(unittest.TestCase):
    @staticmethod
    def add_cols_helper(node, *columns):  # <- Helper function.
        with node._managed_cursor() as cursor:
            manager = node._dal.ColumnManager(cursor)
            manager.add_columns(*columns)

    @staticmethod
    def add_index_helper(node, data):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.IndexRepository(cursor)
            for row in data:
                repository.add(*row)

    @staticmethod
    def get_weight_helper(node):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM weight')
            return cursor.fetchall()

    @staticmethod
    def get_relation_helper(node):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM relation')
            return cursor.fetchall()

    @staticmethod
    def get_index_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.IndexRepository(cursor)
            return list(repository.get_all())

    def setUp(self):
        node = Node()
        self.add_cols_helper(node, 'A', 'B')
        self.add_index_helper(node, [('foo', 'x'), ('bar', 'y')])

        with node._managed_cursor() as cursor:
            prop_repo = node._dal.PropertyRepository(cursor)
            prop_repo.add('index_hash', '5dfadd0e50910f561636c47335ecf8316251cbd85964eadb5c00103502edf177')

        self.node = node

    def test_delete_index_only(self):
        data = [
            ('index_id', 'A', 'B'),
            (1, 'foo', 'x'),
            (2, 'bar', 'y'),
        ]
        self.node.delete_index(data)

        expected = [Index(0, '-', '-')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_delete_with_warning(self):
        data = [
            ('index_id', 'A', 'B'),
            (42, 'qux', 'a'),  # <- Id 42 does not exist in index.
            (1, 'foo', 'x'),
            (2, 'bar', 'zzz'),  # <- Labels don't match index record.
        ]

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.delete_index(data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            ('skipped 1 rows with non-matching index_id values, '
             'skipped 1 rows with mismatched labels, deleted 1 rows'),
        )

        # Check values (index_id 1 deleted).
        expected = [Index(0, '-', '-'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_delete_with_weights(self):
        """Should delete weight records associated with index_id."""
        with self.node._managed_cursor() as cursor:
            weight_group_repo = self.node._dal.WeightGroupRepository(cursor)
            weight_group_repo.add('group1')  # Adds weight_group_id 1.
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 175000)
            weight_repo.add(1, 2,  25000)

        data = [
            ('index_id', 'A', 'B'),
            (2, 'bar', 'y'),
        ]
        self.node.delete_index(data)

        expected = [Index(0, '-', '-'), Index(1, 'foo', 'x')]
        self.assertEqual(self.get_index_helper(self.node), expected)

        expected = [(1, 1, 1, 175000.0)]
        self.assertEqual(self.get_weight_helper(self.node), expected)

    def test_delete_and_weight_group_is_complete_status(self):
        """Deleting unweighted indexes could make weight groups complete."""
        with self.node._managed_cursor() as cursor:
            group_repo = self.node._dal.WeightGroupRepository(cursor)
            group_repo.add('group1', is_complete=False)  # Adds weight_group_id 1.
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 175000)  # <- Only index_id 1 (no weight for index_id 2)

            self.node.delete_index([('index_id', 'A', 'B'), (2, 'bar', 'y')])

            group = group_repo.get_by_name('group1')
            msg = 'since index_id 2 was the only unweighted record, deleting ' \
                  'it should make the weight group complete'
            self.assertTrue(group.is_complete, msg=msg)

    def test_delete_with_relations(self):
        """Should delete relation records associated with index_id."""
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add('111-11-1111', None, 'other1')  # Adds crosswalk_id 1.
            relation_repo = self.node._dal.RelationRepository(cursor)
            relation_repo.add(1, 1, 1, 16350, 0.75, None)
            relation_repo.add(1, 1, 2, 5450,  0.25, None)
            relation_repo.add(1, 2, 2, 13050, 1.00, None)

        data = [
            ('index_id', 'A', 'B'),
            (2, 'bar', 'y'),
        ]
        self.node.delete_index(data)

        expected = [Index(0, '-', '-'), Index(1, 'foo', 'x')]
        self.assertEqual(self.get_index_helper(self.node), expected)

        expected = [(1, 1, 1, 1, 16350.0, 1.0, None)]  # <- Proportion is updated, too (was 0.75).
        self.assertEqual(self.get_relation_helper(self.node), expected)

    def test_delete_with_ambiguous_relations(self):
        """Should not delete records linked to ambiguous relations.

        NOTE: Ideally, this limitation should be removed in the future
        by re-mapping relations using their associated labels.
        """
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add('111-11-1111', None, 'other1')  # Adds crosswalk_id 1.
            relation_repo = self.node._dal.RelationRepository(cursor)
            relation_repo.add(1, 1, 1, 16350, 0.75, b'\x80')  # <- Ambiguous relations.
            relation_repo.add(1, 1, 2, 5450,  0.25, b'\x80')  # <- Ambiguous relations.
            relation_repo.add(1, 2, 2, 13050, 1.00, None)

        data = [('index_id', 'A', 'B'), (2, 'bar', 'y')]

        regex = 'associated crosswalk relations are ambiguous'
        with self.assertRaisesRegex(ValueError, regex):
            self.node.delete_index(data)

    def test_delete_and_is_locally_complete_status(self):
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add('111-11-1111', None, 'other1', is_locally_complete=False)  # Adds crosswalk_id 1.
            relation_repo = self.node._dal.RelationRepository(cursor)
            relation_repo.add(1, 1, 1, 16350, 0.75, None)
            relation_repo.add(1, 2, 1, 5450,  0.25, None)

            data = [('index_id', 'A', 'B'), (2, 'bar', 'y')]
            self.node.delete_index(data)  # Deletes index without a relation (index_id 2).

            # Check that is_locally_complete has been changed to True.
            crosswalk = crosswalk_repo.get(1)
            self.assertTrue(crosswalk.is_locally_complete)

    def test_delete_using_interoperation(self):
        # Add more index rows so there are multiple records to select.
        self.add_index_helper(self.node, [('foo', 'qux'), ('foo', 'quux')])

        self.node.delete_index(A='foo')

        expected = [Index(0, '-', '-'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)

    def test_refresh_structure_granularity(self):
        """Check that update_index() updates granularity."""
        # Set up structure.
        with self.node._managed_cursor() as cursor:
            repository = self.node._dal.StructureRepository(cursor)
            for granularity, *bits in [(None, 0, 0), (None, 1, 1)]:
                repository.add(granularity, *bits)

        # Call delete_index() and verify values.
        data = [('index_id', 'A', 'B'), (1, 'foo', 'x')]
        self.node.delete_index(data)
        expected = [Index(0, '-', '-'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(self.node), expected)

        # Get structure and check for updated values.
        with self.node._managed_cursor() as cursor:
            repository = self.node._dal.StructureRepository(cursor)
            actual = sorted(repository.get_all(), key=lambda x: x.id)

        expected = [
            Structure(id=1, granularity=None, bits=(0, 0)),
            Structure(id=2, granularity=0.0,  bits=(1, 1))  # <- Gets `0.0` because there is only 1 record.
        ]
        self.assertEqual(actual, expected)

    def test_merging_and_index_hash_updates(self):
        with self.node._managed_cursor() as cursor:
            prop_repo = self.node._dal.PropertyRepository(cursor)

            # Check starting 'index_hash' property.
            self.assertEqual(
                prop_repo.get('index_hash'),
                '5dfadd0e50910f561636c47335ecf8316251cbd85964eadb5c00103502edf177',
                msg='hash for index_ids 0, 1, and 2',
            )

            # Call delete_index() and delete index_id 1.
            data = [('index_id', 'A', 'B'), (1, 'foo', 'x')]
            self.node.delete_index(data)

            # Check modified 'index_hash' property.
            self.assertEqual(
                prop_repo.get('index_hash'),
                '692865c9a376a1a82d161b0f9578595554873797fa9ebbb068b797828122e61d',
                msg='hash for index_ids 0 and 2 (index_id 1 was deleted)',
            )


class TestNodeWeightGroupMethods(unittest.TestCase):
    @staticmethod
    def get_weight_group_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.WeightGroupRepository(cursor)
            return list(repository.get_all())

    @staticmethod
    def get_weight_helper(node):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM weight')
            return cursor.fetchall()

    def test_weight_groups_property(self):
        """The `node.weight_groups` property should be list of groups
        ordered by name.
        """
        node = Node()
        with node._managed_cursor() as cursor:
            weight_group_repo = node._dal.WeightGroupRepository(cursor)
            weight_group_repo.add('name_b')
            weight_group_repo.add('name_a', 'Group A', ['"[foo]"'], is_complete=True)
            weight_group_repo.add('name_c', 'Group C')

        expected = [
            WeightGroup(
                id=2,
                name='name_a',
                description='Group A',
                selectors=['"[foo]"'],
                is_complete=1,
            ),
            WeightGroup(
                id=1,
                name='name_b',
                description=None,
                selectors=None,
                is_complete=0,
            ),
            WeightGroup(
                id=3,
                name='name_c',
                description='Group C',
                selectors=None,
                is_complete=0,
            ),
        ]
        self.assertEqual(node.weight_groups, expected)

    def test_get_weight_group(self):
        node = Node()
        with node._managed_cursor() as cursor:
            weight_group_repo = node._dal.WeightGroupRepository(cursor)
            weight_group_repo.add('name_a', 'Group A')
            weight_group_repo.add('name_b', 'Group B')

        actual = node.get_weight_group('name_a')
        expected = WeightGroup(id=1, name='name_a', description='Group A', selectors=None)
        self.assertEqual(actual, expected)

        self.assertIsNone(node.get_weight_group('name_zzz'))

    def test_add_weight_group(self):
        node = Node()

        node.add_weight_group('name_a')  # <- Only `name` is required.
        node.add_weight_group(  # <- Defining all properties.
            name='name_b',
            description='Group B',
            selectors=['"[foo]"'],
            is_complete=True
        )

        expected = [
            WeightGroup(
                id=1,
                name='name_a',
                description=None,
                selectors=None,
                is_complete=False,
            ),
            WeightGroup(
                id=2,
                name='name_b',
                description='Group B',
                selectors=['"[foo]"'],
                is_complete=True,
            ),
        ]
        self.assertEqual(self.get_weight_group_helper(node), expected)

    def test_edit_weight_group(self):
        node = Node()
        with node._managed_cursor() as cursor:
            weight_group_repo = node._dal.WeightGroupRepository(cursor)
            weight_group_repo.add(
                'name_a',
                'Group A',
                ['"[foo]"'],
                is_complete=False,
            )

        node.edit_weight_group('name_a', name='NameA', is_complete=True)
        expected = [
            WeightGroup(
                id=1,
                name='NameA',  # <- Value changed.
                description='Group A',
                selectors=['"[foo]"'],
                is_complete=True,  # <- Value changed.
            ),
        ]
        self.assertEqual(self.get_weight_group_helper(node), expected)

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            node.edit_weight_group('name_x', description='Description of X.')

        self.assertEqual(str(cm.warning), "no weight group named 'name_x'")

    def test_drop_weight_group(self):
        node = Node()
        with node._managed_cursor() as cursor:
            manager = node._dal.ColumnManager(cursor)
            index_repo = node._dal.IndexRepository(cursor)
            weight_group_repo = node._dal.WeightGroupRepository(cursor)
            weight_repo = node._dal.WeightRepository(cursor)

            # Add index columns and records.
            manager.add_columns('A', 'B')
            index_repo.add('foo', 'x')
            index_repo.add('bar', 'y')
            index_repo.add('baz', 'z')

            # Add weight group and associated weights.
            weight_group_repo.add('name_a')
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        node.drop_weight_group('name_a')

        msg = 'weight group and associated weights should be deleted'
        self.assertEqual(self.get_weight_group_helper(node), [], msg=msg)
        self.assertEqual(self.get_weight_helper(node), [], msg=msg)

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            node.drop_weight_group('name_x')

        self.assertEqual(str(cm.warning), "no weight group named 'name_x'")


class TestNodeWeightMethods(unittest.TestCase):
    def setUp(self):
        node = Node()
        with node._managed_cursor() as cursor:
            col_manager = node._dal.ColumnManager(cursor)
            index_repo = node._dal.IndexRepository(cursor)
            weight_group_repo = node._dal.WeightGroupRepository(cursor)

            # Add index columns and records.
            col_manager.add_columns('A', 'B')
            index_repo.add('foo', 'x')
            index_repo.add('bar', 'y')
            index_repo.add('bar', 'z')

            # Add weight_group_id 1.
            weight_group_repo.add('weight1')

        self.node = node

    def get_weights_helper(self):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with self.node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM weight')
            return cursor.fetchall()

    def test_select(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        weights = self.node.select_weights('weight1', header=True)
        expected = [
            ('index_id', 'A', 'B', 'weight1'),
            (1, 'foo', 'x', 10.0),
            (2, 'bar', 'y', 25.0),
            (3, 'bar', 'z', 15.0),
        ]
        self.assertEqual(list(weights), expected)

        # Test with selection `header=False` and `A='bar'`.
        weights = self.node.select_weights('weight1', header=False, A='bar')
        expected = [
            (2, 'bar', 'y', 25.0),
            (3, 'bar', 'z', 15.0),
        ]
        self.assertEqual(list(weights), expected)

        # Test with selection `header=True` and `A='NOMATCH'`.
        weights = self.node.select_weights('weight1', header=True, A='NOMATCH')
        expected = [('index_id', 'A', 'B', 'weight1')]
        msg = 'header row only, when there are no matches'
        self.assertEqual(list(weights), expected, msg=msg)

        # Test with selection `header=False` and `A='NOMATCH'`.
        weights = self.node.select_weights('weight1', header=False, A='NOMATCH')
        self.assertEqual(list(weights), [], msg='iterator should be empty')

    def test_insert_by_label(self):
        data = [
            ('A', 'B', 'weight1'),
            ('foo', 'x', 10.0),
            ('bar', 'y', 25.0),
            ('bar', 'z', 15.0),
        ]
        self.node.insert_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_by_label_different_order(self):
        data = [
            ('B', 'A', 'weight1'),
            ('x', 'foo', 10.0),
            ('y', 'bar', 25.0),
            ('z', 'bar', 15.0),
        ]
        self.node.insert_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_by_label_extra_columns(self):
        data = [
            ('A', 'B', 'C', 'weight1'),
            ('foo', 'x', 'a', 10.0),
            ('bar', 'y', 'b', 25.0),
            ('bar', 'z', 'c', 15.0),
        ]
        self.node.insert_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_by_index_and_label(self):
        data = [
            ('index_id', 'A', 'B', 'weight1'),
            (1, 'foo', 'x', 10.0),
            (2, 'bar', 'y', 25.0),
            (3, 'bar', 'z', 15.0),
        ]
        self.node.insert_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_is_complete_status(self):
        data = [
            ('index_id', 'A', 'B', 'weight1'),
            (1, 'foo', 'x', 10.0),
            (2, 'bar', 'y', 25.0),
            # Omits weight for index_id 3.
        ]
        self.node.insert_weights('weight1', data)

        group = self.node.get_weight_group('weight1')
        self.assertFalse(group.is_complete,
                         msg='no weight for index_id 3, should be false')

        # Add weight for index_id 3 and check again.
        data = [
            ('index_id', 'A', 'B', 'weight1'),
            (3, 'bar', 'z', 15.0),
        ]
        self.node.insert_weights('weight1', data)
        group = self.node.get_weight_group('weight1')
        self.assertTrue(group.is_complete)

    def test_insert_by_index_and_label_extra_columns(self):
        data = [
            ('index_id', 'A', 'B', 'C', 'weight1'),
            (1, 'foo', 'x', 'a', 10.0),
            (2, 'bar', 'y', 'b', 25.0),
            (3, 'bar', 'z', 'c', 15.0),
        ]
        self.node.insert_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_by_index_and_label_different_order(self):
        data = [
            ('B', 'weight1', 'A', 'index_id'),
            ('x', 10.0, 'foo', 1),
            ('y', 25.0, 'bar', 2),
            ('z', 15.0, 'bar', 3),
        ]
        self.node.insert_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_warnings_with_index_id(self):
        data = [
            ('index_id', 'A', 'B', 'weight1'),
            (9, 'foo', 'x', 10.0),    # <- No matching index.
            (2, 'bar', 'YYY', 25.0),  # <- Mismatched labels.
            (3, 'bar', 'z', 15.0),    # <- OK (gets inserted)
        ]

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.insert_weights('weight1', data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            ('skipped 1 rows with non-matching index_id values, '
             'skipped 1 rows with mismatched labels, '
             'loaded 1 rows'),
        )

        # Check inserted records (only one).
        self.assertEqual(self.get_weights_helper(), [(1, 1, 3, 15.0)])

    def test_insert_warnings_not_index_id(self):
        data = [
            ('A', 'B', 'weight1'),
            ('foo', 'XXX', 10.0),  # <- No matching labels.
            ('bar', 'YYY', 25.0),  # <- No matching labels.
            ('bar', 'z', 15.0),    # <- OK (gets inserted)
        ]

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.insert_weights('weight1', data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            ('skipped 2 rows with labels that match no index, '
             'loaded 1 rows'),
        )

        # Check inserted records (only one).
        self.assertEqual(self.get_weights_helper(), [(1, 1, 3, 15.0)])

    def test_update(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        data = [
            ('index_id', 'A', 'B','weight1'),
            (2, 'bar', 'y', 555.0),
        ]
        self.node.update_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 555.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_update_different_order(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        data = [
            ('B', 'index_id', 'A', 'weight1'),
            ('y', 2, 'bar', 555.0),
        ]
        self.node.update_weights('weight1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 555.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_update_different_order_add_new(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)

        # Check that `is_complete` status is False.
        group = self.node.get_weight_group('weight1')
        self.assertFalse(group.is_complete)

        # Upate weights and check that warning is raised.
        data = [
            ('B', 'index_id', 'A', 'weight1'),
            ('x', 1, 'foo', 111.0),
            ('y', 2, 'bar', 222.0),
            ('z', 3, 'bar', 333.0),  # <- Does not previously exist.
        ]
        with self.assertWarns(ToronWarning) as cm:
            self.node.update_weights('weight1', data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            ('inserted 1 rows that did not previously exist, '
             'updated 2 rows'),
        )

        # Check updated values.
        expected = [
            (1, 1, 1, 111.0),  # <- Updated.
            (2, 1, 2, 222.0),  # <- Updated.
            (3, 1, 3, 333.0),  # <- Inserted (new record).
        ]
        self.assertEqual(self.get_weights_helper(), expected)

        # Check that `is_complete` status is now True.
        group = self.node.get_weight_group('weight1')
        self.assertTrue(group.is_complete)

    def test_update_missing_and_mismatched(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        data = [
            ('index_id', 'A', 'B','weight1'),
            (2, 'bar', 'YYY', 444.0),  # <- Mismatch.
            (9, 'bar', 'z', 555.0),    # <- No index_id 9.
        ]

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.update_weights('weight1', data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            ('skipped 1 rows with non-matching index_id values, '
             'skipped 1 rows with mismatched labels, '
             'updated 0 rows'),
        )

        # Check that values are unchanged.
        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_delete(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

            group_repo = self.node._dal.WeightGroupRepository(cursor)
            group = group_repo.get_by_name('weight1')
            group.is_complete = True
            group_repo.update(group)

        data = [
            ('index_id', 'A', 'B'),
            (1, 'foo', 'x'),
            (2, 'bar', 'y'),
        ]
        self.node.delete_weights('weight1', data)
        expected = [(3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

        # Check that `is_complete` was changed to False.
        group = self.node.get_weight_group('weight1')
        self.assertFalse(group.is_complete)

        # Test with weight column (can be present but is ignored).
        data = [
            ('index_id', 'A', 'B', 'weight1'),
            (3, 'bar', 'z', 15.0),
        ]
        self.node.delete_weights('weight1', data)
        self.assertEqual(self.get_weights_helper(), [])

    def test_delete_warnings(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)

        data = [
            ('index_id', 'A', 'B'),
            (7, 'foo', 'x'),    # <- No index match.
            (2, 'bar', 'YYY'),  # <- Label mismatch.
            (3, 'bar', 'z'),    # <- No matching weight.
        ]
        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.delete_weights('weight1', data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            ('skipped 1 rows with non-matching index_id values, '
             'skipped 1 rows with mismatched labels, '
             'skipped 1 rows with no matching weights, '
             'deleted 0 rows'),
        )

        # Check weights (unchanged--only two weights were added).
        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_delete_criteria(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        # Test single criteria (matches 2 rows).
        self.node.delete_weights('weight1', A='bar')
        expected = [(1, 1, 1, 10.0)]
        self.assertEqual(self.get_weights_helper(), expected)

        # Test multiple criteria (matches 1 row).
        self.node.delete_weights('weight1', A='foo', B='x')
        self.assertEqual(self.get_weights_helper(), [])


class TestNodeCrosswalkMethods(unittest.TestCase):
    def setUp(self):
        node = Node()
        with node._managed_cursor() as cursor:
            col_manager = node._dal.ColumnManager(cursor)
            index_repo = node._dal.IndexRepository(cursor)

            # Add index columns and records.
            col_manager.add_columns('A', 'B')
            index_repo.add('foo', 'x')
            index_repo.add('bar', 'y')
            index_repo.add('bar', 'z')

        self.node = node

    @staticmethod
    def get_crosswalk_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.CrosswalkRepository(cursor)
            return list(repository.get_all())

    def test_crosswalks_property(self):
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add('111-11-1111', None, 'crosswalk1')  # Add crosswalk_id 1.
            crosswalk_repo.add('222-22-2222', None, 'crosswalk2')  # Add crosswalk_id 2.

        actual = self.node.crosswalks
        expected = [
            Crosswalk(
                id=1,
                other_unique_id='111-11-1111',
                other_filename_hint=None,
                name='crosswalk1',
                description=None,
                selectors=None,
                is_default=False,
                user_properties=None,
                other_index_hash=None,
                is_locally_complete=False,
            ),
            Crosswalk(
                id=2,
                other_unique_id='222-22-2222',
                other_filename_hint=None,
                name='crosswalk2',
                description=None,
                selectors=None,
                is_default=False,
                user_properties=None,
                other_index_hash=None,
                is_locally_complete=False,
            ),
        ]
        self.assertEqual(actual, expected)

    def test_get_crosswalk(self):
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add('111-111-1111', 'somefile', 'name1')  # Add crosswalk_id 1.
            crosswalk_repo.add('111-111-2222', 'otherfile', 'name1', is_default=True)  # Add crosswalk_id 2.
            crosswalk_repo.add('111-111-2222', 'otherfile', 'name2')  # Add crosswalk_id 3.
            crosswalk_repo.add('333-333-3333', 'anotherfile', 'name1')  # Add crosswalk_id 4.
            crosswalk_repo.add('333-333-3333', 'anotherfile', 'name2')  # Add crosswalk_id 5.

        result = self.node.get_crosswalk('111-111-1111')
        self.assertEqual(result.id, 1, msg='should find distinct match on unique id')

        result = self.node.get_crosswalk('somefile')
        self.assertEqual(result.id, 1, msg='should find distinct match on filename hint')

        regex = (r'node reference matches more than one node:\n'
                 r'  111-111-1111 \(somefile\)\n'
                 r'  111-111-2222 \(otherfile\)')
        msg = 'should raise error if matches multiple nodes'
        with self.assertRaisesRegex(ValueError, regex, msg=msg):
            result = self.node.get_crosswalk('111-111')  # <- Ambiguous shortcode.

        msg = 'should warn if there are multiple matches'
        with self.assertWarns(ToronWarning, msg=msg) as cm:
            result = self.node.get_crosswalk('111-111-2222')
        self.assertEqual(
            str(cm.warning),  # Check warning message.
            "found multiple crosswalks, using default: 'name1'",
            msg='should return default crosswalk'
        )
        self.assertEqual(result.id, 2, msg='should return default crosswalk')

        regex = "found multiple crosswalks, must specify name: 'name1', 'name2'"
        msg = 'should raise error if there are multiples but no default'
        with self.assertRaisesRegex(ValueError, regex, msg=msg):
            result = self.node.get_crosswalk('333-333-3333')

        result = self.node.get_crosswalk('111-111-2222', 'name2')
        self.assertEqual(result.id, 3, msg='specified name should match non-default crosswalk')

        result = self.node.get_crosswalk('333-333-3333', 'name1')
        self.assertEqual(result.id, 4, msg='specified name should match non-default crosswalk')

        msg = "crosswalk 'unknown_name' not found, can be: 'name1', 'name2'"
        with self.assertWarns(ToronWarning, msg=msg) as cm:
            result = self.node.get_crosswalk('333-333-3333', 'unknown_name')
        self.assertIsNone(result, msg='if specified name does not exist, should be None')

        result = self.node.get_crosswalk('000-unknown-0000')
        self.assertIsNone(result, msg='if specified node does not exist, should be None')

    def test_add_crosswalk(self):
        node = Node()

        node.add_crosswalk('111-111-1111', None, 'name1')  # <- Only required args.
        node.add_crosswalk(  # <- Defining all properties.
                other_unique_id='111-111-1111',
                other_filename_hint=None,
                name='name2',
                description='The second crosswalk.',
                selectors=['"[foo]"'],
                is_default=True,
                user_properties={'qux': 'abc', 'quux': 123},
                other_index_hash='12437810',
                is_locally_complete=False,
        )

        expected = [
            Crosswalk(
                id=1,
                other_unique_id='111-111-1111',
                other_filename_hint=None,
                name='name1',
                description=None,
                selectors=None,
                is_default=False,
                user_properties=None,
                other_index_hash=None,
                is_locally_complete=False,
            ),
            Crosswalk(
                id=2,
                other_unique_id='111-111-1111',
                other_filename_hint=None,
                name='name2',
                description='The second crosswalk.',
                selectors=['"[foo]"'],
                is_default=True,
                user_properties={'quux': 123, 'qux': 'abc'},
                other_index_hash='12437810',
                is_locally_complete=False,
            ),
        ]
        self.assertEqual(self.get_crosswalk_helper(node), expected)

    def test_edit_crosswalk(self):
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add('111-111-1111', 'somefile', 'name1')  # Add crosswalk_id 1.
            crosswalk_repo.add('111-111-1111', 'somefile', 'name2', is_default=True)  # Add crosswalk_id 2.
            crosswalk_repo.add('222-222-2222', 'otherfile', 'name1', is_default=True)  # Add crosswalk_id 3.

        # Match on other_unique_id, update `user_properties`.
        self.node.edit_crosswalk('111-111-1111', 'name1', user_properties={'foo': 'bar'})

        # Match on other_filename_hint, update `description`.
        self.node.edit_crosswalk('somefile', 'name2', description='My description.')

        # Match on other_unique_id, update `name`.
        self.node.edit_crosswalk('222-222-2222', 'name1', name='NAME_A')

        expected = [
            Crosswalk(1, '111-111-1111', 'somefile', 'name1',
                      is_default=False, user_properties={'foo': 'bar'}),
            Crosswalk(2, '111-111-1111', 'somefile', 'name2',
                      is_default=True, description='My description.'),
            Crosswalk(3, '222-222-2222', 'otherfile', 'NAME_A',
                      is_default=True),
        ]
        self.assertEqual(self.get_crosswalk_helper(self.node), expected)

        # Check `is_default` handling--other crosswalks from same node
        # should have their `is_default` values set to False.
        self.node.edit_crosswalk('111-111-1111', 'name1', is_default=True)
        expected = [
            Crosswalk(1, '111-111-1111', 'somefile', 'name1',
                      is_default=True, user_properties={'foo': 'bar'}),  # <- is_default=True
            Crosswalk(2, '111-111-1111', 'somefile', 'name2',
                      is_default=False, description='My description.'),  # <- is_default=False
            Crosswalk(3, '222-222-2222', 'otherfile', 'NAME_A',
                      is_default=True),                                  # <- unchanged
        ]
        self.assertEqual(self.get_crosswalk_helper(self.node), expected)

    def test_drop_crosswalk(self):
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add('111-111-1111', 'somefile', 'name1', is_default=True)  # Add crosswalk_id 1.
            crosswalk_repo.add('111-111-1111', 'somefile', 'name2', is_default=False)  # Add crosswalk_id 2.
            crosswalk_repo.add('222-222-2222', 'otherfile', 'name1', is_default=True)  # Add crosswalk_id 3.
            crosswalk_repo.add('222-222-2222', 'otherfile', 'name2', is_default=False)  # Add crosswalk_id 4.

        # Match on `other_unique_id` and `name`.
        self.node.drop_crosswalk('222-222-2222', 'name2')

        # Match on `other_filename_hint` and `name`.
        self.node.drop_crosswalk('somefile', 'name1')

        expected = [
            Crosswalk(2, '111-111-1111', 'somefile', 'name2', is_default=False),
            Crosswalk(3, '222-222-2222', 'otherfile', 'name1', is_default=True),
        ]
        self.assertEqual(self.get_crosswalk_helper(self.node), expected)


class TestNodeRelationMethods(unittest.TestCase):
    def setUp(self):
        node = Node()
        with node._managed_cursor() as cursor:
            col_manager = node._dal.ColumnManager(cursor)
            index_repo = node._dal.IndexRepository(cursor)
            crosswalk_repo = node._dal.CrosswalkRepository(cursor)

            # Add index columns and records.
            col_manager.add_columns('A', 'B')
            index_repo.add('foo', 'x')
            index_repo.add('bar', 'y')
            index_repo.add('bar', 'z')

            # Add crosswalk_id 1.
            crosswalk_repo.add('111-111-1111', 'myfile.toron', 'rel1')

        self.node = node

    def get_relations_helper(self):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with self.node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM relation')
            return cursor.fetchall()

    def test_select(self):
        with self.node._managed_cursor() as cursor:
            relation_repo = self.node._dal.RelationRepository(cursor)
            relation_repo.add(1, other_index_id=0, index_id=0, value=0.0)
            relation_repo.add(1, other_index_id=1, index_id=1, value=10.0)
            relation_repo.add(1, other_index_id=2, index_id=2, value=20.0)
            relation_repo.add(1, other_index_id=3, index_id=2, value=5.0)
            relation_repo.add(1, other_index_id=3, index_id=3, value=15.0)

        relations = self.node.select_relations('myfile', 'rel1', header=True)
        expected = [
            #('other_index_id', 'rel1: myfile -> ???', 'index_id', 'A', 'B')
            ('other_index_id', 'rel1', 'index_id', 'A', 'B', 'ambiguous_fields'),
            (0,  0.0, 0, '-',   '-', None),
            (1, 10.0, 1, 'foo', 'x', None),
            (2, 20.0, 2, 'bar', 'y', None),
            (3,  5.0, 2, 'bar', 'y', None),
            (3, 15.0, 3, 'bar', 'z', None),
        ]
        self.assertEqual(list(relations), expected)

        # Test with selection `header=False` and `A='bar'`.
        relations = self.node.select_relations('myfile', 'rel1', header=False, A='bar')
        expected = [
            (2, 20.0, 2, 'bar', 'y', None),
            (3,  5.0, 2, 'bar', 'y', None),
            (3, 15.0, 3, 'bar', 'z', None),
        ]
        self.assertEqual(list(relations), expected)

        # Test with selection `header=True` and `A='NOMATCH'`.
        relations = self.node.select_relations('myfile', 'rel1', header=True, A='NOMATCH')
        expected = [('other_index_id', 'rel1', 'index_id', 'A', 'B', 'ambiguous_fields')]
        msg = 'header row only, when there are no matches'
        self.assertEqual(list(relations), expected, msg=msg)

        # Test with selection `header=False` and `A='NOMATCH'`.
        relations = self.node.select_relations('myfile', 'rel1', header=False, A='NOMATCH')
        self.assertEqual(list(relations), [], msg='iterator should be empty')

    def test_select_with_ambiguous_mappings(self):
        with self.node._managed_cursor() as cursor:
            col_manager = self.node._dal.ColumnManager(cursor)
            relation_repo = self.node._dal.RelationRepository(cursor)

            col_manager.add_columns('C')  # Add another column.
            relation_repo.add(1, other_index_id=0, index_id=0, value=0.0)
            relation_repo.add(1, other_index_id=1, index_id=1, value=10.0)
            relation_repo.add(1, other_index_id=2, index_id=2, value=20.0)
            relation_repo.add(1, other_index_id=3, index_id=2, value=5.0,  mapping_level=b'\x80')
            relation_repo.add(1, other_index_id=3, index_id=3, value=15.0, mapping_level=b'\x80')

        relations = self.node.select_relations('myfile', 'rel1', header=True)
        expected = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B', 'C', 'ambiguous_fields'),
            (0,  0.0, 0, '-',   '-', '-', None),
            (1, 10.0, 1, 'foo', 'x', '-', None),
            (2, 20.0, 2, 'bar', 'y', '-', None),
            (3,  5.0, 2, 'bar', 'y', '-', 'B, C'),
            (3, 15.0, 3, 'bar', 'z', '-', 'B, C'),
        ]
        self.assertEqual(list(relations), expected)

    def test_insert(self):
        data = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B'),
            (0,  0.0, 0, '-',   '-'),
            (1, 10.0, 1, 'foo', 'x'),
            (2, 20.0, 2, 'bar', 'y'),
            (3,  5.0, 2, 'bar', 'y'),
            (3, 15.0, 3, 'bar', 'z'),
        ]
        self.node.insert_relations('myfile', 'rel1', data)

        expected = [
            (1, 1, 0, 0,  0.0, 1.00, None),
            (2, 1, 1, 1, 10.0, 1.00, None),
            (3, 1, 2, 2, 20.0, 1.00, None),
            (4, 1, 3, 2,  5.0, 0.25, None),
            (5, 1, 3, 3, 15.0, 0.75, None),
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_insert_normalization(self):
        """The first three columns can be given as their numeric types
        or they can be given as strings which should be automatically
        converted to the appropriate numeric type:

            * 1st column (other_index_id) converted to `int`
            * 2nd column (value column, e.g. 'rel1') converted to `float`
            * 3rd column (index_id) converted to `int`

        The label columns should be strings. If 'proportion' is given,
        it should be a ``float``. If 'mapping_level' is given, it should
        be ``bytes``.

        For the DAL1 backend, SQLite casts text characters as numeric
        types based on the columns "Type Affinity":

            https://www.sqlite.org/datatype3.html#type_affinity
        """
        with self.node._managed_cursor() as cursor:
            structure_repo = self.node._dal.StructureRepository(cursor)
            structure_repo.add(None,      0, 0)
            structure_repo.add(0.9140625, 1, 0)
            structure_repo.add(1.5859375, 1, 1)

        # If there's proportion column, it is ignored and proportions are
        # recalculated from the weight value (e.g., rel1) when saving.
        data = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B', 'proportion', 'mapping_level'),
            ('1', '10.0', '1', 'foo', 'x', 0.50, None),
            ('2', '20.0', '2', 'bar', 'y', 0.50, None),
            ('3',  '5.0', '2', 'bar', 'y', None, b'\x80'),
            ('3', '15.0', '3', 'bar', 'z', None, b'\x80'),
        ]
        self.node.insert_relations('myfile', 'rel1', data)

        expected = [
            (1, 1, 1, 1, 10.0, 1.0,  None),
            (2, 1, 2, 2, 20.0, 1.0,  None),
            (3, 1, 3, 2,  5.0, 0.25, b'\x80'),
            (4, 1, 3, 3, 15.0, 0.75, b'\x80'),
        ]
        msg = 'other_index_id and index_id should be int; rel1 should be ' \
              'float; proportions should be auto-calculated'
        self.assertEqual(self.get_relations_helper(), expected, msg=msg)

    def test_insert_proportion_ignored(self):
        """If 'proportion' is given as one of the columns in *data*,
        it's treated as an extra column and is ignored. This is done
        because other relations may already be present in the node that
        would affect the final proportion. So the proportion values are
        automatically recalculated after records are inserted.
        """
        data = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B', 'proportion', 'mapping_level'),
            (1, 10.0, 1, 'foo', 'x', '<ignored>', None),  # <- Value in 'proportion' column should be ignored.
        ]
        self.node.insert_relations('myfile', 'rel1', data)

        expected = [(1, 1, 1, 1, 10.0, 1.0, None)]  # <- Proportion should be 1.0 (auto-calculated).
        self.assertEqual(self.get_relations_helper(), expected)

    def test_insert_skip_bad_mapping_level(self):
        with self.node._managed_cursor() as cursor:
            structure_repo = self.node._dal.StructureRepository(cursor)
            structure_repo.add(None,      0, 0)
            structure_repo.add(0.9140625, 1, 0)
            structure_repo.add(1.5859375, 1, 1)

        data = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B', 'mapping_level'),
            (1, 10.0, 1, 'foo', 'x', b'\x40'),  # <- `\x40` is bad mapping level `(0, 1)`
            (2, 20.0, 2, 'bar', 'y', b'\x80'),
            (3,  5.0, 2, 'bar', 'y', b'\x80'),
            (3, 15.0, 3, 'bar', 'z', None),
        ]

        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.insert_relations('myfile', 'rel1', data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            'skipped 1 rows with invalid mapping levels, loaded 3 rows',
        )

        # Verify the three valid rows that were loaded.
        expected = [
            (1, 1, 2, 2, 20.0, 1.0,  b'\x80'),
            (2, 1, 3, 2,  5.0, 0.25, b'\x80'),
            (3, 1, 3, 3, 15.0, 0.75, None),
        ]
        msg = 'other_index_id and index_id should be int, rel1 should be float'
        self.assertEqual(self.get_relations_helper(), expected, msg=msg)

    def test_insert_different_order_and_extra(self):
        """Label columns in different order and extra column."""
        data = [
            ('other_index_id', 'rel1', 'index_id', 'B', 'extra', 'A'),
            (0,  0.0, 0, '-', 'x1',   '-'),
            (1, 10.0, 1, 'x', 'x2', 'foo'),
            (2, 20.0, 2, 'y', 'x3', 'bar'),
            (3,  5.0, 2, 'y', 'x4', 'bar'),
            (3, 15.0, 3, 'z', 'x5', 'bar'),
        ]
        self.node.insert_relations('myfile', 'rel1', data)

        expected = [
            (1, 1, 0, 0,  0.0, 1.0,  None),
            (2, 1, 1, 1, 10.0, 1.0,  None),
            (3, 1, 2, 2, 20.0, 1.0,  None),
            (4, 1, 3, 2,  5.0, 0.25, None),
            (5, 1, 3, 3, 15.0, 0.75, None),
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_insert_invalid_columns(self):
        data = [
            ('other_index_id', 'rel1', 'BAD_VALUE', 'A', 'B'),
            (1, 10.0, 1, 'foo', 'x'),
            (2, 20.0, 2, 'bar', 'y'),
        ]
        regex = r"columns should be start with \('other_index_id', 'rel1', 'index_id', ...\)"
        with self.assertRaisesRegex(ValueError, regex):
            self.node.insert_relations('myfile', 'rel1', data)

        data = [
            ('other_index_id', 'rel1', 'index_id', 'A'),
            (1, 10.0, 1, 'foo'),
            (2, 20.0, 2, 'bar'),
        ]
        regex = r"missing required columns: 'B'"
        with self.assertRaisesRegex(ValueError, regex):
            self.node.insert_relations('myfile', 'rel1', data)

    def test_insert_is_complete_status_and_hash(self):
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)

            data = [
                ('other_index_id', 'rel1', 'index_id', 'A', 'B'),
                (0,  0.0, 0, '-',   '-'),
                (1, 10.0, 1, 'foo', 'x'),
                (2, 20.0, 2, 'bar', 'y'),
                (3,  5.0, 2, 'bar', 'y'),
                # No record matching to index_id 3 ('bar', 'z').
            ]
            self.node.insert_relations('myfile', 'rel1', data)

            crosswalk = crosswalk_repo.get(1)
            self.assertFalse(crosswalk.is_locally_complete)
            self.assertEqual(
                crosswalk.other_index_hash,
                'c4c96cd71102046c61ec8326b2566d9e48ef2ba26d4252ba84db28ba352a0079',
                msg='hash for other_index_ids 0, 1, 2, and 3',
            )

            data = [
                ('other_index_id', 'rel1', 'index_id', 'A', 'B'),
                (4, 15.0, 3, 'bar', 'z'),  # index_id 3 completes the crosswalk
            ]
            self.node.insert_relations('myfile', 'rel1', data)

            crosswalk = crosswalk_repo.get(1)  # re-fetch the crosswalk
            self.assertTrue(crosswalk.is_locally_complete)
            self.assertEqual(
                crosswalk.other_index_hash,
                'ed545f6c1652e1c90b517e9f653bafc0cf0f7214fb2dd58e3864c1522b089982',
                msg='hash for other_index_ids 0, 1, 2, 3, and 4',
            )
