"""Tests for toron/node.py module."""

import gc
import logging
import os
import re
import sqlite3
import stat
import sys
import tempfile
import unittest
from contextlib import suppress
from decimal import Decimal
from io import StringIO
from itertools import chain
from textwrap import dedent
from unittest.mock import (
    Mock,
    call,
    sentinel,
)
if sys.version_info >= (3, 8):
    from typing import get_args
else:
    from typing_extensions import get_args

from .common import normalize_structures

from toron._utils import ToronWarning, BitFlags
from toron.data_models import (
    Crosswalk,
    Relation,
    Index,
    Location,
    Structure,
    WeightGroup,
    AttributeGroup,
    Quantity,
    QuantityIterator,
)
from toron.node import TopoNode
from toron.reader import NodeReader


class TestInstantiation(unittest.TestCase):
    def test_backend_implicit(self):
        """When no arguments are given, should create empty node."""
        node = TopoNode()
        self.assertEqual(node._dal.backend, 'DAL1')

    def test_backend_explicit(self):
        """The ``backend`` can be given explicitly."""
        node = TopoNode(backend='DAL1')
        self.assertEqual(node._dal.backend, 'DAL1')

    def test_backend_keyword_only(self):
        """The ``backend`` argument is keyword-only (not positional)."""
        with self.assertRaises(TypeError):
            node = TopoNode('DAL1')  # Using positional argument.

    def test_backend_unknown(self):
        """Invalid ``backend`` values should raise an error."""
        with self.assertRaises(RuntimeError):
            node = TopoNode(backend=None)

        with self.assertRaises(RuntimeError):
            node = TopoNode(backend='DAL#')

    def test_kwds(self):
        """The ``**kwds`` are used to create a DataConnector."""
        node = TopoNode(cache_to_drive=True)

    def test_new_node_index_hash(self):
        """Should set index_hash for newly created nodes."""
        node = TopoNode()  # Create empty node.

        with node._managed_cursor() as cursor:
            property_repo = node._dal.PropertyRepository(cursor)
            index_hash = property_repo.get('index_hash')

        self.assertTrue(
            index_hash,
            msg=('index_hash should be set (truthy) for new nodes event if '
                 'they are empty'),
        )


class TestFileHandling(unittest.TestCase):
    """Test ``TopoNode.to_file()`` and ``TopoNode.from_file()`` methods."""
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix='toron-')
        self.addCleanup(self.temp_dir.cleanup)

        if sys.version_info < (3, 7, 17):
            # Fix for old bug https://github.com/python/cpython/issues/70847
            def make_files_readwrite():
                root_dir = self.temp_dir.name
                for f in os.listdir(root_dir):
                    f_path = os.path.join(root_dir, f)
                    os.chmod(f_path, stat.S_IRUSR | stat.S_IWUSR)

            self.addCleanup(make_files_readwrite)

    def test_default_backend(self):
        """Check default backend with standard arguments."""
        file_path = os.path.join(self.temp_dir.name, 'mynode.toron')
        self.assertFalse(os.path.isfile(file_path))

        node = TopoNode()  # <- When unspecified, uses default backend.
        original_unique_id = node.unique_id
        node.to_file(file_path, fsync=True)  # <- Write node to file.
        del node
        self.assertTrue(os.path.isfile(file_path))

        try:
            node = TopoNode.from_file(file_path)  # <- Load node from file.
        except Exception as e:
            self.fail(f'could not load file: {e}')
        self.assertEqual(node.unique_id, original_unique_id,
                         msg='unique_id values should match')

    def test_dal1_backend(self):
        """Specify DAL1 backend and use DAL1-specific **kwds."""
        file_path = os.path.join(self.temp_dir.name, 'mynode-dal1.toron')
        self.assertFalse(os.path.isfile(file_path))

        node = TopoNode(backend='DAL1')  # <- Specify DAL1 explicitly.
        original_unique_id = node.unique_id
        node.to_file(file_path, fsync=True)  # <- Write node to file.
        del node
        self.assertTrue(os.path.isfile(file_path))

        try:
            node = TopoNode.from_file(file_path, cache_to_drive=True)  # <- Uses DAL1-specific `cache_to_drive` argument.
        except Exception as e:
            self.fail(f'could not load file: {e}')
        self.assertEqual(node.unique_id, original_unique_id,
                         msg='unique_id values should match')

    def test_path_hint(self):
        """Check `path_hint` property handling."""
        node = TopoNode()
        file_path = os.path.join(self.temp_dir.name, 'mynode.toron')

        # Should be `None` when created in memory but never saved.
        self.assertIsNone(node.path_hint)

        # Should be set when an instance is first saved to drive.
        node.to_file(file_path)
        self.assertEqual(node.path_hint, file_path)

        # Should keep existing value if it's already set.
        another_file_path = os.path.join(self.temp_dir.name, 'othernode.toron')
        node.to_file(another_file_path)  # <- Save again to another path.
        self.assertEqual(node.path_hint, file_path)

        # Should be set when an instance is loaded from drive.
        node = TopoNode.from_file(file_path)
        self.assertEqual(node.path_hint, file_path)

        # Check overwritten with relative path.
        other_relative_path = os.path.normpath('some/other/path/mynode.toron')
        node.path_hint = other_relative_path  # Overwrite existing value.
        self.assertEqual(node.path_hint, other_relative_path)

        # Check overwritten with absolute path.
        other_absolute_path = os.path.normpath('/some/other/path/mynode.toron')
        node.path_hint = other_absolute_path  # Overwrite existing value.
        self.assertEqual(node.path_hint, other_absolute_path)

        # Check that it remains `None` if `to_file()` fails.
        node = TopoNode()  # <- Created in memory.
        with suppress(TypeError):
            node.to_file(object())  # <- Saving fails with TypeError.
        self.assertIsNone(node.path_hint)

class TestManagedConnectionCursorAndTransaction(unittest.TestCase):
    def test_managed_connection_type(self):
        """Connection manager should return appropriate type."""
        node = TopoNode()  # Create node and get connection type (generic T1).
        connection_type = get_args(node._dal.DataConnector.__orig_bases__[0])[0]

        with node._managed_connection() as connection:
            pass

        self.assertIsInstance(connection, connection_type)

    def test_managed_connection_calls(self):
        """Connection manager should interact with connection methods."""
        node = TopoNode()
        node._connector = Mock()

        with node._managed_connection() as connection:
            self.assertEqual(
                node._connector.method_calls,
                [call.acquire_connection()],  # <- Connection acquired.
            )

        self.assertEqual(
            node._connector.method_calls,
            [call.acquire_connection(),
             call.release_connection(connection)],  # <- Connection released.
        )

    def test_managed_cursor_type(self):
        """Data cursor manager should return appropriate type."""
        node = TopoNode()  # Create node and get cursor type (generic T2).
        cursor_type = get_args(node._dal.DataConnector.__orig_bases__[0])[1]

        with node._managed_connection() as connection:
            with node._managed_cursor(connection) as cursor:
                pass

        self.assertIsInstance(cursor, cursor_type)

    def test_managed_cursor_calls(self):
        """Cursor manager should interact with cursor methods."""
        node = TopoNode()
        node._connector = Mock()

        # The acquire_connection() mock must return unique objects.
        node._connector.acquire_connection.side_effect = lambda: object()

        with node._managed_connection() as connection:
            with node._managed_cursor(connection) as cursor:
                self.assertEqual(
                    node._connector.method_calls,
                    [call.acquire_connection(),  # <- Connection acquired.
                     call.acquire_cursor(connection)],  # <- Cursor acquired.
                )

            self.assertEqual(
                node._connector.method_calls,
                [call.acquire_connection(),
                 call.acquire_cursor(connection),
                 call.release_cursor(cursor)],  # <- Cursor released.
            )

        self.assertEqual(
            node._connector.method_calls,
            [call.acquire_connection(),
             call.acquire_cursor(connection),
             call.release_cursor(cursor),
             call.release_connection(connection)],  # <- Connection released.
        )

    def test_managed_cursor_calls_implicit_connection(self):
        """Test ``_managed_cursor`` called without ``connection`` argument
        (should automatically create a connection internally).
        """
        node = TopoNode()
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
        shared_state = {}

        node = TopoNode()
        node._connector = Mock()
        node._connector.acquire_connection.return_value = sentinel.con
        node._connector.acquire_cursor.return_value = sentinel.cur
        node._connector.transaction_is_active.side_effect = \
            lambda cur: shared_state.get('is_active', False)
        node._connector.transaction_begin.side_effect = \
            lambda cur: shared_state.update({'is_active': True})
        node._connector.transaction_commit.side_effect = \
            lambda cur: shared_state.update({'is_active': False})

        with node._managed_connection() as connection:
            with node._managed_cursor(connection) as cursor:
                with node._managed_transaction(cursor) as cursor:
                    node._connector.assert_has_calls([
                        call.acquire_connection(),
                        call.acquire_cursor(sentinel.con),
                        call.transaction_is_active(sentinel.cur),
                        call.transaction_begin(sentinel.cur),  # <- BEGIN
                    ])

        node._connector.assert_has_calls([
            call.transaction_commit(sentinel.cur),  # <- COMMIT
            call.transaction_is_active(sentinel.cur),
            call.release_cursor(sentinel.cur),
            call.release_connection(sentinel.con)
        ])

    def test_managed_transaction_no_nesting(self):
        """Should not allow a transaction within a transaction."""
        shared_state = {}

        node = TopoNode()
        node._connector = Mock()
        node._connector.acquire_connection.return_value = sentinel.con
        node._connector.acquire_cursor.return_value = sentinel.cur
        node._connector.transaction_is_active.side_effect = \
            lambda cur: shared_state.get('is_active', False)
        node._connector.transaction_begin.side_effect = \
            lambda cur: shared_state.update({'is_active': True})
        node._connector.transaction_rollback.side_effect = \
            lambda cur: shared_state.update({'is_active': False})

        regex = 'cannot start a transaction within a transaction'
        with self.assertRaisesRegex(Exception, regex):
            with node._managed_connection() as connection:
                with node._managed_cursor(connection) as cursor:
                    with node._managed_transaction(cursor) as cursor:
                        node._connector.assert_has_calls([
                            call.acquire_connection(),
                            call.acquire_cursor(sentinel.con),
                            call.transaction_is_active(sentinel.cur),
                            call.transaction_begin(sentinel.cur),  # <- BEGIN
                        ])

                        # ATTEMPT TO START A TRANSACTION INSIDE ANOTHER TRANSACTION!
                        with node._managed_transaction(cursor) as cursor2:  # <- SHOULD RAISE AN ERROR!
                            pass

        node._connector.assert_has_calls([
            call.transaction_is_active(sentinel.cur),
            call.transaction_rollback(sentinel.cur),
            call.transaction_is_active(sentinel.cur),
            call.release_cursor(sentinel.cur),
            call.release_connection(sentinel.con),
        ])

    def test_managed_transaction_rollback(self):
        """Should roll-back changes when an error occurs."""
        shared_state = {}

        node = TopoNode()
        node._connector = Mock()
        node._connector.acquire_connection.return_value = sentinel.con
        node._connector.acquire_cursor.return_value = sentinel.cur
        node._connector.transaction_is_active.side_effect = \
            lambda cur: shared_state.get('is_active', False)
        node._connector.transaction_begin.side_effect = \
            lambda cur: shared_state.update({'is_active': True})
        node._connector.transaction_rollback.side_effect = \
            lambda cur: shared_state.update({'is_active': False})

        with suppress(RuntimeError):
            with node._managed_connection() as connection:
                with node._managed_cursor(connection) as cursor:
                    with node._managed_transaction(cursor) as cursor:
                        raise RuntimeError  # <- Error inside the transaction.

        node._connector.assert_has_calls([
            call.acquire_connection(),
            call.acquire_cursor(sentinel.con),
            call.transaction_is_active(sentinel.cur),
            call.transaction_begin(sentinel.cur),
            call.transaction_rollback(sentinel.cur),  # <- ROLLBACK
            call.transaction_is_active(sentinel.cur),
            call.release_cursor(sentinel.cur),
            call.release_connection(sentinel.con),
        ])

    def test_managed_transaction_implicit_resources_commit(self):
        """When called without args, should auto-acquire resources."""
        node = TopoNode()
        node._connector = Mock()
        node._connector.acquire_connection.return_value = sentinel.con
        node._connector.acquire_cursor.return_value = sentinel.cur
        node._connector.transaction_is_active.return_value = False

        with node._managed_transaction() as cursor:
            pass

        node._connector.assert_has_calls([
            call.acquire_connection(),
            call.acquire_cursor(sentinel.con),
            call.transaction_is_active(sentinel.cur),
            call.transaction_begin(sentinel.cur),
            call.transaction_commit(sentinel.cur),  # <- COMMIT
            call.transaction_is_active(sentinel.cur),
            call.release_cursor(sentinel.cur),
            call.release_connection(sentinel.con),
        ])

    def test_managed_transaction_implicit_resources_rollback(self):
        node = TopoNode()
        node._connector = Mock()
        node._connector.acquire_connection.return_value = sentinel.con
        node._connector.acquire_cursor.return_value = sentinel.cur
        node._connector.transaction_is_active.return_value = False

        with suppress(RuntimeError):
            with node._managed_transaction() as cursor:
                raise RuntimeError  # <- Error inside the transaction.

        node._connector.assert_has_calls([
            call.acquire_connection(),
            call.acquire_cursor(sentinel.con),
            call.transaction_is_active(sentinel.cur),
            call.transaction_begin(sentinel.cur),
            call.transaction_rollback(sentinel.cur),  # <- ROLLBACK
            call.transaction_is_active(sentinel.cur),
            call.release_cursor(sentinel.cur),
            call.release_connection(sentinel.con),
        ])

    def test_managed_transaction_unfinished_rollback(self):
        """Unfinished transactions should be rolled back."""
        shared_state = {}

        node = TopoNode()
        node._connector = Mock()
        node._connector.acquire_connection.return_value = sentinel.con
        node._connector.acquire_cursor.return_value = sentinel.cur
        node._connector.transaction_is_active.side_effect = \
            lambda cur: shared_state.get('is_active', False)
        node._connector.transaction_begin.side_effect = \
            lambda cur: shared_state.update({'is_active': True})
        node._connector.transaction_rollback.side_effect = \
            lambda cur: shared_state.update({'is_active': False})

        def generator_func():
            with node._managed_connection() as connection:
                with node._managed_cursor(connection) as cursor:
                    with node._managed_transaction(cursor) as cursor:
                        yield 1
                        yield 2

        generator = generator_func()

        next(generator)  # Run the generator up to the first `yield`.
        node._connector.assert_has_calls([
            call.acquire_connection(),
            call.acquire_cursor(sentinel.con),
            call.transaction_is_active(sentinel.cur),
            call.transaction_begin(sentinel.cur),  # <- BEGIN
        ])

        del generator  # Delete it before finishing the transaction.
        gc.collect()  # Explicitly trigger full garbage collection.

        node._connector.assert_has_calls([
            call.transaction_is_active(sentinel.cur),
            call.transaction_rollback(sentinel.cur),  # <- ROLLBACK
            call.release_cursor(sentinel.cur),
            call.release_connection(sentinel.con),
        ])


class TestDomainMethods(unittest.TestCase):
    def setUp(self):
        self.node = TopoNode()

    def test_domain_property(self):
        with self.node._managed_cursor() as cur:
            prop_repo = self.node._dal.PropertyRepository(cur)

            # Get domain when no value is set.
            self.assertEqual(self.node.domain, {})

            # Get domain when a value does exist.
            prop_repo.add('domain', {'foo': 'bar'})
            self.assertEqual(self.node.domain, {'foo': 'bar'})

    def test_set_domain(self):
        # Set up initial node values for conflict checks.
        self.node.add_index_columns('A', 'B')
        self.node.insert_quantities(
            value='counts',
            attributes=['corge'],
            data=[('A', 'B',  'corge', 'counts'),
                  ('1', '11', 'xxx',   100),
                  ('2', '22', 'yyy',   175),
                  ('3', '33', 'zzz',   150)],
        )

        with self.node._managed_cursor() as cur:
            prop_repo = self.node._dal.PropertyRepository(cur)

            # Set domain when none exists.
            self.node.set_domain({'foo': 'bar'})
            self.assertEqual(prop_repo.get('domain'), {'foo': 'bar'})

            # Set domain when a value already exists.
            self.node.set_domain({'baz': 'qux'})  # <- Replace existing value.
            self.assertEqual(prop_repo.get('domain'), {'baz': 'qux'})

            # Check for name conflict with index columns.
            regex = "cannot add domain, 'A' is already used as an index column"
            with self.assertRaisesRegex(ValueError, regex):
                self.node.set_domain({'A': '111'})

            # Check for name conflict with attribute.
            regex = "cannot add domain, 'corge' is already used as a quantity attribute"
            with self.assertRaisesRegex(ValueError, regex):
                self.node.set_domain({'corge': 'flurm'})


class TestDiscreteCategoriesMethods(unittest.TestCase):
    def setUp(self):
        self.node = TopoNode()
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
            structures = sorted(resutls, key=lambda structure: structure.id)
        return normalize_structures(structures)

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
        node = TopoNode()

        node.add_index_columns('A', 'B')

        self.assertEqual(self.get_cols_helper(node), ('A', 'B'))

    def test_add_index_columns_atomic(self):
        """Adding columns should be an atomic operation (either all
        columns should be added or none should be added).
        """
        node = TopoNode()

        with suppress(Exception):
            # Second 'baz' causes an error (cannot have duplicate names).
            node.add_index_columns('foo', 'bar', 'baz', 'baz')

        msg = 'should be empty tuple, no column names'
        self.assertEqual(self.get_cols_helper(node), (), msg=msg)

    def test_add_index_columns_domain_conflict(self):
        """An index column cannot be the same as a domain name."""
        node = TopoNode()
        with node._managed_cursor() as cursor:
            node._dal.PropertyRepository(cursor).add('domain', {'baz': '111', 'qux': '222'})

        regex = "cannot alter columns, 'baz' is used in the domain"
        with self.assertRaisesRegex(ValueError, regex):
            node.add_index_columns('foo', 'bar', 'baz')

        regex = "cannot alter columns, 'value' is a reserved identifier"
        with self.assertRaisesRegex(ValueError, regex):
            node.add_index_columns('value')

    def test_index_columns_property(self):
        node = TopoNode()
        self.add_cols_helper(node, 'A', 'B')

        columns = node.index_columns  # Accessed as property attribute.

        self.assertEqual(columns, ['A', 'B'])

    def test_rename_index_columns(self):
        node = TopoNode()
        self.add_cols_helper(node, 'A', 'B', 'C', 'D')

        if sqlite3.sqlite_version_info >= (3, 25, 0) or node._dal.backend != 'DAL1':
            node.rename_index_columns({'B': 'G', 'D': 'T'})
        else:
            import toron.dal1
            toron.dal1.legacy_rename_columns(node, {'B': 'G', 'D': 'T'})

        self.assertEqual(self.get_cols_helper(node), ('A', 'G', 'C', 'T'))

    def test_rename_index_columns_and_categories(self):
        node = TopoNode()
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

    def test_rename_index_columns_domain_conflict(self):
        node = TopoNode()
        self.add_cols_helper(node, 'A', 'B', 'C', 'D')

        with node._managed_cursor() as cursor:
            node._dal.PropertyRepository(cursor).add('domain', {'T': 'xxx'})

        regex = "cannot alter columns, 'T' is used in the domain"
        with self.assertRaisesRegex(ValueError, regex):
            if sqlite3.sqlite_version_info >= (3, 25, 0) or node._dal.backend != 'DAL1':
                node.rename_index_columns({'B': 'G', 'D': 'T'})
            else:
                import toron.dal1
                toron.dal1.legacy_rename_columns(node, {'B': 'G', 'D': 'T'})

    def test_rename_index_columns_reserved_identifier(self):
        node = TopoNode()
        self.add_cols_helper(node, 'A', 'B', 'C', 'D')

        # Check target-name conflict.
        regex = "cannot alter columns, 'value' is a reserved identifier"
        with self.assertRaisesRegex(ValueError, regex):
            if sqlite3.sqlite_version_info >= (3, 25, 0) or node._dal.backend != 'DAL1':
                node.rename_index_columns({'B': 'value'})
            else:
                import toron.dal1
                toron.dal1.legacy_rename_columns(node, {'B': 'value'})

        # Check source-name conflict.
        regex = "cannot alter columns, 'index_id' is a reserved identifier"
        with self.assertRaisesRegex(ValueError, regex):
            if sqlite3.sqlite_version_info >= (3, 25, 0) or node._dal.backend != 'DAL1':
                node.rename_index_columns({'index_id': 'G'})
            else:
                import toron.dal1
                toron.dal1.legacy_rename_columns(node, {'index_id': 'G'})

    def test_drop_index_columns(self):
        node = TopoNode()
        self.add_cols_helper(node, 'A', 'B', 'C', 'D')

        if sqlite3.sqlite_version_info >= (3, 35, 5) or node._dal.backend != 'DAL1':
            node.drop_index_columns('B', 'D')
        else:
            import toron.dal1
            toron.dal1.legacy_drop_columns(node, 'B', 'D')

        self.assertEqual(self.get_cols_helper(node), ('A', 'C'))

    def test_drop_index_columns_all(self):
        node = TopoNode()
        self.add_cols_helper(node, 'A', 'B', 'C')

        if node._dal.backend == 'DAL1' and sqlite3.sqlite_version_info < (3, 35, 5):
            self.skipTest('requires SQLite 3.35.5 or newer')

        regex = 'cannot remove all index columns'
        with self.assertRaisesRegex(RuntimeError, regex):
            node.drop_index_columns('A', 'B', 'C')

    def test_drop_index_columns_reserved_identifier(self):
        node = TopoNode()
        self.add_cols_helper(node, 'A', 'B', 'C')

        regex = "cannot alter columns, 'index_id' is a reserved identifier"
        with self.assertRaisesRegex(ValueError, regex):
            if sqlite3.sqlite_version_info >= (3, 25, 0) or node._dal.backend != 'DAL1':
                node.drop_index_columns('C', 'index_id')
            else:
                import toron.dal1
                toron.dal1.legacy_drop_columns(node, 'C', 'index_id')


class TestIndexMethods(unittest.TestCase):
    def setUp(self):
        # Set up stream object to capture log messages.
        self.log_stream = StringIO()
        self.addCleanup(self.log_stream.close)

        # Add handler to 'app-toron' logger.
        applogger = logging.getLogger('app-toron')
        handler = logging.StreamHandler(self.log_stream)
        handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        applogger.addHandler(handler)
        self.addCleanup(lambda: applogger.removeHandler(handler))

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
            return list(repository.find_all())

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
            structures = sorted(repository.get_all(), key=lambda x: x.id)
        return normalize_structures(structures)

    def test_insert(self):
        node = TopoNode()
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

    def test_insert_no_existing_structure(self):
        """Should auto-add categories and structure if not defined."""
        node = TopoNode()
        self.add_cols_helper(node, 'A', 'B')

        self.assertEqual(self.get_structure_helper(node), [], msg='should start empty')

        node.insert_index([('A', 'B'), ('foo', 'x'), ('bar', 'y')])
        expected = [Index(0, '-', '-'), Index(1, 'foo', 'x'), Index(2, 'bar', 'y')]
        self.assertEqual(self.get_index_helper(node), expected)

        expected = [
            Structure(id=1, granularity=None, bits=(0, 0)),
            Structure(id=2, granularity=1.0,  bits=(1, 1)),
        ]
        self.assertEqual(self.get_structure_helper(node), expected, msg='should be automatically added')

    def test_insert_skip_empty_rows(self):
        """Text based files (like CSV files) often end with a newline
        character. Many parsers interpret this as an empty row of data.
        """
        node = TopoNode()
        self.add_cols_helper(node, 'A', 'B')

        # Insert data where second and last items are empty.
        data = [('foo', 'x'), (), ('bar', 'y'), ()]  # <- Includes empty rows!
        node.insert_index(data, columns=['A', 'B'])

        expected = [
            Index(0, '-', '-'),
            Index(1, 'foo', 'x'),
            Index(2, 'bar', 'y'),
        ]
        self.assertEqual(self.get_index_helper(node), expected)

    def test_insert_different_order(self):
        node = TopoNode()
        self.add_cols_helper(node, 'A', 'B')

        data = [('B', 'A'), ('x', 'foo'), ('y', 'bar')]  # <- Different order.
        node.insert_index(data)

        expected = [
            Index(0, '-', '-'),
            Index(1, 'foo', 'x'),
            Index(2, 'bar', 'y'),
        ]
        self.assertEqual(self.get_index_helper(node), expected)

    def test_insert_missing_columns(self):
        node = TopoNode()
        self.add_cols_helper(node, 'A', 'B', 'C', 'D')

        regex = r"missing required columns: 'C', 'D'"
        with self.assertRaisesRegex(ValueError, regex):
            node.insert_index([('A', 'B'), ('foo', 'x'), ('bar', 'y')])

    def test_insert_extra_columns(self):
        node = TopoNode()
        self.add_cols_helper(node, 'A', 'B')
        node.insert_index([
            ('C',   'B', 'D', 'A'),  # <- Extra columns (C and D).
            ('111', 'x', '1', 'foo'),
            ('222', 'y', '2', 'bar'),
        ])

        # Check the logged messages.
        self.assertEqual(
            self.log_stream.getvalue(),
            ("INFO: loaded 2 index records\n"
             "WARNING: ignored extra columns: 'C', 'D'\n"),
        )

        expected = [
            Index(0, '-', '-'),
            Index(1, 'foo', 'x'),
            Index(2, 'bar', 'y'),
        ]
        self.assertEqual(self.get_index_helper(node), expected)

    def test_insert_duplicate_or_empty_strings(self):
        node = TopoNode()
        self.add_cols_helper(node, 'A', 'B')

        node.insert_index([
            ('A', 'B'),
            ('foo', 'x'),
            ('foo', 'x'),  # <- Duplicate of previous record.
            ('bar', ''),   # <- Contains empty string.
            ('bar', 'y'),
            ('baz', 'z'),
        ])

        # Check the logged messages.
        self.assertEqual(
            self.log_stream.getvalue(),
            ('INFO: loaded 3 index records\n'
             'WARNING: skipped 1 duplicate records\n'
             'WARNING: skipped 1 records having some empty string labels\n'),
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
        node = TopoNode()
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
        node = TopoNode()
        self.add_cols_helper(node, 'A', 'B')
        data = [('foo', 'x'), ('bar', 'y')]
        self.add_index_helper(node, data)

        with node._managed_cursor() as cursor:
            crosswalk_repo = node._dal.CrosswalkRepository(cursor)
            relation_repo = node._dal.RelationRepository(cursor)

            # Add crosswalk_id 1 and weight records.
            crosswalk_repo.add('111-111-1111', 'somenode.toron', 'edge1', is_locally_complete=True)
            relation_repo.add(1, 1, 1, None, 6000)
            relation_repo.add(1, 2, 2, None, 4000)

            # Add crosswalk_id 2 and weight records.
            crosswalk_repo.add('222-222-2222', 'anothernode.toron', 'edge2', is_locally_complete=False)
            relation_repo.add(2, 1, 1, None, 4000)
            relation_repo.add(2, 2, 1, None, 2000)  # <- Maps to local index_id 1 (no relation goes to index_id 2)

            # Insert new index record!
            node.insert_index([('A', 'B'), ('baz', 'z')])

            # Check that edge1's is_locally_complete is changed to False.
            crosswalk = crosswalk_repo.get(1)
            self.assertFalse(crosswalk.is_locally_complete)

            # Check that edge2's is_locally_complete remains False (unchanged).
            crosswalk = crosswalk_repo.get(2)
            self.assertFalse(crosswalk.is_locally_complete)

    def test_insert_index_modifies_index_hash(self):
        node = TopoNode()
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
        node = TopoNode()
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


class TestTopoNodeUpdateIndex(unittest.TestCase):
    @staticmethod
    def get_index_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.IndexRepository(cursor)
            return list(repository.find_all())

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
        node = TopoNode()
        with node._managed_cursor() as cursor:
            manager = node._dal.ColumnManager(cursor)
            manager.add_columns('A', 'B')

            repository = node._dal.IndexRepository(cursor)
            repository.add('foo', 'x')
            repository.add('bar', 'y')

            prop_repo = node._dal.PropertyRepository(cursor)
            prop_repo.update('index_hash', '5dfadd0e50910f561636c47335ecf8316251cbd85964eadb5c00103502edf177')

            weight_group_repo = node._dal.WeightGroupRepository(cursor)
            weight_group_repo.add('group1')  # Adds weight_group_id 1.
            weight_repo = node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 175000)
            weight_repo.add(1, 2,  25000)

            crosswalk_repo = node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add('111-11-1111', None, 'other1')  # Adds crosswalk_id 1.
            relation_repo = node._dal.RelationRepository(cursor)
            relation_repo.add(1, 1, 1, None, 16350, 0.75)
            relation_repo.add(1, 1, 2, None, 5450,  0.25)
            relation_repo.add(1, 2, 2, None, 13050, 1.00)

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
        expected = [(1, 1, 1, 1, None, 21800.0, 1.0), (2, 1, 2, 1, None, 13050.0, 1.0)]
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
        self.assertEqual(normalize_structures(actual), expected)

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
            relation_repo.add(2, 1, 1, None, 4000)
            relation_repo.add(2, 2, 1, None, 2000)  # <- Maps to local index_id 1 (no relation goes to index_id 2)

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


class TestTopoNodeDeleteIndex(unittest.TestCase):
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
            return list(repository.find_all())

    def setUp(self):
        node = TopoNode()
        self.add_cols_helper(node, 'A', 'B')
        self.add_index_helper(node, [('foo', 'x'), ('bar', 'y')])

        with node._managed_cursor() as cursor:
            prop_repo = node._dal.PropertyRepository(cursor)
            prop_repo.update('index_hash', '5dfadd0e50910f561636c47335ecf8316251cbd85964eadb5c00103502edf177')

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
        fully_specified_level = bytes(BitFlags(1, 1))

        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add('111-11-1111', None, 'other1')  # Adds crosswalk_id 1.
            relation_repo = self.node._dal.RelationRepository(cursor)
            relation_repo.add(1, 1, 1, fully_specified_level, 16350, 0.75)
            relation_repo.add(1, 1, 2, fully_specified_level, 5450,  0.25)
            relation_repo.add(1, 2, 2, fully_specified_level, 13050, 1.00)

        data = [
            ('index_id', 'A', 'B'),
            (2, 'bar', 'y'),
        ]
        self.node.delete_index(data)

        expected = [Index(0, '-', '-'), Index(1, 'foo', 'x')]
        self.assertEqual(self.get_index_helper(self.node), expected)

        expected = [(1, 1, 1, 1, fully_specified_level, 16350.0, 1.0)]  # <- Proportion is updated, too (was 0.75).
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
            relation_repo.add(1, 1, 1, bytes(BitFlags(1, 0)), 16350, 0.75)  # <- Ambiguous relations.
            relation_repo.add(1, 1, 2, bytes(BitFlags(1, 0)), 5450,  0.25)  # <- Ambiguous relations.
            relation_repo.add(1, 2, 2, bytes(BitFlags(1, 1)), 13050, 1.00)  # <- Fully specified.

        data = [('index_id', 'A', 'B'), (2, 'bar', 'y')]

        regex = 'associated crosswalk relations are ambiguous'
        with self.assertRaisesRegex(ValueError, regex):
            self.node.delete_index(data)

    def test_delete_and_is_locally_complete_status(self):
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add('111-11-1111', None, 'other1', is_locally_complete=False)  # Adds crosswalk_id 1.
            relation_repo = self.node._dal.RelationRepository(cursor)
            relation_repo.add(1, 1, 1, bytes(BitFlags(1, 1)), 16350, 0.75)
            relation_repo.add(1, 2, 1, bytes(BitFlags(1, 1)), 5450,  0.25)

            data = [('index_id', 'A', 'B'), (2, 'bar', 'y')]
            self.node.delete_index(data)  # Deletes index without a relation (index_id 2).

            # Check that is_locally_complete has been changed to True.
            crosswalk = crosswalk_repo.get(1)
            self.assertTrue(crosswalk.is_locally_complete)

    def test_delete_and_other_index_hash_updates(self):
        fully_specified_level = bytes(BitFlags(1, 1))

        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add('111-11-1111', None, 'other1',
                               other_index_hash='8c7654ecfd7b0b623b803e2f4e02ad1cc84278efdfcd7c4c9208edd81f17e115',
                               is_locally_complete=True)  # Adds crosswalk_id 1.
            relation_repo = self.node._dal.RelationRepository(cursor)
            relation_repo.add(1, 1, 1, fully_specified_level, 16350, 0.75)
            relation_repo.add(1, 1, 2, fully_specified_level,  5450, 0.25)
            relation_repo.add(1, 2, 2, fully_specified_level,  7500, 1.00)

            crosswalk_repo.add('222-22-2222', None, 'other2',
                               other_index_hash='65b5281bf090304aa0255d2af391f164cb81d587a4c7b5b27db04faacb9388df',
                               is_locally_complete=False)  # Adds crosswalk_id 2.
            relation_repo = self.node._dal.RelationRepository(cursor)
            relation_repo.add(2, 7, 1, fully_specified_level, 6000, 1.00)
            relation_repo.add(2, 8, 1, fully_specified_level, 9000, 0.5625)
            relation_repo.add(2, 8, 2, fully_specified_level, 7000, 0.4375)

            data = [('index_id', 'A', 'B'), (2, 'bar', 'y')]
            self.node.delete_index(data)  # Deletes index_id 2.

            crosswalk = crosswalk_repo.get(1)
            self.assertTrue(crosswalk.is_locally_complete, msg='should be unchanged')
            self.assertEqual(
                crosswalk.other_index_hash,
                'cd2662154e6d76b2b2b92e70c0cac3ccf534f9b74eb5b89819ec509083d00a50',
                msg='should be changed (different set of other_index_id values)',

            )

            crosswalk = crosswalk_repo.get(2)
            self.assertTrue(crosswalk.is_locally_complete, msg='should be changed (was False)')
            self.assertEqual(
                crosswalk.other_index_hash,
                '65b5281bf090304aa0255d2af391f164cb81d587a4c7b5b27db04faacb9388df',
                msg='should be unchanged (same set of other_index_id values)',
            )

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
        self.assertEqual(normalize_structures(actual), expected)

    def test_delete_and_index_hash_updates(self):
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


class TestTopoNodeWeightGroupMethods(unittest.TestCase):
    def setUp(self):
        # Set up stream object to capture log messages.
        self.log_stream = StringIO()
        self.addCleanup(self.log_stream.close)

        # Add handler to 'app-toron' logger.
        applogger = logging.getLogger('app-toron')
        handler = logging.StreamHandler(self.log_stream)
        handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        applogger.addHandler(handler)
        self.addCleanup(lambda: applogger.removeHandler(handler))

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

    @staticmethod
    def get_default_weight_group_id_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.PropertyRepository(cursor)
            return repository.get('default_weight_group_id')

    def test_weight_groups_property(self):
        """The `node.weight_groups` property should be list of groups
        ordered by name.
        """
        node = TopoNode()
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
        node = TopoNode()
        with node._managed_cursor() as cursor:
            weight_group_repo = node._dal.WeightGroupRepository(cursor)
            weight_group_repo.add('name_a', 'Group A')
            weight_group_repo.add('name_b', 'Group B')

        actual = node.get_weight_group('name_a')
        expected = WeightGroup(id=1, name='name_a', description='Group A', selectors=None)
        self.assertEqual(actual, expected)

        self.assertIsNone(node.get_weight_group('name_zzz'))

    def test_add_weight_group(self):
        # Test `add_weight_group()` behavior.
        node = TopoNode()
        node.add_weight_group('name_a')  # <- Only `name` is required (should log a warning and set as default).
        node.add_weight_group(  # <- Defining all properties.
            name='name_b',
            description='Group B',
            selectors=['"[foo]"'],
            is_complete=True
        )

        self.assertEqual(
            self.log_stream.getvalue(),
            "WARNING: setting default weight group: 'name_a'\n",
        )

        self.assertEqual(
            self.get_weight_group_helper(node),
            [WeightGroup(id=1,
                         name='name_a',
                         description=None,
                         selectors=None,
                         is_complete=False),
             WeightGroup(id=2,
                         name='name_b',
                         description='Group B',
                         selectors=['"[foo]"'],
                         is_complete=True)]
        )

        self.assertEqual(
            self.get_default_weight_group_id_helper(node),
            1,
            msg='if not specified otherwise, first edge should be set as default',
        )

    def test_edit_weight_group(self):
        node = TopoNode()
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

        node.edit_weight_group('name_x', description='Description of X.')

        # Check warning message.
        self.assertEqual(
            self.log_stream.getvalue(),
            "WARNING: no weight group named 'name_x'\n",
        )

    def test_drop_weight_group(self):
        node = TopoNode()
        with node._managed_cursor() as cursor:
            manager = node._dal.ColumnManager(cursor)
            index_repo = node._dal.IndexRepository(cursor)
            property_repo = node._dal.PropertyRepository(cursor)
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

            # Make weight group the default.
            property_repo.add('default_weight_group_id', 1)

        node.drop_weight_group('name_a')  # <- Method under test.

        self.assertEqual(
            self.log_stream.getvalue(),
            ("INFO: removed weight group 'name_a'\n"
             "WARNING: default weight group was removed\n"),
        )

        msg = 'weight group and associated weights should be deleted'
        self.assertEqual(self.get_weight_group_helper(node), [], msg=msg)
        self.assertEqual(self.get_weight_helper(node), [], msg=msg)

        # Clear `log_stream` buffer (for next assertion).
        self.log_stream.seek(0)
        self.log_stream.truncate()

        node.drop_weight_group('name_x')  # <- Method under test.

        self.assertEqual(
            self.log_stream.getvalue(),
            "WARNING: no weight group named 'name_x'\n",
        )


class TestTopoNodeSelectWeights(unittest.TestCase):
    def setUp(self):
        node = TopoNode()
        with node._managed_cursor() as cursor:
            # Add index columns and records.
            node._dal.ColumnManager(cursor).add_columns('A', 'B')
            index_repo = node._dal.IndexRepository(cursor)
            index_repo.add('foo', 'x')
            index_repo.add('bar', 'y')
            index_repo.add('bar', 'z')

            # Add weight_group_id 1 and weights.
            node._dal.WeightGroupRepository(cursor).add('group1')
            weight_repo = node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

            # Add index hash (needed for QuantityIterator).
            node._dal.PropertyRepository(cursor).update(
                'index_hash',
                'c4c96cd71102046c61ec8326b2566d9e48ef2ba26d4252ba84db28ba352a0079',
            )

        self.node = node

    def test_generator_select_all(self):
        generator = self.node._select_weights('group1')
        expected = [
            (Index(id=1, labels=('foo', 'x')), {'weight': 'group1'}, 10.0),
            (Index(id=2, labels=('bar', 'y')), {'weight': 'group1'}, 25.0),
            (Index(id=3, labels=('bar', 'z')), {'weight': 'group1'}, 15.0),
        ]
        self.assertEqual(list(generator), expected)

    def test_generator_with_criteria(self):
        """Test with selection criteria A='bar'."""
        generator = self.node._select_weights('group1', A='bar')
        expected = [
            (Index(id=2, labels=('bar', 'y')), {'weight': 'group1'}, 25.0),
            (Index(id=3, labels=('bar', 'z')), {'weight': 'group1'}, 15.0),
        ]
        self.assertEqual(list(generator), expected)

    def test_generator_no_matching_criteria(self):
        """When no criteria matches, generator should be empty."""
        weights = self.node._select_weights('group1', A='NOMATCH')
        self.assertEqual(list(weights), [], msg='generator should be empty')

    def test_generator_missing_weights(self):
        """The the full set of index matches should be returned even if
        there are no associated weights.
        """
        with self.node._managed_cursor() as cursor:
            weight_group_repo = self.node._dal.WeightGroupRepository(cursor)
            weight_repo = self.node._dal.WeightRepository(cursor)

            weight_group_repo.add('group2')
            weight_repo.add(2, 1, 12.0)
            weight_repo.add(2, 3, 16.0)

        generator = self.node._select_weights('group2')
        expected = [
            (Index(id=1, labels=('foo', 'x')), {'weight': 'group2'}, 12.0),
            (Index(id=2, labels=('bar', 'y')), {'weight': 'group2'}, None),  # <- Missing weight.
            (Index(id=3, labels=('bar', 'z')), {'weight': 'group2'}, 16.0),
        ]
        self.assertEqual(list(generator), expected, msg='missing weights should be None')

        # Select with criteria (should return weights with matching index records).
        generator = self.node._select_weights('group2', A='bar')
        expected = [
            (Index(id=2, labels=('bar', 'y')), {'weight': 'group2'}, None),  # <- Missing weight.
            (Index(id=3, labels=('bar', 'z')), {'weight': 'group2'}, 16.0),
        ]
        self.assertEqual(list(generator), expected, msg='expected matching indexes only')

    def test_public_wrapper_method(self):
        """The `select_weights()` method wraps generator output."""
        weights = self.node.select_weights('group1')

        self.assertIsInstance(weights, QuantityIterator)
        self.assertEqual(
            weights.columns,
            ('A', 'B', 'weight', 'value'),
        )
        self.assertEqual(
            list(weights),
            [('foo', 'x', 'group1', 10.0),
             ('bar', 'y', 'group1', 25.0),
             ('bar', 'z', 'group1', 15.0)],
        )


class TestTopoNodeWeightMethods(unittest.TestCase):
    def setUp(self):
        node = TopoNode()
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
            weight_group_repo.add('group1')

        self.node = node

        # Set up stream object to capture log messages.
        self.log_stream = StringIO()
        self.addCleanup(self.log_stream.close)

        # Add handler to 'app-toron' logger.
        applogger = logging.getLogger('app-toron')
        handler = logging.StreamHandler(self.log_stream)
        handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        applogger.addHandler(handler)
        self.addCleanup(lambda: applogger.removeHandler(handler))

    def get_weights_helper(self):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with self.node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM weight')
            return cursor.fetchall()

    def test_insert_by_label(self):
        data = [
            ('A', 'B', 'group1'),
            ('foo', 'x', 10.0),
            ('bar', 'y', 25.0),
            ('bar', 'z', 15.0),
        ]
        self.node.insert_weights('group1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_by_label_different_order(self):
        data = [
            ('B', 'A', 'group1'),
            ('x', 'foo', 10.0),
            ('y', 'bar', 25.0),
            ('z', 'bar', 15.0),
        ]
        self.node.insert_weights('group1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_using_value_column(self):
        """Test *value_column* when `data` has no column matching weight_group."""
        data = [
            ('A', 'B', 'weight'),  # <- No column named 'group1'.
            ('foo', 'x', 10.0),
            ('bar', 'y', 25.0),
            ('bar', 'z', 15.0),
        ]
        self.node.insert_weights('group1', data, value_column='weight')

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_using_value_column_with_matching(self):
        """Test *value_column* even if matching weight_group exists."""
        data = [
            ('A', 'B', 'group1', 'group1fixed'),
            ('foo', 'x', 0.0, 10.0),
            ('bar', 'y', 0.0, 25.0),
            ('bar', 'z', 0.0, 15.0),
        ]
        self.node.insert_weights('group1', data, value_column='group1fixed')

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_by_label_extra_columns(self):
        data = [
            ('A', 'B', 'C', 'group1'),
            ('foo', 'x', 'a', 10.0),
            ('bar', 'y', 'b', 25.0),
            ('bar', 'z', 'c', 15.0),
        ]
        self.node.insert_weights('group1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_by_index_and_label(self):
        data = [
            ('index_id', 'A', 'B', 'group1'),
            (1, 'foo', 'x', 10.0),
            (2, 'bar', 'y', 25.0),
            (3, 'bar', 'z', 15.0),
        ]
        self.node.insert_weights('group1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_is_complete_status(self):
        data = [
            ('index_id', 'A', 'B', 'group1'),
            (1, 'foo', 'x', 10.0),
            (2, 'bar', 'y', 25.0),
            # Omits weight for index_id 3.
        ]
        self.node.insert_weights('group1', data)

        group = self.node.get_weight_group('group1')
        self.assertFalse(group.is_complete,
                         msg='no weight for index_id 3, should be false')

        # Add weight for index_id 3 and check again.
        data = [
            ('index_id', 'A', 'B', 'group1'),
            (3, 'bar', 'z', 15.0),
        ]
        self.node.insert_weights('group1', data)
        group = self.node.get_weight_group('group1')
        self.assertTrue(group.is_complete)

    def test_insert_by_index_and_label_extra_columns(self):
        data = [
            ('index_id', 'A', 'B', 'C', 'group1'),
            (1, 'foo', 'x', 'a', 10.0),
            (2, 'bar', 'y', 'b', 25.0),
            (3, 'bar', 'z', 'c', 15.0),
        ]
        self.node.insert_weights('group1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_by_index_and_label_different_order(self):
        data = [
            ('B', 'group1', 'A', 'index_id'),
            ('x', 10.0, 'foo', 1),
            ('y', 25.0, 'bar', 2),
            ('z', 15.0, 'bar', 3),
        ]
        self.node.insert_weights('group1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_warnings_with_index_id(self):
        data = [
            ('index_id', 'A', 'B', 'group1'),
            (9, 'foo', 'x', 10.0),    # <- No matching index.
            (2, 'bar', 'YYY', 25.0),  # <- Mismatched labels.
            (3, 'bar', 'z', 15.0),    # <- OK (gets inserted)
        ]

        self.node.insert_weights('group1', data)

        # Check the logged messages.
        self.assertEqual(
            self.log_stream.getvalue(),
            ("INFO: loaded 1 new records into 'group1'\n"
             "WARNING: skipped 1 rows with no matching index_id\n"
             "WARNING: skipped 1 rows whose labels do not match the given index_id\n"),
        )

        # Check inserted records (only one).
        self.assertEqual(self.get_weights_helper(), [(1, 1, 3, 15.0)])

    def test_insert_warnings_not_index_id(self):
        data = [
            ('A', 'B', 'group1'),
            ('foo', 'XXX', 10.0),  # <- No matching labels.
            ('bar', 'YYY', 25.0),  # <- No matching labels.
            ('bar', 'z', 15.0),    # <- OK (gets inserted)
        ]

        self.node.insert_weights('group1', data)

        # Check the logged messages.
        self.assertEqual(
            self.log_stream.getvalue(),
            ("INFO: loaded 1 new records into 'group1'\n"
             "WARNING: skipped 2 rows whose labels do not match any existing index\n"),
        )

        # Check inserted records (only one).
        self.assertEqual(self.get_weights_helper(), [(1, 1, 3, 15.0)])

    def test_insert_undefined_record(self):
        """Should log a warning when given a weight for the undefined record."""
        data = [
            ('A', 'B', 'C', 'group1'),
            ('foo', 'x', 'a', 10.0),
            ('-',   '-', '-',  0.0),  # <- Undefined record.
            ('bar', 'y', 'b', 25.0),
            ('bar', 'z', 'c', 15.0),
            ('-',   '-', '-',  7.0),  # <- Undefined record.
        ]
        self.node.insert_weights('group1', data)

        # Check the logged messages.
        self.assertEqual(
            self.log_stream.getvalue(),
            ("INFO: loaded 3 new records into 'group1', weight group is complete\n"
             "WARNING: skipped 2 rows matching the undefined record\n"),
        )

        # Check that the other weights were loaded as normal.
        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_insert_other_types(self):
        data = [
            ('A', 'B', 'group1'),
            ('foo', 'x', '10.0'),         # <- String.
            ('bar', 'y', 25),             # <- Integer.
            ('bar', 'z', Decimal('15')),  # <- Decimal.
        ]
        self.node.insert_weights('group1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

        for record in self.get_weights_helper():
            with self.subTest(record=record):
                *_, weight_value = record
                self.assertIsInstance(weight_value, float)

    def test_insert_non_real_nums(self):
        """Should log a warning when given a weight for the undefined record."""
        data = [
            ('A', 'B', 'group1'),
            ('foo', 'x', 'foobar'),      # <- Non-numeric string.
            ('foo', 'x', 10.0),
            ('foo', 'x', float('nan')),  # <- Not A Number
            ('bar', 'y', 25.0),
            ('foo', 'x', float('inf')),  # <- Infinity.
            ('bar', 'z', 15.0),
        ]
        self.node.insert_weights('group1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

        # Check the logged messages.
        self.assertEqual(
            self.log_stream.getvalue(),
            ("INFO: loaded 3 new records into 'group1', weight group is complete\n"
             "WARNING: skipped 3 rows without real number values\n"),
        )

    def test_insert_on_conflict_fail(self):
        data = [
            ('A', 'B', 'group1'),
            ('foo', 'x', 10.0),
            ('bar', 'y', 25.0),
            ('bar', 'z', 15.0),
            ('bar', 'z', 84.0),  # <- Conflicts with previous record.
        ]
        regex = r'weight record already exists'
        with self.assertRaisesRegex(Exception, regex):
            self.node.insert_weights('group1', data)

        self.assertEqual(
            self.get_weights_helper(),
            [],
            msg='no records should be loaded',
        )

        self.assertEqual(
            self.log_stream.getvalue(),
            '',
            msg='should not log any messages',
        )

    def test_insert_on_conflict_ignore(self):
        data = [
            ('A', 'B', 'group1'),
            ('foo', 'x', 10.0),
            ('bar', 'y', 25.0),
            ('bar', 'z', 15.0),
            ('bar', 'z', 84.0),  # <- Conflicts with previous record.
        ]
        self.node.insert_weights('group1', data, on_conflict='skip')

        self.assertEqual(
            self.get_weights_helper(),
            [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 15.0)],
        )

        self.assertEqual(
            self.log_stream.getvalue(),
            ("INFO: loaded 3 new records into 'group1', weight group is complete\n"
             "WARNING: skipped 1 rows that match existing records\n"),
        )

    def test_insert_on_conflict_replace(self):
        data = [
            ('A', 'B', 'group1'),
            ('foo', 'x', 10.0),
            ('bar', 'y', 25.0),
            ('bar', 'z', 15.0),
            ('bar', 'z', 84.0),  # <- Conflicts with previous record.
        ]
        self.node.insert_weights('group1', data, on_conflict='overwrite')

        self.assertEqual(
            self.get_weights_helper(),
            [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 84.0)],
        )

        self.assertEqual(
            self.log_stream.getvalue(),
            ("INFO: loaded 3 new records into 'group1', weight group is complete\n"
             "WARNING: replaced 1 existing records with new weights\n"),
        )

    def test_insert_on_conflict_sum(self):
        data = [
            ('A', 'B', 'group1'),
            ('foo', 'x', 10.0),
            ('bar', 'y', 25.0),
            ('bar', 'z', 15.0),
            ('bar', 'z', 84.0),  # <- Conflicts with previous record.
        ]
        self.node.insert_weights('group1', data, on_conflict='sum')

        self.assertEqual(
            self.get_weights_helper(),
            [(1, 1, 1, 10.0), (2, 1, 2, 25.0), (3, 1, 3, 99.0)],
        )

        self.assertEqual(
            self.log_stream.getvalue(),
            ("INFO: loaded 3 new records into 'group1', weight group is complete\n"
             "WARNING: combined sum of 1 new weights together with existing records\n"),
        )

    def test_update(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        data = [
            ('index_id', 'A', 'B','group1'),
            (2, 'bar', 'y', 555.0),
        ]
        self.node.update_weights('group1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 555.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_update_different_order(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        data = [
            ('B', 'index_id', 'A', 'group1'),
            ('y', 2, 'bar', 555.0),
        ]
        self.node.update_weights('group1', data)

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 555.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_update_different_order_add_new(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)

        # Check that `is_complete` status is False.
        group = self.node.get_weight_group('group1')
        self.assertFalse(group.is_complete)

        # Upate weights and check that warning is raised.
        data = [
            ('B', 'index_id', 'A', 'group1'),
            ('x', 1, 'foo', 111.0),
            ('y', 2, 'bar', 222.0),
            ('z', 3, 'bar', 333.0),  # <- Does not previously exist.
        ]

        self.node.update_weights('group1', data)

        # Check the logged messages.
        self.assertEqual(
            self.log_stream.getvalue(),
            ("INFO: updated 2 existing records in 'group1'\n"
             "WARNING: loaded 1 new records, weight group is complete\n"),
        )

        # Check updated values.
        expected = [
            (1, 1, 1, 111.0),  # <- Updated.
            (2, 1, 2, 222.0),  # <- Updated.
            (3, 1, 3, 333.0),  # <- Inserted (new record).
        ]
        self.assertEqual(self.get_weights_helper(), expected)

        # Check that `is_complete` status is now True.
        group = self.node.get_weight_group('group1')
        self.assertTrue(group.is_complete)

    def test_update_using_value_column(self):
        """Test *value_column* when `data` has no column matching weight_group."""
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        data = [
            ('index_id', 'A', 'B','weight'),
            (2, 'bar', 'y', 555.0),
        ]
        self.node.update_weights('group1', data, value_column='weight')

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 555.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_update_using_value_column_with_matching(self):
        """Test *value_column* even if matching weight_group exists."""
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        data = [
            ('index_id', 'A', 'B', 'group1', 'group1fixed'),
            (2, 'bar', 'y', 25.0, 555.0),
        ]
        self.node.update_weights('group1', data, value_column='group1fixed')

        expected = [(1, 1, 1, 10.0), (2, 1, 2, 555.0), (3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

    def test_update_missing_and_mismatched(self):
        with self.node._managed_cursor() as cursor:
            weight_repo = self.node._dal.WeightRepository(cursor)
            weight_repo.add(1, 1, 10.0)
            weight_repo.add(1, 2, 25.0)
            weight_repo.add(1, 3, 15.0)

        data = [
            ('index_id', 'A', 'B','group1'),
            (2, 'bar', 'YYY', 444.0),  # <- Mismatch.
            (9, 'bar', 'z', 555.0),    # <- No index_id 9.
        ]

        self.node.update_weights('group1', data)

        # Check the logged messages.
        self.assertEqual(
            self.log_stream.getvalue(),
            ("INFO: updated 0 existing records in 'group1'\n"
             "WARNING: skipped 1 rows with no matching index_id\n"
             "WARNING: skipped 1 rows whose labels do not match the "
               "given index_id\n"),
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
            group = group_repo.get_by_name('group1')
            group.is_complete = True
            group_repo.update(group)

        data = [
            ('index_id', 'A', 'B'),
            (1, 'foo', 'x'),
            (2, 'bar', 'y'),
        ]
        self.node.delete_weights('group1', data)
        expected = [(3, 1, 3, 15.0)]
        self.assertEqual(self.get_weights_helper(), expected)

        # Check that `is_complete` was changed to False.
        group = self.node.get_weight_group('group1')
        self.assertFalse(group.is_complete)

        # Test with weight column (can be present but is ignored).
        data = [
            ('index_id', 'A', 'B', 'group1'),
            (3, 'bar', 'z', 15.0),
        ]
        self.node.delete_weights('group1', data)
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

        self.node.delete_weights('group1', data)

        # Check the applogger messages.
        self.assertEqual(
            self.log_stream.getvalue(),
            ("INFO: deleted 0 weights from 'group1'\n"
             "WARNING: skipped 1 rows with mismatched labels\n"
             "WARNING: skipped 1 rows with no matching index_id\n"
             "WARNING: skipped 1 rows with no matching weight record\n"),
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
        self.node.delete_weights('group1', A='bar')
        expected = [(1, 1, 1, 10.0)]
        self.assertEqual(self.get_weights_helper(), expected)

        # Test multiple criteria (matches 1 row).
        self.node.delete_weights('group1', A='foo', B='x')
        self.assertEqual(self.get_weights_helper(), [])


class TestTopoNodeCrosswalkMethods(unittest.TestCase):
    def setUp(self):
        # Set up stream object to capture log messages.
        self.log_stream = StringIO()
        self.addCleanup(self.log_stream.close)

        # Add handler to 'app-toron' logger.
        applogger = logging.getLogger('app-toron')
        handler = logging.StreamHandler(self.log_stream)
        handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        applogger.addHandler(handler)
        self.addCleanup(lambda: applogger.removeHandler(handler))

        node = TopoNode()
        with node._managed_cursor() as cursor:
            col_manager = node._dal.ColumnManager(cursor)
            index_repo = node._dal.IndexRepository(cursor)

            # Add index columns and records.
            col_manager.add_columns('A', 'B')
            index_repo.add('foo', 'x')
            index_repo.add('bar', 'y')
            index_repo.add('bar', 'z')

        self.node = node

    def clear_log_stream_helper(self):  # <- Helper function.
        self.log_stream.seek(0)
        self.log_stream.truncate()

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

        result = self.node.get_crosswalk('111-111-2222')
        self.assertEqual(
            self.log_stream.getvalue(),
            "WARNING: found multiple crosswalks, using default: 'name1'\n",
            msg='should warn if there are multiple matches',
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

        self.clear_log_stream_helper()  # Clear log before calling get_crosswalk().
        result = self.node.get_crosswalk('333-333-3333', 'unknown_name')
        self.assertIsNone(result, msg='if specified name does not exist, should be None')
        self.assertEqual(
            self.log_stream.getvalue(),
            "WARNING: crosswalk 'unknown_name' not found, can be: 'name1', 'name2'\n",
        )

        result = self.node.get_crosswalk('000-unknown-0000')
        self.assertIsNone(result, msg='if specified node does not exist, should be None')

    def test_add_crosswalk(self):
        mock_other_node = unittest.mock.Mock()
        mock_other_node.unique_id = '111-111-1111'

        node = TopoNode()

        node.add_crosswalk(mock_other_node, 'name1')  # <- Only required args (sets as default and logs warning).

        self.assertEqual(
            self.log_stream.getvalue(),
            "WARNING: setting default crosswalk: 'name1'\n",
        )

        node.add_crosswalk(  # <- Defining all properties.
                node=mock_other_node,
                crosswalk_name='name2',
                other_filename_hint='mocked_file',
                description='The second crosswalk.',
                selectors=['"[foo]"'],
                is_default=True,  # <- Becomes new default, replacing 'name1'
                user_properties={'qux': 'abc', 'quux': 123},
                other_index_hash='12437810',
                is_locally_complete=False,
        )

        self.assertEqual(
            self.get_crosswalk_helper(node),
            [Crosswalk(id=1,
                       other_unique_id='111-111-1111',
                       other_filename_hint=None,
                       name='name1',
                       description=None,
                       selectors=None,
                       is_default=False,
                       user_properties=None,
                       other_index_hash=None,
                       is_locally_complete=False),
             Crosswalk(id=2,
                       other_unique_id='111-111-1111',
                       other_filename_hint='mocked_file',
                       name='name2',
                       description='The second crosswalk.',
                       selectors=['"[foo]"'],
                       is_default=True,
                       user_properties={'quux': 123, 'qux': 'abc'},
                       other_index_hash='12437810',
                       is_locally_complete=False)]
        )

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


class TestTopoNodeInsertRelations2(unittest.TestCase):
    def setUp(self):
        # Set up stream object to capture log messages.
        self.log_stream = StringIO()
        self.addCleanup(self.log_stream.close)

        # Add handler to 'app-toron' logger.
        applogger = logging.getLogger('app-toron')
        handler = logging.StreamHandler(self.log_stream)
        handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        applogger.addHandler(handler)
        self.addCleanup(lambda: applogger.removeHandler(handler))

        # Build TopoNode fixture to use in test cases.
        node = TopoNode()
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

        node.add_discrete_categories({'A', 'B'}, {'A'})
        self.node = node

    def get_relations_helper(self):  # <- Helper function.
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)
            relation_repo = self.node._dal.RelationRepository(cursor)

            func = lambda x: relation_repo.find(crosswalk_id=x)
            relation_iters = (func(x.id) for x in crosswalk_repo.get_all())
            return list(chain.from_iterable(relation_iters))

    def test_insert(self):
        data = [
            ('other_index_id', 'index_id', 'mapping_level', 'rel1'),
            (0, 0, None,     0.0),
            (1, 1, b'\xc0', 10.0),
            (2, 2, b'\xc0', 20.0),
            (3, 2, b'\xc0',  5.0),
            (3, 3, b'\xc0', 15.0),
        ]
        self.node.insert_relations2('myfile', 'rel1', data)

        self.assertEqual(
            self.get_relations_helper(),
            [
                Relation(1, 1, 0, 0, mapping_level=None,    value=0.0,  proportion=1.00),
                Relation(2, 1, 1, 1, mapping_level=b'\xc0', value=10.0, proportion=1.00),
                Relation(3, 1, 2, 2, mapping_level=b'\xc0', value=20.0, proportion=1.00),
                Relation(4, 1, 3, 2, mapping_level=b'\xc0', value=5.0,  proportion=0.25),
                Relation(5, 1, 3, 3, mapping_level=b'\xc0', value=15.0, proportion=0.75),
            ],
        )

    def test_string_input(self):
        """When data is given as strings they should be automatically
        converted to the appropriate numeric type:

            * other_index_id: converted to `int`
            * index_id: converted to `int`
            * value column (e.g. 'rel1'): converted to `float`

        If 'mapping_level' is given, it should be ``bytes``.

        For the DAL1 backend, SQLite casts text characters as numeric
        types based on each columns' "Type Affinity":

            https://www.sqlite.org/datatype3.html#type_affinity
        """
        data = [
            ('other_index_id', 'index_id', 'mapping_level', 'rel1'),
            ('0', '0', None,     '0.0'),
            ('1', '1', b'\xc0', '10.0'),
            ('2', '2', b'\xc0', '20.0'),
            ('3', '2', b'\xc0',  '5.0'),
            ('3', '3', b'\xc0', '15.0'),
        ]
        self.node.insert_relations2('myfile', 'rel1', data)

        self.assertEqual(
            self.get_relations_helper(),
            [
                Relation(1, 1, 0, 0, mapping_level=None,    value=0.0,  proportion=1.00),
                Relation(2, 1, 1, 1, mapping_level=b'\xc0', value=10.0, proportion=1.00),
                Relation(3, 1, 2, 2, mapping_level=b'\xc0', value=20.0, proportion=1.00),
                Relation(4, 1, 3, 2, mapping_level=b'\xc0', value=5.0,  proportion=0.25),
                Relation(5, 1, 3, 3, mapping_level=b'\xc0', value=15.0, proportion=0.75),
            ],
        )

    def test_automatic_undefined_record(self):
        """If not given, the unmapped-to-unmapped relation should be
        added automatically.
        """
        data = [
            ('other_index_id', 'index_id', 'mapping_level', 'rel1'),
            (1, 1, b'\xc0', 10.0),
            (2, 2, b'\xc0', 20.0),
            (3, 2, b'\xc0',  5.0),
            (3, 3, b'\xc0', 15.0),
        ]
        self.node.insert_relations2('myfile', 'rel1', data)

        self.assertEqual(
            self.get_relations_helper(),
            [
                Relation(5, 1, 0, 0, mapping_level=None,    value=0.0,  proportion=1.00),  # <- Auto-added.
                Relation(1, 1, 1, 1, mapping_level=b'\xc0', value=10.0, proportion=1.00),
                Relation(2, 1, 2, 2, mapping_level=b'\xc0', value=20.0, proportion=1.00),
                Relation(3, 1, 3, 2, mapping_level=b'\xc0', value=5.0,  proportion=0.25),
                Relation(4, 1, 3, 3, mapping_level=b'\xc0', value=15.0, proportion=0.75),
            ],
        )

    def test_ignore_proportion_in_data(self):
        """If 'proportion' is given as one of the columns in *data*,
        it's treated as an extra column and is ignored. This is done
        because other relations may already be present in the node that
        would affect the final proportion. So the proportion values are
        automatically recalculated after records are inserted.
        """
        data = [
            ('other_index_id', 'index_id', 'mapping_level', 'rel1', 'proportion'),
            (3, 2, b'\xc0',  5.0, 0.375),
            (3, 3, b'\xc0', 15.0, 0.625),
        ]
        self.node.insert_relations2('myfile', 'rel1', data)

        self.assertEqual(
            self.get_relations_helper(),
            [
                Relation(3, 1, 0, 0, mapping_level=None,    value=0.0,  proportion=1.00),  # <- Auto-added.
                Relation(1, 1, 3, 2, mapping_level=b'\xc0', value=5.0,  proportion=0.25),
                Relation(2, 1, 3, 3, mapping_level=b'\xc0', value=15.0, proportion=0.75),
            ],
            msg='should ignore proportion from data and calculate it using values'
        )

    def test_skip_bad_mapping_level(self):
        """Records with bad mapping levels should be logged and skipped.

        Byte and bit-flag equivalence:

            +---------+-----------+
            | bytes   | bit flags |
            +=========+===========+
            | b'\xc0' | 1, 1      |
            +---------+-----------+
            | b'\x80' | 1, 0      |
            +---------+-----------+
            | b'\x40' | 0, 1      |
            +---------+-----------+
        """
        data = [
            ('other_index_id', 'index_id', 'mapping_level', 'rel1'),
            (0, 0, b'\xc0',  0.0),
            (1, 1, b'\x40', 10.0),  # <- Bad mapping level, should be omitted.
            (2, 2, b'\x40', 20.0),  # <- Bad mapping level, should be omitted.
            (3, 2, b'\x80',  5.0),
            (3, 3, b'\x80', 15.0),
        ]
        self.node.insert_relations2('myfile', 'rel1', data)

        self.assertEqual(
            self.log_stream.getvalue(),
            ('INFO: loaded 3 relations\n'
             'WARNING: skipped 2 relations with invalid mapping levels\n'),
        )

        self.assertEqual(
            self.get_relations_helper(),
            [
                Relation(1, 1, 0, 0, mapping_level=b'\xc0', value=0.0,  proportion=1.00),
                Relation(2, 1, 3, 2, mapping_level=b'\x80', value=5.0,  proportion=0.25),
                Relation(3, 1, 3, 3, mapping_level=b'\x80', value=15.0, proportion=0.75),
            ],
        )

    def test_insert_is_complete_status_and_hash(self):
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)

            data = [
                ('other_index_id', 'index_id', 'mapping_level', 'rel1'),
                (0, 0, b'\xc0',  0.0),
                (1, 1, b'\xc0', 10.0),
                (2, 2, b'\xc0', 20.0),
                (3, 2, b'\xc0',  5.0),
                # No record matching to index_id 3.
            ]
            self.node.insert_relations2('myfile', 'rel1', data)

            crosswalk = crosswalk_repo.get(1)
            self.assertFalse(crosswalk.is_locally_complete)
            self.assertEqual(
                crosswalk.other_index_hash,
                'c4c96cd71102046c61ec8326b2566d9e48ef2ba26d4252ba84db28ba352a0079',
                msg='hash for other_index_ids 0, 1, 2, and 3',
            )

            data = [
                ('other_index_id', 'index_id', 'mapping_level', 'rel1'),
                (4, 3, b'\xc0', 15.0),  # index_id 3 completes the crosswalk
            ]
            self.node.insert_relations2('myfile', 'rel1', data)

            crosswalk = crosswalk_repo.get(1)  # re-fetch the crosswalk
            self.assertTrue(crosswalk.is_locally_complete)
            self.assertEqual(
                crosswalk.other_index_hash,
                'ed545f6c1652e1c90b517e9f653bafc0cf0f7214fb2dd58e3864c1522b089982',
                msg='hash for other_index_ids 0, 1, 2, 3, and 4',
            )


class TestTopoNodeRelationMethods(unittest.TestCase):
    def setUp(self):
        node = TopoNode()
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
            # mapping_level b'\xc0' corresponds to BitFlags(1, 1).
            relation_repo = self.node._dal.RelationRepository(cursor)
            relation_repo.add(1, other_index_id=0, index_id=0, mapping_level=None,    value=0.0)
            relation_repo.add(1, other_index_id=1, index_id=1, mapping_level=b'\xc0', value=10.0)
            relation_repo.add(1, other_index_id=2, index_id=2, mapping_level=b'\xc0', value=20.0)
            relation_repo.add(1, other_index_id=3, index_id=2, mapping_level=b'\xc0', value=5.0)
            relation_repo.add(1, other_index_id=3, index_id=3, mapping_level=b'\xc0', value=15.0)

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

            relation_repo.add(1, other_index_id=0, index_id=0, mapping_level=None,    value=0.0)
            relation_repo.add(1, other_index_id=1, index_id=1, mapping_level=b'\xc0', value=10.0)
            relation_repo.add(1, other_index_id=2, index_id=2, mapping_level=b'\xc0', value=20.0)
            relation_repo.add(1, other_index_id=3, index_id=2, mapping_level=b'\x80', value=5.0)
            relation_repo.add(1, other_index_id=3, index_id=3, mapping_level=b'\x80', value=15.0)

            # Adding another column makes existing relations ambiguous
            # because they were mapped without knowledge of the new column.
            col_manager.add_columns('C')

        relations = self.node.select_relations('myfile', 'rel1', header=True)
        expected = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B', 'C', 'ambiguous_fields'),
            (0,  0.0, 0, '-',   '-', '-', None),
            (1, 10.0, 1, 'foo', 'x', '-', 'C'),
            (2, 20.0, 2, 'bar', 'y', '-', 'C'),
            (3,  5.0, 2, 'bar', 'y', '-', 'B, C'),
            (3, 15.0, 3, 'bar', 'z', '-', 'B, C'),
        ]
        self.assertEqual(list(relations), expected)

    def test_select_with_missing_relations(self):
        """Should return unmapped records with ``None`` vals for origin."""

        # Add relations for index_id values 0 and 1, but not for 2 or 3.
        with self.node._managed_cursor() as cursor:
            relation_repo = self.node._dal.RelationRepository(cursor)
            relation_repo.add(1, other_index_id=0, index_id=0, mapping_level=None,    value=0.0)
            relation_repo.add(1, other_index_id=1, index_id=1, mapping_level=b'\xc0', value=10.0)

        relations = self.node.select_relations('myfile', 'rel1', header=True)
        expected = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B', 'ambiguous_fields'),
            (0,     0.0, 0, '-',   '-', None),
            (1,    10.0, 1, 'foo', 'x', None),
            (None, None, 2, 'bar', 'y', None),  # <- Left-side not mapped.
            (None, None, 3, 'bar', 'z', None),  # <- Left-side not mapped.
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
            (1, 1, 0, 0, None,  0.0, 1.00),
            (2, 1, 1, 1, None, 10.0, 1.00),
            (3, 1, 2, 2, None, 20.0, 1.00),
            (4, 1, 3, 2, None,  5.0, 0.25),
            (5, 1, 3, 3, None, 15.0, 0.75),
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
            ('other_index_id', 'rel1', 'index_id', 'A', 'B', 'mapping_level', 'proportion'),
            ('1', '10.0', '1', 'foo', 'x', None,    0.50),
            ('2', '20.0', '2', 'bar', 'y', None,    0.50),
            ('3',  '5.0', '2', 'bar', 'y', b'\x80', None),
            ('3', '15.0', '3', 'bar', 'z', b'\x80', None),
        ]
        self.node.insert_relations('myfile', 'rel1', data)

        expected = [
            (1, 1, 1, 1, None,    10.0, 1.0),
            (2, 1, 2, 2, None,    20.0, 1.0),
            (3, 1, 3, 2, b'\x80',  5.0, 0.25),
            (4, 1, 3, 3, b'\x80', 15.0, 0.75),
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

        expected = [(1, 1, 1, 1, None, 10.0, 1.0)]  # <- Proportion should be 1.0 (auto-calculated).
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
            (1, 1, 2, 2, b'\x80', 20.0, 1.00),
            (2, 1, 3, 2, b'\x80',  5.0, 0.25),
            (3, 1, 3, 3, None,    15.0, 0.75),
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
            (1, 1, 0, 0, None,  0.0, 1.0),
            (2, 1, 1, 1, None, 10.0, 1.0),
            (3, 1, 2, 2, None, 20.0, 1.0),
            (4, 1, 3, 2, None,  5.0, 0.25),
            (5, 1, 3, 3, None, 15.0, 0.75),
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


class TestTopoNodeUpdateRelations(unittest.TestCase):
    def get_relations_helper(self):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with self.node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM relation')
            return cursor.fetchall()

    def setUp(self):
        node = TopoNode()
        with node._managed_cursor() as cursor:
            col_manager = node._dal.ColumnManager(cursor)
            index_repo = node._dal.IndexRepository(cursor)
            crosswalk_repo = node._dal.CrosswalkRepository(cursor)
            relation_repo = node._dal.RelationRepository(cursor)

            # Add index columns and records.
            col_manager.add_columns('A', 'B')
            index_repo.add('foo', 'x')
            index_repo.add('bar', 'y')
            index_repo.add('bar', 'z')

            # Add crosswalk and relations.
            crosswalk_repo.add('111-111-1111', 'myfile.toron', 'rel1',
                other_index_hash='c4c96cd71102046c61ec8326b2566d9e48ef2ba26d4252ba84db28ba352a0079')  # crosswalk_id 1
            relation_repo.add(1, 0, 0, None,  0.0, 1.00)  # relation_id 1 (-, -)
            relation_repo.add(1, 1, 1, None, 10.0, 1.00)  # relation_id 2 (foo, x)
            relation_repo.add(1, 2, 2, None, 20.0, 1.00)  # relation_id 3 (bar, y)
            relation_repo.add(1, 3, 3, None, 15.0, 1.00)  # relation_id 4 (bar, z)

        self.node = node

    def test_update(self):
        data = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B'),
            (2, 60.0, 2, 'bar', 'y'),
        ]
        self.node.update_relations('myfile', 'rel1', data)

        expected = [
            (1, 1, 0, 0, None,  0.0, 1.00),
            (2, 1, 1, 1, None, 10.0, 1.00),
            (3, 1, 2, 2, None, 60.0, 1.00),  # <- Updated from 20 to 60.
            (4, 1, 3, 3, None, 15.0, 1.00),
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_update_normalization(self):
        data = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B'),
            ('2', '60.0', '2', 'bar', 'y'),  # <- All values given as strings.
        ]
        self.node.update_relations('myfile', 'rel1', data)

        expected = [
            (1, 1, 0, 0, None,  0.0, 1.00),
            (2, 1, 1, 1, None, 10.0, 1.00),
            (3, 1, 2, 2, None, 60.0, 1.00),  # <- Updated from 20 to 60.
            (4, 1, 3, 3, None, 15.0, 1.00),
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_update_non_existant_record(self):
        data = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B'),
            (3,  10.0,  3, 'bar', 'z'),
            (3,  6.0,   2, 'bar', 'y'),
        ]
        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.update_relations('myfile', 'rel1', data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            'inserted 1 rows that did not previously exist, updated 1 rows',
        )

        # Verify final records.
        expected = [
            (1, 1, 0, 0, None,  0.0, 1.0),
            (2, 1, 1, 1, None, 10.0, 1.0),
            (3, 1, 2, 2, None, 20.0, 1.0),
            (4, 1, 3, 3, None, 10.0, 0.625),  # <- Weight updated from 15 to 10, proportion recalculated.
            (5, 1, 3, 2, None,  6.0, 0.375),  # <- Non-existant record inserted, proportion added.
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_update_proportion_ignored(self):
        """If 'proportion' is given as one of the columns in *data*,
        it's treated as an extra column and is ignored. The proportion
        values are automatically calculated after records are inserted.
        """
        data = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B', 'proportion'),
            (2, 60.0, 2, 'bar', 'y', 0.75),  # <- Proportion (0.75) gets ignored.
        ]
        self.node.update_relations('myfile', 'rel1', data)

        expected = [
            (1, 1, 0, 0, None,  0.0, 1.00),
            (2, 1, 1, 1, None, 10.0, 1.00),
            (3, 1, 2, 2, None, 60.0, 1.00),  # <- Proportion auto-calculated (1.0), value updated from 20 to 60.
            (4, 1, 3, 3, None, 15.0, 1.00),
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_update_skip_bad_mapping_level(self):
        with self.node._managed_cursor() as cursor:
            structure_repo = self.node._dal.StructureRepository(cursor)
            structure_repo.add(None,      0, 0)
            structure_repo.add(0.9140625, 1, 0)
            structure_repo.add(1.5859375, 1, 1)

        data = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B', 'mapping_level'),
            (1, 10.0, 1, 'foo', 'x', b'\x40'),  # <- `\x40` is bad mapping level `(0, 1)`
            (3, 15.0, 3, 'bar', 'z', b'\x80'),
            (3, 5.0,  2, 'bar', 'y', b'\x80'),
        ]
        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.update_relations('myfile', 'rel1', data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            ('skipped 1 rows with invalid mapping levels, inserted '
             '1 rows that did not previously exist, updated 1 rows'),
        )

        # Verify final records.
        expected = [
            (1, 1, 0, 0, None,     0.0, 1.0),
            (2, 1, 1, 1, None,    10.0, 1.0),
            (3, 1, 2, 2, None,    20.0, 1.0),
            (4, 1, 3, 3, b'\x80', 15.0, 0.75),  # <- Mapping level updated.
            (5, 1, 3, 2, b'\x80',  5.0, 0.25),  # <- Mapping level updated.
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_update_different_order_and_extra(self):
        """Label columns in different order and extra column."""
        data = [
            ('other_index_id', 'rel1', 'index_id', 'B', 'EXTRACOL', 'A'),
            (1, 99.0, 1, 'x', 'EXTRA', 'foo'),
            (2, 99.0, 2, 'y', 'EXTRA', 'bar'),
        ]
        self.node.update_relations('myfile', 'rel1', data)

        expected = [
            (1, 1, 0, 0, None,  0.0, 1.0),
            (2, 1, 1, 1, None, 99.0, 1.0),  # <- Updated from 10 to 99.
            (3, 1, 2, 2, None, 99.0, 1.0),  # <- Updated from 20 to 99.
            (4, 1, 3, 3, None, 15.0, 1.0),
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_update_invalid_columns(self):
        data = [
            ('other_index_id', 'rel1', 'BAD_VALUE', 'A', 'B'),
            (2, 60.0, 2, 'bar', 'y'),
        ]
        regex = r"columns should be start with \('other_index_id', 'rel1', 'index_id', ...\)"
        with self.assertRaisesRegex(ValueError, regex):
            self.node.update_relations('myfile', 'rel1', data)

        data = [
            ('other_index_id', 'rel1', 'index_id', 'A'),
            (2, 60.0, 2, 'bar'),
        ]
        regex = r"missing required columns: 'B'"
        with self.assertRaisesRegex(ValueError, regex):
            self.node.update_relations('myfile', 'rel1', data)

    def test_update_is_complete_status_and_hash(self):
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)

            # Check initial status.
            crosswalk = crosswalk_repo.get(1)
            self.assertEqual(
                crosswalk.other_index_hash,
                'c4c96cd71102046c61ec8326b2566d9e48ef2ba26d4252ba84db28ba352a0079',
                msg='hash for other_index_ids 0, 1, 2, and 3',
            )

            # Perform update that inserts previously non-existant record.
            data = [
                ('other_index_id', 'rel1', 'index_id', 'A', 'B'),
                (4,  5.0,   2, 'bar', 'y'),
            ]
            with self.assertWarns(ToronWarning) as cm:
                self.node.update_relations('myfile', 'rel1', data)

            # Check updated status status.
            crosswalk = crosswalk_repo.get(1)
            self.assertTrue(crosswalk.is_locally_complete)
            self.assertEqual(
                crosswalk.other_index_hash,
                'ed545f6c1652e1c90b517e9f653bafc0cf0f7214fb2dd58e3864c1522b089982',
                msg='hash for other_index_ids 0, 1, 2, 3, and 4',
            )


class TestTopoNodeDeleteRelations(unittest.TestCase):
    def get_relations_helper(self):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        with self.node._managed_cursor() as cursor:
            cursor.execute('SELECT * FROM relation')
            return cursor.fetchall()

    def setUp(self):
        node = TopoNode()
        with node._managed_cursor() as cursor:
            col_manager = node._dal.ColumnManager(cursor)
            index_repo = node._dal.IndexRepository(cursor)
            crosswalk_repo = node._dal.CrosswalkRepository(cursor)
            relation_repo = node._dal.RelationRepository(cursor)

            # Add index columns and records.
            col_manager.add_columns('A', 'B')
            index_repo.add('foo', 'x')
            index_repo.add('bar', 'y')
            index_repo.add('bar', 'z')

            # Add crosswalk (crosswalk_id 1) and relations.
            crosswalk_repo.add(
                other_unique_id='111-111-1111',
                other_filename_hint='myfile.toron',
                name='rel1',
                other_index_hash='c4c96cd71102046c61ec8326b2566d9e48ef2ba26d4252ba84db28ba352a0079',
                is_locally_complete=True
            )
            relation_repo.add(1, 0, 0, None,  0.0, 1.00)  # relation_id 1 (-, -)
            relation_repo.add(1, 1, 1, None, 10.0, 1.00)  # relation_id 2 (foo, x)
            relation_repo.add(1, 2, 2, None, 20.0, 1.00)  # relation_id 3 (bar, y)
            relation_repo.add(1, 3, 3, None, 15.0, 1.00)  # relation_id 4 (bar, z)

        self.node = node

    def test_delete(self):
        data = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B'),
            (2, 20.0, 2, 'bar', 'y'),  # <- Matches relation_id 3.
        ]
        self.node.delete_relations('myfile', 'rel1', data)

        expected = [
            (1, 1, 0, 0, None,  0.0, 1.00),
            (2, 1, 1, 1, None, 10.0, 1.00),
            # Record with relation_id 3 is deleted.
            (4, 1, 3, 3, None, 15.0, 1.00),
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_delete_normalization(self):
        data = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B'),
            ('2', '20', '2', 'bar', 'y'),  # <- All values given as strings.
        ]
        self.node.delete_relations('myfile', 'rel1', data)

        expected = [
            (1, 1, 0, 0, None,  0.0, 1.00),
            (2, 1, 1, 1, None, 10.0, 1.00),
            # Record with relation_id 3 is deleted.
            (4, 1, 3, 3, None, 15.0, 1.00),
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_delete_non_existant_record(self):
        data = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B'),
            (9, 20.0, 2, 'bar', 'y'),  # <- No match (other_index_id 9 not present).
            (2, 20.0, 2, 'bar', 'y'),  # <- Matches relation_id 3.
        ]
        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.delete_relations('myfile', 'rel1', data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            'skipped 1 rows with no matching relations, deleted 1 rows',
        )

        # Verify final records.
        expected = [
            (1, 1, 0, 0, None,  0.0, 1.00),
            (2, 1, 1, 1, None, 10.0, 1.00),
            # Record with relation_id 3 is deleted.
            (4, 1, 3, 3, None, 15.0, 1.00),
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_delete_proportion_ignored(self):
        """If 'proportion' is given as one of the columns in *data*,
        it's treated as an extra column and is ignored. The proportion
        values of the remaining records are automatically calculated
        after records are deleted.
        """
        data = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B', 'proportion'),
            (2, 20.0, 2, 'bar', 'y', 0.75),  # <- Proportion (0.75) gets ignored.
        ]
        self.node.delete_relations('myfile', 'rel1', data)

        expected = [
            (1, 1, 0, 0, None,  0.0, 1.00),
            (2, 1, 1, 1, None, 10.0, 1.00),
            # Record with relation_id 3 is deleted.
            (4, 1, 3, 3, None, 15.0, 1.00),
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_delete_with_mapping_level(self):
        with self.node._managed_cursor() as cursor:
            structure_repo = self.node._dal.StructureRepository(cursor)
            structure_repo.add(None,      0, 0)
            structure_repo.add(0.9140625, 1, 0)
            structure_repo.add(1.5859375, 1, 1)

            relation_repo = self.node._dal.RelationRepository(cursor)
            relation_repo.add(1, 1, 2, b'\x80', 30.0, 1.00)  # relation_id 3 (bar, y)
            relation_repo.add(1, 1, 3, b'\x80', 10.0, 1.00)  # relation_id 4 (bar, z)

        data = [
            ('other_index_id', 'rel1', 'index_id', 'A', 'B'),
            (2, 20.0, 2, 'bar', 'y'),  # <- Deletes
            (1, 30.0, 2, 'bar', 'y'),  # <- Skips (matches approximate rel)
            (1, 10.0, 3, 'bar', 'z'),  # <- Skips (matches approximate rel)
        ]
        # Check that a warning is raised.
        with self.assertWarns(ToronWarning) as cm:
            self.node.delete_relations('myfile', 'rel1', data)

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            'skipped 2 approximate relations (reify to delete), deleted 1 rows',
        )

        # Verify final records.
        expected = [
            (1, 1, 0, 0, None,     0.0, 1.0),
            (2, 1, 1, 1, None,    10.0, 0.2),
            # relation_id 3 is deleted (not approximate)
            (4, 1, 3, 3, None,    15.0, 1.0),
            (5, 1, 1, 2, b'\x80', 30.0, 0.6),  # <- Not removed (approximate rel)
            (6, 1, 1, 3, b'\x80', 10.0, 0.2),  # <- Not removed (approximate rel)
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_delete_different_order_and_extra(self):
        """Label columns in different order and extra column."""
        data = [
            ('other_index_id', 'rel1', 'index_id', 'B', 'EXTRACOL', 'A'),
            (2, 20.0, 2, 'y', 'EXTRA', 'bar'),  # <- Matches index_id 3.
        ]
        self.node.delete_relations('myfile', 'rel1', data)

        expected = [
            (1, 1, 0, 0, None,  0.0, 1.00),
            (2, 1, 1, 1, None, 10.0, 1.00),
            # Record with relation_id 3 is deleted.
            (4, 1, 3, 3, None, 15.0, 1.00),
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_delete_invalid_columns(self):
        data = [
            ('other_index_id', 'rel1', 'BAD_VALUE', 'A', 'B'),
            (2, 20.0, 2, 'bar', 'y'),
        ]
        regex = r"columns should be start with \('other_index_id', 'rel1', 'index_id', ...\)"
        with self.assertRaisesRegex(ValueError, regex):
            self.node.delete_relations('myfile', 'rel1', data)

        data = [
            ('other_index_id', 'rel1', 'index_id', 'A'),
            (2, 20.0, 2, 'bar'),
        ]
        regex = r"missing required columns: 'B'"
        with self.assertRaisesRegex(ValueError, regex):
            self.node.delete_relations('myfile', 'rel1', data)

        # Check that data is not changed.
        expected = [
            (1, 1, 0, 0, None,  0.0, 1.00),
            (2, 1, 1, 1, None, 10.0, 1.00),
            (3, 1, 2, 2, None, 20.0, 1.00),  # <- Not removed.
            (4, 1, 3, 3, None, 15.0, 1.00),
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_delete_is_complete_status_and_hash(self):
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)

            # Check initial status.
            crosswalk = crosswalk_repo.get(1)
            self.assertTrue(crosswalk.is_locally_complete)
            self.assertEqual(
                crosswalk.other_index_hash,
                'c4c96cd71102046c61ec8326b2566d9e48ef2ba26d4252ba84db28ba352a0079',
                msg='hash for other_index_ids 0, 1, 2, and 3',
            )

            data = [
                ('other_index_id', 'rel1', 'index_id', 'A', 'B'),
                (2, 20.0, 2, 'bar', 'y'),  # <- Matches relation_id 3 (other_index_id 2)
            ]
            self.node.delete_relations('myfile', 'rel1', data)

            # Check updated status status.
            crosswalk = crosswalk_repo.get(1)
            self.assertFalse(crosswalk.is_locally_complete)
            self.assertEqual(
                crosswalk.other_index_hash,
                'a07d14c1929fe9ef2d5276645e7133d165e0e7b7065ae9f33bd0718f593d774f',
                msg='hash for other_index_ids 0, 1, and 3',
            )

    def test_delete_criteria_single(self):
        self.node.delete_relations('myfile', 'rel1', A='bar')

        expected = [
            (1, 1, 0, 0, None,  0.0, 1.0),  # relation_id 1 (-, -)
            (2, 1, 1, 1, None, 10.0, 1.0),  # relation_id 2 (foo, x)
            # relation_id 3 (bar, y) should be deleted
            # relation_id 4 (bar, z) should be deleted
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_delete_criteria_multiple(self):
        self.node.delete_relations('myfile', 'rel1', A='bar', B='y')

        expected = [
            (1, 1, 0, 0, None,  0.0, 1.0),  # relation_id 1 (-, -)
            (2, 1, 1, 1, None, 10.0, 1.0),  # relation_id 2 (foo, x)
            # relation_id 3 (bar, y) should be deleted
            (4, 1, 3, 3, None, 15.0, 1.0),  # relation_id 4 (bar, z)
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_delete_criteria_mapping_levels(self):
        with self.node._managed_cursor() as cursor:
            structure_repo = self.node._dal.StructureRepository(cursor)
            structure_repo.add(None,      0, 0)
            structure_repo.add(0.9140625, 1, 0)
            structure_repo.add(1.5859375, 1, 1)

            relation_repo = self.node._dal.RelationRepository(cursor)
            relation_repo.add(1, 2, 1, b'\x80', 10.0, None)  # relation_id 5 (foo, x)
            relation_repo.add(1, 1, 2, b'\x80', 30.0, None)  # relation_id 6 (bar, y)
            relation_repo.add(1, 1, 3, b'\x80', 10.0, None)  # relation_id 7 (bar, z)

        # Since mapping levels for 6 and 7 use `(1, 0)`, we can delete using 'A'.
        self.node.delete_relations('myfile', 'rel1', A='foo')
        expected = [
            (1, 1, 0, 0, None,     0.0, 1.0),
            # Deleted relation_id 2 (foo, x)
            (3, 1, 2, 2, None,    20.0, 1.0),
            (4, 1, 3, 3, None,    15.0, 1.0),
            # Deleted relation_id 5 (foo, x)
            (6, 1, 1, 2, b'\x80', 30.0, 0.75),
            (7, 1, 1, 3, b'\x80', 10.0, 0.25),
        ]
        self.assertEqual(self.get_relations_helper(), expected)

        # Check deletion using criteria column not used in a mapping level.
        with self.assertWarns(ToronWarning) as cm:
            self.node.delete_relations('myfile', 'rel1', B='y')

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            'skipped 1 rows with mismatched mapping levels, deleted 1 rows',
        )

        expected = [
            (1, 1, 0, 0, None,     0.0, 1.0),
            # Deleted relation_id 3 (bar, y)
            (4, 1, 3, 3, None,    15.0, 1.0),
            (6, 1, 1, 2, b'\x80', 30.0, 0.75),  # <- Not deleted because of mapping level `(1, 0)` is not a subset of `(0, 1)`.
            (7, 1, 1, 3, b'\x80', 10.0, 0.25),
        ]
        self.assertEqual(self.get_relations_helper(), expected)


class TestTopoNodeRefiyRelations(unittest.TestCase):
    def setUp(self):
        node = TopoNode()
        with node._managed_cursor() as cursor:
            col_manager = node._dal.ColumnManager(cursor)
            index_repo = node._dal.IndexRepository(cursor)
            crosswalk_repo = node._dal.CrosswalkRepository(cursor)
            relation_repo = node._dal.RelationRepository(cursor)
            structure_repo = node._dal.StructureRepository(cursor)

            # Add index columns and records.
            col_manager.add_columns('A', 'B')
            index_repo.add('foo', 'x')
            index_repo.add('bar', 'y')
            index_repo.add('bar', 'z')

            # Add granularity and structure records.
            structure_repo.add(None,      0, 0)
            structure_repo.add(0.9140625, 1, 0)
            structure_repo.add(1.5859375, 0, 1)
            structure_repo.add(1.5859375, 1, 1)

            # Add crosswalk (crosswalk_id 1) and relations.
            crosswalk_repo.add(
                other_unique_id='111-111-1111',
                other_filename_hint='myfile.toron',
                name='rel1',
                other_index_hash='c4c96cd71102046c61ec8326b2566d9e48ef2ba26d4252ba84db28ba352a0079',
                is_locally_complete=True
            )

            # Bit flags and corresponding byte strings:
            #
            # | bit flags      | byte string |
            # | -------------- | ----------- |
            # | BitFlags(1, 1) | b'\xc0'     |
            # | BitFlags(1, 0) | b'\x80'     |
            # | BitFlags(0, 1) | b'\x40'     |

            relation_repo.add(1, 0, 0, None,                   0.0, 1.00)  # relation_id 1 (-, -)
            relation_repo.add(1, 1, 1, bytes(BitFlags(0, 1)), 10.0, 1.00)  # relation_id 2 (foo, x)
            relation_repo.add(1, 1, 2, bytes(BitFlags(0, 1)), 10.0, 1.00)  # relation_id 3 (bar, y)
            relation_repo.add(1, 2, 2, bytes(BitFlags(1, 1)), 20.0, 1.00)  # relation_id 4 (bar, y)
            relation_repo.add(1, 2, 3, bytes(BitFlags(1, 1)), 20.0, 1.00)  # relation_id 5 (bar, z)
            relation_repo.add(1, 3, 1, bytes(BitFlags(1, 0)), 15.0, 1.00)  # relation_id 6 (foo, x)
            relation_repo.add(1, 3, 2, bytes(BitFlags(1, 0)), 15.0, 1.00)  # relation_id 7 (bar, y)
            relation_repo.add(1, 3, 3, bytes(BitFlags(1, 0)), 15.0, 1.00)  # relation_id 8 (bar, z)

        self.node = node

    def get_relations_helper(self):
        """Helper function to return list of all relation records."""
        with self.node._managed_cursor() as cursor:
            crosswalk_repo = self.node._dal.CrosswalkRepository(cursor)
            relation_repo = self.node._dal.RelationRepository(cursor)
            crosswalks = crosswalk_repo.get_all()
            get_rels = lambda id: relation_repo.find(crosswalk_id=id)
            rels = (get_rels(crosswalk.id) for crosswalk in crosswalks)
            return list(chain.from_iterable(rels))

    def test_reify_all_records(self):
        self.node.reify_relations('myfile', 'rel1')
        expected = [
            Relation(1, 1, 0, 0, None,                   0.0, 1.0),
            Relation(2, 1, 1, 1, bytes(BitFlags(1, 1)), 10.0, 1.0),
            Relation(3, 1, 1, 2, bytes(BitFlags(1, 1)), 10.0, 1.0),
            Relation(4, 1, 2, 2, bytes(BitFlags(1, 1)), 20.0, 1.0),
            Relation(5, 1, 2, 3, bytes(BitFlags(1, 1)), 20.0, 1.0),
            Relation(6, 1, 3, 1, bytes(BitFlags(1, 1)), 15.0, 1.0),
            Relation(7, 1, 3, 2, bytes(BitFlags(1, 1)), 15.0, 1.0),
            Relation(8, 1, 3, 3, bytes(BitFlags(1, 1)), 15.0, 1.0),
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_reify_selected_records(self):
        self.node.reify_relations('myfile', 'rel1', A='foo', B='x')
        self.node.reify_relations('myfile', 'rel1', A='bar', B='y')

        expected = [
            Relation(1, 1, 0, 0, None,                   0.0, 1.0),
            Relation(2, 1, 1, 1, bytes(BitFlags(1, 1)), 10.0, 1.0),  # <- mapping_level changed (foo, x)
            Relation(3, 1, 1, 2, bytes(BitFlags(1, 1)), 10.0, 1.0),  # <- mapping_level changed (bar, y)
            Relation(4, 1, 2, 2, bytes(BitFlags(1, 1)), 20.0, 1.0),
            Relation(5, 1, 2, 3, bytes(BitFlags(1, 1)), 20.0, 1.0),
            Relation(6, 1, 3, 1, bytes(BitFlags(1, 1)), 15.0, 1.0),  # <- mapping_level changed (foo, x)
            Relation(7, 1, 3, 2, bytes(BitFlags(1, 1)), 15.0, 1.0),  # <- mapping_level changed (bar, y)
            Relation(8, 1, 3, 3, bytes(BitFlags(1, 0)), 15.0, 1.0),
        ]
        self.assertEqual(self.get_relations_helper(), expected)

    def test_reify_selected_records_with_warning(self):
        # Check deletion using criteria column not used in a mapping level.
        with self.assertWarns(ToronWarning) as cm:
            self.node.reify_relations('myfile', 'rel1', A='bar')

        # Check the warning's message.
        self.assertEqual(
            str(cm.warning),
            'skipped 1 rows with mismatched mapping levels, reified 2 records',
        )

        expected = [
            Relation(1, 1, 0, 0, None,                   0.0, 1.0),
            Relation(2, 1, 1, 1, bytes(BitFlags(0, 1)), 10.0, 1.0),
            Relation(3, 1, 1, 2, bytes(BitFlags(0, 1)), 10.0, 1.0),  # <- not changed* (see note below)
            Relation(4, 1, 2, 2, bytes(BitFlags(1, 1)), 20.0, 1.0),
            Relation(5, 1, 2, 3, bytes(BitFlags(1, 1)), 20.0, 1.0),
            Relation(6, 1, 3, 1, bytes(BitFlags(1, 0)), 15.0, 1.0),
            Relation(7, 1, 3, 2, bytes(BitFlags(1, 1)), 15.0, 1.0),  # <- mapping_level changed
            Relation(8, 1, 3, 3, bytes(BitFlags(1, 1)), 15.0, 1.0),  # <- mapping_level changed
        ]
        self.assertEqual(self.get_relations_helper(), expected)
        # * Note regarding relation 3: This relation maps a portion of
        #   other_index_id 1 to index_id 2. The labels associated with
        #   index_id 2 are `bar, y`. And even though `reify_relations()`
        #   is selecting records using A='bar' (which matches the first
        #   item associated with index_id 2), it is not altered because
        #   this relation has a mapping_level that corresponds to
        #   `(0, 1)`. This means that it was only matched by the `y`
        #   portion of its labels. This record's association with the
        #   label `bar` is probabilistic and selections should only
        #   match based on definitive associations.


class TestTopoNodeInsertQuantities(unittest.TestCase):
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
    def get_location_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.LocationRepository(cursor)
            return sorted(repository.find_all(), key=lambda x: x.id)

    @staticmethod
    def get_attributes_helper(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.AttributeGroupRepository(cursor)
            return sorted(repository.find_all(), key=lambda x: x.id)

    @staticmethod
    def get_quantities_helper(node):  # <- Helper function.
        with node._managed_cursor(n=2) as (cur1, cur2):
            location_repo = node._dal.LocationRepository(cur1)
            quantity_repo = node._dal.QuantityRepository(cur2)
            quantities = []
            for location in location_repo.find_all():
                quantity = quantity_repo.find(location_id=location.id)
                quantities.extend(quantity)
            return sorted(quantities, key=lambda x: x.id)

    def setUp(self):
        self.node = TopoNode()
        self.add_cols_helper(self.node, 'state', 'county')
        self.add_index_helper(self.node, [('OH', 'BUTLER'), ('OH', 'FRANKLIN'), ('IN', 'KNOX')])

        # Set up stream object to capture log messages.
        self.log_stream = StringIO()
        self.addCleanup(self.log_stream.close)

        # Add handler to 'app-toron' logger.
        applogger = logging.getLogger('app-toron')
        handler = logging.StreamHandler(self.log_stream)
        handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        applogger.addHandler(handler)
        self.addCleanup(lambda: applogger.removeHandler(handler))

    def test_insert_quantities(self):
        data = [
            ('state', 'county', 'category', 'sex', 'counts'),
            ('OH', 'BUTLER', 'TOTAL', 'MALE', 180140),
            ('OH', 'BUTLER', 'TOTAL', 'FEMALE', 187990),
            ('OH', 'FRANKLIN', 'TOTAL', 'MALE', 566499),
            ('OH', 'FRANKLIN', 'TOTAL', 'FEMALE', 596915),
        ]

        self.node.insert_quantities(
            value='counts',
            attributes=['category', 'sex'],
            data=data,
        )

        self.assertEqual(
            self.get_location_helper(self.node),
            [Location(1, 'OH', 'BUTLER'),
             Location(2, 'OH', 'FRANKLIN')],
        )

        self.assertEqual(
            self.get_attributes_helper(self.node),
            [AttributeGroup(1, {'category': 'TOTAL', 'sex': 'MALE'}),
             AttributeGroup(2, {'category': 'TOTAL', 'sex': 'FEMALE'})],
        )

        self.assertEqual(
            self.get_quantities_helper(self.node),
            [Quantity(1, 1, 1, 180140),
             Quantity(2, 1, 2, 187990),
             Quantity(3, 2, 1, 566499),
             Quantity(4, 2, 2, 596915)],
        )

    def test_insert_quantities_some_attr_empty(self):
        """Attribute keys with empty values should be omitted."""
        data = [
            ('state', 'county', 'category', 'sex', 'counts'),
            ('OH', 'BUTLER', 'TOTAL', 'MALE', 180140),
            ('OH', 'BUTLER', 'TOTAL', 'FEMALE', 187990),
            ('OH', 'FRANKLIN', '', 'MALE', 566499),  # <- 'category' value is empty string!
            ('OH', 'FRANKLIN', '', 'FEMALE', 596915),  # <- 'category' value is empty string!
        ]

        self.node.insert_quantities(
            value='counts',
            attributes=['category', 'sex'],
            data=data,
        )

        self.assertEqual(
            self.get_location_helper(self.node),
            [Location(1, 'OH', 'BUTLER'),
             Location(2, 'OH', 'FRANKLIN')],
        )

        self.assertEqual(
            self.get_attributes_helper(self.node),
            [AttributeGroup(1, {'category': 'TOTAL', 'sex': 'MALE'}),
             AttributeGroup(2, {'category': 'TOTAL', 'sex': 'FEMALE'}),
             AttributeGroup(3, {'sex': 'MALE'}),  # <- should not have 'category'
             AttributeGroup(4, {'sex': 'FEMALE'})],  # <- should not have 'category'
        )

        self.assertEqual(
            self.get_quantities_helper(self.node),
            [Quantity(1, 1, 1, 180140),
             Quantity(2, 1, 2, 187990),
             Quantity(3, 2, 3, 566499),
             Quantity(4, 2, 4, 596915)],
        )

    def test_insert_quantities_all_attr_empty(self):
        """When rows are missing all attribute values they should be
        omitted entirely.
        """
        data = [
            ('state', 'county', 'category', 'sex', 'counts'),
            ('OH', 'BUTLER', 'TOTAL', 'MALE', 180140),
            ('OH', 'BUTLER', 'TOTAL', 'FEMALE', 187990),
            ('OH', 'FRANKLIN', '', '', 566499),  # <- all attr values are empty string!
            ('OH', 'FRANKLIN', '', '', 596915),  # <- all attr values are empty string!
        ]

        self.node.insert_quantities(
            value='counts',
            attributes=['category', 'sex'],
            data=data,
        )

        self.assertEqual(
            self.get_location_helper(self.node),
            [Location(1, 'OH', 'BUTLER')],
        )

        self.assertEqual(
            self.get_attributes_helper(self.node),
            [AttributeGroup(1, {'category': 'TOTAL', 'sex': 'MALE'}),
             AttributeGroup(2, {'category': 'TOTAL', 'sex': 'FEMALE'})],
        )

        self.assertEqual(
            self.get_quantities_helper(self.node),
            [Quantity(1, 1, 1, 180140),
             Quantity(2, 1, 2, 187990)],
        )

    def test_insert_quantities_domain_included(self):
        self.node.set_domain({'countryiso': 'US'})

        self.node.insert_quantities(
            value='counts',
            attributes=['category', 'sex'],
            data=[
                ('countryiso', 'state', 'county', 'category', 'sex', 'counts'),
                ('US', 'OH', 'BUTLER', 'TOTAL', 'MALE', 180140),
                ('US', 'OH', 'BUTLER', 'TOTAL', 'FEMALE', 187990),
                ('US', 'OH', 'FRANKLIN', 'TOTAL', 'MALE', 566499),
                ('US', 'OH', 'FRANKLIN', 'TOTAL', 'FEMALE', 596915),
            ],
        )

        self.assertEqual(
            self.get_quantities_helper(self.node),
            [Quantity(1, 1, 1, 180140),
             Quantity(2, 1, 2, 187990),
             Quantity(3, 2, 1, 566499),
             Quantity(4, 2, 2, 596915)],
        )

    def test_insert_quantities_domain_missing(self):
        self.node.set_domain({'countryiso': 'US'})

        regex = "invalid column names\n  missing required columns: 'countryiso'"
        with self.assertRaisesRegex(ValueError, regex):
            self.node.insert_quantities(
                value='counts',
                attributes=['category', 'sex'],
                data=[
                    ('state', 'county', 'category', 'sex', 'counts'),
                    ('OH', 'BUTLER', 'TOTAL', 'MALE', 180140),
                    ('OH', 'BUTLER', 'TOTAL', 'FEMALE', 187990),
                    ('OH', 'FRANKLIN', 'TOTAL', 'MALE', 566499),
                    ('OH', 'FRANKLIN', 'TOTAL', 'FEMALE', 596915),
                ],
            )

    def test_insert_quantities_domain_listed_in_attributes(self):
        self.node.set_domain({'countryiso': 'US'})

        self.node.insert_quantities(
            value='counts',
            attributes=['category', 'sex', 'countryiso'],
            data=[
                ('countryiso', 'state', 'county', 'category', 'sex', 'counts'),
                ('US', 'OH', 'BUTLER', 'TOTAL', 'MALE', 180140),
                ('US', 'OH', 'BUTLER', 'TOTAL', 'FEMALE', 187990),
                ('US', 'OH', 'FRANKLIN', 'TOTAL', 'MALE', 566499),
                ('US', 'OH', 'FRANKLIN', 'TOTAL', 'FEMALE', 596915),
            ],
        )

        self.assertEqual(
            self.log_stream.getvalue(),
            ('WARNING: removing domain columns from attributes\n'
             'INFO: loaded 4 quantities\n'),
        )

        self.assertEqual(
            self.get_quantities_helper(self.node),
            [Quantity(1, 1, 1, 180140),
             Quantity(2, 1, 2, 187990),
             Quantity(3, 2, 1, 566499),
             Quantity(4, 2, 2, 596915)],
        )

    def test_insert_quantities_domain_bad_values(self):
        self.node.set_domain({'countryiso': 'US'})

        self.node.insert_quantities(
            value='counts',
            attributes=['category', 'sex'],
            data=[
                ('countryiso', 'state', 'county', 'category', 'sex', 'counts'),
                ('US', 'OH', 'BUTLER', 'TOTAL', 'MALE', 180140),
                ('US', 'OH', 'BUTLER', 'TOTAL', 'FEMALE', 187990),
                ('', 'OH', 'FRANKLIN', 'TOTAL', 'MALE', 566499),
                ('', 'OH', 'FRANKLIN', 'TOTAL', 'FEMALE', 596915),
            ],
        )

        self.assertEqual(
            self.log_stream.getvalue(),
            ("INFO: loaded 2 quantities\n"
             "WARNING: skipped 2 quantities with bad domain values: "
             "countryiso must be 'US'\n"),
        )

        self.assertEqual(
            self.get_quantities_helper(self.node),
            [Quantity(1, 1, 1, 180140),
             Quantity(2, 1, 2, 187990)],
        )


class TestTopoNodeQuantityHandlingMethods(unittest.TestCase):
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
    def get_attribute_groups(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.AttributeGroupRepository(cursor)
            for attr_group in repository.find_all():
                yield attr_group

    @staticmethod
    def get_locations(node):  # <- Helper function.
        with node._managed_cursor() as cursor:
            repository = node._dal.LocationRepository(cursor)
            for attr_group in repository.find_all():
                yield attr_group

    def setUp(self):
        self.node = TopoNode()
        self.add_cols_helper(self.node, 'state', 'county')
        self.add_index_helper(
            self.node,
            [('OH', 'BUTLER'), ('OH', 'FRANKLIN'), ('IN', 'KNOX')],
        )

        # Set up stream object to capture log messages.
        self.log_stream = StringIO()
        self.addCleanup(self.log_stream.close)

        # Add handler to 'app-toron' logger.
        applogger = logging.getLogger('app-toron')
        handler = logging.StreamHandler(self.log_stream)
        handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        applogger.addHandler(handler)
        self.addCleanup(lambda: applogger.removeHandler(handler))

    def test_select_quantities(self):
        self.node.set_domain({'group': 'A', 'year': '2025'})
        data = [
            ['group', 'year', 'state', 'county', 'category', 'sex', 'quantity'],
            ['A', '2025', 'OH', 'BUTLER', 'TOTAL', 'MALE', 180140],
            ['A', '2025', 'OH', 'BUTLER', 'TOTAL', 'FEMALE', 187990],
            ['A', '2025', 'IN', '', 'TOTAL', None, 6924275],
            ['A', '2025', 'AL', '', 'TOTAL', None, 5024279],  # <- No matching index.
        ]
        self.node.insert_quantities(
            value='quantity',
            attributes=['category', 'sex'],
            data=data,
        )

        results = self.node.select_quantities()  # <- Method under test.

        self.assertEqual(list(results), data, msg='should match original data')

    def test_select_quantities_without_index(self):
        self.node.set_domain({'group': 'A', 'year': '2025'})
        self.node.insert_quantities(
            value='quantity',
            attributes=['category', 'sex'],
            data=[
                ['group', 'year', 'state', 'county', 'category', 'sex', 'quantity'],
                ['A', '2025', 'OH', 'BUTLER', 'TOTAL', 'MALE', 180140],
                ['A', '2025', 'OH', 'BUTLER', 'TOTAL', 'FEMALE', 187990],
                ['A', '2025', 'RI', '', 'TOTAL', None, 1112308],  # <- No matching index.
                ['A', '2025', 'AL', '', 'TOTAL', None, 5024279],  # <- No matching index.
            ],
        )

        results = self.node.select_quantities_without_index()  # <- Method under test.

        expected = [
            ['group', 'year', 'state', 'county', 'category', 'sex', 'quantity'],
            ['A', '2025', 'RI', '', 'TOTAL', None, 1112308],
            ['A', '2025', 'AL', '', 'TOTAL', None, 5024279],
        ]
        self.assertEqual(list(results), expected)

    def test_delete_quantities_without_index(self):
        self.node.set_domain({'group': 'A', 'year': '2025'})
        self.node.insert_quantities(
            value='quantity',
            attributes=['category', 'sex'],
            data=[
                ['group', 'year', 'state', 'county', 'category', 'sex', 'quantity'],
                ['A', '2025', 'OH', 'BUTLER', 'TOTAL', 'MALE', 180140],
                ['A', '2025', 'OH', 'BUTLER', 'TOTAL', 'FEMALE', 187990],
                ['A', '2025', 'RI', '', 'TOTAL', None, 1112308],  # <- No matching index.
                ['A', '2025', 'AL', '', 'TOTAL', None, 5024279],  # <- No matching index.
            ],
        )

        self.node.delete_quantities_without_index()  # <- Method under test.

        results = self.node.select_quantities()
        expected = [
            ['group', 'year', 'state', 'county', 'category', 'sex', 'quantity'],
            ['A', '2025', 'OH', 'BUTLER', 'TOTAL', 'MALE', 180140],
            ['A', '2025', 'OH', 'BUTLER', 'TOTAL', 'FEMALE', 187990],
        ]
        self.assertEqual(list(results), expected)

        # Orphan attribute group `{'category': 'TOTAL'}` should have been removed.
        results = self.get_attribute_groups(self.node)
        expected = [
            AttributeGroup(1, {'category': 'TOTAL', 'sex': 'MALE'}),
            AttributeGroup(2, {'category': 'TOTAL', 'sex': 'FEMALE'}),
        ]
        self.assertEqual(list(results), expected)

        # Orphan locations should have been removed.
        results = self.get_locations(self.node)
        expected = [Location(id=1, labels=('OH', 'BUTLER'))]
        self.assertEqual(list(results), expected)

    def test_select_unmatched_quantities(self):
        """Check quantities without matching index or structure."""
        self.node.set_domain({'group': 'A', 'year': '2025'})
        self.node.add_discrete_categories({'state'})
        self.node.insert_quantities(
            value='quantity',
            attributes=['category', 'sex'],
            data=[
                ['group', 'year', 'state', 'county', 'category', 'sex', 'quantity'],
                ['A', '2025', 'OH', 'BUTLER', 'TOTAL', 'MALE', 180140],
                ['A', '2025', 'OH', 'BUTLER', 'TOTAL', 'FEMALE', 187990],
                ['A', '2025', 'OH', '', 'TOTAL', None, 1112308],
                ['A', '2025', '',   'BUTLER', 'TOTAL', None, 52967],  # <- structure mismatch
                ['A', '2025', 'AL', '', 'TOTAL', None, 233687],  # <- index mismatch
            ],
        )

        results = self.node.select_unmatched_quantities()  # <- Method under test.

        expected = [
            ['group', 'year', 'state', 'county', 'category', 'sex', 'quantity'],
            ['A', '2025', '', 'BUTLER', 'TOTAL', None, 52967.0],
            ['A', '2025', 'AL', '', 'TOTAL', None, 233687.0],
        ]
        self.assertEqual(list(results), expected)

    def test_delete_unmatched_quantities(self):
        """Check quantities without matching index or structure."""
        self.node.set_domain({'group': 'A', 'year': '2025'})
        self.node.add_discrete_categories({'state'})
        self.node.insert_quantities(
            value='quantity',
            attributes=['category', 'sex'],
            data=[
                ['group', 'year', 'state', 'county', 'category', 'sex', 'quantity'],
                ['A', '2025', 'OH', 'BUTLER', 'TOTAL', 'MALE', 180140],
                ['A', '2025', 'OH', 'BUTLER', 'TOTAL', 'FEMALE', 187990],
                ['A', '2025', 'OH', '', 'TOTAL', None, 1112308],
                ['A', '2025', '',   'BUTLER', 'SUBTOTAL', None, 52967],  # <- structure mismatch
                ['A', '2025', 'AL', '', 'SUBTOTAL', None, 233687],  # <- index mismatch
            ],
        )

        self.node.delete_unmatched_quantities()  # <- Method under test.

        self.assertEqual(
            list(self.node.select_quantities()),
            [['group', 'year', 'state', 'county', 'category', 'sex', 'quantity'],
             ['A', '2025', 'OH', 'BUTLER', 'TOTAL', 'MALE', 180140.0],
             ['A', '2025', 'OH', 'BUTLER', 'TOTAL', 'FEMALE', 187990.0],
             ['A', '2025', 'OH', '', 'TOTAL', None, 1112308.0]],
            msg='quantities associated with mismatched structures or indexes should be removed',
        )

        self.assertEqual(
            list(self.get_locations(self.node)),
            [Location(id=1, labels=('OH', 'BUTLER')),
             Location(id=2, labels=('OH', ''))],
            msg="should have removed orphan locations ('', 'BUTLER') and ('AL', '')"
        )

        self.assertEqual(
            list(self.get_attribute_groups(self.node)),
            [AttributeGroup(id=1, attributes={'category': 'TOTAL', 'sex': 'MALE'}),
             AttributeGroup(id=2, attributes={'category': 'TOTAL', 'sex': 'FEMALE'}),
             AttributeGroup(id=3, attributes={'category': 'TOTAL'})],
            msg="orphan attribute group {'category': 'SUBTOTAL'} should have been removed",
        )


class TestTopoNodeDisaggregateGenerator(unittest.TestCase):
    def setUp(self):
        node = TopoNode()

        with node._managed_cursor() as cursor:
            manager = node._dal.ColumnManager(cursor)
            manager.add_columns('state', 'county')

            structure_repo = node._dal.StructureRepository(cursor)
            structure_repo.add(1.0, 1, 0)
            structure_repo.add(2.0, 1, 1)

            index_repo = node._dal.IndexRepository(cursor)
            index_repo.add('OH', 'BUTLER')    # index_id 1
            index_repo.add('OH', 'FRANKLIN')  # index_id 2
            index_repo.add('IN', 'KNOX')      # index_id 3
            index_repo.add('IN', 'LAPORTE')   # index_id 4

            weight_group_repo = node._dal.WeightGroupRepository(cursor)
            weight_group_repo.add('totpop', is_complete=True)  # weight_group_id 1

            weight_repo = node._dal.WeightRepository(cursor)
            weight_repo.add(weight_group_id=1, index_id=1, value=374150)
            weight_repo.add(weight_group_id=1, index_id=2, value=1336250)
            weight_repo.add(weight_group_id=1, index_id=3, value=36864)
            weight_repo.add(weight_group_id=1, index_id=4, value=110592)

            property_repo = node._dal.PropertyRepository(cursor)
            property_repo.add('default_weight_group_id', 1)

            location_repo = node._dal.LocationRepository(cursor)
            location_repo.add('OH', 'BUTLER')    # location_id 1
            location_repo.add('OH', 'FRANKLIN')  # location_id 2
            location_repo.add('OH', '')          # location_id 3
            location_repo.add('IN', '')          # location_id 4

            attribute_repo = node._dal.AttributeGroupRepository(cursor)
            attribute_repo.add(value={'category': 'TOTAL', 'sex': 'MALE'})    # attribute_group_id 1
            attribute_repo.add(value={'category': 'TOTAL', 'sex': 'FEMALE'})  # attribute_group_id 2

            quantity_repo = node._dal.QuantityRepository(cursor)
            quantity_repo.add(location_id=1, attribute_group_id=1, value=187075)
            quantity_repo.add(location_id=1, attribute_group_id=2, value=187075)
            quantity_repo.add(location_id=2, attribute_group_id=1, value=668125)
            quantity_repo.add(location_id=2, attribute_group_id=2, value=668125)
            quantity_repo.add(location_id=3, attribute_group_id=1, value=1000)
            quantity_repo.add(location_id=3, attribute_group_id=2, value=1000)
            quantity_repo.add(location_id=4, attribute_group_id=1, value=73728)
            quantity_repo.add(location_id=4, attribute_group_id=2, value=73728)

        self.node = node

    def test_default_weight_group(self):
        """Disaggregate using default weight group."""
        results = self.node._disaggregate()
        expected = [
            (1, {'category': 'TOTAL', 'sex': 'MALE'},   187075.0),
            (1, {'category': 'TOTAL', 'sex': 'FEMALE'}, 187075.0),
            (2, {'category': 'TOTAL', 'sex': 'MALE'},   668125.0),
            (2, {'category': 'TOTAL', 'sex': 'FEMALE'}, 668125.0),
            (1, {'category': 'TOTAL', 'sex': 'MALE'},   218.75),   # <- Disaggreated.
            (2, {'category': 'TOTAL', 'sex': 'MALE'},   781.25),   # <- Disaggreated.
            (1, {'category': 'TOTAL', 'sex': 'FEMALE'}, 218.75),   # <- Disaggreated.
            (2, {'category': 'TOTAL', 'sex': 'FEMALE'}, 781.25),   # <- Disaggreated.
            (3, {'category': 'TOTAL', 'sex': 'MALE'},   18432.0),  # <- Disaggreated.
            (4, {'category': 'TOTAL', 'sex': 'MALE'},   55296.0),  # <- Disaggreated.
            (3, {'category': 'TOTAL', 'sex': 'FEMALE'}, 18432.0),  # <- Disaggreated.
            (4, {'category': 'TOTAL', 'sex': 'FEMALE'}, 55296.0),  # <- Disaggreated.
        ]
        self.assertEqual(list(results), expected)

    def test_matching_group_and_default(self):
        with self.node._managed_cursor() as cursor:
            weight_group_repo = self.node._dal.WeightGroupRepository(cursor)
            weight_repo = self.node._dal.WeightRepository(cursor)

            weight_group_repo.add('men', is_complete=True, selectors=['[sex="MALE"]'])  # weight_group_id 2
            weight_repo.add(weight_group_id=2, index_id=1, value=10000)
            weight_repo.add(weight_group_id=2, index_id=2, value=10000)
            weight_repo.add(weight_group_id=2, index_id=3, value=10000)
            weight_repo.add(weight_group_id=2, index_id=4, value=10000)

        results = self.node._disaggregate()
        expected = [
            (1, {'category': 'TOTAL', 'sex': 'MALE'},   187075.0),
            (1, {'category': 'TOTAL', 'sex': 'FEMALE'}, 187075.0),
            (2, {'category': 'TOTAL', 'sex': 'MALE'},   668125.0),
            (2, {'category': 'TOTAL', 'sex': 'FEMALE'}, 668125.0),
            (1, {'category': 'TOTAL', 'sex': 'MALE'},   500.0),    # <- Disaggreated by group 2
            (2, {'category': 'TOTAL', 'sex': 'MALE'},   500.0),    # <- Disaggreated by group 2
            (1, {'category': 'TOTAL', 'sex': 'FEMALE'}, 218.75),   # <- Disaggreated by default (group 1)
            (2, {'category': 'TOTAL', 'sex': 'FEMALE'}, 781.25),   # <- Disaggreated by default (group 1)
            (3, {'category': 'TOTAL', 'sex': 'MALE'},   36864.0),  # <- Disaggreated by group 2
            (4, {'category': 'TOTAL', 'sex': 'MALE'},   36864.0),  # <- Disaggreated by group 2
            (3, {'category': 'TOTAL', 'sex': 'FEMALE'}, 18432.0),  # <- Disaggreated by default (group 1)
            (4, {'category': 'TOTAL', 'sex': 'FEMALE'}, 55296.0),  # <- Disaggreated by default (group 1)
        ]
        self.assertEqual(list(results), expected)

    def test_matching_multiple_groups(self):
        with self.node._managed_cursor() as cursor:
            weight_group_repo = self.node._dal.WeightGroupRepository(cursor)
            weight_repo = self.node._dal.WeightRepository(cursor)

            weight_group_repo.add('men', is_complete=True, selectors=['[sex="MALE"]'])  # weight_group_id 2
            weight_repo.add(weight_group_id=2, index_id=1, value=10000)
            weight_repo.add(weight_group_id=2, index_id=2, value=10000)
            weight_repo.add(weight_group_id=2, index_id=3, value=0)  # <- Values in 0-weight group are divided evenly.
            weight_repo.add(weight_group_id=2, index_id=4, value=0)  # <- Values in 0-weight group are divided evenly.

            weight_group_repo.add('women', is_complete=True, selectors=['[sex="FEMALE"]'])  # weight_group_id 3
            weight_repo.add(weight_group_id=3, index_id=1, value=5000)
            weight_repo.add(weight_group_id=3, index_id=2, value=15000)
            weight_repo.add(weight_group_id=3, index_id=3, value=10000)
            weight_repo.add(weight_group_id=3, index_id=4, value=10000)

        results = self.node._disaggregate()
        expected = [
            (1, {'category': 'TOTAL', 'sex': 'MALE'},   187075.0),
            (1, {'category': 'TOTAL', 'sex': 'FEMALE'}, 187075.0),
            (2, {'category': 'TOTAL', 'sex': 'MALE'},   668125.0),
            (2, {'category': 'TOTAL', 'sex': 'FEMALE'}, 668125.0),
            (1, {'category': 'TOTAL', 'sex': 'MALE'},   500.0),    # <- Disaggreated by group 2
            (2, {'category': 'TOTAL', 'sex': 'MALE'},   500.0),    # <- Disaggreated by group 2
            (1, {'category': 'TOTAL', 'sex': 'FEMALE'}, 250.0),    # <- Disaggreated by group 3
            (2, {'category': 'TOTAL', 'sex': 'FEMALE'}, 750.0),    # <- Disaggreated by group 3
            (3, {'category': 'TOTAL', 'sex': 'MALE'},   36864.0),  # <- Disaggreated by group 2
            (4, {'category': 'TOTAL', 'sex': 'MALE'},   36864.0),  # <- Disaggreated by group 2
            (3, {'category': 'TOTAL', 'sex': 'FEMALE'}, 36864.0),  # <- Disaggreated by group 3
            (4, {'category': 'TOTAL', 'sex': 'FEMALE'}, 36864.0),  # <- Disaggreated by group 3
        ]
        self.assertEqual(list(results), expected)

    def test_whole_domain_location(self):
        """Values with no location labels are applied to the entire domain."""
        with self.node._managed_transaction() as cursor:
            structure_repo = self.node._dal.StructureRepository(cursor)
            structure_repo.add(None, 0, 0)

            location_repo = self.node._dal.LocationRepository(cursor)
            location_repo.add('', '')  # location_id 5

            attribute_repo = self.node._dal.AttributeGroupRepository(cursor)
            attribute_repo.add(value={'category': 'OTHER'})  # attribute_group_id 3

            quantity_repo = self.node._dal.QuantityRepository(cursor)
            quantity_repo.add(location_id=5, attribute_group_id=3, value=232232)

        results = self.node._disaggregate(attribute_id_filter=[3])
        expected = [
            (0, {'category': 'OTHER'}, 0.0),
            (1, {'category': 'OTHER'}, 46768.75),
            (2, {'category': 'OTHER'}, 167031.25),
            (3, {'category': 'OTHER'}, 4608.0),
            (4, {'category': 'OTHER'}, 13824.0)
        ]
        self.assertEqual(list(results), expected)

    def test_undefined_location(self):
        """It's possible for a quantity to explicitly match the undefined index."""
        with self.node._managed_transaction() as cursor:
            location_repo = self.node._dal.LocationRepository(cursor)
            location_repo.add('-', '-')  # location_id 5, matches undefined index

            attribute_repo = self.node._dal.AttributeGroupRepository(cursor)
            attribute_repo.add(value={'category': 'OTHER'})  # attribute_group_id 3

            quantity_repo = self.node._dal.QuantityRepository(cursor)
            quantity_repo.add(location_id=5, attribute_group_id=3, value=232232)

        results = self.node._disaggregate(attribute_id_filter=[3])
        expected = [
            (0, {'category': 'OTHER'}, 232232),  # <- index_id 0 is for undefined record
        ]
        self.assertEqual(list(results), expected)

    def test_missing_location_finest_granularity(self):
        """Check error message for location without a matching index."""
        with self.node._managed_cursor() as cursor:
            location_repo = self.node._dal.LocationRepository(cursor)
            quantity_repo = self.node._dal.QuantityRepository(cursor)

            location_repo.add('OH', 'DELAWARE')  # location_id 5
            quantity_repo.add(location_id=5, attribute_group_id=1, value=119000)
            quantity_repo.add(location_id=5, attribute_group_id=2, value=118500)

        regex = (r"no index matching: state='OH', county='DELAWARE'\n"
                 r"  Location\(id=5, labels=\('OH', 'DELAWARE'\)\)")
        with self.assertRaisesRegex(RuntimeError, regex):
            results = self.node._disaggregate()
            list(results)  # Consume iterator.

    def test_missing_location_coarser_granularity(self):
        """Check error message for location without a matching index."""
        with self.node._managed_cursor() as cursor:
            location_repo = self.node._dal.LocationRepository(cursor)
            quantity_repo = self.node._dal.QuantityRepository(cursor)

            location_repo.add('AZ', '')  # location_id 5
            quantity_repo.add(location_id=5, attribute_group_id=1, value=119000)
            quantity_repo.add(location_id=5, attribute_group_id=2, value=118500)

        regex = (r"no index matching: state='AZ'\n"
                 r"  Location\(id=5, labels=\('AZ', ''\)\)")
        with self.assertRaisesRegex(RuntimeError, regex):
            results = self.node._disaggregate()
            list(results)  # Consume iterator.


class TestTopoNodeDisaggregate(unittest.TestCase):
    def setUp(self):
        node = TopoNode()
        node.add_index_columns('state', 'county')
        node.add_discrete_categories({'state'}, {'state', 'county'})
        node.insert_index([('state', 'county'),
                           ('OH', 'BUTLER'),
                           ('OH', 'FRANKLIN'),
                           ('IN', 'KNOX'),
                           ('IN', 'LAPORTE')])
        node.add_weight_group('totpop', make_default=True)
        node.insert_weights(
            'totpop',
            [('state', 'county',   'totpop'),
             ('OH',    'BUTLER',   374150),
             ('OH',    'FRANKLIN', 1336250),
             ('IN',    'KNOX',     36864),
             ('IN',    'LAPORTE',  110592)]
        )
        node.insert_quantities(
            value='counts',
            attributes=['category', 'sex'],
            data=[('state', 'county',   'category', 'sex',    'counts'),
                  ('OH',    'BUTLER',   'TOTAL',    'MALE',   187075),
                  ('OH',    'BUTLER',   'TOTAL',    'FEMALE', 187075),
                  ('OH',    'FRANKLIN', 'TOTAL',    'MALE',   668125),
                  ('OH',    'FRANKLIN', 'TOTAL',    'FEMALE', 668125),
                  ('OH',    '',         'TOTAL',    'MALE',   1000),
                  ('OH',    '',         'TOTAL',    'FEMALE', 1000),
                  ('IN',    '',         'TOTAL',    'MALE',   73728),
                  ('IN',    '',         'TOTAL',    'FEMALE', 73728)],
        )
        self.node = node

    def test_default_weight_group(self):
        """Disaggregate to tabular format (uses NodeReader)."""
        node_reader = self.node()  # <- Disaggregate.

        self.assertIsInstance(node_reader, NodeReader)

        self.assertFalse(node_reader.quantize_default)

        self.assertEqual(
            node_reader.columns,
            ['state', 'county', 'category', 'sex', 'value'],
        )

        self.assertEqual(
            set(node_reader),
            {('OH', 'BUTLER',   'TOTAL', 'FEMALE', 187293.75),
             ('OH', 'BUTLER',   'TOTAL', 'MALE',   187293.75),
             ('OH', 'FRANKLIN', 'TOTAL', 'FEMALE', 668906.25),
             ('OH', 'FRANKLIN', 'TOTAL', 'MALE',   668906.25),
             ('IN', 'KNOX',     'TOTAL', 'FEMALE',  18432.0),
             ('IN', 'KNOX',     'TOTAL', 'MALE',    18432.0),
             ('IN', 'LAPORTE',  'TOTAL', 'FEMALE',  55296.0),
             ('IN', 'LAPORTE',  'TOTAL', 'MALE',    55296.0)},
        )

    def test_quantize(self):
        """Testing quantization process with default weight."""
        node_reader = self.node(quantize=True)  # <- Disaggregate.

        self.assertTrue(node_reader.quantize_default)

        self.assertEqual(
            set(node_reader),
            {('OH', 'BUTLER',   'TOTAL', 'FEMALE', 187294.0),  # <- Gets whole remainder (instead of 187293.75)
             ('OH', 'BUTLER',   'TOTAL', 'MALE',   187294.0),  # <- Gets whole remainder (instead of 187293.75)
             ('OH', 'FRANKLIN', 'TOTAL', 'FEMALE', 668906.0),  # <- Loses fractional part (instead of 668906.25)
             ('OH', 'FRANKLIN', 'TOTAL', 'MALE',   668906.0),  # <- Loses fractional part (instead of 668906.25)
             ('IN', 'KNOX',     'TOTAL', 'FEMALE',  18432.0),
             ('IN', 'KNOX',     'TOTAL', 'MALE',    18432.0),
             ('IN', 'LAPORTE',  'TOTAL', 'FEMALE',  55296.0),
             ('IN', 'LAPORTE',  'TOTAL', 'MALE',    55296.0)},
        )

    def test_sum_by_attribute(self):
        """Test summing by specified attributes."""
        node_reader = self.node(sum_by_attrs=['category'])  # <- Sum by 'category' attribute.

        self.assertEqual(
            node_reader.columns,
            ['state', 'county', 'category', 'value'],
        )

        self.assertEqual(
            set(node_reader),
            {('OH', 'BUTLER',   'TOTAL', 374587.5),
             ('OH', 'FRANKLIN', 'TOTAL', 1337812.5),
             ('IN', 'KNOX',     'TOTAL', 36864.0),
             ('IN', 'LAPORTE',  'TOTAL', 110592.0)},
        )

    def test_attribute_selector(self):
        """Testing single selector."""
        quant_iter = self.node('[sex="MALE"][category="TOTAL"]')  # <- Disaggregate.

        self.assertEqual(
            set(quant_iter),
            {('OH', 'BUTLER',   'TOTAL', 'MALE', 187293.75),
             ('OH', 'FRANKLIN', 'TOTAL', 'MALE', 668906.25),
             ('IN', 'KNOX',     'TOTAL', 'MALE', 18432.0),
             ('IN', 'LAPORTE',  'TOTAL', 'MALE', 55296.0)},
        )

    def test_domain_inclusion(self):
        """Domain items should be included with attributes."""
        self.node.set_domain({'country': 'USA'})
        reader = self.node('[sex="MALE"][category="TOTAL"]')  # <- Disaggregate.

        self.assertEqual(
            reader.columns,
            ['state', 'county', 'category', 'country', 'sex', 'value'],
        )
        self.assertEqual(
            set(reader),
            {('OH', 'BUTLER',   'TOTAL', 'USA', 'MALE', 187293.75),
             ('OH', 'FRANKLIN', 'TOTAL', 'USA', 'MALE', 668906.25),
             ('IN', 'KNOX',     'TOTAL', 'USA', 'MALE', 18432.0),
             ('IN', 'LAPORTE',  'TOTAL', 'USA', 'MALE', 55296.0)},
        )

    def test_multiple_attribute_selectors(self):
        """Testing multiple selectors."""
        quant_iter = self.node('[sex="MALE"]', '[sex="FEMALE"]')  # <- Disaggregate.

        self.assertEqual(
            set(quant_iter),
            {('OH', 'BUTLER',   'TOTAL', 'FEMALE', 187293.75),
             ('OH', 'BUTLER',   'TOTAL', 'MALE',   187293.75),
             ('OH', 'FRANKLIN', 'TOTAL', 'FEMALE', 668906.25),
             ('OH', 'FRANKLIN', 'TOTAL', 'MALE',   668906.25),
             ('IN', 'KNOX',     'TOTAL', 'FEMALE', 18432.0),
             ('IN', 'KNOX',     'TOTAL', 'MALE',   18432.0),
             ('IN', 'LAPORTE',  'TOTAL', 'FEMALE', 55296.0),
             ('IN', 'LAPORTE',  'TOTAL', 'MALE',   55296.0)},
        )

    def test_no_matching_attribute_selectors(self):
        """Selector has no match, should return no results."""
        quant_iter = self.node('[sex="MALE"][category="BLERG"]')  # <- Disaggregate.
        self.assertEqual(list(quant_iter), [])

    def test_malformed_selector(self):
        """Malformed selector should raise an error."""
        with self.assertRaises(Exception):
            quant_iter = self.node('sex="MALE"')  # <- Disaggregate.

    def test_explicit_cache_to_drive(self):
        """Testing single selector."""
        expected = {
            ('OH', 'BUTLER',   'TOTAL', 'MALE', 187293.75),
            ('OH', 'FRANKLIN', 'TOTAL', 'MALE', 668906.25),
            ('IN', 'KNOX',     'TOTAL', 'MALE', 18432.0),
            ('IN', 'LAPORTE',  'TOTAL', 'MALE', 55296.0),
        }

        quant_iter = self.node('[sex="MALE"][category="TOTAL"]', cache_to_drive=False)
        self.assertEqual(set(quant_iter), expected)

        quant_iter = self.node('[sex="MALE"][category="TOTAL"]', cache_to_drive=True)
        self.assertEqual(set(quant_iter), expected)

        regex = r'.+Did you mean to use a keyword-only argument\?$'
        with self.assertRaisesRegex(TypeError, regex):
            quant_iter = self.node('[sex="MALE"][category="TOTAL"]', False)


class TestTopoNodeRepr(unittest.TestCase):
    @staticmethod
    def strip_first_line(text):  # <- Helper function.
        """Return given text without the first line."""
        return text[text.find('\n')+1:]

    def test_first_line(self):
        node = TopoNode()

        self.assertEqual(node.__module__, 'toron')

        repr_text = repr(node)
        first_line = repr_text[:repr_text.find('\n')]

        self.assertRegex(
            first_line,
            r'^<toron.TopoNode object at 0x[0-9A-Fa-f]+>$',
        )

    def test_empty_node(self):
        node = TopoNode()

        expected = """
            domain:
              None
            index:
              None
            granularity:
              None
            weights:
              None
            attributes:
              None
            incoming crosswalks:
              None
        """

        self.assertEqual(
            self.strip_first_line(repr(node)),
            dedent(expected).strip(),
        )

    def test_domain(self):
        node = TopoNode()
        node.set_domain({'foo': 'bar', 'baz': 'qux'})

        expected = """
            domain:
              baz: qux
              foo: bar
            index:
              None
            granularity:
              None
            weights:
              None
            attributes:
              None
            incoming crosswalks:
              None
        """

        self.assertEqual(
            self.strip_first_line(repr(node)),
            dedent(expected).strip(),
        )

    def test_index_columns(self):
        node = TopoNode()
        node.add_index_columns('foo', 'bar', 'baz')

        expected = """
            domain:
              None
            index:
              foo, bar, baz
            granularity:
              None
            weights:
              None
            attributes:
              None
            incoming crosswalks:
              None
        """

        self.assertEqual(
            self.strip_first_line(repr(node)),
            dedent(expected).strip(),
        )

    def test_granularity(self):
        node = TopoNode()
        node.add_index_columns('A', 'B', 'C')
        with node._managed_cursor() as cursor:
            structure_repo = node._dal.StructureRepository(cursor)
            structure_repo.add(None, 0, 0, 0)
            structure_repo.add(2.75, 1, 1, 1)

        expected = """
            domain:
              None
            index:
              A, B, C
            granularity:
              2.75
            weights:
              None
            attributes:
              None
            incoming crosswalks:
              None
        """

        self.assertEqual(
            self.strip_first_line(repr(node)),
            dedent(expected).strip(),
        )

    def test_weight_groups(self):
        node = TopoNode()
        node.add_index_columns('A', 'B', 'C')
        node.add_weight_group('foo', is_complete=False, make_default=False)
        node.add_weight_group('bar', is_complete=True, make_default=False)
        node.add_weight_group('baz', is_complete=False, make_default=True)

        expected = """
            domain:
              None
            index:
              A, B, C
            granularity:
              None
            weights:
              bar, baz (default, incomplete), foo (incomplete)
            attributes:
              None
            incoming crosswalks:
              None
        """

        self.assertEqual(
            self.strip_first_line(repr(node)),
            dedent(expected).strip(),
        )

    def test_attributes(self):
        node = TopoNode()
        with node._managed_cursor() as cursor:
            attrgroup_repo = node._dal.AttributeGroupRepository(cursor)
            attrgroup_repo.add({'foo': 'aaa', 'bar': 'bbb'})
            attrgroup_repo.add({'foo': 'ccc', 'baz': 'ddd'})

        expected = """
            domain:
              None
            index:
              None
            granularity:
              None
            weights:
              None
            attributes:
              bar, baz, foo
            incoming crosswalks:
              None
        """

        self.assertEqual(
            self.strip_first_line(repr(node)),
            dedent(expected).strip(),
        )

    def test_crosswalks(self):
        node = TopoNode()
        with node._managed_cursor() as cursor:
            crosswalk_repo = node._dal.CrosswalkRepository(cursor)
            crosswalk_repo.add(
                other_unique_id='111-111-1111',
                other_filename_hint='node1.toron',
                name='foo',
                is_locally_complete=True,
            )
            crosswalk_repo.add(
                other_unique_id='111-111-1111',
                other_filename_hint='node1.toron',
                name='bar',
                is_default=True,
                is_locally_complete=False,
            )
            crosswalk_repo.add(
                other_unique_id='222-222-2222',
                other_filename_hint='node2.toron',
                name='baz',
                is_locally_complete=True,
            )
            crosswalk_repo.add(
                other_unique_id='222-222-2222',
                other_filename_hint=None,
                name='qux',
                is_locally_complete=False,
            )
            crosswalk_repo.add(
                other_unique_id='222-222-2222',
                other_filename_hint='node2.toron',
                name='corge',
                is_default=True,
                is_locally_complete=True,
            )

        expected = """
            domain:
              None
            index:
              None
            granularity:
              None
            weights:
              None
            attributes:
              None
            incoming crosswalks:
              node1.toron: bar (default, locally incomplete), foo
              node2.toron: baz, corge (default), qux (locally incomplete)
        """

        self.assertEqual(
            self.strip_first_line(repr(node)),
            dedent(expected).strip(),
        )
