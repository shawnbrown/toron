"""Tests for data models and data model integration.

Currently this module is focused on integration tests while most of
the behavior is covered by unit tests in the dal1/ folder. But in the
future, it may be best to move or adapt relevant tests to this module
as well.
"""

import os
import tempfile
import unittest
from abc import ABC, abstractmethod
from contextlib import closing, suppress


#######################################################################
# Abstract Test Cases
#######################################################################

from toron.data_models import (
    BaseDataConnector,
    Index, BaseIndexRepository,
    Location, BaseLocationRepository,
    Structure, BaseStructureRepository,
    Weight, BaseWeightRepository,
    AttributeGroup, BaseAttributeGroupRepository,
    Quantity, BaseQuantityRepository,
    Relation, BaseRelationRepository,
    BasePropertyRepository,
    QuantityIterator,
)


class DataConnectorBaseTest(ABC):
    @property
    @abstractmethod
    def dal(self):
        ...

    def test_inheritance(self):
        """Should subclass from BaseDataConnector."""
        self.assertTrue(issubclass(self.dal.DataConnector, BaseDataConnector))

    def test_instantiation(self):
        """Without args, should create an empty node structure."""
        try:
            connector = self.dal.DataConnector()
        except Exception:
            self.fail('should instantiate with no args')

    def test_unique_id(self):
        """Each node should get a unique ID value."""
        connector1 = self.dal.DataConnector()
        connector2 = self.dal.DataConnector()
        self.assertNotEqual(connector1.unique_id, connector2.unique_id)

    def test_acquire_release_interoperation(self):
        """The acquire and release methods should interoperate."""
        connector = self.dal.DataConnector()
        try:
            connection = connector.acquire_connection()
            connector.release_connection(connection)
        except Exception:
            self.fail('acquired connection should be releasable')

    def test_to_file(self):
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            file_path = os.path.join(tmpdir, 'mynode.toron')
            self.assertFalse(os.path.exists(file_path))

            connector = self.dal.DataConnector()
            connector.to_file(file_path, fsync=True)
            self.assertTrue(os.path.exists(file_path))

            file_size = os.path.getsize(file_path)
            self.assertGreater(file_size, 0, msg='file should not be empty')

    def test_from_file(self):
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            file_path = os.path.join(tmpdir, 'mynode.toron')
            original = self.dal.DataConnector()
            original.to_file(file_path)

            loadedfromfile = self.dal.DataConnector.from_file(file_path)
            self.assertEqual(original.unique_id, loadedfromfile.unique_id)

    def test_from_file_missing(self):
        """Should raise FileNotFoundError if file doesn't exist."""
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            file_path = os.path.join(tmpdir, 'does_not_exist.toron')

            with self.assertRaises(FileNotFoundError):
                self.dal.DataConnector.from_file(file_path)

    def test_from_file_unknown_format(self):
        """Should raise RuntimeError if file uses unknown format."""
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            file_path = os.path.join(tmpdir, 'unknown_format.xyz')
            with closing(open(file_path, 'w')) as f:
                f.write('Hello World\n')

            with self.assertRaises(Exception):
                self.dal.DataConnector.from_file(file_path)


class ColumnManagerBaseTest(ABC):
    @property
    @abstractmethod
    def dal(self):
        ...

    def setUp(self):
        connector = self.dal.DataConnector()

        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        cursor = connector.acquire_cursor(connection)
        self.addCleanup(lambda: connector.release_cursor(cursor))

        self.manager = self.dal.ColumnManager(cursor)


class IndexRepositoryBaseTest(ABC):
    @property
    @abstractmethod
    def dal(self):
        ...

    def setUp(self):
        connector = self.dal.DataConnector()
        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        cursor = connector.acquire_cursor(connection)
        self.addCleanup(lambda: connector.release_cursor(cursor))

        self.manager = self.dal.ColumnManager(cursor)
        self.repository = self.dal.IndexRepository(cursor)

    def test_inheritance(self):
        """Must inherit from appropriate abstract base class."""
        self.assertTrue(issubclass(self.dal.IndexRepository, BaseIndexRepository))

    def test_integration(self):
        """Test add(), get(), update() and delete() interaction."""
        self.manager.add_columns('A', 'B')

        self.repository.add('foo', 'x')
        self.repository.add('bar', 'y')

        self.assertEqual(self.repository.get(1), Index(1, 'foo', 'x'))
        self.assertEqual(self.repository.get(2), Index(2, 'bar', 'y'))

        self.repository.update(Index(2, 'bar', 'z'))
        self.assertEqual(self.repository.get(2), Index(2, 'bar', 'z'))

        self.repository.delete(2)
        self.assertIsNone(self.repository.get(2))

    def test_add_duplicate_labels(self):
        """Attempting to add duplicate labels should raise ValueError."""
        self.manager.add_columns('A', 'B')
        self.repository.add('foo', 'bar')

        msg = "should not add ('foo', 'bar') again, duplicates not allowed"
        with self.assertRaises(ValueError, msg=msg):
            self.repository.add('foo', 'bar')

    def test_add_empty_string(self):
        """Attempting to add empty strings should raise ValueError."""
        self.manager.add_columns('A', 'B')

        msg = "adding ('foo', '') should fail, empty strings not allowed"
        with self.assertRaises(ValueError, msg=msg):
            self.repository.add('foo', '')

    def test_get_all(self):
        self.manager.add_columns('A', 'B')
        self.repository.add('foo', 'x')
        self.repository.add('bar', 'y')

        results = self.repository.get_all()
        expected = [
            Index(0, '-', '-'),
            Index(1, 'foo', 'x'),
            Index(2, 'bar', 'y'),
        ]
        self.assertEqual(list(results), expected)

        results = self.repository.get_all(include_undefined=False)
        expected = [
            Index(1, 'foo', 'x'),
            Index(2, 'bar', 'y'),
        ]
        self.assertEqual(list(results), expected, msg='should not include index_id 0')

    def test_get_index_ids(self):
        self.manager.add_columns('A', 'B')
        self.repository.add('foo', 'x')
        self.repository.add('bar', 'y')
        self.repository.add('baz', 'z')

        results = self.repository.get_index_ids()
        self.assertEqual(set(results), {0, 1, 2, 3})

        results = self.repository.get_index_ids(ordered=True)
        self.assertEqual(list(results), [0, 1, 2, 3])

    def test_get_distinct_labels(self):
        self.manager.add_columns('A', 'B', 'C')
        self.repository.add('foo', 'x', 'aaa')
        self.repository.add('foo', 'y', 'bbb')
        self.repository.add('bar', 'x', 'bbb')
        self.repository.add('bar', 'x', 'ccc')

        results = self.repository.get_distinct_labels('A')
        expected = {('-',), ('foo',), ('bar',)}
        self.assertEqual(set(results), expected)

        results = self.repository.get_distinct_labels('A', include_undefined=False)
        expected = {('foo',), ('bar',)}
        self.assertEqual(set(results), expected)

        results = self.repository.get_distinct_labels('A', 'B')
        expected = {('-', '-'), ('foo', 'x'), ('foo', 'y'), ('bar', 'x')}
        self.assertEqual(set(results), expected)

        results = self.repository.get_distinct_labels('A', 'B', include_undefined=False)
        expected = {('foo', 'x'), ('foo', 'y'), ('bar', 'x')}
        self.assertEqual(set(results), expected)

    def test_find_by_label(self):
        self.manager.add_columns('A', 'B')
        self.repository.add('foo', 'x')
        self.repository.add('foo', 'y')
        self.repository.add('bar', 'x')
        self.repository.add('bar', '-')

        results = self.repository.find_by_label({'A': 'foo'})
        expected = [Index(1, 'foo', 'x'), Index(2, 'foo', 'y')]
        self.assertEqual(list(results), expected)

        results = self.repository.find_by_label({'B': 'x'})
        expected = [Index(1, 'foo', 'x'), Index(3, 'bar', 'x')]
        self.assertEqual(list(results), expected)

        results = self.repository.find_by_label({'A': 'bar', 'B': 'x'})
        expected = [Index(3, 'bar', 'x')]
        self.assertEqual(list(results), expected)

        regex = 'find_by_label requires at least 1 criteria value, got 0'
        with self.assertRaisesRegex(ValueError, regex):
            results = self.repository.find_by_label(dict())  # <- Empty dict.

        # Explicit `include_undefined=True` (this is the default).
        results = self.repository.find_by_label({'B': '-'}, include_undefined=True)
        self.assertEqual(list(results), [Index(0, '-', '-'), Index(4, 'bar', '-')])

        # Check `include_undefined=False`.
        results = self.repository.find_by_label({'B': '-'}, include_undefined=False)
        self.assertEqual(list(results), [Index(4, 'bar', '-')])


class LocationRepositoryBaseTest(ABC):
    @property
    @abstractmethod
    def dal(self):
        ...

    def setUp(self):
        connector = self.dal.DataConnector()
        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        cursor = connector.acquire_cursor(connection)
        self.addCleanup(lambda: connector.release_cursor(cursor))

        self.manager = self.dal.ColumnManager(cursor)
        self.repository = self.dal.LocationRepository(cursor)

    def test_inheritance(self):
        """Must inherit from appropriate abstract base class."""
        self.assertTrue(issubclass(self.dal.LocationRepository, BaseLocationRepository))

    def test_integration(self):
        """Test add(), get(), update() and delete() interaction."""
        self.manager.add_columns('A', 'B')

        self.repository.add('foo', 'x')
        self.repository.add('bar', 'y')

        self.assertEqual(self.repository.get(1), Location(1, 'foo', 'x'))
        self.assertEqual(self.repository.get(2), Location(2, 'bar', 'y'))

        self.repository.update(Index(2, 'bar', 'z'))
        self.assertEqual(self.repository.get(2), Location(2, 'bar', 'z'))

        self.repository.delete(2)
        self.assertIsNone(self.repository.get(2))

    def test_add_duplicate_labels(self):
        """Attempting to add duplicate labels should raise ValueError."""
        self.manager.add_columns('A', 'B')
        self.repository.add('foo', 'bar')

        msg = "should not add ('foo', 'bar') again, duplicates not allowed"
        with self.assertRaises(Exception, msg=msg):
            self.repository.add('foo', 'bar')

    def test_add_empty_string(self):
        """Empty strings are allowed in 'location' (unlike 'index')."""
        self.manager.add_columns('A', 'B')

        try:
            self.repository.add('foo', '')
        except Exception:
            self.fail("should ('foo', ''), empty strings must be allowed")

    def test_get_label_columns(self):
        self.manager.add_columns('A', 'B', 'C')
        result = self.repository.get_label_columns()
        self.assertEqual(result, ('A', 'B', 'C'))

    def test_find_by_label(self):
        self.manager.add_columns('A', 'B')
        self.repository.add('foo', 'x')
        self.repository.add('foo', 'y')
        self.repository.add('bar', 'x')
        self.repository.add('bar', '')  # <- Location labels can be empty strings (indicating approximate location)

        results = self.repository.find_by_label({'A': 'foo'})
        expected = [Location(1, 'foo', 'x'), Location(2, 'foo', 'y')]
        self.assertEqual(list(results), expected)

        results = self.repository.find_by_label({'B': 'x'})
        expected = [Location(1, 'foo', 'x'), Location(3, 'bar', 'x')]
        self.assertEqual(list(results), expected)

        results = self.repository.find_by_label({'B': ''})  # <- Empty string.
        expected = [Location(4, 'bar', '')]
        self.assertEqual(list(results), expected)

        results = self.repository.find_by_label({'A': 'bar', 'B': 'x'})
        expected = [Location(3, 'bar', 'x')]
        self.assertEqual(list(results), expected)

        results = self.repository.find_by_label({'A': 'baz', 'B': 'z'})  # <- No match.
        self.assertEqual(list(results), [])  # <- Empty result.

        regex = 'find_by_label requires at least 1 criteria value, got 0'
        with self.assertRaisesRegex(ValueError, regex):
            results = self.repository.find_by_label(dict())  # <- Empty dict.

    def test_get_by_labels_add_if_missing(self):
        self.manager.add_columns('A', 'B')
        self.repository.add('foo', 'x')  # <- Create existing location.

        self.assertEqual(
            self.repository.get_by_labels_add_if_missing({'A': 'foo', 'B': 'x'}),
            Location(1, 'foo', 'x'),
            msg='should return existing location',
        )

        self.assertEqual(
            self.repository.get_by_labels_add_if_missing({'A': 'bar', 'B': 'y'}),
            Location(2, 'bar', 'y'),
            msg='should create and return a new location since no match existed',
        )

        self.assertEqual(
            self.repository.get_by_labels_add_if_missing({'A': 'bar', 'B': 'y'}),
            Location(2, 'bar', 'y'),
            msg='should now return existing (newly created) location',
        )

        regex = r'requires all label columns, got: A \(needs A, B\)'
        with self.assertRaisesRegex(ValueError, regex):
            self.repository.get_by_labels_add_if_missing({'A': 'foo'})

        regex = r'requires all label columns, got: nothing \(needs A, B\)'
        with self.assertRaisesRegex(ValueError, regex):
            self.repository.get_by_labels_add_if_missing(dict())

        regex = r'requires all label columns, got: A, B, C \(needs A, B\)'
        with self.assertRaisesRegex(ValueError, regex):
            self.repository.get_by_labels_add_if_missing({'A': 'bar', 'B': 'y', 'C': 'z'})

    def test_find_by_structure(self):
        self.manager.add_columns('A', 'B')
        self.repository.add('foo', 'x')  # <- Matches bits (1, 1)
        self.repository.add('bar', 'y')  # <- Matches bits (1, 1)
        self.repository.add('foo', '')   # <- Matches bits (1, 0)
        self.repository.add('bar', '')   # <- Matches bits (1, 0)
        self.repository.add('', 'x')     # <- Matches bits (0, 1)
        self.repository.add('', 'y')     # <- Matches bits (0, 1)

        self.assertEqual(
            list(self.repository.find_by_structure(Structure(1, None, bits=(1, 1)))),
            [Location(1, 'foo', 'x'), Location(2, 'bar', 'y')],
        )

        self.assertEqual(
            list(self.repository.find_by_structure(Structure(2, None, bits=(1, 0)))),
            [Location(3, 'foo', ''), Location(4, 'bar', '')],
        )

        self.assertEqual(
            list(self.repository.find_by_structure(Structure(3, None, bits=(0, 1)))),
            [Location(5, '', 'x'), Location(6, '', 'y')],
        )

        self.assertEqual(
            list(self.repository.find_by_structure(Structure(3, None, bits=(0, 0)))),
            [],
        )


class StructureRepositoryBaseTest(ABC):
    @property
    @abstractmethod
    def dal(self):
        ...

    def setUp(self):
        connector = self.dal.DataConnector()
        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        cursor = connector.acquire_cursor(connection)
        self.addCleanup(lambda: connector.release_cursor(cursor))

        self.manager = self.dal.ColumnManager(cursor)
        self.repository = self.dal.StructureRepository(cursor)

    def test_inheritance(self):
        """Must inherit from appropriate abstract base class."""
        self.assertTrue(issubclass(self.dal.StructureRepository, BaseStructureRepository))

    def test_integration(self):
        """Test add(), get(), update() and delete() interaction."""
        self.manager.add_columns('A', 'B', 'C')

        self.repository.add(None, 0, 0, 0)
        self.repository.add(1.25, 1, 0, 0)
        self.repository.add(2.75, 1, 1, 1)

        self.assertEqual(self.repository.get(1), Structure(1, None, 0, 0, 0))
        self.assertEqual(self.repository.get(2), Structure(2, 1.25, 1, 0, 0))
        self.assertEqual(self.repository.get(3), Structure(3, 2.75, 1, 1, 1))

        self.repository.update(Structure(2, 1.5, 1, 1, 0))
        self.assertEqual(self.repository.get(2), Structure(2, 1.5, 1, 1, 0))

        self.repository.delete(2)
        self.assertIsNone(self.repository.get(2))


class WeightRepositoryBaseTest(ABC):
    @property
    @abstractmethod
    def dal(self):
        ...

    def setUp(self):
        connector = self.dal.DataConnector()
        self.connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(self.connection))

        cursor = connector.acquire_cursor(self.connection)
        self.addCleanup(lambda: connector.release_cursor(cursor))

        self.manager = self.dal.ColumnManager(cursor)
        self.index_repo = self.dal.IndexRepository(cursor)

        self.manager.add_columns('A', 'B')
        self.index_repo.add('foo', 'x')
        self.index_repo.add('bar', 'y')
        self.index_repo.add('baz', 'z')

        self.weight_group_repo = self.dal.WeightGroupRepository(cursor)

        self.repository = self.dal.WeightRepository(cursor)
        self.weight_group_repo.add('population')  # Adds weight_group_id 1.
        self.weight_group_repo.add('square_miles')  # Adds weight_group_id 2.

        self.repository.add(1, 1, 175000)
        self.repository.add(1, 2,  25000)
        self.repository.add(1, 3, 100000)

        self.repository.add(2, 1, 583.75)
        self.repository.add(2, 2, 416.25)
        self.repository.add(2, 3, 500.0)

    def get_weights_helper(self):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        cur = self.connection.execute('SELECT * FROM main.weight')
        return cur.fetchall()

    def test_inheritance(self):
        """Must inherit from appropriate abstract base class."""
        self.assertTrue(issubclass(self.dal.WeightRepository, BaseWeightRepository))

    def test_find_by_index_id(self):
        results = self.repository.find_by_index_id(1)
        expected = [
            Weight(id=1, weight_group_id=1, index_id=1, value=175000.0),
            Weight(id=4, weight_group_id=2, index_id=1, value=583.75),
        ]
        self.assertEqual(list(results), expected)

        results = self.repository.find_by_index_id(99)  # No index_id 99
        self.assertEqual(list(results), [], msg='should return empty iterator')

    def test_merge_one_and_two(self):
        self.repository.merge_by_index_id(index_ids={1, 2}, target=1)
        results = self.get_weights_helper()
        expected = [
            (3, 1, 3, 100000.0),
            (6, 2, 3, 500.0),
            (7, 1, 1, 200000.0),
            (8, 2, 1, 1000.0),
        ]
        self.assertEqual(results, expected)

    def test_merge_two_and_three(self):
        self.repository.merge_by_index_id(index_ids={2, 3}, target=2)
        results = self.get_weights_helper()
        expected = [
            (1, 1, 1, 175000.0),
            (4, 2, 1, 583.75),
            (5, 1, 2, 125000.0),
            (6, 2, 2, 916.25),
        ]
        self.assertEqual(results, expected)

    def test_merge_one_two_and_three(self):
        self.repository.merge_by_index_id(index_ids={1, 2, 3}, target=1)
        results = self.get_weights_helper()
        expected = [
            (1, 1, 1, 300000.0),
            (2, 2, 1, 1500.0),
        ]
        self.assertEqual(results, expected)

    def test_merge_target_inclusion(self):
        """Target id must be auto-added to index_ids if not included."""
        # The target (1) is not in index_ids (but should be included internally).
        self.repository.merge_by_index_id(index_ids={2, 3}, target=1)
        results = self.get_weights_helper()
        expected = [
            (1, 1, 1, 300000.0),
            (2, 2, 1, 1500.0),
        ]
        self.assertEqual(results, expected)


class AttributeGroupRepositoryBaseTest(ABC):
    @property
    @abstractmethod
    def dal(self):
        ...

    def setUp(self):
        connector = self.dal.DataConnector()
        self.connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(self.connection))

        cursor = connector.acquire_cursor(self.connection)
        self.addCleanup(lambda: connector.release_cursor(cursor))

        self.repository = self.dal.AttributeGroupRepository(cursor)

    def test_inheritance(self):
        """Must inherit from appropriate abstract base class."""
        self.assertTrue(isinstance(self.repository, BaseAttributeGroupRepository))

    def test_integration(self):
        """Test interoperation of add, get, update, and delete."""
        repository = self.repository

        repository.add({'foo': 'A'})
        self.assertEqual(repository.get(1), AttributeGroup(1, {'foo': 'A'}))

        repository.update(AttributeGroup(1, {'foo': 'B'}))
        self.assertEqual(repository.get(1), AttributeGroup(1, {'foo': 'B'}))

        repository.delete(1)
        self.assertIsNone(repository.get(1))

    def test_add_empty_string_error(self):
        """If keys or values are empty string, should raise ValueError."""
        with self.assertRaises(ValueError, msg='keys must not be empty strings'):
            self.repository.add({'foo': 'A', '': 'B'})  # <- Second key is empty string.

        with self.assertRaises(ValueError, msg='values must not be empty strings'):
            self.repository.add({'foo': 'A', 'bar': ''})  # <- Second value is empty string.

    def test_update_empty_key_error(self):
        """If keys or values are empty string, should raise ValueError."""
        self.repository.add({'foo': 'A', 'bar': 'B'})

        attribute_group = self.repository.get(1)
        attribute_group.attributes = {'foo': 'A', '': 'B'}  # <- Second key is empty string.
        with self.assertRaises(ValueError, msg='keys must not be empty strings'):
            self.repository.update(attribute_group)

        attribute_group = self.repository.get(1)
        attribute_group.attributes = {'foo': 'A', 'bar': ''}  # <- Second value is empty string.
        with self.assertRaises(ValueError, msg='keys must not be empty strings'):
            self.repository.update(attribute_group)

    def test_get_by_value(self):
        self.repository.add({'foo': 'A'})
        self.assertEqual(
            self.repository.get_by_value({'foo': 'A'}),
            AttributeGroup(1, {'foo': 'A'}),
        )

    def test_get_by_value_add_if_missing(self):
        self.repository.add({'foo': 'A'})  # <- Create attribute_group_id 1.

        self.assertEqual(
            self.repository.get_by_value_add_if_missing({'foo': 'A'}),
            AttributeGroup(1, {'foo': 'A'}),
            msg='should return existing attribute_group_id 1',
        )

        self.assertEqual(
            self.repository.get_by_value_add_if_missing({'foo': 'B'}),  # <- Creates attribute_group_id 2.
            AttributeGroup(2, {'foo': 'B'}),
            msg='should create and return record with attribute_group_id 2'
        )

        self.assertEqual(
            self.repository.get_by_value_add_if_missing({'foo': 'B'}),  # <- Gets existing attribute_group_id 2.
            AttributeGroup(2, {'foo': 'B'}),
            msg='should return existing record with attribute_group_id 2'
        )

    def test_find_all(self):
        self.repository.add({'A': 'foo'})
        self.repository.add({'A': 'bar'})
        self.repository.add({'A': 'baz'})

        self.assertEqual(
            list(self.repository.find_all()),
            [AttributeGroup(id=1, attributes={'A': 'foo'}),
             AttributeGroup(id=2, attributes={'A': 'bar'}),
             AttributeGroup(id=3, attributes={'A': 'baz'})]
        )

    def _helper_find_by_criteria(self, method_under_test):
        """Helper method to check ``find_by_criteria()``."""

        self.repository.add({'A': 'foo'})
        self.repository.add({'A': 'foo', 'B': 'qux'})
        self.repository.add({'A': 'bar', 'B': 'qux'})

        self.assertEqual(
            list(method_under_test(A='foo')),
            [AttributeGroup(id=1, attributes={'A': 'foo'}),
             AttributeGroup(id=2, attributes={'A': 'foo', 'B': 'qux'})],
        )

        self.assertEqual(
            list(method_under_test(B='qux')),
            [AttributeGroup(id=2, attributes={'A': 'foo', 'B': 'qux'}),
             AttributeGroup(id=3, attributes={'A': 'bar', 'B': 'qux'})],
        )

        self.assertEqual(
            list(method_under_test(A='foo', B='qux')),
            [AttributeGroup(id=2, attributes={'A': 'foo', 'B': 'qux'})],
        )

        self.assertEqual(
            list(method_under_test(A='foo', B=None)),
            [AttributeGroup(id=1, attributes={'A': 'foo'})],
            msg='criteria B=None should match records without B',
        )

        self.assertEqual(
            list(method_under_test(B='corge')),
            [],
            msg="no match for B='corge', iterator should be empty",
        )

        self.assertEqual(
            list(method_under_test(A='foo', C='bar')),
            [],
            msg='no column C, iterator should be empty',
        )

        # Check that attributes special characters can survive round-trip.
        ugly_attr = {'A \\ "B", \'C\'': 'baz * "123"'}  # <- Uses special characters.
        self.repository.add(ugly_attr)
        self.assertEqual(
            list(method_under_test(**ugly_attr)),
            [AttributeGroup(id=4, attributes=ugly_attr)],
            msg='special characters should survive round-trip matching',
        )

    def test_find_by_criteria_abstract(self):
        """Test BaseAttributeGroupRepository.find_by_criteria() method."""
        obj_type = self.dal.AttributeGroupRepository
        obj_instance = self.repository
        method_under_test = super(obj_type, obj_instance).find_by_criteria
        self._helper_find_by_criteria(method_under_test)

    def test_find_by_criteria_concrete(self):
        """Test AttributeGroupRepository.find_by_criteria() method."""
        method_under_test = self.repository.find_by_criteria
        self._helper_find_by_criteria(method_under_test)

    def test_get_all_attribute_names(self):
        self.repository.add({'C': 'foo'})
        self.repository.add({'A': 'foo', 'B': 'qux'})
        self.repository.add({'A': 'bar', 'B': 'qux'})

        result = self.repository.get_all_attribute_names()
        self.assertEqual(result, ['A', 'B', 'C'])


class QuantityRepositoryBaseTest(ABC):
    @property
    @abstractmethod
    def dal(self):
        ...

    def setUp(self):
        connector = self.dal.DataConnector()
        self.connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(self.connection))

        cursor = connector.acquire_cursor(self.connection)
        self.addCleanup(lambda: connector.release_cursor(cursor))

        # Set-up test values for quantity table to use.
        manager = self.dal.ColumnManager(cursor)
        manager.add_columns('A', 'B')

        location_repo = self.dal.LocationRepository(cursor)
        location_repo.add('foo', 'qux')   # Add location_id 1
        location_repo.add('bar', 'quux')  # Add location_id 2

        attribute_repo = self.dal.AttributeGroupRepository(cursor)
        attribute_repo.add({'aaa': 'one'})  # Add attribute_group_id 1
        attribute_repo.add({'bbb': 'two'})  # Add attribute_group_id 2

        # Create QuantityRepository for testing.
        self.repository = self.dal.QuantityRepository(cursor)

    def test_inheritance(self):
        """Must inherit from appropriate abstract base class."""
        self.assertTrue(isinstance(self.repository, BaseQuantityRepository))

    def test_get_value_type(self):
        """The type of ``Quantity.value`` objects should be ``float``
        regardless of input or storage type.
        """
        self.repository.add(location_id=1, attribute_group_id=1, value=10.5)  # Add quantity_id 1
        self.repository.add(location_id=2, attribute_group_id=1, value=20.0)  # Add quantity_id 2
        self.repository.add(location_id=1, attribute_group_id=2, value=20)    # Add quantity_id 3

        self.assertIsInstance(self.repository.get(1).value, float)
        self.assertIsInstance(self.repository.get(2).value, float)
        self.assertIsInstance(self.repository.get(3).value, float)

    def test_find_by_location_id(self):
        self.repository.add(location_id=1, attribute_group_id=1, value=15.0)  # Add quantity_id 1
        self.repository.add(location_id=2, attribute_group_id=1, value=20.0)  # Add quantity_id 2
        self.repository.add(location_id=1, attribute_group_id=2, value=25.0)  # Add quantity_id 3
        self.repository.add(location_id=2, attribute_group_id=2, value=10.0)  # Add quantity_id 4
        self.repository.add(location_id=2, attribute_group_id=2, value=35.0)  # Add quantity_id 5

        result = self.repository.find_by_location_id(1)
        self.assertEqual(
            list(result),
            [Quantity(id=1, location_id=1, attribute_group_id=1, value=15.0),
             Quantity(id=3, location_id=1, attribute_group_id=2, value=25.0)],
        )

        result = self.repository.find_by_location_id(2)
        self.assertEqual(
            list(result),
            [Quantity(id=2, location_id=2, attribute_group_id=1, value=20.0),
             Quantity(id=4, location_id=2, attribute_group_id=2, value=10.0),
             Quantity(id=5, location_id=2, attribute_group_id=2, value=35.0)],
        )

    def test_find_by_ids(self):
        self.repository.add(location_id=1, attribute_group_id=1, value=15.0)  # Add quantity_id 1
        self.repository.add(location_id=2, attribute_group_id=1, value=20.0)  # Add quantity_id 2
        self.repository.add(location_id=1, attribute_group_id=2, value=25.0)  # Add quantity_id 3
        self.repository.add(location_id=2, attribute_group_id=2, value=10.0)  # Add quantity_id 4
        self.repository.add(location_id=2, attribute_group_id=2, value=35.0)  # Add quantity_id 5

        result = list(self.repository.find_by_ids(location_id=1, attribute_group_id=2))
        self.assertEqual(
            result,
            [Quantity(id=3, location_id=1, attribute_group_id=2, value=25.0)],
            msg='matches location_id 1 and attribute_group_id 2',
        )
        self.assertIsInstance(result[0].value, float)

        self.assertEqual(
            list(self.repository.find_by_ids(location_id=1)),
            [Quantity(id=1, location_id=1, attribute_group_id=1, value=15.0),
             Quantity(id=3, location_id=1, attribute_group_id=2, value=25.0)],
            msg='matches location_id 1',
        )

        self.assertEqual(
            list(self.repository.find_by_ids(attribute_group_id=1)),
            [Quantity(id=1, location_id=1, attribute_group_id=1, value=15.0),
             Quantity(id=2, location_id=2, attribute_group_id=1, value=20.0)],
            msg='matches attribute_group_id 1',
        )

        self.assertEqual(
            list(self.repository.find_by_ids(location_id=2, attribute_group_id=2)),
            [Quantity(id=4, location_id=2, attribute_group_id=2, value=10.0),
             Quantity(id=5, location_id=2, attribute_group_id=2, value=35.0)],
            msg='matches location_id 2 and attribute_group_id 2 (two matching records)',
        )

        self.assertEqual(
            list(self.repository.find_by_ids(location_id=4, attribute_group_id=2)),
            [],
            msg='matches location_id 4 and attribute_group_id 2 (zero matching records)',
        )

        self.assertEqual(
            list(self.repository.find_by_ids()),
            [],
            msg='when no ids given, return empty iterator',
        )


class RelationRepositoryBaseTest(ABC):
    @property
    @abstractmethod
    def dal(self):
        ...

    def setUp(self):
        connector = self.dal.DataConnector()
        self.connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(self.connection))

        cursor = connector.acquire_cursor(self.connection)
        self.addCleanup(lambda: connector.release_cursor(cursor))

        self.manager = self.dal.ColumnManager(cursor)
        self.index_repo = self.dal.IndexRepository(cursor)

        self.manager.add_columns('A', 'B')
        self.index_repo.add('foo', 'x')
        self.index_repo.add('bar', 'y')
        self.index_repo.add('baz', 'z')

        self.crosswalk = self.dal.CrosswalkRepository(cursor)
        self.repository = self.dal.RelationRepository(cursor)

        self.crosswalk.add('111-11-1111', None, 'other1')  # Adds crosswalk_id 1.
        self.crosswalk.add('222-22-2222', None, 'other2')  # Adds crosswalk_id 2.

        self.repository.add(1, 1, 1, None,    131250, 1.0)
        self.repository.add(1, 2, 1, b'\x40',  40960, 0.625)
        self.repository.add(1, 2, 2, b'\x40',  24576, 0.375)
        self.repository.add(1, 3, 3, None,    100000, 1.0)

        self.repository.add(2, 1, 1, None,    583.75, 1.0)
        self.repository.add(2, 2, 2, None,    416.25, 1.0)
        self.repository.add(2, 3, 1, None,    336.00, 0.328125)
        self.repository.add(2, 3, 2, None,    112.00, 0.109375)
        self.repository.add(2, 3, 3, None,    576.00, 0.5625)

    def get_relations_helper(self):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        cur = self.connection.execute('SELECT * FROM main.relation')
        return set(cur.fetchall())

    def test_inheritance(self):
        """Must inherit from appropriate abstract base class."""
        self.assertTrue(issubclass(self.dal.RelationRepository, BaseRelationRepository))

    def test_add_type_coersion(self):
        """String values should get converted to proper types."""
        self.repository.add('2', '2', '3', '', '9393', '1.0')  # <- String values.
        expected = {
            # First crosswalk.
            (1,  1, 1, 1, None,   131250.00, 1.0),
            (2,  1, 2, 1, b'\x40', 40960.00, 0.625),
            (3,  1, 2, 2, b'\x40', 24576.00, 0.375),
            (4,  1, 3, 3, None,   100000.00, 1.0),
            # Second crosswalk.
            (5,  2, 1, 1, None,      583.75, 1.0),
            (6,  2, 2, 2, None,      416.25, 1.0),
            (7,  2, 3, 1, None,      336.00, 0.328125),
            (8,  2, 3, 2, None,      112.00, 0.109375),
            (9,  2, 3, 3, None,      576.00, 0.5625),
            (10, 2, 2, 3, None,      9393.0, 1.0),  # <- Values coerced to proper types.
        }
        self.assertEqual(self.get_relations_helper(), expected)

    def test_add_bad_types(self):
        """String values that cannot be coerced should raise an error."""
        with self.assertRaises(Exception):
            self.repository.add('foo', 4, 1, None, 4242, 1.0)

        with self.assertRaises(Exception):
            self.repository.add(1, 'foo', 1, None, 4242, 1.0)

        with self.assertRaises(Exception):
            self.repository.add(1, 4, 'foo', None, 4242, 1.0)

        with self.assertRaises(Exception):
            self.repository.add(1, 4, 1, 'foo', 4242, 1.0)

        with self.assertRaises(Exception):
            self.repository.add(1, 4, 1, None, 'foo', 1.0)

        with self.assertRaises(Exception):
            self.repository.add(1, 4, 1, None, 4242, 'foo')

    def test_find_by_index_id(self):
        results = self.repository.find_by_ids(index_id=3)
        expected = [
            Relation(
                id=4,
                crosswalk_id=1,
                other_index_id=3,
                index_id=3,
                mapping_level=None,
                value=100000.0,
                proportion=1.0,
            ),
            Relation(
                id=9,
                crosswalk_id=2,
                other_index_id=3,
                index_id=3,
                mapping_level=None,
                value=576.0,
                proportion=0.5625,
            )
        ]
        self.assertEqual(list(results), expected)

        results = self.repository.find_by_ids(index_id=93)  # No index_id 93
        self.assertEqual(list(results), [], msg='should return empty iterator')

    def test_merge_one_and_two(self):
        self.repository.merge_by_index_id(index_ids=(1, 2), target=1)
        results = self.get_relations_helper()
        expected = {
            # First crosswalk.
            (10, 1, 1, 1, None,    131250.0,  1.0),
            (11, 1, 2, 1, b'\x40',  65536.0,  1.0),
            (4,  1, 3, 3, None,    100000.0,  1.0),
            # Second crosswalk.
            (12, 2, 1, 1, None,       583.75, 1.0),
            (14, 2, 2, 1, None,       416.25, 1.0),
            (13, 2, 3, 1, None,       448.0,  0.4375),
            (9,  2, 3, 3, None,       576.0,  0.5625),
        }
        self.assertEqual(results, expected)

    def test_merge_two_and_three(self):
        self.repository.merge_by_index_id(index_ids=(2, 3), target=2)
        results = self.get_relations_helper()
        expected = {
            # First crosswalk.
            (1,  1, 1, 1, None,   131250.0,  1.0),
            (2,  1, 2, 1, b'\x40', 40960.0,  0.625),
            (8,  1, 2, 2, b'\x40', 24576.0,  0.375),
            (11, 1, 3, 2, None,   100000.0,  1.0),
            # Second crosswalk.
            (5,  2, 1, 1, None,      583.75, 1.0),
            (9,  2, 2, 2, None,      416.25, 1.0),
            (7,  2, 3, 1, None,      336.0,  0.328125),
            (10, 2, 3, 2, None,      688.0,  0.671875),
        }
        self.assertEqual(results, expected)

    def test_merge_one_two_and_three(self):
        self.repository.merge_by_index_id(index_ids=(1, 2, 3), target=1)
        results = self.get_relations_helper()
        expected = {
            # First crosswalk.
            (1, 1, 1, 1, None,   131250.0,  1.0),
            (2, 1, 2, 1, b'\x40', 65536.0,  1.0),
            (6, 1, 3, 1, None,   100000.0,  1.0),
            # Second crosswalk.
            (3, 2, 1, 1, None,      583.75, 1.0),
            (5, 2, 2, 1, None,      416.25, 1.0),
            (4, 2, 3, 1, None,     1024.0,  1.0),
        }
        self.assertEqual(results, expected)

    def test_merge_target_inclusion(self):
        """Target id must be auto-added to index_ids if not included."""
        # The target (1) is not in index_ids (but should be included internally).
        self.repository.merge_by_index_id(index_ids=(2, 3), target=1)
        results = self.get_relations_helper()
        expected = {
            # First crosswalk.
            (1, 1, 1, 1, None,   131250.0,  1.0),
            (2, 1, 2, 1, b'\x40', 65536.0,  1.0),
            (6, 1, 3, 1, None,   100000.0,  1.0),
            # Second crosswalk.
            (3, 2, 1, 1, None,      583.75, 1.0),
            (5, 2, 2, 1, None,      416.25, 1.0),
            (4, 2, 3, 1, None,     1024.0,  1.0),
        }
        self.assertEqual(results, expected)

    def test_none_proportion(self):
        """When proportion is None, result should be None. It should
        not raise an error.
        """
        # Set one of the original proportions to None.
        relation = self.repository.get(3)
        relation.proportion = None
        self.repository.update(relation)

        # Merge index_ids 1, 2, and 3 into index_id 1.
        self.repository.merge_by_index_id(index_ids=(1, 2, 3), target=1)
        results = self.get_relations_helper()
        expected = {
            # First crosswalk.
            (1, 1, 1, 1, None,   131250.00, 1.0),
            (2, 1, 2, 1, b'\x40', 65536.00, None),  # <- Proportion should be None.
            (6, 1, 3, 1, None,   100000.00, 1.0),
            # Second crosswalk.
            (3, 2, 1, 1, None,      583.75, 1.0),
            (5, 2, 2, 1, None,      416.25, 1.0),
            (4, 2, 3, 1, None,     1024.00, 1.0),
        }
        self.assertEqual(results, expected)

    def test_get_distinct_other_index_ids(self):
        results = self.repository.get_distinct_other_index_ids(1)
        self.assertEqual(set(results), {1, 2, 3})

        results = self.repository.get_distinct_other_index_ids(1, ordered=True)
        self.assertEqual(list(results), [1, 2, 3])

    def test_find_by_ids(self):
        self.assertEqual(
            list(self.repository.find_by_ids(crosswalk_id=1)),
            [Relation(1, 1, 1, 1, None,   131250.0, 1.0),
             Relation(2, 1, 2, 1, b'\x40', 40960.0, 0.625),
             Relation(3, 1, 2, 2, b'\x40', 24576.0, 0.375),
             Relation(4, 1, 3, 3, None,   100000.0, 1.0)],
            msg='matches crosswalk_id 1',
        )

        self.assertEqual(
            list(self.repository.find_by_ids(other_index_id=2)),
            [Relation(2, 1, 2, 1, b'\x40', 40960.00, 0.625),
             Relation(3, 1, 2, 2, b'\x40', 24576.00, 0.375),
             Relation(6, 2, 2, 2, None,      416.25, 1.0)],
            msg='matches other_index_id 2 (includes records from crosswalks 1 and 2)',
        )

        self.assertEqual(
            list(self.repository.find_by_ids(index_id=1)),
            [Relation(1, 1, 1, 1, None,   131250.00, 1.0),
             Relation(2, 1, 2, 1, b'\x40', 40960.00, 0.625),
             Relation(5, 2, 1, 1, None,      583.75, 1.0),
             Relation(7, 2, 3, 1, None,      336.00, 0.328125)],
            msg='matches index_id 1 (includes records from crosswalks 1 and 2)',
        )

        self.assertEqual(
            list(self.repository.find_by_ids(other_index_id=1, index_id=1)),
            [Relation(1, 1, 1, 1, None, 131250.00, 1.0),
             Relation(5, 2, 1, 1, None,    583.75, 1.0)],
            msg='matches other_index_id 1 and index_id 1 (includes records from crosswalks 1 and 2)',
        )

        self.assertEqual(
            list(self.repository.find_by_ids(crosswalk_id=1, other_index_id=2)),
            [Relation(2, 1, 2, 1, b'\x40', 40960.0, 0.625),
             Relation(3, 1, 2, 2, b'\x40', 24576.0, 0.375)],
            msg='matches crosswalk_id 1 and other_index_id 2',
        )

        self.assertEqual(
            list(self.repository.find_by_ids(crosswalk_id=2, other_index_id=2, index_id=2)),
            [Relation(6, 2, 2, 2, None, 416.25, 1.0)],
            msg='matches crosswalk_id 2 and other_index_id 2 and index_id 2',
        )

        self.assertEqual(
            list(self.repository.find_by_ids()),
            [],
            msg='when no ids given, return empty iterator',
        )

    def test_refresh_proportions(self):
        # Delete some relations to introduce inconsistent proportions.
        self.repository.delete(2)
        self.repository.delete(9)

        # Fix inconsistencies with refresh_proportions().
        self.repository.refresh_proportions(crosswalk_id=1, other_index_id=2)
        self.repository.refresh_proportions(crosswalk_id=1, other_index_id=3)
        self.repository.refresh_proportions(crosswalk_id=2, other_index_id=2)
        self.repository.refresh_proportions(crosswalk_id=2, other_index_id=3)

        results = self.get_relations_helper()
        expected = {
            # First crosswalk.
            (1, 1, 1, 1, None,   131250.00, 1.00),
            (3, 1, 2, 2, b'\x40', 24576.00, 1.00),  # <- Proportion was 0.375
            (4, 1, 3, 3, None,   100000.00, 1.00),
            # Second crosswalk.
            (5, 2, 1, 1, None,      583.75, 1.00),
            (6, 2, 2, 2, None,      416.25, 1.00),
            (7, 2, 3, 1, None,      336.00, 0.75),  # <- Proportion was 0.328125
            (8, 2, 3, 2, None,      112.00, 0.25),  # <- Proportion was 0.109375
        }
        self.assertEqual(results, expected)

    def test_refresh_proportions_undefined_handling(self):
        """Check proportion handling for undefined points.

        * undefined-to-undefined (0 -> 0) should be 100%
        * undefined-to-defined (0 -> non-zero) should be 0%
        * defined-to-undefined (non-zero -> 0) is calculated normally.
        """
        self.crosswalk.add('333-33-3333', None, 'other3')  # Adds crosswalk_id 3.
        self.repository.add(3, 0, 0, None, 100.0, None)
        self.repository.add(3, 0, 1, None, 100.0, None)
        self.repository.add(3, 1, 1, None, 100.0, None)
        self.repository.add(3, 1, 0, None, 100.0, None)
        self.repository.add(3, 2, 2, None, 100.0, None)
        self.repository.add(3, 3, 1, None, 100.0, None)
        self.repository.add(3, 3, 2, None, 100.0, None)
        self.repository.add(3, 3, 0, None, 100.0, None)
        self.repository.add(3, 3, 3, None, 100.0, None)
        self.repository.add(3, 0, 3, None, 100.0, None)

        self.repository.refresh_proportions(crosswalk_id=3, other_index_id=0)
        self.repository.refresh_proportions(crosswalk_id=3, other_index_id=1)
        self.repository.refresh_proportions(crosswalk_id=3, other_index_id=2)
        self.repository.refresh_proportions(crosswalk_id=3, other_index_id=3)

        self.assertEqual(
            list(self.repository.find_by_ids(crosswalk_id=3)),
            [Relation(10, 3, 0, 0, None, 100.0, 1.0),  # <- 0 to 0 (100%, undefined to undefined)
             Relation(11, 3, 0, 1, None, 100.0, 0.0),  # <- 0 to 1 (0%)
             Relation(19, 3, 0, 3, None, 100.0, 0.0),  # <- 0 to 3 (0%)
             Relation(13, 3, 1, 0, None, 100.0, 0.5),
             Relation(12, 3, 1, 1, None, 100.0, 0.5),
             Relation(14, 3, 2, 2, None, 100.0, 1.0),
             Relation(17, 3, 3, 0, None, 100.0, 0.25),
             Relation(15, 3, 3, 1, None, 100.0, 0.25),
             Relation(16, 3, 3, 2, None, 100.0, 0.25),
             Relation(18, 3, 3, 3, None, 100.0, 0.25)],
        )


class PropertyRepositoryBaseTest(ABC):
    @property
    @abstractmethod
    def dal(self):
        ...

    def setUp(self):
        connector = self.dal.DataConnector()
        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        cursor = connector.acquire_cursor(connection)
        self.addCleanup(lambda: connector.release_cursor(cursor))

        self.repository = self.dal.PropertyRepository(cursor)

    def test_inheritance(self):
        """Should subclass from appropriate abstract base class."""
        self.assertTrue(issubclass(self.dal.PropertyRepository, BasePropertyRepository))

    def test_initial_properties(self):
        """Before adding any new properties, a newly-created node
        should have three pre-set keys:

        * toron_schema_version
        * toron_app_version
        * unique_id
        """
        repository = self.repository

        self.assertIsNotNone(repository.get('toron_schema_version'))
        self.assertIsNotNone(repository.get('toron_app_version'))
        self.assertIsNotNone(repository.get('unique_id'))

    def test_integration(self):
        """Test interoperation of add, get, update, and delete."""
        repository = self.repository

        value = {'foo': ['bar', 1234, 1234.5, True, False, None]}
        repository.add('foo', value)
        self.assertEqual(repository.get('foo'), value)

        value = {'baz': 42, 'qux': [True, False]}
        repository.update('foo', value)
        self.assertEqual(repository.get('foo'), value)

        repository.delete('foo')
        self.assertIsNone(repository.get('foo'))

    def test_keys_are_unique(self):
        """Attempting to add an existing key should raise an error."""
        self.repository.add('mykey', 'my value')

        with self.assertRaises(Exception):
            self.repository.add('mykey', 'some other value')

    def test_add_or_update(self):
        """Adding an existing key should replace the value."""
        self.repository.add('mykey', 'my value')  # Add initial value.

        self.repository.add_or_update('mykey', 'some other value')  # <- Method under test.
        self.assertEqual(self.repository.get('mykey'), 'some other value')


#######################################################################
# Test Cases for Concrete Data Model Classes
#######################################################################

class TestQuantityIterator(unittest.TestCase):
    def test_iterator_protocol(self):
        iterator = QuantityIterator(
            unique_id='0000-00-00-00-000000',
            index_hash='00000000000000000000000000000000',
            domain={},
            data=iter([]),  # <- Empty iterable for testing.
            label_names=['x', 'y'],
            attribute_keys=['a'],
        )

        self.assertIs(iter(iterator), iter(iterator))
        self.assertTrue(hasattr(iterator, '__next__'))
        with self.assertRaises(StopIteration):
            next(iterator)

    def test_properties(self):
        unique_id = '0000-00-00-00-000000'
        index_hash = '00000000000000000000000000000000'
        domain = {'foo': 'bar'}
        data = iter([])  # <- Empty iterable for testing.
        label_names = ('x', 'y')
        attribute_keys = ('a',)

        iterator = QuantityIterator(
            unique_id=unique_id,
            index_hash=index_hash,
            domain=domain,
            data=data,
            label_names=label_names,
            attribute_keys=attribute_keys,
        )

        # Check for expected getters.
        self.assertEqual(iterator.unique_id, unique_id)
        self.assertEqual(iterator.index_hash, index_hash)
        self.assertEqual(iterator.domain, domain)
        self.assertIs(iterator.data, data, msg='should be exact same object')
        self.assertEqual(iterator.label_names, label_names)
        self.assertEqual(iterator.attribute_keys, attribute_keys)

        # Check for read-only (no setters).
        with self.assertRaises(AttributeError):
            iterator.unique_id = '9999-99-99-99-999999'

        with self.assertRaises(AttributeError):
            iterator.index_hash = '99999999999999999999999999999999'

        with self.assertRaises(AttributeError):
            iterator.domain = {'baz': 'qux'}

        with self.assertRaises(AttributeError):
            iterator.data = iter([])

        with self.assertRaises(AttributeError):
            iterator.label_names = ('q', 'r')

        with self.assertRaises(AttributeError):
            iterator.attribute_keys = ('b',)

    def test_formatted_output(self):
        """Basic iteration should yield flattened, CSV-like rows."""
        iterator = QuantityIterator(
            unique_id='0000-00-00-00-000000',
            index_hash='00000000000000000000000000000000',
            domain={'xxx': 'yyy'},
            data=[
                (Index(1, 'FOO'), {'a': 'baz'}, 50.0),
                (Index(1, 'FOO'), {'a': 'qux'}, 55.0),
                (Index(2, 'BAR'), {'a': 'baz'}, 60.0),
                (Index(2, 'BAR'), {'a': 'qux'}, 65.0),
            ],
            label_names=['x'],
            attribute_keys=['a'],
        )

        self.assertEqual(
            iterator.columns,
            ('x', 'xxx', 'a', 'value'),
            msg='`columns` should be usable as a header row',
        )

        self.assertEqual(
            list(iterator),
            [('FOO', 'yyy', 'baz', 50.0),
             ('FOO', 'yyy', 'qux', 55.0),
             ('BAR', 'yyy', 'baz', 60.0),
             ('BAR', 'yyy', 'qux', 65.0)],
            msg='iteration should yield flattened rows',
        )


#######################################################################
# Concrete Test Cases for SQLite Backend
#######################################################################

from toron import dal1


class TestDataConnectorDAL1(DataConnectorBaseTest, unittest.TestCase):
    dal = dal1

class ColumnManagerDAL1(ColumnManagerBaseTest, unittest.TestCase):
    dal = dal1

class IndexRepositoryDAL1(IndexRepositoryBaseTest, unittest.TestCase):
    dal = dal1

class LocationRepositoryDAL1(LocationRepositoryBaseTest, unittest.TestCase):
    dal = dal1

class StructureRepositoryDAL1(StructureRepositoryBaseTest, unittest.TestCase):
    dal = dal1

class WeightRepositoryDAL1(WeightRepositoryBaseTest, unittest.TestCase):
    dal = dal1

class AttributeGroupRepositoryDAL1(AttributeGroupRepositoryBaseTest, unittest.TestCase):
    dal = dal1

class QuantityRepositoryDAL1(QuantityRepositoryBaseTest, unittest.TestCase):
    dal = dal1

class RelationRepositoryDAL1(RelationRepositoryBaseTest, unittest.TestCase):
    dal = dal1

class PropertyRepositoryDAL1(PropertyRepositoryBaseTest, unittest.TestCase):
    dal = dal1
