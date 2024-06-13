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
    Weight, BaseWeightRepository,
    Attribute, BaseAttributeRepository,
    Relation, BaseRelationRepository,
    BasePropertyRepository,
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

            with self.assertRaises(RuntimeError):
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

    def test_get_by_all_labels(self):
        self.manager.add_columns('A', 'B')
        self.repository.add('foo', 'x')  # <- Create existing location.

        self.assertEqual(
            self.repository.get_by_all_labels({'A': 'foo', 'B': 'x'}),
            Location(1, 'foo', 'x'),
            msg='should return existing location',
        )

        self.assertIsNone(
            self.repository.get_by_all_labels({'A': 'bar', 'B': 'y'}),
            msg='should return None if specified labels are missing',
        )

        self.assertEqual(
            self.repository.get_by_all_labels({'A': 'bar', 'B': 'y'}, add_if_missing=True),
            Location(2, 'bar', 'y'),
            msg='should create and return a new location when `add_if_missing` is True',
        )

        self.assertEqual(
            self.repository.get_by_all_labels({'A': 'bar', 'B': 'y'}),
            Location(2, 'bar', 'y'),
            msg='should now return existing (previously created) location',
        )

        regex = r'requires all label columns, got: A \(needs A, B\)'
        with self.assertRaisesRegex(ValueError, regex):
            self.repository.get_by_all_labels({'A': 'foo'})

        regex = r'requires all label columns, got: nothing \(needs A, B\)'
        with self.assertRaisesRegex(ValueError, regex):
            self.repository.get_by_all_labels(dict())

        regex = r'requires all label columns, got: A, B, C \(needs A, B\)'
        with self.assertRaisesRegex(ValueError, regex):
            self.repository.get_by_all_labels({'A': 'bar', 'B': 'y', 'C': 'z'})


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


class AttributeRepositoryBaseTest(ABC):
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

        self.repository = self.dal.AttributeRepository(cursor)

    def test_inheritance(self):
        """Must inherit from appropriate abstract base class."""
        self.assertTrue(isinstance(self.repository, BaseAttributeRepository))

    def test_integration(self):
        """Test interoperation of add, get, update, and delete."""
        repository = self.repository

        repository.add({'foo': 'A'})
        self.assertEqual(repository.get(1), Attribute(1, {'foo': 'A'}))

        repository.update(Attribute(1, {'foo': 'B'}))
        self.assertEqual(repository.get(1), Attribute(1, {'foo': 'B'}))

        repository.delete(1)
        self.assertIsNone(repository.get(1))

    def test_get_by_value(self):
        self.repository.add({'foo': 'A'})
        self.assertEqual(
            self.repository.get_by_value({'foo': 'A'}),
            Attribute(1, {'foo': 'A'}),
        )

    def test_get_or_add_by_value(self):
        self.repository.add({'foo': 'A'})  # <- Create attribute_id 1.

        self.assertEqual(
            self.repository.get_or_add_by_value({'foo': 'A'}),
            Attribute(1, {'foo': 'A'}),
            msg='should return existing attribute_id 1',
        )

        self.assertEqual(
            self.repository.get_or_add_by_value({'foo': 'B'}),  # <- Creates attribute_id 2.
            Attribute(2, {'foo': 'B'}),
            msg='should create and return record with attribute_id 2'
        )

        self.assertEqual(
            self.repository.get_or_add_by_value({'foo': 'B'}),  # <- Gets existing attribute_id 2.
            Attribute(2, {'foo': 'B'}),
            msg='should return existing record with attribute_id 2'
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

        self.repository.add(1, 1, 1, 131250, None,    1.0)
        self.repository.add(1, 2, 1,  40960, b'\x40', 0.625)
        self.repository.add(1, 2, 2,  24576, b'\x40', 0.375)
        self.repository.add(1, 3, 3, 100000, None,    1.0)

        self.repository.add(2, 1, 1, 583.75, None, 1.0)
        self.repository.add(2, 2, 2, 416.25, None, 1.0)
        self.repository.add(2, 3, 1, 336.0,  None, 0.328125)
        self.repository.add(2, 3, 2, 112.0,  None, 0.109375)
        self.repository.add(2, 3, 3, 576.0,  None, 0.5625)

    def get_relations_helper(self):  # <- Helper function.
        # TODO: Update this helper when proper interface is available.
        cur = self.connection.execute('SELECT * FROM main.relation')
        return set(cur.fetchall())

    def test_inheritance(self):
        """Must inherit from appropriate abstract base class."""
        self.assertTrue(issubclass(self.dal.RelationRepository, BaseRelationRepository))

    def test_find_by_index_id(self):
        results = self.repository.find_by_ids(index_id=3)
        expected = [
            Relation(
                id=4,
                crosswalk_id=1,
                other_index_id=3,
                index_id=3,
                value=100000.0,
                mapping_level=None,
                proportion=1.0,
            ),
            Relation(
                id=9,
                crosswalk_id=2,
                other_index_id=3,
                index_id=3,
                value=576.0,
                mapping_level=None,
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
            (10, 1, 1, 1, 131250.0, None,    1.0),
            (11, 1, 2, 1, 65536.0,  b'\x40', 1.0),
            (4,  1, 3, 3, 100000.0, None,    1.0),
            # Second crosswalk.
            (12, 2, 1, 1, 583.75,   None,    1.0),
            (14, 2, 2, 1, 416.25,   None,    1.0),
            (13, 2, 3, 1, 448.0,    None,    0.4375),
            (9,  2, 3, 3, 576.0,    None,    0.5625),
        }
        self.assertEqual(results, expected)

    def test_merge_two_and_three(self):
        self.repository.merge_by_index_id(index_ids=(2, 3), target=2)
        results = self.get_relations_helper()
        expected = {
            # First crosswalk.
            (1,  1, 1, 1, 131250.0, None,    1.0),
            (2,  1, 2, 1, 40960.0,  b'\x40', 0.625),
            (8,  1, 2, 2, 24576.0,  b'\x40', 0.375),
            (11, 1, 3, 2, 100000.0, None,    1.0),
            # Second crosswalk.
            (5,  2, 1, 1, 583.75, None,      1.0),
            (9,  2, 2, 2, 416.25, None,      1.0),
            (7,  2, 3, 1, 336.0,  None,      0.328125),
            (10, 2, 3, 2, 688.0,  None,      0.671875),
        }
        self.assertEqual(results, expected)

    def test_merge_one_two_and_three(self):
        self.repository.merge_by_index_id(index_ids=(1, 2, 3), target=1)
        results = self.get_relations_helper()
        expected = {
            # First crosswalk.
            (1, 1, 1, 1, 131250.0, None,    1.0),
            (2, 1, 2, 1, 65536.0,  b'\x40', 1.0),
            (6, 1, 3, 1, 100000.0, None,    1.0),
            # Second crosswalk.
            (3, 2, 1, 1, 583.75,   None,    1.0),
            (5, 2, 2, 1, 416.25,   None,    1.0),
            (4, 2, 3, 1, 1024.0,   None,    1.0),
        }
        self.assertEqual(results, expected)

    def test_merge_target_inclusion(self):
        """Target id must be auto-added to index_ids if not included."""
        # The target (1) is not in index_ids (but should be included internally).
        self.repository.merge_by_index_id(index_ids=(2, 3), target=1)
        results = self.get_relations_helper()
        expected = {
            # First crosswalk.
            (1, 1, 1, 1, 131250.0, None,    1.0),
            (2, 1, 2, 1, 65536.0,  b'\x40', 1.0),
            (6, 1, 3, 1, 100000.0, None,    1.0),
            # Second crosswalk.
            (3, 2, 1, 1, 583.75,   None,    1.0),
            (5, 2, 2, 1, 416.25,   None,    1.0),
            (4, 2, 3, 1, 1024.0,   None,    1.0),
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
            (1, 1, 1, 1, 131250.0, None,    1.0),
            (2, 1, 2, 1, 65536.0,  b'\x40', None),  # <- Proportion should be None.
            (6, 1, 3, 1, 100000.0, None,    1.0),
            # Second crosswalk.
            (3, 2, 1, 1, 583.75,   None,    1.0),
            (5, 2, 2, 1, 416.25,   None,    1.0),
            (4, 2, 3, 1, 1024.0,   None,    1.0),
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
            [Relation(1, 1, 1, 1, 131250.0, None,    1.0),
             Relation(2, 1, 2, 1, 40960.0,  b'\x40', 0.625),
             Relation(3, 1, 2, 2, 24576.0,  b'\x40', 0.375),
             Relation(4, 1, 3, 3, 100000.0, None,    1.0)],
            msg='matches crosswalk_id 1',
        )

        self.assertEqual(
            list(self.repository.find_by_ids(other_index_id=2)),
            [Relation(2, 1, 2, 1, 40960.0, b'\x40', 0.625),
             Relation(3, 1, 2, 2, 24576.0, b'\x40', 0.375),
             Relation(6, 2, 2, 2, 416.25,  None,    1.0)],
            msg='matches other_index_id 2 (includes records from crosswalks 1 and 2)',
        )

        self.assertEqual(
            list(self.repository.find_by_ids(index_id=1)),
            [Relation(1, 1, 1, 1, 131250.0, None,    1.0),
             Relation(2, 1, 2, 1, 40960.0,  b'\x40', 0.625),
             Relation(5, 2, 1, 1, 583.75,   None,    1.0),
             Relation(7, 2, 3, 1, 336.0,    None,    0.328125)],
            msg='matches index_id 1 (includes records from crosswalks 1 and 2)',
        )

        self.assertEqual(
            list(self.repository.find_by_ids(other_index_id=1, index_id=1)),
            [Relation(1, 1, 1, 1, 131250.0, None, 1.0),
             Relation(5, 2, 1, 1, 583.75,   None, 1.0)],
            msg='matches other_index_id 1 and index_id 1 (includes records from crosswalks 1 and 2)',
        )

        self.assertEqual(
            list(self.repository.find_by_ids(crosswalk_id=1, other_index_id=2)),
            [Relation(2, 1, 2, 1, 40960.0,  b'\x40', 0.625),
             Relation(3, 1, 2, 2, 24576.0,  b'\x40', 0.375)],
            msg='matches crosswalk_id 1 and other_index_id 2',
        )

        self.assertEqual(
            list(self.repository.find_by_ids(crosswalk_id=2, other_index_id=2, index_id=2)),
            [Relation(6, 2, 2, 2, 416.25, None, 1.0)],
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
            (1, 1, 1, 1, 131250.0, None,    1.0),
            (3, 1, 2, 2, 24576.0,  b'\x40', 1.0),  # <- Proportion was 0.375
            (4, 1, 3, 3, 100000.0, None,    1.0),
            # Second crosswalk.
            (5, 2, 1, 1, 583.75,   None,    1.0),
            (6, 2, 2, 2, 416.25,   None,    1.0),
            (7, 2, 3, 1, 336.0,    None,    0.75),  # <- Proportion was 0.328125
            (8, 2, 3, 2, 112.0,    None,    0.25),  # <- Proportion was 0.109375
        }
        self.assertEqual(results, expected)


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

class WeightRepositoryDAL1(WeightRepositoryBaseTest, unittest.TestCase):
    dal = dal1

class AttributeRepositoryDAL1(AttributeRepositoryBaseTest, unittest.TestCase):
    dal = dal1

class RelationRepositoryDAL1(RelationRepositoryBaseTest, unittest.TestCase):
    dal = dal1

class PropertyRepositoryDAL1(PropertyRepositoryBaseTest, unittest.TestCase):
    dal = dal1
