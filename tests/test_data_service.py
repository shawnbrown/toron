"""Tests for toron/data_service.py module."""

import unittest

from toron.data_models import (
    Structure,
    Crosswalk,
)
from toron import data_access
from toron.data_service import (
    find_crosswalks_by_node_reference,
    rename_discrete_categories,
    rebuild_structure_table,
)


class TestFindCrosswalksByNodeReference(unittest.TestCase):
    def setUp(self):
        dal = data_access.get_data_access_layer()

        connector = dal.DataConnector()
        con = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(con))
        cur = connector.acquire_cursor(con)
        self.addCleanup(lambda: connector.release_cursor(cur))

        col_manager = dal.ColumnManager(cur)
        index_repo = dal.IndexRepository(cur)
        crosswalk_repo = dal.CrosswalkRepository(cur)

        col_manager.add_columns('A', 'B')
        index_repo.add('foo', 'x')
        index_repo.add('bar', 'y')
        index_repo.add('bar', 'z')
        crosswalk_repo.add('111-111-1111', 'file1.toron', 'crosswalk1')  # Add crosswalk_id 1.
        crosswalk_repo.add('111-111-1111', 'file1.toron', 'crosswalk2')  # Add crosswalk_id 2.
        crosswalk_repo.add('222-222-2222', 'file2.toron', 'crosswalk2')  # Add crosswalk_id 3.

        self.crosswalk_repo = crosswalk_repo

    def test_other_unique_id(self):
        """Should return exact match on 'other_unique_id' value."""
        crosswalks = find_crosswalks_by_node_reference('111-111-1111', self.crosswalk_repo)
        self.assertEqual([x.id for x in crosswalks], [1, 2])

        crosswalks = find_crosswalks_by_node_reference('222-222-2222', self.crosswalk_repo)
        self.assertEqual([x.id for x in crosswalks], [3])

    def test_other_filename_hint(self):
        """Should return exact match on 'other_filename_hint' value."""
        crosswalks = find_crosswalks_by_node_reference('file1.toron', self.crosswalk_repo)
        self.assertEqual([x.id for x in crosswalks], [1, 2])

    def test_other_filename_hint_stem_only(self):
        """Should return match on filename stem of 'other_filename_hint'
        value (matches name without ".toron" extension).
        """
        crosswalks = find_crosswalks_by_node_reference('file1', self.crosswalk_repo)  # <- Stem 'file1'.
        self.assertEqual([x.id for x in crosswalks], [1, 2])

    def test_other_unique_id_shortcode(self):
        """If node reference is 7 characters or more, try to match the
        start of 'other_unique_id' values.
        """
        crosswalks = find_crosswalks_by_node_reference('111-111', self.crosswalk_repo)  # <- Short code.
        self.assertEqual([x.id for x in crosswalks], [1, 2])

    def test_no_match(self):
        crosswalks = find_crosswalks_by_node_reference('unknown-reference', self.crosswalk_repo)
        self.assertEqual(crosswalks, [])

    def test_empty(self):
        crosswalks = find_crosswalks_by_node_reference('', self.crosswalk_repo)
        self.assertEqual(crosswalks, [])

        crosswalks = find_crosswalks_by_node_reference(None, self.crosswalk_repo)
        self.assertEqual(crosswalks, [])


class TestRenameDiscreteCategories(unittest.TestCase):
    def setUp(self):
        dal = data_access.get_data_access_layer()

        connector = dal.DataConnector()
        con = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(con))
        cur = connector.acquire_cursor(con)
        self.addCleanup(lambda: connector.release_cursor(cur))

        self.column_manager = dal.ColumnManager(cur)
        self.column_manager.add_columns('A', 'B', 'C')

        self.property_repo = dal.PropertyRepository(cur)

    def test_rename(self):
        self.property_repo.add('discrete_categories', [['A'], ['B'], ['A', 'C']])

        rename_discrete_categories({'B': 'X', 'C': 'Z'}, self.column_manager, self.property_repo)

        categories = self.property_repo.get('discrete_categories')
        self.assertEqual(
            [set(cat) for cat in categories],
            [{'A'}, {'X'}, {'A', 'Z'}],
        )

class TestRebuildStructureTable(unittest.TestCase):
    def setUp(self):
        dal = data_access.get_data_access_layer()

        connector = dal.DataConnector()
        con = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(con))
        cur = connector.acquire_cursor(con)
        self.addCleanup(lambda: connector.release_cursor(cur))
        alt_cur = connector.acquire_cursor(con)
        self.addCleanup(lambda: connector.release_cursor(alt_cur))

        self.column_manager = dal.ColumnManager(cur)
        self.property_repo = dal.PropertyRepository(cur)
        self.structure_repo = dal.StructureRepository(cur)
        self.index_repo = dal.IndexRepository(cur)
        self.alt_index_repo = dal.IndexRepository(alt_cur)

        self.column_manager.add_columns('A', 'B', 'C')
        self.index_repo.add('a1', 'b1', 'c1')
        self.index_repo.add('a1', 'b1', 'c2')
        self.index_repo.add('a1', 'b2', 'c3')
        self.index_repo.add('a1', 'b2', 'c4')
        self.index_repo.add('a2', 'b3', 'c5')
        self.index_repo.add('a2', 'b3', 'c6')
        self.index_repo.add('a2', 'b4', 'c7')
        self.index_repo.add('a2', 'b4', 'c8')

    def test_rebuild_structure(self):
        self.property_repo.add('discrete_categories', [['A'], ['A', 'B'], ['A', 'B', 'C']])

        rebuild_structure_table(
            self.column_manager,
            self.property_repo,
            self.structure_repo,
            self.index_repo,
            self.alt_index_repo,
        )

        expected = [
            Structure(id=4, granularity=3.0, bits=(1, 1, 1)),
            Structure(id=3, granularity=2.0, bits=(1, 1, 0)),
            Structure(id=2, granularity=1.0, bits=(1, 0, 0)),
            Structure(id=1, granularity=0.0, bits=(0, 0, 0)),
        ]
        self.assertEqual(self.structure_repo.get_all(), expected)

    def test_rebuild_structure_no_categories(self):
        """When no discrete categories are defined, the function should
        build the "trivial topology".

        The trivial topology (also called the "indiscrete topology")
        is one where the only open sets are the empty set (represented
        by bits ``(0, 0, 0)``) and the entire space (represented by
        ``(1, 1, 1)``).
        """
        self.property_repo.delete('discrete_categories')  # <- No categories!

        rebuild_structure_table(
            self.column_manager,
            self.property_repo,
            self.structure_repo,
            self.index_repo,
            self.alt_index_repo,
        )

        trivial_topology = [
            Structure(id=2, granularity=3.0, bits=(1, 1, 1)),
            Structure(id=1, granularity=0.0, bits=(0, 0, 0)),
        ]
        self.assertEqual(self.structure_repo.get_all(), trivial_topology)
