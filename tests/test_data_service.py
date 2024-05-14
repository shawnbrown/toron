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

        column_manager = dal.ColumnManager(cur)
        column_manager.add_columns('A', 'B', 'C')

        self.property_repo = dal.PropertyRepository(cur)

    def test_rename(self):
        self.property_repo.add('discrete_categories', [['A'], ['B'], ['A', 'C']])

        rename_discrete_categories({'B': 'X', 'C': 'Z'}, self.property_repo)

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

        self.column_manager = dal.ColumnManager(cur)
        self.column_manager.add_columns('A', 'B', 'C')

        self.property_repo = dal.PropertyRepository(cur)
        self.structure_repo = dal.StructureRepository(cur)

    def test_generate(self):
        self.property_repo.add('discrete_categories', [['A'], ['B'], ['A', 'C']])

        rebuild_structure_table(self.column_manager, self.property_repo, self.structure_repo)

        expected = [
            Structure(1, None, 0, 0, 0),
            Structure(2, None, 1, 0, 0),
            Structure(3, None, 0, 1, 0),
            Structure(4, None, 1, 0, 1),
            Structure(5, None, 1, 1, 0),
            Structure(6, None, 1, 1, 1),
        ]
        self.assertEqual(self.structure_repo.get_all(), expected)
