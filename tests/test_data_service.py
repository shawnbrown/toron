"""Tests for toron/data_service.py module."""

import array
import unittest
from .common import normalize_structures

from toron.data_models import (
    Index,
    WeightGroup,
    Structure,
    Crosswalk,
)
from toron import data_access
from toron._utils import ToronWarning
from toron.data_service import (
    validate_new_index_columns,
    get_quantity_value_sum,
    disaggregate_value,
    find_crosswalks_by_ref,
    set_default_weight_group,
    get_default_weight_group,
    rename_discrete_categories,
    rebuild_structure_table,
    add_discrete_categories,
    refresh_structure_granularity,
    set_domain,
    get_domain,
)


class TestValidateNewIndexColumns(unittest.TestCase):
    def setUp(self):
        dal = data_access.get_data_access_layer()

        connector = dal.DataConnector()
        con = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(con))
        cur = connector.acquire_cursor(con)
        self.addCleanup(lambda: connector.release_cursor(cur))

        self.reserved_identifiers = dal.reserved_identifiers
        self.column_manager = dal.ColumnManager(cur)
        self.property_repo = dal.PropertyRepository(cur)
        self.attribute_repo = dal.AttributeGroupRepository(cur)

    def test_valid(self):
        validate_new_index_columns(
            new_column_names=iter(['baz', 'qux']),
            reserved_identifiers=self.reserved_identifiers,
            column_manager=self.column_manager,
            property_repo=self.property_repo,
            attribute_repo=self.attribute_repo,
        )

    def test_reserved_identifier_collision(self):
        regex = "cannot alter columns, 'value' is a reserved identifier"
        with self.assertRaisesRegex(ValueError, regex):
            validate_new_index_columns(
                new_column_names=iter(['value']),
                reserved_identifiers=self.reserved_identifiers,
                column_manager=self.column_manager,
                property_repo=self.property_repo,
                attribute_repo=self.attribute_repo,
            )

    def test_column_collision(self):
        self.column_manager.add_columns('foo', 'bar', 'baz')

        regex = "cannot alter columns, 'baz' is already an index column"
        with self.assertRaisesRegex(ValueError, regex):
            validate_new_index_columns(
                new_column_names=iter(['baz', 'qux']),
                reserved_identifiers=self.reserved_identifiers,
                column_manager=self.column_manager,
                property_repo=self.property_repo,
                attribute_repo=self.attribute_repo,
            )

    def test_domain_collision(self):
        self.property_repo.add('domain', {'qux': '444'})

        regex = "cannot alter columns, 'qux' is used in the domain"
        with self.assertRaisesRegex(ValueError, regex):
            validate_new_index_columns(
                new_column_names=iter(['baz', 'qux']),
                reserved_identifiers=self.reserved_identifiers,
                column_manager=self.column_manager,
                property_repo=self.property_repo,
                attribute_repo=self.attribute_repo,
            )

    def test_attribute_collision(self):
        self.attribute_repo.add({'corge': '555'})

        regex = "cannot alter columns, 'corge' is used as an attribute name"
        with self.assertRaisesRegex(ValueError, regex):
            validate_new_index_columns(
                new_column_names=iter(['qux', 'corge']),
                reserved_identifiers=self.reserved_identifiers,
                column_manager=self.column_manager,
                property_repo=self.property_repo,
                attribute_repo=self.attribute_repo,
            )


class TestGetQuantityValueSum(unittest.TestCase):
    def setUp(self):
        dal = data_access.get_data_access_layer()

        connector = dal.DataConnector()
        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        cursor = connector.acquire_cursor(connection)
        self.addCleanup(lambda: connector.release_cursor(cursor))

        # Set-up test values.
        manager = dal.ColumnManager(cursor)
        manager.add_columns('A', 'B')

        location_repo = dal.LocationRepository(cursor)
        location_repo.add('foo', 'qux')   # Add location_id 1
        location_repo.add('bar', 'quux')  # Add location_id 2

        attribute_repo = dal.AttributeGroupRepository(cursor)
        attribute_repo.add({'aaa': 'one'})  # Add attribute_group_id 1
        attribute_repo.add({'bbb': 'two'})  # Add attribute_group_id 2

        quantity_repo = dal.QuantityRepository(cursor)
        quantity_repo.add(location_id=1, attribute_group_id=1, value=20.0)  # Add quantity_id 1
        quantity_repo.add(location_id=1, attribute_group_id=2, value=0.0)   # Add quantity_id 2
        quantity_repo.add(location_id=2, attribute_group_id=2, value=10.0)  # Add quantity_id 3
        quantity_repo.add(location_id=2, attribute_group_id=2, value=35.0)  # Add quantity_id 4

        self.quantity_repo = quantity_repo

    def test_sum_of_single_item(self):
        self.assertEqual(get_quantity_value_sum(1, 1, self.quantity_repo), 20.0)

    def test_sum_of_single_item_zero(self):
        self.assertEqual(get_quantity_value_sum(1, 2, self.quantity_repo), 0.0)

    def test_sum_of_multiple_items(self):
        """Should sum the ``value`` of multiple matching quantities."""
        self.assertIsInstance(get_quantity_value_sum(2, 2, self.quantity_repo), float)

    def test_missing_item(self):
        """Should return None when there are no matching quantities."""
        self.assertIsNone(get_quantity_value_sum(3, 1, self.quantity_repo))


class TestDisaggregateValue(unittest.TestCase):
    def setUp(self):
        dal = data_access.get_data_access_layer()

        connector = dal.DataConnector()
        connection = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(connection))

        # The index and weight repositories must use different cursors.
        aux1_cursor = connector.acquire_cursor(connection)
        self.addCleanup(lambda: connector.release_cursor(aux1_cursor))
        aux2_cursor = connector.acquire_cursor(connection)
        self.addCleanup(lambda: connector.release_cursor(aux2_cursor))

        index_repo = dal.IndexRepository(aux1_cursor)
        weight_repo = dal.WeightRepository(aux2_cursor)

        # Set-up test values.
        try:
            cursor = connector.acquire_cursor(connection)
            manager = dal.ColumnManager(cursor)
            weight_group_repo = dal.WeightGroupRepository(cursor)

            manager.add_columns('A', 'B')
            index_repo.add('OH', 'BUTLER')    # index_id 1
            index_repo.add('OH', 'FRANKLIN')  # index_id 2
            index_repo.add('IN', 'KNOX')      # index_id 3
            index_repo.add('IN', 'LAPORTE')   # index_id 4
            weight_group_repo.add('totpop', is_complete=True)  # weight_group_id 1
            weight_repo.add(weight_group_id=1, index_id=1, value=374150)
            weight_repo.add(weight_group_id=1, index_id=2, value=1336250)
            weight_repo.add(weight_group_id=1, index_id=3, value=36864)
            weight_repo.add(weight_group_id=1, index_id=4, value=110592)
            weight_group_repo.add('empty', is_complete=True)  # weight_group_id 2
            weight_repo.add(weight_group_id=2, index_id=1, value=0)
            weight_repo.add(weight_group_id=2, index_id=2, value=0)
            weight_repo.add(weight_group_id=2, index_id=3, value=0)
            weight_repo.add(weight_group_id=2, index_id=4, value=0)
        finally:
            connector.release_cursor(cursor)

        self.weight_repo = weight_repo

    def test_single_result(self):
        """Result should keep whole quantity with only matching index."""
        results = disaggregate_value(
            quantity_value=10000,
            index_ids=[2],
            weight_group_id=1,
            weight_repo=self.weight_repo,
        )
        expected = [
            (2, 10000.0),  # 'OH', 'FRANKLIN'
        ]
        self.assertEqual(list(results), expected)

    def test_multiple_results(self):
        """Result should divide quantity across multiple matching indexes."""
        results = disaggregate_value(
            quantity_value=10000,
            index_ids=[3, 4],
            weight_group_id=1,
            weight_repo=self.weight_repo,
        )
        expected = [
            (3, 2500.0),  # 'IN', 'KNOX'
            (4, 7500.0),  # 'IN', 'LAPORTE'
        ]
        self.assertEqual(list(results), expected)

    def test_multiple_results_using_array_of_index_ids(self):
        """Should work with `array` type input, too."""
        results = disaggregate_value(
            quantity_value=10000,
            index_ids=array.array('i', [3, 4]),
            weight_group_id=1,
            weight_repo=self.weight_repo,
        )
        expected = [
            (3, 2500.0),  # 'IN', 'KNOX'
            (4, 7500.0),  # 'IN', 'LAPORTE'
        ]
        self.assertEqual(list(results), expected)

    def test_zero_weight_single_result(self):
        """When weight sum is 0, should still keep whole quantity when
        only matching index.
        """
        results = disaggregate_value(
            quantity_value=10000,
            index_ids=[2],
            weight_group_id=2,  # <- Weight group 2 has weights of 0.
            weight_repo=self.weight_repo,
        )
        expected = [
            (2, 10000.0),  # 'OH', 'FRANKLIN'
        ]
        self.assertEqual(list(results), expected)

    def test_zero_weight_multiple_results(self):
        """When weight sum is 0, should be divided evenly among indexes."""
        results = disaggregate_value(
            quantity_value=10000,
            index_ids=[3, 4],
            weight_group_id=2,  # <- Weight group 2 has weights of 0.
            weight_repo=self.weight_repo,
        )
        expected = [
            (3, 5000.0),  # <- Divided evenly among indexes ('IN', 'KNOX')
            (4, 5000.0),  # <- Divided evenly among indexes ('IN', 'LAPORTE').
        ]
        self.assertEqual(list(results), expected)

    def test_matching_undefined_record(self):
        """When matching undefined record, should return value as-is."""
        results = disaggregate_value(
            quantity_value=10000,
            index_ids=[0],
            weight_group_id=1,
            weight_repo=self.weight_repo,
        )
        expected = [
            (0, 10000),  # <- index_id 0 is undefined record ('-', '-').
        ]
        self.assertEqual(list(results), expected)

    def test_no_matching_weight(self):
        regex = 'no weight value matching weight_group_id 9 and index_id 3'
        with self.assertRaisesRegex(RuntimeError, regex):
            results = disaggregate_value(
                quantity_value=10000,
                index_ids=[3],
                weight_group_id=9,  # <- No weight_group_id 9 exists!
                weight_repo=self.weight_repo,
            )
            list(results)  # Consume iterator.

    def test_no_matching_index(self):
        regex = 'no weight value matching weight_group_id 1 and index_id 999'
        with self.assertRaisesRegex(RuntimeError, regex):
            results = disaggregate_value(
                quantity_value=10000,
                index_ids=[999],  # <- No index matching 999!
                weight_group_id=1,
                weight_repo=self.weight_repo,
            )
            list(results)  # Consume iterator.


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
        crosswalks = find_crosswalks_by_ref('111-111-1111', self.crosswalk_repo)
        self.assertEqual([x.id for x in crosswalks], [1, 2])

        crosswalks = find_crosswalks_by_ref('222-222-2222', self.crosswalk_repo)
        self.assertEqual([x.id for x in crosswalks], [3])

    def test_other_filename_hint(self):
        """Should return exact match on 'other_filename_hint' value."""
        crosswalks = find_crosswalks_by_ref('file1.toron', self.crosswalk_repo)
        self.assertEqual([x.id for x in crosswalks], [1, 2])

    def test_other_filename_hint_stem_only(self):
        """Should return match on filename stem of 'other_filename_hint'
        value (matches name without ".toron" extension).
        """
        crosswalks = find_crosswalks_by_ref('file1', self.crosswalk_repo)  # <- Stem 'file1'.
        self.assertEqual([x.id for x in crosswalks], [1, 2])

    def test_other_unique_id_shortcode(self):
        """If node reference is 7 characters or more, try to match the
        start of 'other_unique_id' values.
        """
        crosswalks = find_crosswalks_by_ref('111-111', self.crosswalk_repo)  # <- Short code.
        self.assertEqual([x.id for x in crosswalks], [1, 2])

    def test_no_match(self):
        crosswalks = find_crosswalks_by_ref('unknown-reference', self.crosswalk_repo)
        self.assertEqual(crosswalks, [])

    def test_empty(self):
        crosswalks = find_crosswalks_by_ref('', self.crosswalk_repo)
        self.assertEqual(crosswalks, [])

        crosswalks = find_crosswalks_by_ref(None, self.crosswalk_repo)
        self.assertEqual(crosswalks, [])


class TestGetAndSetDefaultWeightGroup(unittest.TestCase):
    def setUp(self):
        dal = data_access.get_data_access_layer()

        connector = dal.DataConnector()
        con = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(con))
        cur = connector.acquire_cursor(con)
        self.addCleanup(lambda: connector.release_cursor(cur))

        self.weight_group_repo = dal.WeightGroupRepository(cur)
        self.property_repo = dal.PropertyRepository(cur)

    def test_set_default_weight_group(self):
        # Set default group when none previously exists.
        set_default_weight_group(
            weight_group=WeightGroup(3, 'name1', None, None),
            property_repo=self.property_repo,
        )
        self.assertEqual(
            self.property_repo.get('default_weight_group_id'),
            3,
            msg="expecting weight_group's `id` value (an int)",
        )

        # Update existing default group to new value.
        set_default_weight_group(
            weight_group=WeightGroup(6, 'name2', None, None),
            property_repo=self.property_repo,
        )
        self.assertEqual(
            self.property_repo.get('default_weight_group_id'),
            6,
            msg='expecting updated id value',
        )

        # Replace existing default group with None.
        set_default_weight_group(
            weight_group=None,
            property_repo=self.property_repo,
        )
        self.assertIsNone(self.property_repo.get('default_weight_group_id'))

    def test_get_default_weight_group(self):
        self.weight_group_repo.add('foo')  # Adds weight_group_id 1
        self.weight_group_repo.add('bar')  # Adds weight_group_id 2
        self.property_repo.add('default_weight_group_id', 2)  # <- Save id 2 as default.

        weight_group = get_default_weight_group(
            property_repo=self.property_repo,
            weight_group_repo=self.weight_group_repo,
        )
        expected = self.weight_group_repo.get(2)
        self.assertEqual(weight_group, expected)

    def test_get_default_weight_group_missing(self):
        weight_group = get_default_weight_group(
            property_repo=self.property_repo,
            weight_group_repo=self.weight_group_repo,
            required=False,  # <- When not required, missing default returns None.
        )
        self.assertIsNone(weight_group)

        regex = 'no default weight group is defined'
        with self.assertRaisesRegex(RuntimeError, regex):
            weight_group = get_default_weight_group(
                property_repo=self.property_repo,
                weight_group_repo=self.weight_group_repo,
                required=True,  # <- When required, missing default raises error.
            )


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
        self.optimizations = dal.optimizations

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

        expected = [
            Structure(id=4, granularity=3.0,  bits=(1, 1, 1)),
            Structure(id=3, granularity=2.0,  bits=(1, 1, 0)),
            Structure(id=2, granularity=1.0,  bits=(1, 0, 0)),
            Structure(id=1, granularity=None, bits=(0, 0, 0)),
        ]

        # Using standard granularity function.
        rebuild_structure_table(
            self.column_manager,
            self.property_repo,
            self.structure_repo,
            self.index_repo,
            self.alt_index_repo,
            optimizations=None,
        )
        self.assertEqual(self.structure_repo.get_all(), expected)

        # Using optimized granularity function.
        rebuild_structure_table(
            self.column_manager,
            self.property_repo,
            self.structure_repo,
            self.index_repo,
            self.alt_index_repo,
            optimizations=self.optimizations,
        )
        self.assertEqual(normalize_structures(self.structure_repo.get_all()), expected)

    def test_rebuild_structure_no_categories(self):
        """When no discrete categories are defined, the function should
        build the "trivial topology".

        The trivial topology (also called the "indiscrete topology")
        is one where the only open sets are the empty set (represented
        by all zeros e.g., ``(0, 0, 0)``) and the entire space
        (represented by all ones, e.g., ``(1, 1, 1)``).
        """
        self.property_repo.delete('discrete_categories')  # <- No categories!

        trivial_topology = [
            Structure(id=2, granularity=3.0,  bits=(1, 1, 1)),
            Structure(id=1, granularity=None, bits=(0, 0, 0)),
        ]

        # Using standard granularity function.
        rebuild_structure_table(
            self.column_manager,
            self.property_repo,
            self.structure_repo,
            self.index_repo,
            self.alt_index_repo,
            optimizations=None,
        )
        self.assertEqual(self.structure_repo.get_all(), trivial_topology)

        # Using optimized granularity function.
        rebuild_structure_table(
            self.column_manager,
            self.property_repo,
            self.structure_repo,
            self.index_repo,
            self.alt_index_repo,
            optimizations=self.optimizations,
        )
        self.assertEqual(normalize_structures(self.structure_repo.get_all()), trivial_topology)


class TestAddDiscreteCategories(unittest.TestCase):
    def setUp(self):
        dal = data_access.get_data_access_layer()

        connector = dal.DataConnector()
        con = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(con))
        cur = connector.acquire_cursor(con)
        self.addCleanup(lambda: connector.release_cursor(cur))

        self.column_manager = dal.ColumnManager(cur)
        self.property_repo = dal.PropertyRepository(cur)

    def get_categories_helper(self):
        """Helper function to return existing categories."""
        return [set(x) for x in self.property_repo.get('discrete_categories')]

    def test_create_new_categories(self):
        """Test creating new categories when none previously exist."""
        self.column_manager.add_columns('A', 'B')

        add_discrete_categories(
            categories=[{'A', 'B'}, {'A'}],
            column_manager=self.column_manager,
            property_repo=self.property_repo,
        )

        self.assertEqual(self.get_categories_helper(), [{'A'}, {'A', 'B'}])

    def test_add_to_existing(self):
        """Test adding new categories to previously existing categories."""
        self.column_manager.add_columns('A', 'B')
        add_discrete_categories([{'A', 'B'}], self.column_manager, self.property_repo)

        add_discrete_categories(
            categories=[{'A'}],  # <- Adds {'A'} to list of existing columns.
            column_manager=self.column_manager,
            property_repo=self.property_repo,
        )

        self.assertEqual(self.get_categories_helper(), [{'A'}, {'A', 'B'}])

    def test_add_whole_space_if_missing(self):
        """The whole space ({'A', 'B'}) should be included when necessary."""
        self.column_manager.add_columns('A', 'B')

        add_discrete_categories(
            categories=[{'A'}],
            column_manager=self.column_manager,
            property_repo=self.property_repo,
        )

        self.assertEqual(self.get_categories_helper(), [{'A'}, {'A', 'B'}])

    def test_warn_on_redundent_categories(self):
        """Check that a warning is raised on redundant categories."""
        self.column_manager.add_columns('A', 'B')
        add_discrete_categories([{'A'}, {'B'}], self.column_manager, self.property_repo)

        with self.assertWarns(ToronWarning) as cm:
            add_discrete_categories(
                categories=[{'A', 'B'}],  # <- Category already covered by existing categories.
                column_manager=self.column_manager,
                property_repo=self.property_repo,
            )

        # Check warning message.
        regex = r"omitting redundant categories: \{(?:'A', 'B'|'B', 'A')\}"
        self.assertRegex(str(cm.warning), regex)

    def test_no_columns_defined(self):
        regex = 'must add index columns before defining categories'
        with self.assertRaisesRegex(RuntimeError, regex):
            add_discrete_categories(
                categories=[{'A', 'B'}, {'A'}],
                column_manager=self.column_manager,
                property_repo=self.property_repo,
            )

    def test_bad_column_name(self):
        self.column_manager.add_columns('A', 'B')

        regex = "invalid category value 'C', values must be present in index columns"
        with self.assertRaisesRegex(ValueError, regex):
            add_discrete_categories(
                categories=[{'A', 'B'}, {'C'}],
                column_manager=self.column_manager,
                property_repo=self.property_repo,
            )


class TestRefreshStructureGranularity(unittest.TestCase):
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
        self.optimizations = dal.optimizations

        self.column_manager.add_columns('A', 'B', 'C', 'D')
        self.index_repo.add('a1', 'b1', 'c1', 'd1')
        self.index_repo.add('a1', 'b1', 'c1', 'd2')
        self.index_repo.add('a1', 'b1', 'c2', 'd3')
        self.index_repo.add('a1', 'b1', 'c2', 'd4')
        self.index_repo.add('a1', 'b2', 'c3', 'd5')
        self.index_repo.add('a1', 'b2', 'c3', 'd6')
        self.index_repo.add('a1', 'b2', 'c4', 'd7')
        self.index_repo.add('a1', 'b2', 'c4', 'd8')

    def test_refresh_structure_granularity(self):
        # Define categories and create structure with `None` for granularity.
        self.property_repo.add(
            'discrete_categories',
            [['A'], ['A', 'B'], ['A', 'B', 'C'], ['A', 'B', 'C', 'D']],
        )
        self.structure_repo.add(None, 0, 0, 0, 0)
        self.structure_repo.add(None, 1, 0, 0, 0)
        self.structure_repo.add(None, 1, 1, 0, 0)
        self.structure_repo.add(None, 1, 1, 1, 0)
        self.structure_repo.add(None, 1, 1, 1, 1)

        expected = [
                Structure(id=5, granularity=3.0,  bits=(1, 1, 1, 1)),
                Structure(id=4, granularity=2.0,  bits=(1, 1, 1, 0)),
                Structure(id=3, granularity=1.0,  bits=(1, 1, 0, 0)),
                Structure(id=2, granularity=0.0,  bits=(1, 0, 0, 0)),  # <- Only one unique value gives granularity of 0.0.
                Structure(id=1, granularity=None, bits=(0, 0, 0, 0)),
        ]

        # Calculate and assign granularity (standard function).
        refresh_structure_granularity(
            column_manager=self.column_manager,
            structure_repo=self.structure_repo,
            index_repo=self.index_repo,
            aux_index_repo=self.alt_index_repo,
            optimizations=None,
        )
        self.assertEqual(self.structure_repo.get_all(), expected)

        # Calculate and assign granularity (using optimizations).
        refresh_structure_granularity(
            column_manager=self.column_manager,
            structure_repo=self.structure_repo,
            index_repo=self.index_repo,
            aux_index_repo=self.alt_index_repo,
            optimizations=self.optimizations,
        )
        self.assertEqual(normalize_structures(self.structure_repo.get_all()), expected)


class TestDomainMethods(unittest.TestCase):
    def setUp(self):
        dal = data_access.get_data_access_layer()

        connector = dal.DataConnector()
        con = connector.acquire_connection()
        self.addCleanup(lambda: connector.release_connection(con))
        cur = connector.acquire_cursor(con)
        self.addCleanup(lambda: connector.release_cursor(cur))

        self.column_manager = dal.ColumnManager(cur)
        self.attribute_repo = dal.AttributeGroupRepository(cur)
        self.property_repo = dal.PropertyRepository(cur)

    def test_set_domain_no_value(self):
        """Should assign 'domain' to property repository."""
        set_domain(
            domain={'foo': 'bar'},
            column_manager=self.column_manager,
            attribute_repo=self.attribute_repo,
            property_repo=self.property_repo,
        )
        self.assertEqual(self.property_repo.get('domain'), {'foo': 'bar'})

    def test_set_domain_existing_value(self):
        """Should assign 'domain' even if one already exists."""
        self.property_repo.add('domain', {'foo': 'bar'})
        set_domain(
            domain={'baz': 'qux'},
            column_manager=self.column_manager,
            attribute_repo=self.attribute_repo,
            property_repo=self.property_repo,
        )
        self.assertEqual(self.property_repo.get('domain'), {'baz': 'qux'})

    def test_set_domain_index_conflict(self):
        """A domain name cannot be the same as an index column."""
        self.column_manager.add_columns('foo', 'bar', 'baz')
        regex = "cannot add domain, 'baz' is already used as an index column"
        with self.assertRaisesRegex(ValueError, regex):
            set_domain(
                domain={'baz': '111', 'qux': '222'},
                column_manager=self.column_manager,
                attribute_repo=self.attribute_repo,
                property_repo=self.property_repo,
            )

    def test_set_domain_attribute_conflict(self):
        """A domain name cannot be the same as a quantity attribute."""
        self.column_manager.add_columns('foo', 'bar')
        self.attribute_repo.add({'baz': 'xxx'})
        regex = "cannot add domain, 'baz' is already used as a quantity attribute"
        with self.assertRaisesRegex(ValueError, regex):
            set_domain(
                domain={'baz': '111', 'qux': '222'},
                column_manager=self.column_manager,
                attribute_repo=self.attribute_repo,
                property_repo=self.property_repo,
            )

    def test_get_domain_no_value(self):
        """Should return empty dict if 'domain' property is not set."""
        self.assertEqual(get_domain(self.property_repo), {})

    def test_get_domain_existing_value(self):
        """Should return existing 'domain' property."""
        self.property_repo.add('domain', {'foo': 'bar'})
        self.assertEqual(get_domain(self.property_repo), {'foo': 'bar'})
