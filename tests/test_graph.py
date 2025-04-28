"""Tests for toron/graph.py module."""

import logging
import unittest
import warnings
from io import StringIO

from toron.node import TopoNode
from toron.xnode import xNode
from toron._utils import (
    ToronWarning,
    BitFlags,
)
from toron.data_models import (
    Index,
    QuantityIterator,
)
from toron.graph import (
    normalize_mapping_data,
    normalize_filename_hints,
    _get_mapping_stats,
    load_mapping,
    _get_mapping_elements,
    get_mapping,
    _translate,
    translate,
    xadd_edge,
)


class TestNormalizeMappingData(unittest.TestCase):
    def setUp(self):
        self.columns = [
            'dom1', 'dom2', 'idx1', 'value', 'dom1', 'dom2', 'idx1'
        ]
        self.data = [
            ['foo', 'bar', 'A', 25, 'baz', 'qux', 'a'],
            ['foo', 'bar', 'B', 50, 'baz', 'qux', 'b'],
            ['foo', 'bar', 'C', 55, 'baz', 'qux', 'c'],
        ]

    def test_full_domain(self):
        data, columns = normalize_mapping_data(
            data=self.data,
            columns=self.columns,
            crosswalk_name='value',
            left_domain={'dom1': 'foo', 'dom2': 'bar'},
            right_domain={'dom1': 'baz', 'dom2': 'qux'}
        )
        expected_columns = ['idx1', 'value', 'idx1']
        self.assertEqual(columns, expected_columns)

        expected_data = [
            ['A', 25, 'a'],
            ['B', 50, 'b'],
            ['C', 55, 'c'],
        ]
        self.assertEqual(list(data), expected_data)

    def test_no_domains_specified(self):
        data, columns = normalize_mapping_data(
            data=self.data,
            columns=self.columns,
            crosswalk_name='value',
            left_domain={},
            right_domain={}
        )
        self.assertEqual(columns, self.columns)
        self.assertEqual(list(data), self.data)

    def test_varrying_domain(self):
        data, columns = normalize_mapping_data(
            data=self.data,
            columns=self.columns,
            crosswalk_name='value',
            left_domain={'dom1': 'foo'},
            right_domain={'dom2': 'qux'}
        )
        expected_columns = ['dom2', 'idx1', 'value', 'dom1', 'idx1']
        self.assertEqual(columns, expected_columns)

        expected_data = [
            ['bar', 'A', 25, 'baz', 'a'],
            ['bar', 'B', 50, 'baz', 'b'],
            ['bar', 'C', 55, 'baz', 'c'],
        ]
        self.assertEqual(list(data), expected_data)

    def test_invalid_domain(self):
        # Append row to `data` with invalid righ-side domain value.
        self.data.append(
            ['foo', 'bar', 'D', 65, 'baz', 'corge', 'd']
        )

        data, columns = normalize_mapping_data(
            data=self.data,
            columns=self.columns,
            crosswalk_name='value',
            left_domain={'dom1': 'foo', 'dom2': 'bar'},
            right_domain={'dom1': 'baz', 'dom2': 'qux'}
        )

        expected_columns = ['idx1', 'value', 'idx1']
        self.assertEqual(columns, expected_columns)

        regex = "error in right-side domain: 'dom2' should be 'qux', got 'corge'"
        with self.assertRaisesRegex(ValueError, regex):
            list(data)  # Use list to consume iterator.


class TestNormalizeFilenameHints(unittest.TestCase):
    def test_none_handling(self):
        a, b = normalize_filename_hints(None, None)
        self.assertIsNone(a)
        self.assertIsNone(b)

        a, b = normalize_filename_hints('foo', None)
        self.assertEqual(a, 'foo')
        self.assertIsNone(b)

        a, b = normalize_filename_hints(None, 'bar')
        self.assertIsNone(a)
        self.assertEqual(b, 'bar')

        a, b = normalize_filename_hints('', '')
        self.assertIsNone(a)
        self.assertIsNone(b)

    def test_extension_removal(self):
        a, b = normalize_filename_hints('foo.toron', 'bar.blerg')
        self.assertEqual(a, 'foo', msg='toron extension should be removed')
        self.assertEqual(b, 'bar.blerg', msg='other extensions should be unchanged')

    def test_directory_prefix_handling(self):
        # Basic prefix handling.
        a, b = normalize_filename_hints('dir/foo.toron', 'dir/bar.toron')
        self.assertEqual(a, 'foo')
        self.assertEqual(b, 'bar')

        # Should match whole directory prefixes (not character prefix).
        a, b = normalize_filename_hints('dir-a/foo.toron', 'dir-b/bar.toron')
        self.assertEqual(a, 'dir-a/foo')
        self.assertEqual(b, 'dir-b/bar')

    def test_directory_sep_normalization(self):
        a, b = normalize_filename_hints('dir1/foo.toron', 'dir1\\dir2\\bar.toron')
        self.assertEqual(a, 'foo')
        self.assertEqual(b, 'dir2/bar')

    def test_abs_and_rel_paths(self):
        a, b = normalize_filename_hints('/dir/foo.toron', 'dir/bar.toron')
        self.assertEqual(a, '/dir/foo')
        self.assertEqual(b, 'dir/bar')


class TwoNodesBaseTestCase(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

        self.node1 = TopoNode()
        self.node1.path_hint = 'file1.toron'
        self.node1.add_index_columns('idx1', 'idx2', 'idx3')
        self.node1.add_discrete_categories({'idx1'}, {'idx1', 'idx2'})
        self.node1.insert_index([
            ['idx1', 'idx2', 'idx3'],
            ['A', 'z', 'a'],
            ['B', 'x', 'b'],
            ['B', 'y', 'c'],
            ['C', 'x', 'd'],
            ['C', 'y', 'e'],
            ['D', 'x', 'f'],
            ['D', 'x', 'g'],
            ['D', 'y', 'h'],
            ['D', 'y', 'i'],
        ])
        self.node1.add_weight_group('wght', make_default=True)
        self.node1.insert_weights(
            weight_group_name='wght',
            data=[
                ['idx1', 'idx2', 'idx3', 'wght'],
                ['A', 'z', 'a', 72],
                ['B', 'x', 'b', 37.5],
                ['B', 'y', 'c', 62.5],
                ['C', 'x', 'd', 75],
                ['C', 'y', 'e', 25],
                ['D', 'x', 'f', 25],
                ['D', 'x', 'g', 0],
                ['D', 'y', 'h', 50],
                ['D', 'y', 'i', 25],
            ],
        )

        self.node2 = TopoNode()
        self.node2.add_index_columns('idx1', 'idx2', 'idx3')
        self.node2.add_discrete_categories({'idx1'}, {'idx1', 'idx2'})
        self.node2.insert_index([
            ['idx1', 'idx2', 'idx3'],
            ['A', 'z', 'a'],
            ['A', 'z', 'b'],
            ['B', 'x', 'c'],
            ['C', 'x', 'd'],
            ['C', 'y', 'e'],
            ['D', 'x', 'f'],
            ['D', 'x', 'g'],
            ['D', 'y', 'h'],
            ['D', 'y', 'i'],
        ])
        self.node2.add_weight_group('wght', make_default=True)
        self.node2.insert_weights(
            weight_group_name='wght',
            data=[
                ['idx1', 'idx2', 'idx3', 'wght'],
                ['A', 'z', 'a', 25],
                ['A', 'z', 'b', 75],
                ['B', 'x', 'c', 80],
                ['C', 'x', 'd', 25],
                ['C', 'y', 'e', 75],
                ['D', 'x', 'f', 37.5],
                ['D', 'x', 'g', 43.75],
                ['D', 'y', 'h', 31.25],
                ['D', 'y', 'i', 31.25],
            ],
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


class TestdGetMappingStats(TwoNodesBaseTestCase):
    def test_all_matched(self):
        self.node2.add_crosswalk(self.node1, 'population', other_filename_hint='file1')
        self.node2.insert_relations2(
            node_or_ref=self.node1,
            crosswalk_name='population',
            data=[
                (1, 1, b'\xe0',  25.0),
                (1, 2, b'\xe0',  25.0),
                (2, 3, b'\xe0',  50.0),
                (3, 3, b'\xe0',  50.0),
                (4, 4, b'\xe0',  55.0),
                (5, 5, b'\xe0',  50.0),
                (6, 6, b'\xe0', 100.0),
                (7, 7, b'\xe0', 100.0),
                (8, 8, b'\xe0', 100.0),
                (9, 9, b'\xe0', 100.0),
                (0, 0,    None,   0.0),
            ],
            columns=['other_index_id', 'index_id', 'mapping_level', 'population'],
        )
        crosswalk = self.node2.get_crosswalk(self.node1, 'population')

        stats = _get_mapping_stats(self.node1, self.node2, crosswalk)
        expected = {
            'src_cardinality': 10,
            'src_index_matched': 10,
            'src_index_missing': 0,
            'src_index_stale': 0,
            'trg_cardinality': 10,
            'trg_index_matched': 10,
            'trg_index_missing': 0,
        }
        self.assertEqual(stats, expected)

    def test_source_missing(self):
        self.node2.add_crosswalk(self.node1, 'population', other_filename_hint='file1')
        self.node2.insert_relations2(
            node_or_ref=self.node1,
            crosswalk_name='population',
            data=[
                (1, 1, b'\xe0',  25.0),
                (1, 2, b'\xe0',  25.0),
                (2, 3, b'\xe0',  50.0),
                # Source element 3 is omitted.
                (4, 4, b'\xe0',  55.0),
                (5, 5, b'\xe0',  50.0),
                (6, 6, b'\xe0', 100.0),
                (7, 7, b'\xe0', 100.0),
                (8, 8, b'\xe0', 100.0),
                (9, 9, b'\xe0', 100.0),
                (0, 0,    None,   0.0),
            ],
            columns=['other_index_id', 'index_id', 'mapping_level', 'population'],
        )
        crosswalk = self.node2.get_crosswalk(self.node1, 'population')

        stats = _get_mapping_stats(self.node1, self.node2, crosswalk)
        expected = {
            'src_cardinality': 10,
            'src_index_matched': 9,
            'src_index_missing': 1,  # <- Missing 1 source side element.
            'src_index_stale': 0,
            'trg_cardinality': 10,
            'trg_index_matched': 10,
            'trg_index_missing': 0,
        }
        self.assertEqual(stats, expected)

    def test_target_missing(self):
        self.node2.add_crosswalk(self.node1, 'population', other_filename_hint='file1')
        self.node2.insert_relations2(
            node_or_ref=self.node1,
            crosswalk_name='population',
            data=[
                # Target element 1 is omitted.
                (1, 2, b'\xe0',  25.0),
                (2, 3, b'\xe0',  50.0),
                (3, 3, b'\xe0',  50.0),
                (4, 4, b'\xe0',  55.0),
                (5, 5, b'\xe0',  50.0),
                (6, 6, b'\xe0', 100.0),
                (7, 7, b'\xe0', 100.0),
                (8, 8, b'\xe0', 100.0),
                (9, 9, b'\xe0', 100.0),
                (0, 0,    None,   0.0),
            ],
            columns=['other_index_id', 'index_id', 'mapping_level', 'population'],
        )
        crosswalk = self.node2.get_crosswalk(self.node1, 'population')

        stats = _get_mapping_stats(self.node1, self.node2, crosswalk)
        expected = {
            'src_cardinality': 10,
            'src_index_matched': 10,
            'src_index_missing': 0,
            'src_index_stale': 0,
            'trg_cardinality': 10,
            'trg_index_matched': 9,
            'trg_index_missing': 1,  # <- Missing 1 target side element.
        }
        self.assertEqual(stats, expected)

    def test_source_stale(self):
        self.node2.add_crosswalk(self.node1, 'population', other_filename_hint='file1')
        self.node2.insert_relations2(
            node_or_ref=self.node1,
            crosswalk_name='population',
            data=[
                (99, 1, b'\xe0', 25.0),  # <- Element 99 is stale (not in current source).
                (1, 2, b'\xe0',  25.0),
                (2, 3, b'\xe0',  50.0),
                (3, 3, b'\xe0',  50.0),
                (4, 4, b'\xe0',  55.0),
                (5, 5, b'\xe0',  50.0),
                (6, 6, b'\xe0', 100.0),
                (7, 7, b'\xe0', 100.0),
                (8, 8, b'\xe0', 100.0),
                (9, 9, b'\xe0', 100.0),
                (0, 0,    None,   0.0),
            ],
            columns=['other_index_id', 'index_id', 'mapping_level', 'population'],
        )
        crosswalk = self.node2.get_crosswalk(self.node1, 'population')

        stats = _get_mapping_stats(self.node1, self.node2, crosswalk)
        expected = {
            'src_cardinality': 10,
            'src_index_matched': 10,
            'src_index_missing': 0,
            'src_index_stale': 1,  # <- Contains 1 stale element.
            'trg_cardinality': 10,
            'trg_index_matched': 10,
            'trg_index_missing': 0,
        }
        self.assertEqual(stats, expected)


class TestLoadMapping(TwoNodesBaseTestCase):
    def test_all_exact(self):
        mapping_data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['A', 'z', 'a',  25, 'A', 'z', 'a'],
            ['A', 'z', 'a',  25, 'A', 'z', 'b'],
            ['B', 'x', 'b',  50, 'B', 'x', 'c'],
            ['B', 'y', 'c',  50, 'B', 'x', 'c'],
            ['C', 'x', 'd',  55, 'C', 'x', 'd'],
            ['C', 'y', 'e',  50, 'C', 'y', 'e'],
            ['D', 'x', 'f', 100, 'D', 'x', 'f'],
            ['D', 'x', 'g', 100, 'D', 'x', 'g'],
            ['D', 'y', 'h', 100, 'D', 'y', 'h'],
            ['D', 'y', 'i', 100, 'D', 'y', 'i'],
        ]
        load_mapping(  # <- The method under test.
            left_node=self.node1,
            direction='->',
            right_node=self.node2,
            crosswalk_name='population',
            data=mapping_data,
        )

        self.assertEqual(
            self.log_stream.getvalue(),
            ("INFO: loading mapping from left to right\n"
             "WARNING: setting default crosswalk: 'population'\n"
             "INFO: loaded 10 relations\n"
             "INFO: mapping verified, cleanly matches both sides\n")
        )

        with self.node2._managed_cursor() as cur:
            results = cur.execute('SELECT other_filename_hint FROM crosswalk').fetchone()
            self.assertEqual(results, ('file1',))

            results = cur.execute('SELECT * FROM relation').fetchall()
            expected = [
                (1,  1, 1, 1, b'\xe0',  25.0, 0.5),
                (2,  1, 1, 2, b'\xe0',  25.0, 0.5),
                (3,  1, 2, 3, b'\xe0',  50.0, 1.0),
                (4,  1, 3, 3, b'\xe0',  50.0, 1.0),
                (5,  1, 4, 4, b'\xe0',  55.0, 1.0),
                (6,  1, 5, 5, b'\xe0',  50.0, 1.0),
                (7,  1, 6, 6, b'\xe0', 100.0, 1.0),
                (8,  1, 7, 7, b'\xe0', 100.0, 1.0),
                (9,  1, 8, 8, b'\xe0', 100.0, 1.0),
                (10, 1, 9, 9, b'\xe0', 100.0, 1.0),
                (11, 1, 0, 0, None,      0.0, 1.0),
            ]
            self.assertEqual(results, expected)

    def test_some_ambiguous(self):
        mapping_data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['A', 'z', '',   50, 'A', 'z', ''],   # <- Matched to 2 right-side records.
            ['B', '',  '',  100, 'B', '',  ''],   # <- Exact right-side match because there's only one "B".
            ['C', '',  '',  105, 'C', '',  ''],   # <- Matched to 2 right-side records.
            ['D', 'x', 'g', 100, 'D', 'x', 'g'],  # <- Exact match (overlaps the records matched on "D" alone).
            ['D', '',  '',  300, 'D', '',  ''],   # <- Matched to 3 right-side records (4-ambiguous, minus 1-exact overlap).
        ]

        load_mapping(  # <- The method under test.
            left_node=self.node1,
            direction='->',
            right_node=self.node2,
            crosswalk_name='population',
            data=mapping_data,
            is_default=True,
            match_limit=4,
        )

        self.assertEqual(
            self.log_stream.getvalue(),
            ('WARNING: omitted 1 ambiguous matches that overlap with records that were already matched at a finer level of granularity\n'
             'WARNING: omitted 1 ambiguous matches that overlap with records that were already matched at a finer level of granularity\n'
             'INFO: loading mapping from left to right\n'
             'INFO: loaded 18 relations\n'
             'INFO: mapping verified, cleanly matches both sides\n'),
        )

        with self.node2._managed_cursor() as cur:
            results = cur.execute('SELECT * FROM relation').fetchall()
            expected = [
                (1,  1, 1, 1, b'\xc0', 12.5,    0.25),
                (2,  1, 1, 2, b'\xc0', 37.5,    0.75),
                (3,  1, 2, 3, b'\x80', 37.5,    1.0),
                (4,  1, 3, 3, b'\x80', 62.5,    1.0),
                (5,  1, 4, 4, b'\x80', 19.6875, 0.25),
                (6,  1, 4, 5, b'\x80', 59.0625, 0.75),
                (7,  1, 5, 4, b'\x80', 6.5625,  0.25),
                (8,  1, 5, 5, b'\x80', 19.6875, 0.75),
                (9,  1, 6, 6, b'\x80', 28.125,  0.375),
                (10, 1, 6, 8, b'\x80', 23.4375, 0.3125),
                (11, 1, 6, 9, b'\x80', 23.4375, 0.3125),
                (12, 1, 7, 7, b'\xe0', 100.0,   1.0),
                (13, 1, 8, 6, b'\x80', 56.25,   0.375),
                (14, 1, 8, 8, b'\x80', 46.875,  0.3125),
                (15, 1, 8, 9, b'\x80', 46.875,  0.3125),
                (16, 1, 9, 6, b'\x80', 28.125,  0.375),
                (17, 1, 9, 8, b'\x80', 23.4375, 0.3125),
                (18, 1, 9, 9, b'\x80', 23.4375, 0.3125),
                (19, 1, 0, 0, None,    0.0,     1.0),
            ]
            self.assertEqual(results, expected)

    def test_bidirectional_mapping(self):
        mapping_data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['A', 'z', 'a',  50, 'A', 'z', ''],   # <- Matched to 2 right-side records.
            ['B', '',  '',  100, 'B', '',  ''],   # <- Exact right-side match because there's only one "B".
            ['C', 'x', 'd',  55, 'C', 'x', 'd'],
            ['C', 'y', 'e',  50, 'C', 'y', 'e'],
        ]

        load_mapping(  # <- The method under test.
            left_node=self.node1,
            direction='<-->',  # <- Makes crosswalks in both directions!
            right_node=self.node2,
            crosswalk_name='population',
            data=mapping_data,
            match_limit=2,
            is_default=True,
        )

        self.assertEqual(
            self.log_stream.getvalue(),
            ('INFO: loading mapping from left to right\n'
             'INFO: loaded 6 relations\n'
             'WARNING: missing 4 indexes on left-side\n'
             'WARNING: missing 4 indexes on right-side\n'
             'INFO: loading mapping from right to left\n'
             'INFO: loaded 6 relations\n'
             'WARNING: missing 4 indexes on right-side\n'
             'WARNING: missing 4 indexes on left-side\n'),
        )

        # Check left-to-right relations (node2 -> node1).
        with self.node1._managed_cursor() as cur:
            results = cur.execute('SELECT * FROM relation').fetchall()
            expected = [
                (1, 1, 1, 1, b'\xe0', 12.5, 1.0),
                (2, 1, 2, 1, b'\xe0', 37.5, 1.0),
                (3, 1, 3, 2, b'\x80', 37.5, 0.375),
                (4, 1, 3, 3, b'\x80', 62.5, 0.625),
                (5, 1, 4, 4, b'\xe0', 55.0, 1.0),
                (6, 1, 5, 5, b'\xe0', 50.0, 1.0),
                (7, 1, 0, 0, None,     0.0, 1.0),
            ]
            self.assertEqual(results, expected)

        # Check right-to-left relations (node1 -> node2).
        with self.node2._managed_cursor() as cur:
            results = cur.execute('SELECT * FROM relation').fetchall()
            expected = [
                (1, 1, 1, 1, b'\xc0', 12.5, 0.25),
                (2, 1, 1, 2, b'\xc0', 37.5, 0.75),
                (3, 1, 2, 3, b'\x80', 37.5, 1.0),
                (4, 1, 3, 3, b'\x80', 62.5, 1.0),
                (5, 1, 4, 4, b'\xe0', 55.0, 1.0),
                (6, 1, 5, 5, b'\xe0', 50.0, 1.0),
                (7, 1, 0, 0, None,     0.0, 1.0),
            ]
            self.assertEqual(results, expected)

    def test_mapping_with_domains(self):
        self.node1.set_domain({'foo': 'bar'})
        self.node2.set_domain({'baz': 'qux'})

        mapping_data = [
            ['foo', 'idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['bar', 'A', 'z', 'a',  25, 'A', 'z', 'a'],
            ['bar', 'A', 'z', 'a',  25, 'A', 'z', 'b'],
            ['bar', 'B', 'x', 'b',  50, 'B', 'x', 'c'],
            ['bar', 'B', 'y', 'c',  50, 'B', 'x', 'c'],
            ['bar', 'C', 'x', 'd',  55, 'C', 'x', 'd'],
            ['bar', 'C', 'y', 'e',  50, 'C', 'y', 'e'],
            ['bar', 'D', 'x', 'f', 100, 'D', 'x', 'f'],
            ['bar', 'D', 'x', 'g', 100, 'D', 'x', 'g'],
            ['bar', 'D', 'y', 'h', 100, 'D', 'y', 'h'],
            ['bar', 'D', 'y', 'i', 100, 'D', 'y', 'i'],
        ]
        load_mapping(  # <- The method under test.
            left_node=self.node1,
            direction='->',
            right_node=self.node2,
            crosswalk_name='population',
            data=mapping_data,
        )

        self.assertEqual(
            self.log_stream.getvalue(),
            ("INFO: loading mapping from left to right\n"
             "WARNING: setting default crosswalk: 'population'\n"
             "INFO: loaded 10 relations\n"
             "INFO: mapping verified, cleanly matches both sides\n")
        )

        with self.node2._managed_cursor() as cur:
            results = cur.execute('SELECT * FROM relation').fetchall()
            expected = [
                (1,  1, 1, 1, b'\xe0',  25.0, 0.5),
                (2,  1, 1, 2, b'\xe0',  25.0, 0.5),
                (3,  1, 2, 3, b'\xe0',  50.0, 1.0),
                (4,  1, 3, 3, b'\xe0',  50.0, 1.0),
                (5,  1, 4, 4, b'\xe0',  55.0, 1.0),
                (6,  1, 5, 5, b'\xe0',  50.0, 1.0),
                (7,  1, 6, 6, b'\xe0', 100.0, 1.0),
                (8,  1, 7, 7, b'\xe0', 100.0, 1.0),
                (9,  1, 8, 8, b'\xe0', 100.0, 1.0),
                (10, 1, 9, 9, b'\xe0', 100.0, 1.0),
                (11, 1, 0, 0, None,      0.0, 1.0),
            ]
            self.assertEqual(results, expected)


class TestGetMappingElements(TwoNodesBaseTestCase):
    def test_fully_joined(self):
        """Check fully mapped crosswalk."""
        self.node2.add_crosswalk(self.node1, 'population', is_default=True)
        self.node2.insert_relations2(
            node_or_ref=self.node1,
            crosswalk_name='population',
            data=[
                (1, 1, b'\xe0',  25.0),
                (1, 2, b'\xe0',  25.0),
                (2, 3, b'\xe0',  50.0),
                (3, 3, b'\xe0',  50.0),
                (4, 4, b'\xe0',  55.0),
                (5, 5, b'\xe0',  50.0),
                (6, 6, b'\xe0', 100.0),
                (7, 7, b'\xe0', 100.0),
                (8, 8, b'\xe0', 100.0),
                (9, 9, b'\xe0', 100.0),
            ],
            columns=['other_index_id', 'index_id', 'mapping_level', 'relation_value'],
        )

        actual = _get_mapping_elements(self.node1, self.node2, 'population')
        expected = [
            (0, 0, None,      0.0),
            (1, 1, b'\xe0',  25.0),
            (1, 2, b'\xe0',  25.0),
            (2, 3, b'\xe0',  50.0),
            (3, 3, b'\xe0',  50.0),
            (4, 4, b'\xe0',  55.0),
            (5, 5, b'\xe0',  50.0),
            (6, 6, b'\xe0', 100.0),
            (7, 7, b'\xe0', 100.0),
            (8, 8, b'\xe0', 100.0),
            (9, 9, b'\xe0', 100.0),
        ]
        self.assertEqual(list(actual), expected)

    def test_missing_left(self):
        """Check unmapped left-side elemenets."""
        self.node2.add_crosswalk(self.node1, 'population', is_default=True)
        self.node2.insert_relations2(
            node_or_ref=self.node1,
            crosswalk_name='population',
            data=[
                (1, 1, b'\xe0',  25.0),
                (1, 2, b'\xe0',  25.0),
                (2, 3, b'\xe0',  50.0),
                (3, 3, b'\xe0',  50.0),
                (4, 4, b'\xe0',  55.0),
                (5, 5, b'\xe0',  50.0),
                (5, 6, b'\xe0', 100.0),
                (5, 7, b'\xe0', 100.0),
                (5, 8, b'\xe0', 100.0),
                (5, 9, b'\xe0', 100.0),
            ],
            columns=['other_index_id', 'index_id', 'mapping_level', 'population'],
        )

        actual = _get_mapping_elements(self.node1, self.node2, 'population')
        expected = [
            (0, 0, None,      0.0),
            (1, 1, b'\xe0',  25.0),
            (1, 2, b'\xe0',  25.0),
            (2, 3, b'\xe0',  50.0),
            (3, 3, b'\xe0',  50.0),
            (4, 4, b'\xe0',  55.0),
            (5, 5, b'\xe0',  50.0),
            (5, 6, b'\xe0', 100.0),
            (5, 7, b'\xe0', 100.0),
            (5, 8, b'\xe0', 100.0),
            (5, 9, b'\xe0', 100.0),
            (6, None, None,     None),  # <- Unmapped left-side element.
            (7, None, None,     None),  # <- Unmapped left-side element.
            (8, None, None,     None),  # <- Unmapped left-side element.
            (9, None, None,     None),  # <- Unmapped left-side element.
        ]
        self.assertEqual(list(actual), expected)

    def test_missing_right(self):
        """Check unmapped right-side elemenets."""
        self.node2.add_crosswalk(self.node1, 'population', is_default=True)
        self.node2.insert_relations2(
            node_or_ref=self.node1,
            crosswalk_name='population',
            data=[
                (1, 1, b'\xe0',  25.0),
                (1, 2, b'\xe0',  25.0),
                (2, 3, b'\xe0',  50.0),
                (3, 3, b'\xe0',  50.0),
                (4, 4, b'\xe0',  55.0),
                (5, 5, b'\xe0',  50.0),
                (6, 5, b'\xe0', 100.0),
                (7, 5, b'\xe0', 100.0),
                (8, 5, b'\xe0', 100.0),
                (9, 5, b'\xe0', 100.0),
            ],
            columns=['other_index_id', 'index_id', 'mapping_level', 'population'],
        )

        actual = _get_mapping_elements(self.node1, self.node2, 'population')
        expected = [
            (0,    0, None,      0.0),
            (1,    1, b'\xe0',  25.0),
            (1,    2, b'\xe0',  25.0),
            (2,    3, b'\xe0',  50.0),
            (3,    3, b'\xe0',  50.0),
            (4,    4, b'\xe0',  55.0),
            (5,    5, b'\xe0',  50.0),
            (6,    5, b'\xe0', 100.0),
            (7,    5, b'\xe0', 100.0),
            (8,    5, b'\xe0', 100.0),
            (9,    5, b'\xe0', 100.0),
            (None, 6, None,     None),  # <- Unmapped right-side element.
            (None, 7, None,     None),  # <- Unmapped right-side element.
            (None, 8, None,     None),  # <- Unmapped right-side element.
            (None, 9, None,     None),  # <- Unmapped right-side element.
        ]
        self.assertEqual(list(actual), expected)

    def test_missing_left_and_right(self):
        """Check unmapped left-side and right-side elemenets."""
        self.node2.add_crosswalk(self.node1, 'population', is_default=True)
        self.node2.insert_relations2(
            node_or_ref=self.node1,
            crosswalk_name='population',
            data=[
                (1, 1, b'\xe0',  25.0),
                (1, 2, b'\xe0',  25.0),
                (2, 3, b'\xe0',  50.0),
                (3, 3, b'\xe0',  50.0),
                (4, 4, b'\xe0',  55.0),
                (5, 5, b'\xe0',  50.0),
            ],
            columns=['other_index_id', 'index_id', 'mapping_level', 'population'],
        )

        actual = _get_mapping_elements(self.node1, self.node2, 'population')
        expected = [
            (0,    0, None,      0.0),
            (1,    1, b'\xe0',  25.0),
            (1,    2, b'\xe0',  25.0),
            (2,    3, b'\xe0',  50.0),
            (3,    3, b'\xe0',  50.0),
            (4,    4, b'\xe0',  55.0),
            (5,    5, b'\xe0',  50.0),
            (None, 6, None,     None),  # <- Unmapped right-side element.
            (None, 7, None,     None),  # <- Unmapped right-side element.
            (None, 8, None,     None),  # <- Unmapped right-side element.
            (None, 9, None,     None),  # <- Unmapped right-side element.
            (6, None, None,     None),  # <- Unmapped left-side element.
            (7, None, None,     None),  # <- Unmapped left-side element.
            (8, None, None,     None),  # <- Unmapped left-side element.
            (9, None, None,     None),  # <- Unmapped left-side element.
        ]
        self.assertEqual(list(actual), expected)


class TestGetMapping(TwoNodesBaseTestCase):
    def test_fully_joined_no_domain_some_ambiguous(self):
        """Check fully mapped crosswalk."""
        self.node2.add_crosswalk(self.node1, 'population', is_default=True)
        self.node2.insert_relations2(
            node_or_ref=self.node1,
            crosswalk_name='population',
            data=[
                (1, 1, b'\x80',  25.0),  # ambiguous: b'\x80' -> 1, 0, 0
                (1, 2, b'\x80',  25.0),  # ambiguous: b'\x80' -> 1, 0, 0
                (2, 3, b'\xc0',  50.0),  # ambiguous: b'\xc0' -> 1, 1, 0
                (3, 3, b'\xc0',  50.0),  # ambiguous: b'\xc0' -> 1, 1, 0
                (4, 4, b'\xe0',  55.0),  # b'\xe0' (1, 1, 1)
                (5, 5, b'\xe0',  50.0),  # b'\xe0' (1, 1, 1)
                (6, 6, b'\xe0', 100.0),  # b'\xe0' (1, 1, 1)
                (7, 7, b'\xe0', 100.0),  # b'\xe0' (1, 1, 1)
                (8, 8, b'\xe0', 100.0),  # b'\xe0' (1, 1, 1)
                (9, 9, b'\xe0', 100.0),  # b'\xe0' (1, 1, 1)
            ],
            columns=['other_index_id', 'index_id', 'mapping_level', 'relation_value'],
        )

        actual = get_mapping(self.node1, self.node2, 'population')
        expected = [
            ('index_id', 'idx1', 'idx2', 'idx3', 'population', 'index_id', 'idx1', 'idx2', 'idx3', 'ambiguous_fields'),
            (0, '-', '-', '-', 0.0, 0, '-', '-', '-', None),
            (1, 'A', 'z', 'a', 25.0, 1, 'A', 'z', 'a', 'idx2, idx3'),
            (1, 'A', 'z', 'a', 25.0, 2, 'A', 'z', 'b', 'idx2, idx3'),
            (2, 'B', 'x', 'b', 50.0, 3, 'B', 'x', 'c', 'idx3'),
            (3, 'B', 'y', 'c', 50.0, 3, 'B', 'x', 'c', 'idx3'),
            (4, 'C', 'x', 'd', 55.0, 4, 'C', 'x', 'd', None),
            (5, 'C', 'y', 'e', 50.0, 5, 'C', 'y', 'e', None),
            (6, 'D', 'x', 'f', 100.0, 6, 'D', 'x', 'f', None),
            (7, 'D', 'x', 'g', 100.0, 7, 'D', 'x', 'g', None),
            (8, 'D', 'y', 'h', 100.0, 8, 'D', 'y', 'h', None),
            (9, 'D', 'y', 'i', 100.0, 9, 'D', 'y', 'i', None),
        ]
        self.assertEqual(list(actual), expected)

    def test_fully_joined_with_domain(self):
        """Check fully mapped crosswalk."""
        self.node1.set_domain({'dataset': 'AAA', 'group': 'XXX'})
        self.node2.set_domain({'dataset': 'BBB'})

        self.node2.add_crosswalk(self.node1, 'population', is_default=True)
        self.node2.insert_relations2(
            node_or_ref=self.node1,
            crosswalk_name='population',
            data=[
                (1, 1, b'\xe0',  25.0),
                (1, 2, b'\xe0',  25.0),
                (2, 3, b'\xe0',  50.0),
                (3, 3, b'\xe0',  50.0),
                (4, 4, b'\xe0',  55.0),
                (5, 5, b'\xe0',  50.0),
                (6, 6, b'\xe0', 100.0),
                (7, 7, b'\xe0', 100.0),
                (8, 8, b'\xe0', 100.0),
                (9, 9, b'\xe0', 100.0),
            ],
            columns=['other_index_id', 'index_id', 'mapping_level', 'relation_value'],
        )

        actual = get_mapping(self.node1, self.node2, 'population')
        expected = [
            ('index_id', 'dataset', 'group', 'idx1', 'idx2', 'idx3', 'population', 'index_id', 'dataset', 'idx1', 'idx2', 'idx3', 'ambiguous_fields'),
            (0, 'AAA', 'XXX', '-', '-', '-', 0.0, 0, 'BBB', '-', '-', '-', None),
            (1, 'AAA', 'XXX', 'A', 'z', 'a', 25.0, 1, 'BBB', 'A', 'z', 'a', None),
            (1, 'AAA', 'XXX', 'A', 'z', 'a', 25.0, 2, 'BBB', 'A', 'z', 'b', None),
            (2, 'AAA', 'XXX', 'B', 'x', 'b', 50.0, 3, 'BBB', 'B', 'x', 'c', None),
            (3, 'AAA', 'XXX', 'B', 'y', 'c', 50.0, 3, 'BBB', 'B', 'x', 'c', None),
            (4, 'AAA', 'XXX', 'C', 'x', 'd', 55.0, 4, 'BBB', 'C', 'x', 'd', None),
            (5, 'AAA', 'XXX', 'C', 'y', 'e', 50.0, 5, 'BBB', 'C', 'y', 'e', None),
            (6, 'AAA', 'XXX', 'D', 'x', 'f', 100.0, 6, 'BBB', 'D', 'x', 'f', None),
            (7, 'AAA', 'XXX', 'D', 'x', 'g', 100.0, 7, 'BBB', 'D', 'x', 'g', None),
            (8, 'AAA', 'XXX', 'D', 'y', 'h', 100.0, 8, 'BBB', 'D', 'y', 'h', None),
            (9, 'AAA', 'XXX', 'D', 'y', 'i', 100.0, 9, 'BBB', 'D', 'y', 'i', None),
        ]
        self.assertEqual(list(actual), expected)

    def test_missing_left(self):
        """Check unmapped left-side elemenets."""
        self.node2.add_crosswalk(self.node1, 'population', is_default=True)
        self.node2.insert_relations2(
            node_or_ref=self.node1,
            crosswalk_name='population',
            data=[
                (1, 1, b'\xe0',  25.0),
                (1, 2, b'\xe0',  25.0),
                (2, 3, b'\xe0',  50.0),
                (3, 3, b'\xe0',  50.0),
                (4, 4, b'\xe0',  55.0),
                (5, 5, b'\xe0',  50.0),
                (5, 6, b'\xe0', 100.0),
                (5, 7, b'\xe0', 100.0),
                (5, 8, b'\xe0', 100.0),
                (5, 9, b'\xe0', 100.0),
            ],
            columns=['other_index_id', 'index_id', 'mapping_level', 'relation_value'],
        )

        actual = get_mapping(self.node1, self.node2, 'population')
        expected = [
            ('index_id', 'idx1', 'idx2', 'idx3', 'population', 'index_id', 'idx1', 'idx2', 'idx3', 'ambiguous_fields'),
            (0, '-', '-', '-', 0.0, 0, '-', '-', '-', None),
            (1, 'A', 'z', 'a', 25.0, 1, 'A', 'z', 'a', None),
            (1, 'A', 'z', 'a', 25.0, 2, 'A', 'z', 'b', None),
            (2, 'B', 'x', 'b', 50.0, 3, 'B', 'x', 'c', None),
            (3, 'B', 'y', 'c', 50.0, 3, 'B', 'x', 'c', None),
            (4, 'C', 'x', 'd', 55.0, 4, 'C', 'x', 'd', None),
            (5, 'C', 'y', 'e', 50.0, 5, 'C', 'y', 'e', None),
            (5, 'C', 'y', 'e', 100.0, 6, 'D', 'x', 'f', None),
            (5, 'C', 'y', 'e', 100.0, 7, 'D', 'x', 'g', None),
            (5, 'C', 'y', 'e', 100.0, 8, 'D', 'y', 'h', None),
            (5, 'C', 'y', 'e', 100.0, 9, 'D', 'y', 'i', None),
            (6, 'D', 'x', 'f', None, None, None, None, None, None),
            (7, 'D', 'x', 'g', None, None, None, None, None, None),
            (8, 'D', 'y', 'h', None, None, None, None, None, None),
            (9, 'D', 'y', 'i', None, None, None, None, None, None),
        ]
        self.assertEqual(list(actual), expected)

    def test_missing_right(self):
        """Check unmapped right-side elemenets."""
        self.node2.add_crosswalk(self.node1, 'population', is_default=True)
        self.node2.insert_relations2(
            node_or_ref=self.node1,
            crosswalk_name='population',
            data=[
                (1, 1, b'\xe0',  25.0),
                (1, 2, b'\xe0',  25.0),
                (2, 3, b'\xe0',  50.0),
                (3, 3, b'\xe0',  50.0),
                (4, 4, b'\xe0',  55.0),
                (5, 5, b'\xe0',  50.0),
                (6, 5, b'\xe0', 100.0),
                (7, 5, b'\xe0', 100.0),
                (8, 5, b'\xe0', 100.0),
                (9, 5, b'\xe0', 100.0),
            ],
            columns=['other_index_id', 'index_id', 'mapping_level', 'relation_value'],
        )

        actual = get_mapping(self.node1, self.node2, 'population')
        expected = [
            ('index_id', 'idx1', 'idx2', 'idx3', 'population', 'index_id', 'idx1', 'idx2', 'idx3', 'ambiguous_fields'),
            (0, '-', '-', '-', 0.0, 0, '-', '-', '-', None),
            (1, 'A', 'z', 'a', 25.0, 1, 'A', 'z', 'a', None),
            (1, 'A', 'z', 'a', 25.0, 2, 'A', 'z', 'b', None),
            (2, 'B', 'x', 'b', 50.0, 3, 'B', 'x', 'c', None),
            (3, 'B', 'y', 'c', 50.0, 3, 'B', 'x', 'c', None),
            (4, 'C', 'x', 'd', 55.0, 4, 'C', 'x', 'd', None),
            (5, 'C', 'y', 'e', 50.0, 5, 'C', 'y', 'e', None),
            (6, 'D', 'x', 'f', 100.0, 5, 'C', 'y', 'e', None),
            (7, 'D', 'x', 'g', 100.0, 5, 'C', 'y', 'e', None),
            (8, 'D', 'y', 'h', 100.0, 5, 'C', 'y', 'e', None),
            (9, 'D', 'y', 'i', 100.0, 5, 'C', 'y', 'e', None),
            (None, None, None, None, None, 6, 'D', 'x', 'f', None),
            (None, None, None, None, None, 7, 'D', 'x', 'g', None),
            (None, None, None, None, None, 8, 'D', 'y', 'h', None),
            (None, None, None, None, None, 9, 'D', 'y', 'i', None),
        ]
        self.assertEqual(list(actual), expected)

    def test_missing_left_and_right(self):
        """Check unmapped left-side and right-side elemenets."""
        self.node1.set_domain({'dataset': 'AAA', 'group': 'XXX'})
        self.node2.set_domain({'dataset': 'BBB'})

        self.node2.add_crosswalk(self.node1, 'population', is_default=True)
        self.node2.insert_relations2(
            node_or_ref=self.node1,
            crosswalk_name='population',
            data=[
                (1, 1, b'\xe0',  25.0),
                (1, 2, b'\xe0',  25.0),
                (2, 3, b'\xe0',  50.0),
                (3, 3, b'\xe0',  50.0),
                (4, 4, b'\xe0',  55.0),
                (5, 5, b'\xe0',  50.0),
            ],
            columns=['other_index_id', 'index_id', 'mapping_level', 'population'],
        )

        actual = get_mapping(self.node1, self.node2, 'population')
        expected = [
            ('index_id', 'dataset', 'group', 'idx1', 'idx2', 'idx3', 'population', 'index_id', 'dataset', 'idx1', 'idx2', 'idx3', 'ambiguous_fields'),
            (0, 'AAA', 'XXX', '-', '-', '-', 0.0, 0, 'BBB', '-', '-', '-', None),
            (1, 'AAA', 'XXX', 'A', 'z', 'a', 25.0, 1, 'BBB', 'A', 'z', 'a', None),
            (1, 'AAA', 'XXX', 'A', 'z', 'a', 25.0, 2, 'BBB', 'A', 'z', 'b', None),
            (2, 'AAA', 'XXX', 'B', 'x', 'b', 50.0, 3, 'BBB', 'B', 'x', 'c', None),
            (3, 'AAA', 'XXX', 'B', 'y', 'c', 50.0, 3, 'BBB', 'B', 'x', 'c', None),
            (4, 'AAA', 'XXX', 'C', 'x', 'd', 55.0, 4, 'BBB', 'C', 'x', 'd', None),
            (5, 'AAA', 'XXX', 'C', 'y', 'e', 50.0, 5, 'BBB', 'C', 'y', 'e', None),
            (None, None, None, None, None, None, None, 6, 'BBB', 'D', 'x', 'f', None),
            (None, None, None, None, None, None, None, 7, 'BBB', 'D', 'x', 'g', None),
            (None, None, None, None, None, None, None, 8, 'BBB', 'D', 'y', 'h', None),
            (None, None, None, None, None, None, None, 9, 'BBB', 'D', 'y', 'i', None),
            (6, 'AAA', 'XXX', 'D', 'x', 'f', None, None, None, None, None, None, None),
            (7, 'AAA', 'XXX', 'D', 'x', 'g', None, None, None, None, None, None, None),
            (8, 'AAA', 'XXX', 'D', 'y', 'h', None, None, None, None, None, None, None),
            (9, 'AAA', 'XXX', 'D', 'y', 'i', None, None, None, None, None, None, None),
        ]
        self.assertEqual(list(actual), expected)


class TestTranslate(unittest.TestCase):
    def setUp(self):
        mock_node = unittest.mock.Mock()
        mock_node.unique_id = '00000000-0000-0000-0000-000000000000'

        self.node = TopoNode()
        self.node.add_index_columns('A', 'B', 'C')
        self.node.add_discrete_categories({'A', 'B', 'C'})
        self.node.insert_index([
            ['A', 'B', 'C'],
            ['a1', 'b1', 'c1'],  # <- index_id=1
            ['a1', 'b1', 'c2'],  # <- index_id=2
            ['a1', 'b2', 'c3'],  # <- index_id=3
            ['a1', 'b2', 'c4'],  # <- index_id=4
        ])
        self.node.add_crosswalk(
            node=mock_node,
            crosswalk_name='edge 1',
            other_filename_hint='other-file',
            description='Edge one description.',
            selectors=['[foo="bar"]'],
            is_default=True,
        )
        self.node.insert_relations(
            node_or_ref='other-file',
            crosswalk_name='edge 1',
            data=[
                ('other_index_id', 'edge 1', 'index_id', 'A', 'B', 'C'),
                (1,  39.0, 1, 'a1', 'b1', 'c1'),  # proportion: 0.6
                (1,  26.0, 2, 'a1', 'b1', 'c2'),  # proportion: 0.4
                (2,  16.0, 2, 'a1', 'b1', 'c2'),  # proportion: 1.0
                (3,  50.0, 2, 'a1', 'b1', 'c2'),  # proportion: 0.250
                (3,  25.0, 3, 'a1', 'b2', 'c3'),  # proportion: 0.125
                (3, 125.0, 4, 'a1', 'b2', 'c4'),  # proportion: 0.625
                (4,  64.0, 3, 'a1', 'b2', 'c3'),  # proportion: 1.0
                (5,  19.0, 3, 'a1', 'b2', 'c3'),  # proportion: 0.38
                (5,  31.0, 4, 'a1', 'b2', 'c4'),  # proportion: 0.62
                (0,   0.0, 0, '-',  '-',  '-' ),  # proportion: 1.0
            ],
        )
        self.node.add_crosswalk(
            node=mock_node,
            crosswalk_name='edge 2',
            other_filename_hint='other-file',
            description='Edge two description.',
            selectors=['[foo]'],
        )
        self.node.insert_relations(
            node_or_ref='other-file',
            crosswalk_name='edge 2',
            data=[
                ('other_index_id', 'edge 2', 'index_id', 'A', 'B', 'C'),
                (1, 32.0,  1, 'a1', 'b1', 'c1'),  # proportion: 0.5
                (1, 32.0,  2, 'a1', 'b1', 'c2'),  # proportion: 0.5
                (2, 15.0,  2, 'a1', 'b1', 'c2'),  # proportion: 1.0
                (3, 85.5,  2, 'a1', 'b1', 'c2'),  # proportion: 0.333984375
                (3, 85.25, 3, 'a1', 'b2', 'c3'),  # proportion: 0.3330078125
                (3, 85.25, 4, 'a1', 'b2', 'c4'),  # proportion: 0.3330078125
                (4, 64.0,  3, 'a1', 'b2', 'c3'),  # proportion: 1.0
                (5, 50.0,  3, 'a1', 'b2', 'c3'),  # proportion: 0.5
                (5, 50.0,  4, 'a1', 'b2', 'c4'),  # proportion: 0.5
                (0,  0.0,  0, '-',  '-',  '-' ),  # proportion: 1.0
            ],
        )

    def test_translate_generator(self):
        quantities = QuantityIterator(
            unique_id='00000000-0000-0000-0000-000000000000',
            index_hash='55e56a09c8793714d050eb888d945ca3b66d10ce5c5b489946df6804dd60324e',
            domain={},
            data=[(Index(1, 'aaa'), {'foo': 'bar'}, 100),
                  (Index(2, 'bbb'), {'foo': 'bar'}, 100),
                  (Index(3, 'ccc'), {'foo': 'bar'}, 100),
                  (Index(4, 'ddd'), {'foo': 'bar'}, 100),
                  (Index(5, 'eee'), {'foo': 'bar'}, 100)],
            label_names=['X'],
            attribute_keys=['foo'],
        )

        results_generator = _translate(quantities, self.node)

        self.assertEqual(
            list(results_generator),
            [(Index(id=1, labels=('a1', 'b1', 'c1')), {'foo': 'bar'}, 60.0),
             (Index(id=2, labels=('a1', 'b1', 'c2')), {'foo': 'bar'}, 40.0),
             (Index(id=2, labels=('a1', 'b1', 'c2')), {'foo': 'bar'}, 100.0),
             (Index(id=2, labels=('a1', 'b1', 'c2')), {'foo': 'bar'}, 25.0),
             (Index(id=3, labels=('a1', 'b2', 'c3')), {'foo': 'bar'}, 12.5),
             (Index(id=4, labels=('a1', 'b2', 'c4')), {'foo': 'bar'}, 62.5),
             (Index(id=3, labels=('a1', 'b2', 'c3')), {'foo': 'bar'}, 100.0),
             (Index(id=3, labels=('a1', 'b2', 'c3')), {'foo': 'bar'}, 38.0),
             (Index(id=4, labels=('a1', 'b2', 'c4')), {'foo': 'bar'}, 62.0)],
        )

    def test_simple_case(self):
        quantities = QuantityIterator(
            unique_id='00000000-0000-0000-0000-000000000000',
            index_hash='55e56a09c8793714d050eb888d945ca3b66d10ce5c5b489946df6804dd60324e',
            domain={},
            data=[(Index(1, 'aaa'), {'foo': 'bar'}, 100),
                  (Index(2, 'bbb'), {'foo': 'bar'}, 100),
                  (Index(3, 'ccc'), {'foo': 'bar'}, 100),
                  (Index(4, 'ddd'), {'foo': 'bar'}, 100),
                  (Index(5, 'eee'), {'foo': 'bar'}, 100)],
            label_names=['X'],
            attribute_keys=['foo'],
        )

        new_quantities = translate(quantities, self.node)

        self.assertIsInstance(new_quantities, QuantityIterator)
        self.assertEqual(
            new_quantities.columns,
            ('A', 'B', 'C', 'foo', 'value'),
        )
        self.assertNotEqual(
            new_quantities.unique_id,
            quantities.unique_id,
            msg='new result should NOT match previous unique_id',
        )
        self.assertEqual(
            new_quantities.unique_id,
            self.node.unique_id,
            msg='new result should match unique_id of target node',
        )
        self.assertEqual(
            list(new_quantities),
            [('a1', 'b1', 'c1', 'bar', 60.0),
             ('a1', 'b1', 'c2', 'bar', 40.0),
             ('a1', 'b1', 'c2', 'bar', 100.0),
             ('a1', 'b1', 'c2', 'bar', 25.0),
             ('a1', 'b2', 'c3', 'bar', 12.5),
             ('a1', 'b2', 'c4', 'bar', 62.5),
             ('a1', 'b2', 'c3', 'bar', 100.0),
             ('a1', 'b2', 'c3', 'bar', 38.0),
             ('a1', 'b2', 'c4', 'bar', 62.0)]
        )

        # If `new_quantities` were accumulated, it would be:
        #[('a1', 'b1', 'c1', 'bar', 60),
        # ('a1', 'b1', 'c2', 'bar', 165),
        # ('a1', 'b2', 'c3', 'bar', 150.5),
        # ('a1', 'b2', 'c4', 'bar', 124.5)]

    def test_handling_multiple_edges(self):
        """Check that quantities are translated using appropriate edges.

        Quantities should be matched by their attributes to the edge
        with the greatest unique specificity or the default edge if
        there is no unique match.
        """
        quantities = QuantityIterator(
            unique_id='00000000-0000-0000-0000-000000000000',
            index_hash='55e56a09c8793714d050eb888d945ca3b66d10ce5c5b489946df6804dd60324e',
            domain={},
            data=[
                # Attributes {'foo': 'bar'} match 'edge 1' ([foo="bar"])
                # and 'edge 2' ([foo]), but 'edge 1' is used because it
                # has a greater specificity.
                (Index(1, 'aaa'), {'foo': 'bar'}, 100),
                (Index(2, 'bbb'), {'foo': 'bar'}, 100),
                (Index(3, 'ccc'), {'foo': 'bar'}, 100),
                (Index(4, 'ddd'), {'foo': 'bar'}, 100),
                (Index(5, 'eee'), {'foo': 'bar'}, 100),

                # Attributes {'foo': 'baz'} match 'edge 2' ([foo]).
                (Index(1, 'aaa'), {'foo': 'baz'}, 100),
                (Index(2, 'bbb'), {'foo': 'baz'}, 100),
                (Index(3, 'ccc'), {'foo': 'baz'}, 100),
                (Index(4, 'ddd'), {'foo': 'baz'}, 100),

                # Attributes {'qux': 'corge'} has no match, uses default ('edge 1').
                (Index(5, 'eee'), {'qux': 'corge'}, 100),
            ],
            label_names=['X'],
            attribute_keys=['foo', 'qux'],
        )

        new_quantities = translate(quantities, self.node)

        expected = [
            ('a1', 'b1', 'c1', 'bar', None, 60.0),         # <- Edge 1
            ('a1', 'b1', 'c2', 'bar', None, 40.0),         # <- Edge 1
            ('a1', 'b1', 'c2', 'bar', None, 100.0),        # <- Edge 1
            ('a1', 'b1', 'c2', 'bar', None, 25.0),         # <- Edge 1
            ('a1', 'b2', 'c3', 'bar', None, 12.5),         # <- Edge 1
            ('a1', 'b2', 'c4', 'bar', None, 62.5),         # <- Edge 1
            ('a1', 'b2', 'c3', 'bar', None, 100.0),        # <- Edge 1
            ('a1', 'b2', 'c3', 'bar', None, 38.0),         # <- Edge 1
            ('a1', 'b2', 'c4', 'bar', None, 62.0),         # <- Edge 1
            ('a1', 'b1', 'c1', 'baz', None, 50.0),         # <- Edge 2
            ('a1', 'b1', 'c2', 'baz', None, 50.0),         # <- Edge 2
            ('a1', 'b1', 'c2', 'baz', None, 100.0),        # <- Edge 2
            ('a1', 'b1', 'c2', 'baz', None, 33.3984375),   # <- Edge 2
            ('a1', 'b2', 'c3', 'baz', None, 33.30078125),  # <- Edge 2
            ('a1', 'b2', 'c4', 'baz', None, 33.30078125),  # <- Edge 2
            ('a1', 'b2', 'c3', 'baz', None, 100.0),        # <- Edge 2
            ('a1', 'b2', 'c3', None, 'corge', 38.0),       # <- Default (Edge 1)
            ('a1', 'b2', 'c4', None, 'corge', 62.0),       # <- Default (Edge 1)
        ]
        self.assertEqual(list(new_quantities), expected)

        # If `new_quantities` were accumulated, it would be:
        #[('a1', 'b1', 'c1', 'bar', None, 60),            # <- Edge 1
        # ('a1', 'b1', 'c2', 'bar', None, 165),           # <- Edge 1
        # ('a1', 'b2', 'c3', 'bar', None, 150.5),         # <- Edge 1
        # ('a1', 'b2', 'c4', 'bar', None, 124.5),         # <- Edge 1
        # ('a1', 'b1', 'c1', 'baz', None, 50),            # <- Edge 2
        # ('a1', 'b1', 'c2', 'baz', None, 183.3984375),   # <- Edge 2
        # ('a1', 'b2', 'c3', 'baz', None, 133.30078125),  # <- Edge 2
        # ('a1', 'b2', 'c4', 'baz', None, 33.30078125),   # <- Edge 2
        # ('a1', 'b2', 'c3', None, 'corge', 38.0),        # <- Default (Edge 1)
        # ('a1', 'b2', 'c4', None, 'corge', 62.0)]        # <- Default (Edge 1)

    def test_rshift(self):
        # TODO: Move this test into a different module when
        # QuantityIterator class definition is moved.

        quantities = QuantityIterator(
            unique_id='00000000-0000-0000-0000-000000000000',
            index_hash='55e56a09c8793714d050eb888d945ca3b66d10ce5c5b489946df6804dd60324e',
            domain={},
            data=[(Index(1, 'aaa'), {'foo': 'bar'}, 100),
                  (Index(2, 'bbb'), {'foo': 'bar'}, 100),
                  (Index(3, 'ccc'), {'foo': 'bar'}, 100),
                  (Index(4, 'ddd'), {'foo': 'bar'}, 100),
                  (Index(5, 'eee'), {'foo': 'bar'}, 100)],
            label_names=['X'],
            attribute_keys=['foo'],
        )

        new_quantities = quantities >> self.node  # Translate with right-shift.

        self.assertIsInstance(new_quantities, QuantityIterator)
        self.assertEqual(
            list(new_quantities),
            [('a1', 'b1', 'c1', 'bar', 60.0),
             ('a1', 'b1', 'c2', 'bar', 40.0),
             ('a1', 'b1', 'c2', 'bar', 100.0),
             ('a1', 'b1', 'c2', 'bar', 25.0),
             ('a1', 'b2', 'c3', 'bar', 12.5),
             ('a1', 'b2', 'c4', 'bar', 62.5),
             ('a1', 'b2', 'c3', 'bar', 100.0),
             ('a1', 'b2', 'c3', 'bar', 38.0),
             ('a1', 'b2', 'c4', 'bar', 62.0)]
        )

        # If `new_quantities` were accumulated, it would be:
        #[('a1', 'b1', 'c1', 'bar', 60),
        # ('a1', 'b1', 'c2', 'bar', 165),
        # ('a1', 'b2', 'c3', 'bar', 150.5),
        # ('a1', 'b2', 'c4', 'bar', 124.5)]


class TestXAddEdge(unittest.TestCase):
    def setUp(self):
        node1_data = [
            ['idx1', 'idx2', 'idx3', 'wght'],
            ['A', 'z', 'a', 72],
            ['B', 'x', 'b', 37.5],
            ['B', 'y', 'c', 62.5],
            ['C', 'x', 'd', 75],
            ['C', 'y', 'e', 25],
            ['D', 'x', 'f', 25],
            ['D', 'x', 'g', None],
            ['D', 'y', 'h', 50],
            ['D', 'y', 'i', 25],
        ]
        node1 = xNode()
        node1.add_index_columns(['idx1', 'idx2', 'idx3'])
        node1.add_index_records(node1_data)
        node1.add_weights(node1_data, 'wght', selectors=['[attr1]'])
        self.node1 = node1

        node2_data = [
            ['idx1', 'idx2', 'idx3', 'wght'],
            ['A', 'z', 'a', 25],
            ['A', 'z', 'b', 75],
            ['B', 'x', 'c', 80],
            ['C', 'x', 'd', 25],
            ['C', 'y', 'e', 75],
            ['D', 'x', 'f', 37.5],
            ['D', 'x', 'g', 43.75],
            ['D', 'y', 'h', 31.25],
            ['D', 'y', 'i', 31.25],
        ]
        node2 = xNode()
        node2.add_index_columns(['idx1', 'idx2', 'idx3'])
        node2.add_index_records(node2_data)
        node2.add_weights(node2_data, 'wght', selectors=['[attr1]'])
        self.node2 = node2

    def test_all_exact(self):
        mapping_data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['A', 'z', 'a',  25, 'A', 'z', 'a'],
            ['A', 'z', 'a',  25, 'A', 'z', 'b'],
            ['B', 'x', 'b',  50, 'B', 'x', 'c'],
            ['B', 'y', 'c',  50, 'B', 'x', 'c'],
            ['C', 'x', 'd',  55, 'C', 'x', 'd'],
            ['C', 'y', 'e',  50, 'C', 'y', 'e'],
            ['D', 'x', 'f', 100, 'D', 'x', 'f'],
            ['D', 'x', 'g', 100, 'D', 'x', 'g'],
            ['D', 'y', 'h', 100, 'D', 'y', 'h'],
            ['D', 'y', 'i', 100, 'D', 'y', 'i'],
        ]
        xadd_edge(  # <- The method under test.
            data=mapping_data,
            name='population',
            left_node=self.node1,
            direction='-->',
            right_node=self.node2,
        )

        con = self.node2._dal._get_connection()
        results = con.execute('SELECT * FROM relation').fetchall()
        expected = [
            (1,  1, 1, 1,  25.0, 0.5, None),
            (2,  1, 1, 2,  25.0, 0.5, None),
            (3,  1, 2, 3,  50.0, 1.0, None),
            (4,  1, 3, 3,  50.0, 1.0, None),
            (5,  1, 4, 4,  55.0, 1.0, None),
            (6,  1, 5, 5,  50.0, 1.0, None),
            (7,  1, 6, 6, 100.0, 1.0, None),
            (8,  1, 7, 7, 100.0, 1.0, None),
            (9,  1, 8, 8, 100.0, 1.0, None),
            (10, 1, 9, 9, 100.0, 1.0, None),
            (11, 1, 0, 0,   0.0, 1.0, None),
        ]
        self.assertEqual(results, expected)

    def test_some_ambiguous(self):
        self.maxDiff = None

        self.node1.add_discrete_categories([{'idx1'}])
        self.node2.add_discrete_categories([{'idx1'}, {'idx1', 'idx2'}])

        mapping_data = [
            ['idx1', 'idx2', 'idx3', 'population', 'idx1', 'idx2', 'idx3'],
            ['A', 'z', '',   50, 'A', 'z', ''],   # <- Matched to 2 right-side records.
            ['B', '',  '',  100, 'B', '',  ''],   # <- Exact right-side match because there's only one "B".
            ['C', '',  '',  105, 'C', '',  ''],   # <- Matched to 2 right-side records.
            ['D', 'x', 'g', 100, 'D', 'x', 'g'],  # <- Exact match (overlaps the records matched on "D" alone).
            ['D', '',  '',  300, 'D', '',  ''],   # <- Matched to 3 right-side records (4-ambiguous, minus 1-exact overlap).
        ]
        xadd_edge(  # <- The method under test.
            data=mapping_data,
            name='population',
            left_node=self.node1,
            direction='-->',
            right_node=self.node2,
            match_limit=4,
        )

        con = self.node2._dal._get_connection()
        results = con.execute('SELECT * FROM main.relation').fetchall()
        expected = [
            (1,  1, 1, 1, 12.5,    0.25,   BitFlags(1, 1, 0)),
            (2,  1, 1, 2, 37.5,    0.75,   BitFlags(1, 1, 0)),
            (3,  1, 2, 3, 37.5,    1.0,    None),
            (4,  1, 3, 3, 62.5,    1.0,    None),
            (5,  1, 4, 4, 19.6875, 0.25,   BitFlags(1, 0, 0)),
            (6,  1, 4, 5, 59.0625, 0.75,   BitFlags(1, 0, 0)),
            (7,  1, 5, 4, 6.5625,  0.25,   BitFlags(1, 0, 0)),
            (8,  1, 5, 5, 19.6875, 0.75,   BitFlags(1, 0, 0)),
            (9,  1, 6, 6, 28.125,  0.375,  BitFlags(1, 0, 0)),
            (10, 1, 6, 8, 23.4375, 0.3125, BitFlags(1, 0, 0)),
            (11, 1, 6, 9, 23.4375, 0.3125, BitFlags(1, 0, 0)),
            (12, 1, 7, 7, 100.0,   1.0,    None),
            (13, 1, 8, 6, 56.25,   0.375,  BitFlags(1, 0, 0)),
            (14, 1, 8, 8, 46.875,  0.3125, BitFlags(1, 0, 0)),
            (15, 1, 8, 9, 46.875,  0.3125, BitFlags(1, 0, 0)),
            (16, 1, 9, 6, 28.125,  0.375,  BitFlags(1, 0, 0)),
            (17, 1, 9, 8, 23.4375, 0.3125, BitFlags(1, 0, 0)),
            (18, 1, 9, 9, 23.4375, 0.3125, BitFlags(1, 0, 0)),
            (19, 1, 0, 0, 0.0,     1.0,    None)
        ]
        self.assertEqual(results, expected)
