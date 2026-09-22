"""Tests for toron/cli/command_index.py module."""
import argparse
import os
import tempfile
from .. import _unittest as unittest
from ..common import DummyRedirection
from toron import DataSpace

from toron.cli import command_index


class TestIndexImportRecords(unittest.TestCase):
    def test_input_labels_and_weights(self):
        ds = DataSpace()
        ds.add_index_columns('state', 'county')
        ds.add_weight_group('population', make_default=True)

        reader = iter([
            ['state', 'county', 'population'],
            ['Illinois', 'Cook', '5275541'],
            ['Indiana', 'Porter', '175860'],
            ['Michigan', 'Cass', '51589'],
        ])

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index._import_records(ds, reader, 'abort', 'abort')  # <- Function under test.

        self.assertEqual(
            logs_cm.output,
            ['INFO:app-toron.space:loaded 3 index labels',
             'INFO:app-toron.space:loaded 3 index weights'],
        )
        index_values = list(ds.select_index(header=True))
        expected_values = [
            ('index_id', 'state', 'county'),
            (0, '-', '-'),
            (1, 'Illinois', 'Cook'),
            (2, 'Indiana', 'Porter'),
            (3, 'Michigan', 'Cass'),
        ]
        self.assertEqual(index_values, expected_values)

    def test_abort_on_label_conflict(self):
        ds = DataSpace()
        ds._connector._unique_id = '11111111-1111-1111-1111-111111111111'
        ds.add_index_columns('state', 'county')
        ds.add_weight_group('population', make_default=True)
        ds.insert_index([['state', 'county'],
                         ['Illinois', 'Cook'],
                         ['Indiana', 'Porter'],
                         ['Michigan', 'Cass']])

        reader = iter([
            ['index_code', 'state', 'county', 'population'],
            ['1XA0157D6E', 'Illinois', 'Cook', '5275541'],
            ['2XF38F26EA', 'Indiana', 'Porter', '175860'],
            ['3X7429EDA9', 'Michigan', 'OTHERVALUE', '51589'],  # <- Will abort operation.
        ])

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index._import_records(ds, reader, 'abort', 'abort')  # <- Function under test.

        self.assertEqual(
            logs_cm.output,
            ["ERROR:app-toron:index code 3X7429EDA9 and labels ('Michigan', 'OTHERVALUE') "
               "do not match Index(id=3, labels=('Michigan', 'Cass'))\n"
               "  load behavior can be changed using --on-label-conflict "
               "and --on-weight-conflict"]
        )

    def test_ignore_on_label_conflict(self):
        ds = DataSpace()
        ds._connector._unique_id = '11111111-1111-1111-1111-111111111111'
        ds.add_index_columns('state', 'county')
        ds.add_weight_group('population', make_default=True)
        ds.insert_index([['state', 'county'],
                         ['Illinois', 'Cook'],
                         ['Indiana', 'Porter'],
                         ['Michigan', 'Cass']])

        reader = iter([
            ['index_code', 'state', 'county', 'population'],
            ['1XA0157D6E', 'Illinois', 'Cook', '5275541'],
            ['2XF38F26EA', 'Indiana', 'Porter', '175860'],
            ['3X7429EDA9', 'Michigan', 'OTHERVALUE', '51589'],  # <- Label will be ignored.
        ])

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index._import_records(ds, reader, 'ignore', 'abort')  # <- Function under test.

        self.assertEqual(
            logs_cm.output,
            ['INFO:app-toron.space:ignored 1 non-matching index labels',
             'INFO:app-toron.space:loaded 3 index weights']
        )

    def test_replace_on_label_conflict(self):
        ds = DataSpace()
        ds._connector._unique_id = '11111111-1111-1111-1111-111111111111'
        ds.add_index_columns('state', 'county')
        ds.add_weight_group('population', make_default=True)
        ds.insert_index([['state', 'county'],
                         ['Illinois', 'Cook'],
                         ['Indiana', 'Porter'],
                         ['Michigan', 'Cass']])

        reader = iter([
            ['index_id', 'state', 'county', 'population'],
            ['1XA0157D6E', 'Illinois', 'Cook', '5275541'],
            ['2XF38F26EA', 'Indiana', 'Porter', '175860'],
            ['3X7429EDA9', 'Michigan', 'OTHERVALUE', '51589'],  # <- Will replace with new label.
        ])

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index._import_records(ds, reader, 'replace', 'abort')  # <- Function under test.

        self.assertEqual(
            logs_cm.output,
            ['INFO:app-toron.space:replaced 1 index labels',
             'INFO:app-toron.space:loaded 3 index weights']
        )

    def test_abort_on_weight_conflict(self):
        ds = DataSpace()
        ds.add_index_columns('state', 'county')
        ds.add_weight_group('population', make_default=True)

        reader = iter([
            ['state', 'county', 'population'],
            ['Illinois', 'Cook', '5275541'],
            ['Indiana', 'Porter', '175860'],
            ['Michigan', 'Cass', '51589'],
            ['Michigan', 'Cass', '50000'],  # <- Will abort operation.
        ])

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index._import_records(ds, reader, 'abort', 'abort')  # <- Function under test.

        index_values = list(ds.select_index(header=True))
        expected_values = [('index_id', 'state', 'county'), (0, '-', '-')]
        self.assertEqual(index_values, expected_values)
        self.assertEqual(
            logs_cm.output,
            ["ERROR:app-toron:weight group 'population' already has "
               "a value for Index(id=3, labels=('Michigan', 'Cass'))\n"
               "  load behavior can be changed using --on-label-conflict "
               "and --on-weight-conflict"],
        )

    def test_replace_on_weight_conflict(self):
        ds = DataSpace()
        ds.add_index_columns('state', 'county')
        ds.add_weight_group('population', make_default=True)

        reader = iter([
            ['state', 'county', 'population'],
            ['Illinois', 'Cook', '5275541'],
            ['Indiana', 'Porter', '175860'],
            ['Michigan', 'Cass', '0'],  # <- Will get replaced by later record.
            ['Michigan', 'Cass', '51589'],
        ])

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index._import_records(ds, reader, 'abort', 'replace')  # <- Function under test.

        index_values = list(ds.select_index(header=True))
        expected_values = [
            ('index_id', 'state', 'county'),
            (0, '-', '-'),
            (1, 'Illinois', 'Cook'),
            (2, 'Indiana', 'Porter'),
            (3, 'Michigan', 'Cass'),
        ]
        self.assertEqual(index_values, expected_values)
        self.assertEqual(
            logs_cm.output,
            ['INFO:app-toron.space:loaded 3 index labels',
             'INFO:app-toron.space:loaded 3 index weights',
             'INFO:app-toron.space:replaced 1 index weights'],
        )


class TestIndexExportRecords(unittest.TestCase):
    @staticmethod
    def unsafe_set_unique_id(node, unique_id):
        """Helper function to set unique_id values for testing."""
        node._connector._unique_id = unique_id
        with node._managed_transaction() as cur:
            property_repo = node._dal.PropertyRepository(cur)
            property_repo.add_or_update('unique_id', unique_id)

    def setUp(self):
        ds = DataSpace()
        self.unsafe_set_unique_id(ds, '11111111-1111-1111-1111-111111111111')

        ds.add_index_columns('state', 'county')
        ds.add_weight_group('wght3', make_default=False)
        ds.add_weight_group('wght2', make_default=True)
        ds.add_weight_group('wght1')

        ds.insert_index([
            ['state',    'county', 'wght1', 'wght2', 'wght3'],
            ['Illinois', 'Cook',   100,     200,     300],
            ['Indiana',  'Porter', 100,     200,     300],
            ['Michigan', 'Cass',   100,     200,     300],
        ])
        self.ds = ds

    def test_basic_behavior(self):
        """Test `_export_records()` function."""
        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            records = list(command_index._export_records(self.ds))  # <- Function under test.

        self.assertEqual(logs_cm.output, ['INFO:app-toron:written 4 records'])

        expected_values = [
            ['index_code', 'state',    'county', 'wght2', 'wght1', 'wght3'],
            ['0X27B3B62D', '-',        '-',        0.0,     0.0,     0.0],
            ['1XA0157D6E', 'Illinois', 'Cook',   200.0,   100.0,   300.0],
            ['2XF38F26EA', 'Indiana',  'Porter', 200.0,   100.0,   300.0],
            ['3X7429EDA9', 'Michigan', 'Cass',   200.0,   100.0,   300.0],
        ]
        self.assertEqual(records, expected_values)

    def test_export_to_file(self):
        """Test `export_records()` wrapper function."""
        with tempfile.TemporaryDirectory(prefix='toron-') as tmpdir:
            target_path = os.path.join(tmpdir, 'index-file1.csv')
            ds_path = os.path.join(tmpdir, 'file1.ds')
            self.ds.to_file(ds_path)

            command_index.export_records(argparse.Namespace(  # <- Function under test.
                filepath=ds_path,
                command='index',
                subcommand='export',
                force=False,
                target=target_path,
            ))

            with open(target_path) as f:
                target_contents = f.read()

            expected = (
                'index_code,state,county,wght2,wght1,wght3\n'
                '0X27B3B62D,-,-,0.0,0.0,0.0\n'
                '1XA0157D6E,Illinois,Cook,200.0,100.0,300.0\n'
                '2XF38F26EA,Indiana,Porter,200.0,100.0,300.0\n'
                '3X7429EDA9,Michigan,Cass,200.0,100.0,300.0\n'
            )
            self.assertEqual(target_contents, expected)


class TestIndexReadFromStdin(unittest.TestCase):
    def test_input_labels_and_weights(self):
        node = DataSpace()
        node.add_index_columns('state', 'county')
        node.add_weight_group('population', make_default=True)

        args = argparse.Namespace(
            on_label_conflict='abort',
            on_weight_conflict='abort',
            stdin=DummyRedirection(
                'state,county,population\n'
                'Illinois,Cook,5275541\n'
                'Indiana,Porter,175860\n'
                'Michigan,Cass,51589\n'
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index.read_from_stdin(args, node)  # <- Function under test.

        index_values = list(node.select_index(header=True))
        expected_values = [
            ('index_id', 'state', 'county'),
            (0, '-', '-'),
            (1, 'Illinois', 'Cook'),
            (2, 'Indiana', 'Porter'),
            (3, 'Michigan', 'Cass'),
        ]
        self.assertEqual(index_values, expected_values)
        self.assertEqual(
            logs_cm.output,
            ['INFO:app-toron.space:loaded 3 index labels',
             'INFO:app-toron.space:loaded 3 index weights'],
        )

    def test_abort_on_label_conflict(self):
        node = DataSpace()
        node._connector._unique_id = '11111111-1111-1111-1111-111111111111'
        node.add_index_columns('state', 'county')
        node.add_weight_group('population', make_default=True)
        node.insert_index([['state', 'county'],
                           ['Illinois', 'Cook'],
                           ['Indiana', 'Porter'],
                           ['Michigan', 'Cass']])

        args = argparse.Namespace(
            on_label_conflict='abort',
            on_weight_conflict='abort',
            stdin=DummyRedirection(
                'index_code,state,county,population\n'
                '1XA0157D6E,Illinois,Cook,5275541\n'
                '2XF38F26EA,Indiana,Porter,175860\n'
                '3X7429EDA9,Michigan,OTHERVALUE,51589\n'  # <- Will abort operation.
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index.read_from_stdin(args, node)  # <- Function under test.

        self.assertEqual(
            logs_cm.output,
            ["ERROR:app-toron:index code 3X7429EDA9 and labels ('Michigan', 'OTHERVALUE') "
               "do not match Index(id=3, labels=('Michigan', 'Cass'))\n"
               "  load behavior can be changed using --on-label-conflict "
               "and --on-weight-conflict"]
        )

    def test_ignore_on_label_conflict(self):
        node = DataSpace()
        node._connector._unique_id = '11111111-1111-1111-1111-111111111111'
        node.add_index_columns('state', 'county')
        node.add_weight_group('population', make_default=True)
        node.insert_index([['state', 'county'],
                           ['Illinois', 'Cook'],
                           ['Indiana', 'Porter'],
                           ['Michigan', 'Cass']])

        args = argparse.Namespace(
            on_label_conflict='ignore',
            on_weight_conflict='abort',
            stdin=DummyRedirection(
                'index_code,state,county,population\n'
                '1XA0157D6E,Illinois,Cook,5275541\n'
                '2XF38F26EA,Indiana,Porter,175860\n'
                '3X7429EDA9,Michigan,OTHERVALUE,51589\n'  # <- Label will be ignored.
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index.read_from_stdin(args, node)  # <- Function under test.

        self.assertEqual(
            logs_cm.output,
            ['INFO:app-toron.space:ignored 1 non-matching index labels',
             'INFO:app-toron.space:loaded 3 index weights']
        )

    def test_replace_on_label_conflict(self):
        node = DataSpace()
        node._connector._unique_id = '11111111-1111-1111-1111-111111111111'
        node.add_index_columns('state', 'county')
        node.add_weight_group('population', make_default=True)
        node.insert_index([['state', 'county'],
                           ['Illinois', 'Cook'],
                           ['Indiana', 'Porter'],
                           ['Michigan', 'Cass']])

        args = argparse.Namespace(
            on_label_conflict='replace',
            on_weight_conflict='abort',
            stdin=DummyRedirection(
                'index_id,state,county,population\n'
                '1XA0157D6E,Illinois,Cook,5275541\n'
                '2XF38F26EA,Indiana,Porter,175860\n'
                '3X7429EDA9,Michigan,OTHERVALUE,51589\n'  # <- Will replace with new label.
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index.read_from_stdin(args, node)  # <- Function under test.

        self.assertEqual(
            logs_cm.output,
            ['INFO:app-toron.space:replaced 1 index labels',
             'INFO:app-toron.space:loaded 3 index weights']
        )

    def test_abort_on_weight_conflict(self):
        node = DataSpace()
        node.add_index_columns('state', 'county')
        node.add_weight_group('population', make_default=True)

        args = argparse.Namespace(
            on_label_conflict='abort',
            on_weight_conflict='abort',
            stdin=DummyRedirection(
                'state,county,population\n'
                'Illinois,Cook,5275541\n'
                'Indiana,Porter,175860\n'
                'Michigan,Cass,51589\n'
                'Michigan,Cass,50000\n'  # <- Will abort operation.
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index.read_from_stdin(args, node)  # <- Function under test.

        index_values = list(node.select_index(header=True))
        expected_values = [('index_id', 'state', 'county'), (0, '-', '-')]
        self.assertEqual(index_values, expected_values)
        self.assertEqual(
            logs_cm.output,
            ["ERROR:app-toron:weight group 'population' already has "
               "a value for Index(id=3, labels=('Michigan', 'Cass'))\n"
               "  load behavior can be changed using --on-label-conflict "
               "and --on-weight-conflict"],
        )

    def test_replace_on_weight_conflict(self):
        node = DataSpace()
        node.add_index_columns('state', 'county')
        node.add_weight_group('population', make_default=True)

        args = argparse.Namespace(
            on_label_conflict='abort',
            on_weight_conflict='replace',
            stdin=DummyRedirection(
                'state,county,population\n'
                'Illinois,Cook,5275541\n'
                'Indiana,Porter,175860\n'
                'Michigan,Cass,0\n'  # <- Will get replaced by later record.
                'Michigan,Cass,51589\n'
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index.read_from_stdin(args, node)  # <- Function under test.

        index_values = list(node.select_index(header=True))
        expected_values = [
            ('index_id', 'state', 'county'),
            (0, '-', '-'),
            (1, 'Illinois', 'Cook'),
            (2, 'Indiana', 'Porter'),
            (3, 'Michigan', 'Cass'),
        ]
        self.assertEqual(index_values, expected_values)
        self.assertEqual(
            logs_cm.output,
            ['INFO:app-toron.space:loaded 3 index labels',
             'INFO:app-toron.space:loaded 3 index weights',
             'INFO:app-toron.space:replaced 1 index weights'],
        )


class TestIndexWriteToStdout(unittest.TestCase):
    def test_basic_behavior(self):
        node = DataSpace()
        node._connector._unique_id = '11111111-1111-1111-1111-111111111111'
        node.add_index_columns('state', 'county')
        node.add_weight_group('wght3', make_default=False)
        node.add_weight_group('wght2', make_default=True)
        node.add_weight_group('wght1')

        node.insert_index([
            ['state', 'county', 'wght1', 'wght2', 'wght3'],
            ['Illinois', 'Cook', 100, 200, 300],
            ['Indiana', 'Porter', 100, 200, 300],
            ['Michigan', 'Cass', 100, 200, 300],
        ])
        dummy_stdout = DummyRedirection()
        args = argparse.Namespace(command='index', node=node, stdout=dummy_stdout)

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_index.write_to_stdout(args, node)  # <- Function under test.

        expected_values = (
            'index_code,state,county,wght2,wght1,wght3\n'
            '0X27B3B62D,-,-,0.0,0.0,0.0\n'
            '1XA0157D6E,Illinois,Cook,200.0,100.0,300.0\n'
            '2XF38F26EA,Indiana,Porter,200.0,100.0,300.0\n'
            '3X7429EDA9,Michigan,Cass,200.0,100.0,300.0\n'
        )
        self.assertEqual(dummy_stdout.getvalue(), expected_values)
        self.assertEqual(logs_cm.output, ['INFO:app-toron:written 4 records'])
