"""Tests for toron/cli/command_quantity.py module."""
import argparse
from .. import _unittest as unittest
from ..common import DummyRedirection
from toron import DataSpace, ToronError

from toron.cli import command_quantity


class QuantityMixin(object):
    @staticmethod
    def set_unique_id(ds, unique_id):
        ds._connector._unique_id = unique_id
        with ds._managed_transaction() as cur:
            property_repo = ds._dal.PropertyRepository(cur)
            property_repo.update('unique_id', unique_id)

    def setUp(self):
        self.maxDiff = None

        self.ds = DataSpace()
        self.set_unique_id(self.ds, '11111111-1111-1111-1111-111111111111')
        self.ds.set_domain('iso_US')
        self.ds.add_index_columns('state', 'county')
        self.ds.add_weight_group('population', make_default=True)
        self.ds.insert_index([('state', 'county',   'population'),
                              ('OH',    'BUTLER',   374150),
                              ('OH',    'FRANKLIN', 1336250),
                              ('IN',    'KNOX',     36864),
                              ('IN',    'LAPORTE',  110592)])


class TestQuantityImportRecords(QuantityMixin, unittest.TestCase):
    def test_import_records(self):
        self.ds.set_registered_attributes(['category', 'sex'])

        reader = iter([
            ['domain', 'state', 'county', 'category', 'sex', 'quantity'],
            ['iso_US', 'OH', 'BUTLER', 'TOTAL', 'MALE', '180140'],
            ['iso_US', 'OH', 'BUTLER', 'TOTAL', 'FEMALE', '187990'],
            ['iso_US', 'OH', 'FRANKLIN', 'TOTAL', 'MALE', '566499'],
            ['iso_US', 'OH', 'FRANKLIN', 'TOTAL', 'FEMALE', '596915'],
        ])

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_quantity._import_records(  # <- Function under test.
                ds=self.ds,
                reader=reader,
                value_column='quantity',  # <- This is the default column name.
                allow_invalid_label=False,
                allow_invalid_partition=False,
                on_existing='abort',
            )

        self.assertEqual(
            logs_cm.output,
            ['INFO:app-toron.space:loaded 4 quantities'],
        )

        self.assertEqual(
            list(self.ds.select_quantities(header=True)),
            [['domain', 'state', 'county', 'category', 'sex', 'quantity'],
             ['iso_US', 'OH', 'BUTLER', 'TOTAL', 'MALE', 180140.0],
             ['iso_US', 'OH', 'BUTLER', 'TOTAL', 'FEMALE', 187990.0],
             ['iso_US', 'OH', 'FRANKLIN', 'TOTAL', 'MALE', 566499.0],
             ['iso_US', 'OH', 'FRANKLIN', 'TOTAL', 'FEMALE', 596915.0]],
        )

    def test_no_attributes(self):
        regex = 'operation cancelled, no attributes registered'
        with self.assertRaisesRegex(ToronError, regex) as cm:
            command_quantity._import_records(  # <- Function under test.
                ds=self.ds,
                reader=iter([
                    ['domain', 'state', 'county', 'category', 'sex', 'quantity'],
                    ['iso_US', 'OH', 'BUTLER', 'TOTAL', 'MALE', '180140'],
                    ['iso_US', 'OH', 'BUTLER', 'TOTAL', 'FEMALE', '187990'],
                ]),
                value_column='quantity',
                allow_invalid_label=False,
                allow_invalid_partition=False,
                on_existing='abort',
            )

    def test_no_index_records(self):
        ds = DataSpace()  # <- Empty DataSpace.
        ds.set_domain('iso_US')
        ds.set_registered_attributes(['category', 'sex'])

        regex = 'operation cancelled, file contains no index records'
        with self.assertRaisesRegex(ToronError, regex) as cm:
            command_quantity._import_records(  # <- Function under test.
                ds=ds,
                reader=iter([
                    ['domain', 'state', 'county', 'category', 'sex', 'quantity'],
                    ['iso_US', 'OH', 'BUTLER', 'TOTAL', 'MALE', '180140'],
                    ['iso_US', 'OH', 'BUTLER', 'TOTAL', 'FEMALE', '187990'],
                ]),
                value_column='quantity',
                allow_invalid_label=False,
                allow_invalid_partition=False,
                on_existing='abort',
            )


class TestQuantityExportRecords(QuantityMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.ds.set_registered_attributes(['category', 'sex'])
        self.ds.insert_quantities2(
            value_column='quantity',
            data=[['domain', 'state', 'county',   'category', 'sex',    'quantity'],
                  ['iso_US', 'OH',    'BUTLER',   'TOTAL',    'MALE',   180140.0],
                  ['iso_US', 'OH',    'BUTLER',   'TOTAL',    'FEMALE', 187990.0],
                  ['iso_US', 'OH',    'FRANKLIN', 'TOTAL',    'MALE',   566499.0],
                  ['iso_US', 'OH',    'FRANKLIN', 'TOTAL',    'FEMALE', 596915.0]],
        )

    def test_export_records_generator(self):
        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            generator = command_quantity._export_records(self.ds)  # <- Function under test.
            records = list(generator)

        self.assertEqual(logs_cm.output, ['INFO:app-toron:written 4 records'])

        expected_values = [
            ['domain', 'state', 'county',   'category', 'sex',    'quantity'],
            ['iso_US', 'OH',    'BUTLER',   'TOTAL',    'MALE',   180140.0],
            ['iso_US', 'OH',    'BUTLER',   'TOTAL',    'FEMALE', 187990.0],
            ['iso_US', 'OH',    'FRANKLIN', 'TOTAL',    'MALE',   566499.0],
            ['iso_US', 'OH',    'FRANKLIN', 'TOTAL',    'FEMALE', 596915.0],
        ]
        self.assertEqual(records, expected_values)


class TestReadFromStdin(QuantityMixin, unittest.TestCase):
    def test_standard_input_columns(self):
        """Check input with domain, all labels, and all attributes."""
        self.ds.set_registered_attributes(['category', 'sex'])

        args = argparse.Namespace(
            filepath='file1.toron',
            command='quantity',
            value_column='quantity',  # <- This is the default column name.
            allow_invalid_label=False,
            allow_invalid_partition=False,
            on_existing='abort',
            stdin=DummyRedirection(
                'domain,state,county,category,sex,quantity\n'
                'iso_US,OH,BUTLER,TOTAL,MALE,180140\n'
                'iso_US,OH,BUTLER,TOTAL,FEMALE,187990\n'
                'iso_US,OH,FRANKLIN,TOTAL,MALE,566499\n'
                'iso_US,OH,FRANKLIN,TOTAL,FEMALE,596915\n'
            ),
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_quantity.read_from_stdin(args, self.ds)  # <- Function under test.

        self.assertEqual(
            logs_cm.output,
            ['INFO:app-toron.space:loaded 4 quantities'],
        )

        self.assertEqual(
            list(self.ds.select_quantities(header=True)),
            [['domain', 'state', 'county',   'category', 'sex',    'quantity'],
             ['iso_US', 'OH',    'BUTLER',   'TOTAL',    'MALE',   180140.0],
             ['iso_US', 'OH',    'BUTLER',   'TOTAL',    'FEMALE', 187990.0],
             ['iso_US', 'OH',    'FRANKLIN', 'TOTAL',    'MALE',   566499.0],
             ['iso_US', 'OH',    'FRANKLIN', 'TOTAL',    'FEMALE', 596915.0]],
        )

    def test_alternate_value_column(self):
        """Check data with non-default value column."""
        self.ds.set_registered_attributes(['category', 'sex'])

        args = argparse.Namespace(
            filepath='file1.toron',
            command='quantity',
            value_column='counts',  # <- Non-default value column.
            allow_invalid_label=False,
            allow_invalid_partition=False,
            on_existing='abort',
            stdin=DummyRedirection(
                'domain,state,county,category,sex,counts\n'  # <- Value in "counts" column.
                'iso_US,OH,BUTLER,TOTAL,MALE,180140\n'
                'iso_US,OH,BUTLER,TOTAL,FEMALE,187990\n'
                'iso_US,OH,FRANKLIN,TOTAL,MALE,566499\n'
                'iso_US,OH,FRANKLIN,TOTAL,FEMALE,596915\n'
            ),
        )

        command_quantity.read_from_stdin(args, self.ds)  # <- Function under test.

        self.assertEqual(
            list(self.ds.select_quantities(header=True)),
            [['domain', 'state', 'county',   'category', 'sex',    'quantity'],
             ['iso_US', 'OH',    'BUTLER',   'TOTAL',    'MALE',   180140.0],
             ['iso_US', 'OH',    'BUTLER',   'TOTAL',    'FEMALE', 187990.0],
             ['iso_US', 'OH',    'FRANKLIN', 'TOTAL',    'MALE',   566499.0],
             ['iso_US', 'OH',    'FRANKLIN', 'TOTAL',    'FEMALE', 596915.0]],
        )


class TestWriteToStdout(QuantityMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.ds.set_registered_attributes(['category', 'sex'])
        self.ds.insert_quantities2(
            value_column='quantity',
            data=[['domain', 'state', 'county',   'category', 'sex',    'quantity'],
                  ['iso_US', 'OH',    'BUTLER',   'TOTAL',    'MALE',   180140.0],
                  ['iso_US', 'OH',    'BUTLER',   'TOTAL',    'FEMALE', 187990.0],
                  ['iso_US', 'OH',    'FRANKLIN', 'TOTAL',    'MALE',   566499.0],
                  ['iso_US', 'OH',    'FRANKLIN', 'TOTAL',    'FEMALE', 596915.0]],
        )

    def test_basic_behavior(self):
        dummy_stdout = DummyRedirection()
        args = argparse.Namespace(
            command='quantity',
            ds=self.ds,
            stdout=dummy_stdout,
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_quantity.write_to_stdout(args, self.ds)  # <- Function under test.

        self.assertEqual(logs_cm.output, ['INFO:app-toron:written 4 records'])

        expected_values = (
            'domain,state,county,category,sex,quantity\n'
            'iso_US,OH,BUTLER,TOTAL,MALE,180140.0\n'
            'iso_US,OH,BUTLER,TOTAL,FEMALE,187990.0\n'
            'iso_US,OH,FRANKLIN,TOTAL,MALE,566499.0\n'
            'iso_US,OH,FRANKLIN,TOTAL,FEMALE,596915.0\n'
        )
        self.assertEqual(dummy_stdout.getvalue(), expected_values)
