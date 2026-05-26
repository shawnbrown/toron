"""Tests for toron/cli/command_quantity.py module."""
import argparse
from .. import _unittest as unittest
from ..common import DummyRedirection
from toron import TopoNode

from toron.cli import command_quantity


class QuantityMixin(object):
    @staticmethod
    def set_unique_id(node, unique_id):
        node._connector._unique_id = unique_id
        with node._managed_transaction() as cur:
            property_repo = node._dal.PropertyRepository(cur)
            property_repo.update('unique_id', unique_id)

    def setUp(self):
        self.maxDiff = None

        self.node = TopoNode()
        self.set_unique_id(self.node, '11111111-1111-1111-1111-111111111111')
        self.node.set_domain('iso_US')
        self.node.add_index_columns('state', 'county')
        self.node.add_weight_group('population', make_default=True)
        self.node.insert_index([('state', 'county',   'population'),
                                ('OH',    'BUTLER',   374150),
                                ('OH',    'FRANKLIN', 1336250),
                                ('IN',    'KNOX',     36864),
                                ('IN',    'LAPORTE',  110592)])


class TestReadFromStdin(QuantityMixin, unittest.TestCase):
    def test_standard_input_columns(self):
        """Check input with domain, all labels, and all attributes."""
        self.node.set_registered_attributes(['category', 'sex'])

        args = argparse.Namespace(
            command='quantity',
            node=self.node,
            value_column='quantity',  # <- This is the default column name.
            allow_invalid_label='abort',
            allow_invalid_category='abort',
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
            command_quantity.read_from_stdin(args)  # <- Function under test.

        self.assertEqual(
            logs_cm.output,
            ['INFO:app-toron.node:loaded 4 quantities'],
        )

        self.assertEqual(
            list(self.node.select_quantities(header=True)),
            [['domain', 'state', 'county',   'category', 'sex',    'quantity'],
             ['iso_US', 'OH',    'BUTLER',   'TOTAL',    'MALE',   180140.0],
             ['iso_US', 'OH',    'BUTLER',   'TOTAL',    'FEMALE', 187990.0],
             ['iso_US', 'OH',    'FRANKLIN', 'TOTAL',    'MALE',   566499.0],
             ['iso_US', 'OH',    'FRANKLIN', 'TOTAL',    'FEMALE', 596915.0]],
        )

    def test_alternate_value_column(self):
        """Check data with non-default value column."""
        self.node.set_registered_attributes(['category', 'sex'])

        args = argparse.Namespace(
            command='quantity',
            node=self.node,
            value_column='counts',  # <- Non-default value column.
            allow_invalid_label='abort',
            allow_invalid_category='abort',
            on_existing='abort',
            stdin=DummyRedirection(
                'domain,state,county,category,sex,counts\n'  # <- Value in "counts" column.
                'iso_US,OH,BUTLER,TOTAL,MALE,180140\n'
                'iso_US,OH,BUTLER,TOTAL,FEMALE,187990\n'
                'iso_US,OH,FRANKLIN,TOTAL,MALE,566499\n'
                'iso_US,OH,FRANKLIN,TOTAL,FEMALE,596915\n'
            ),
        )

        command_quantity.read_from_stdin(args)  # <- Function under test.

        self.assertEqual(
            list(self.node.select_quantities(header=True)),
            [['domain', 'state', 'county',   'category', 'sex',    'quantity'],
             ['iso_US', 'OH',    'BUTLER',   'TOTAL',    'MALE',   180140.0],
             ['iso_US', 'OH',    'BUTLER',   'TOTAL',    'FEMALE', 187990.0],
             ['iso_US', 'OH',    'FRANKLIN', 'TOTAL',    'MALE',   566499.0],
             ['iso_US', 'OH',    'FRANKLIN', 'TOTAL',    'FEMALE', 596915.0]],
        )


class TestWriteToStdout(QuantityMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.node.set_registered_attributes(['category', 'sex'])
        self.node.insert_quantities2(
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
            node=self.node,
            stdout=dummy_stdout,
        )

        with self.assertLogs('app-toron', level='INFO') as logs_cm:
            command_quantity.write_to_stdout(args)  # <- Function under test.

        self.assertEqual(logs_cm.output, ['INFO:app-toron:written 4 records'])

        expected_values = (
            'domain,state,county,category,sex,quantity\n'
            'iso_US,OH,BUTLER,TOTAL,MALE,180140.0\n'
            'iso_US,OH,BUTLER,TOTAL,FEMALE,187990.0\n'
            'iso_US,OH,FRANKLIN,TOTAL,MALE,566499.0\n'
            'iso_US,OH,FRANKLIN,TOTAL,FEMALE,596915.0\n'
        )
        self.assertEqual(dummy_stdout.getvalue(), expected_values)
