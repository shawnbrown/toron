"""Integration tests for idiomatic use cases.

A handful of integration tests to check for use cases that we
want to make sure are as convinient as possible for users.
"""

import unittest

import toron


@unittest.skip('not used')
class TestColumnNameHandling(unittest.TestCase):
    def test_special_char_col_names(self):
        pass
        # create node
        # add column names (including one with asterisk and one with spaces)
        #
        node = toron.Node()
        node.add_index_columns('foo', 'ba r')
        node.add_discrete_categories({'foo', 'ba r'})

        data = [
            ['foo', 'ba r', 'baz'],
            ['a', 'x', 100],
            ['b', 'y', 100],
            ['c', 'x', 100],
            ['d', 'y', 100],
            #['d', 'y', 100],
            ['-', '-', 10],

            #['foo', 'ba r', 'baz', 'qux', 'quux', 'value'],
            #['a', 'x', 100, 'e', 'E', 300],
            #['b', 'y', 100, 'g', 'G', 300],
            #['c', 'x', 100, 'o', 'O', 300],
            #['d', 'y', 100, 't', 'T', 300],
        ]
        node.insert_index(data)

        node.add_weight_group('baz', make_default=True)
        node.insert_weights('baz', data)
        #quantities = [
        #    ['foo', 'ba r', 'baz', 'qux', 'quux', 'value'],
        #    ['a', 'x', 100, 'e', 'E', 300],
        #    ['b', 'y', 100, 'g', 'G', 300],
        #    ['c', 'x', 100, 'o', 'O', 300],
        #    ['d', 'y', 100, 't', 'T', 300],
        #]
        #node.insert_quantities(
        #    value='value',
        #    attributes=['qux', 'quux'],
        #    data=quantities,
        #)


        #print(node)


#import logging
#import warnings
#from io import StringIO

#from toron.node import Node
#from toron._utils import (
#    ToronWarning,
#    BitFlags,
#)
#from toron.data_models import (
#    Index,
#    QuantityIterator,
#)
#from toron.graph import (
#    load_mapping,
#    _translate,
#    translate,
#    xadd_edge,
#)

