# -*- coding: utf-8 -*-
import os
import pprint
import warnings

from gpn.partition import Partition

suffix = '.node'
suffix_default = '.node-default'


class Graph(object):
    def __init__(self, path=None, partitions=None):
        global suffix
        global suffix_default
        assert not path or not partitions, ('Cannot specify both path '
                                            'and partitions.')
        # Get partitions.
        if not partitions:
            if not path:
                path = os.getcwd()  # Defaule to cwd.

            def is_node(x):
                return x.endswith(suffix) or x.endswith(suffix_default)
            partitions = [Partition(x) for x in os.listdir(path) if is_node(x)]

            self.path = path

        else:
            self.path = '<from collection>'

        # Set nodes.
        def node_item(p):
            assert isinstance(p, Partition), '%r is not a Partition.' % p
            if p.name:
                key = p.name
            else:
                key = p.get_hash()[:12]  # <- TODO!!!: Implement get_hash().
                warnings.warn("Partition is unnamed--using "
                              "short hash '%s'." % key)
            return (key, p)
        self.nodes = dict(node_item(p) for p in partitions)

        # Set edges.
        self.edges = [None]
