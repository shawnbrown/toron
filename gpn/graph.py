# -*- coding: utf-8 -*-
import os
import pprint
import warnings

from gpn.node import Node

suffix = '.node'
suffix_default = '.node-default'


class Graph(object):
    def __init__(self, path=None, nodes=None):
        global suffix
        global suffix_default
        assert not path or not nodes, ('Cannot specify both path '
                                            'and nodes.')
        # Get nodes.
        if not nodes:
            if not path:
                path = os.getcwd()  # Defaule to cwd.

            def is_node(x):
                return x.endswith(suffix) or x.endswith(suffix_default)
            nodes = [Node(x) for x in os.listdir(path) if is_node(x)]

            self.path = path

        else:
            self.path = '<from collection>'

        # Set nodes.
        def node_item(p):
            assert isinstance(p, Node), '%r is not a Node.' % p
            if p.name:
                key = p.name
            else:
                key = p.get_hash()[:12]  # <- TODO!!!: Implement get_hash().
                warnings.warn("Node is unnamed--using "
                              "short hash '%s'." % key)
            return (key, p)
        self.nodes = dict(node_item(p) for p in nodes)

        # Set edges.
        self.edges = [None]
