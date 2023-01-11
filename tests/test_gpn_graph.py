# -*- coding: utf-8 -*-
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO  # New stdlib location in 3.0

from . import _unittest as unittest
from .common import TempDirTestCase

from toron._gpn_graph import Graph
from toron._gpn_node import Node
from toron.connector import IN_MEMORY


class TestInstantiation(TempDirTestCase):
    def setUp(self):
        self.addCleanup(self.cleanup_temp_files)

    def test_from_collection(self):
        old_boundary = Node(mode=IN_MEMORY, name='old_boundary')
        new_boundary = Node(mode=IN_MEMORY, name='new_boundary')
        collection = [old_boundary, new_boundary]
        graph = Graph(nodes=collection)  # Load nodes from list.

        node_names = set(graph.nodes.keys())
        self.assertSetEqual(set(['old_boundary', 'new_boundary']), node_names)

    def test_from_cwd(self):
        old_boundary = Node('old_boundary.node')
        new_boundary = Node('new_boundary.node')
        graph = Graph(path='.')  # Load node files in current directory.

        node_names = set(graph.nodes.keys())
        self.assertSetEqual(set(['old_boundary', 'new_boundary']), node_names)


if __name__ == '__main__':
    unittest.main()
