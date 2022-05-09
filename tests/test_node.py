"""Tests for toron/node.py module."""

import unittest

from .common import TempDirTestCase

from toron.node import Node


class TestNode(TempDirTestCase):
    def test_new_node(self):
        node = Node('mynode.toron')
        self.assertEqual(node.path, 'mynode.toron')
        self.assertEqual(node.mode, 'rwc')

