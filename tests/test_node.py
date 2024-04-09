"""Tests for toron/_node.py module."""

import sys
import unittest
from unittest.mock import (
    Mock,
    call,
    ANY,
)
if sys.version_info >= (3, 8):
    from typing import get_args
else:
    from typing_extensions import get_args

from toron._node import Node


class TestInstantiation(unittest.TestCase):
    def test_backend_implicit(self):
        """When no arguments are given, should create empty node."""
        node = Node()
        self.assertEqual(node._dal.backend, 'DAL1')

    def test_backend_explicit(self):
        """The ``backend`` can be given explicitly."""
        node = Node(backend='DAL1')
        self.assertEqual(node._dal.backend, 'DAL1')

    def test_backend_keyword_only(self):
        """The ``backend`` argument is keyword-only (not positional)."""
        with self.assertRaises(TypeError):
            node = Node('DAL1')  # Using positional argument.

    def test_backend_unknown(self):
        """Invalid ``backend`` values should raise an error."""
        with self.assertRaises(RuntimeError):
            node = Node(backend=None)

        with self.assertRaises(RuntimeError):
            node = Node(backend='DAL#')

    def test_kwds(self):
        """The ``**kwds`` are used to create a DataConnector."""
        node = Node(cache_to_drive=True)


class TestManagedResourceAndReader(unittest.TestCase):
    def test_managed_resource_type(self):
        """Resource manager should return appropriate type."""
        node = Node()  # Create node and get resource type (generic T1).
        resource_type = get_args(node._dal.DataConnector.__orig_bases__[0])[0]

        with node._managed_resource() as resource:
            pass

        self.assertIsInstance(resource, resource_type)

    def test_managed_resource_calls(self):
        """Resource manager should interact with resource methods."""
        node = Node()
        node._connector = Mock()

        with node._managed_resource() as resource:
            node._connector.assert_has_calls([
                call.acquire_resource(),  # <- Resource acquired.
            ])

        node._connector.assert_has_calls([
            call.acquire_resource(),
            call.release_resource(resource),  # <- Resource released.
        ])

    def test_managed_reader_type(self):
        """Data reader manager should return appropriate type."""
        node = Node()  # Create node and get reader type (generic T2).
        reader_type = get_args(node._dal.DataConnector.__orig_bases__[0])[1]

        with node._managed_resource() as resource:
            with node._managed_reader(resource) as reader:
                pass

        self.assertIsInstance(reader, reader_type)

    def test_managed_reader_calls(self):
        """Data reader manager should interact with reader methods."""
        node = Node()
        node._connector = Mock()

        with node._managed_resource() as resource:
            with node._managed_reader(resource) as reader:
                node._connector.assert_has_calls([
                    call.acquire_data_reader(resource),  # <- Reader acquired.
                ])

            node._connector.assert_has_calls([
                call.acquire_data_reader(resource),
                call.release_data_reader(reader),  # <- Reader released.
            ])

    def test_managed_reader_calls_implicit_resource(self):
        """Test ``_managed_reader`` called without ``resource`` argument
        (should automatically create a resource internally).
        """
        node = Node()
        node._connector = Mock()

        with node._managed_reader() as reader:  # <- No `resource` passed.
            node._connector.assert_has_calls([
                call.acquire_resource(),  # <- Resource acquired automatically.
                call.acquire_data_reader(ANY),  # <- Reader acquired.
            ])

        node._connector.assert_has_calls([
            call.acquire_resource(),
            call.acquire_data_reader(ANY),
            call.release_data_reader(reader),  # <- Reader released.
            call.release_resource(ANY),  # <- Resource released.
        ])


class TestColumnMethods(unittest.TestCase):
    @staticmethod
    def get_cols_helper(node):  # <- Helper function.
        with node._managed_reader() as data_reader:
            return node._dal.ColumnManager(data_reader).get_columns()

    def test_add_columns(self):
        node = Node()

        node.add_columns('A', 'B')

        self.assertEqual(self.get_cols_helper(node), ('A', 'B'))
