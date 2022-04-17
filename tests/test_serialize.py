"""Tests for toron._serialize module."""

import unittest
from toron._serialize import _is_primitive


class TestIsPrimitive(unittest.TestCase):
    def test_supported_types(self):
        """Check that all supported instance types test as True."""
        supported_instances = [
            'abc',   # str
            b'xyz',  # bytes
            123,     # int
            1.125,   # float
            True,    # bool
            None,    # NoneType
            (3+0j),  # complex
        ]
        for obj in supported_instances:
            with self.subTest(obj=obj):
                self.assertTrue(_is_primitive(obj))

    def test_unsupported_types(self):
        """Should return False for non-supported types (containers, etc.)"""
        self.assertFalse(_is_primitive(Ellipsis))
        self.assertFalse(_is_primitive([1, 2]))
        self.assertFalse(_is_primitive({'a': 1}))

    def test_exact_type_matching(self):
        """Should not match instances of supported type subclasses."""
        class StrSubclass(str):
            pass

        instance_of_str_subclass = StrSubclass('abc')
        self.assertFalse(_is_primitive(instance_of_str_subclass))

    def test_no_valid_literal_repr(self):
        """Values that don't have a literal representation must test
        as False even if the instance is of a supported type.
        """
        self.assertFalse(_is_primitive(float('nan')))
        self.assertFalse(_is_primitive(float('inf')))

