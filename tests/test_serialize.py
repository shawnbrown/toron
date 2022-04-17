"""Tests for toron._serialize module."""

import unittest
from collections import namedtuple, OrderedDict
from toron._serialize import _is_primitive
from toron._serialize import dumps


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


class TestDumpS(unittest.TestCase):
    def test_primitive_types(self):
        self.assertEqual(dumps(1.125), '1.125')
        self.assertEqual(dumps(b'abc'), "b'abc'")

    def test_list_or_tuple(self):
        self.assertEqual(dumps([4, 8, 2]), "[4, 8, 2]")
        self.assertEqual(dumps((1, 'a', 2.25)), "(1, 'a', 2.25)")

        msg = 'should not serialize nested containers'
        with self.assertRaises(TypeError, msg=msg):
            dumps([1, [2, 3]])

        msg = 'should not serialize instances of subclasses'
        with self.assertRaises(TypeError, msg=msg):
            coord = namedtuple('coord', ['x', 'y'])
            dumps(coord(1, 2))

    def test_set(self):
        msg = 'serialized form should always be in sorted order'
        self.assertEqual(dumps({4, 8, 2}), "{2, 4, 8}", msg=msg)

        msg = 'mixed types should sort without problems'
        self.assertEqual(dumps({None, 2, 'a', 1.25}), "{'a', 1.25, 2, None}", msg=msg)

        msg = 'should not serialize nested containers'
        with self.assertRaises(TypeError, msg=msg):
            dumps({4, (8, 2)})

        msg = 'should not serialize instances of subclasses'
        with self.assertRaises(TypeError, msg=msg):
            dumps(frozenset([1, 2, 3]))

    def test_dict(self):
        msg = 'serialized form should always be in sorted order'
        self.assertEqual(dumps({'b': 2, 'a': 1}), "{'a': 1, 'b': 2}", msg=msg)

        msg = 'mixed types should sort without problems'
        self.assertEqual(dumps({None: 2, 'a': 1.25}), "{'a': 1.25, None: 2}", msg=msg)

        msg = 'should not serialize nested containers'
        with self.assertRaises(TypeError, msg=msg):
            dumps({4: (8, 2)})

        msg = 'should not serialize non-primitive keys'
        with self.assertRaises(TypeError, msg=msg):
            dumps({(4, 8): 2})

        msg = 'should not serialize instances of subclasses'
        with self.assertRaises(TypeError, msg=msg):
            dumps(OrderedDict([('b', 2), ('a', 1)]))

    def test_unsupported_types(self):
        with self.assertRaises(TypeError):
            dumps(frozenset([1, 2, 3]))

        with self.assertRaises(TypeError):
            dumps(Ellipsis)

