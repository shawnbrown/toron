"""Tests for toron._serialize module."""

import unittest
from collections import namedtuple, OrderedDict, UserString
from toron._serialize import get_primitive_repr
from toron._serialize import dumps, loads
from toron._serialize import InvalidSerialization


class TestGetPrimitiveRepr(unittest.TestCase):
    def test_supported_types(self):
        """Check that all supported instance types get expected reprs."""
        supported_instances = [
            ('abc',  "'abc'"),   # str
            (b'xyz', "b'xyz'"),  # bytes
            (123,    '123'),     # int
            (1.125,  '1.125'),   # float
            (True,   'True'),    # bool
            (None,   'None'),    # NoneType
            ((3+0j), '(3+0j)'),  # complex
        ]
        for obj, obj_repr in supported_instances:
            with self.subTest(obj=obj):
                self.assertEqual(get_primitive_repr(obj), obj_repr)

    def test_unsupported_types(self):
        """Should return None for unsupported types (containers, etc.)"""
        self.assertIsNone(get_primitive_repr(Ellipsis))
        self.assertIsNone(get_primitive_repr([1, 2]))
        self.assertIsNone(get_primitive_repr({'a': 1}))

    def test_exact_type_matching(self):
        """Values that are a subclass of supported types should get None."""
        class StrSubclass(UserString):
            pass

        instance_of_str_subclass = StrSubclass('abc')
        self.assertIsNone(get_primitive_repr(instance_of_str_subclass))

    def test_no_valid_literal_repr(self):
        """Values that don't have a literal representation must return
        a None value even if the instance is of a supported type.
        """
        self.assertIsNone(get_primitive_repr(float('nan')))
        self.assertIsNone(get_primitive_repr(float('inf')))


class TestInvalidSerialization(unittest.TestCase):
    def test_initialization(self):
        bad_string = '[1, 2,'
        invalid = InvalidSerialization(bad_string)

        self.assertIsInstance(invalid, InvalidSerialization)
        self.assertEqual(invalid.data, bad_string)

    def test_representation(self):
        invalid = InvalidSerialization('[1, 2,')
        self.assertEqual(repr(invalid), "InvalidSerialization('[1, 2,')")

    def test_equality(self):
        bad_string = '[1, 2,'
        invalid_a = InvalidSerialization(bad_string)
        invalid_b = InvalidSerialization(bad_string)

        self.assertEqual(invalid_a, invalid_b)
        self.assertNotEqual(bad_string, invalid_a)
        self.assertNotEqual(invalid_a, InvalidSerialization("'foo"))


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


class TestLoadS(unittest.TestCase):
    def test_valid_strings(self):
        self.assertEqual(loads('1.125'), 1.125)
        self.assertEqual(loads("('a', 1, 2.25)"), ('a', 1, 2.25))
        self.assertEqual(loads("{'a': 1, 'b': 2}"), {'a': 1, 'b': 2})

