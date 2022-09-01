"""Tests for toron._selectors module."""

import unittest

from toron._selectors import Selector
from toron._selectors import CompoundSelector
from toron._selectors import _selector_comparison_key
from toron._selectors import parse_selector


class TestSelector(unittest.TestCase):
    def test_instantiation(self):
        Selector('aaa')
        Selector('aaa', '=', 'xxx')
        Selector('aaa', '=', 'xxx', ignore_case=True)

        with self.assertRaises(TypeError):
            Selector()

        with self.assertRaises(TypeError):
            Selector('aaa', '=', None)

        with self.assertRaises(TypeError):
            Selector('aaa', None, 'xxx')

        with self.assertRaises(TypeError):
            Selector('aaa', ignore_case=True)

    def test_match_any_value(self):
        selector = Selector('aaa')
        self.assertTrue(selector({'aaa': 'xxx'}))
        self.assertTrue(selector({'aaa': 'yyy'}))
        self.assertFalse(selector({'BBB': 'yyy'}))  # <- No attribute 'aaa'.
        self.assertFalse(selector({'aaa': ''}))  # <- Value is not truthy.

    def test_match_exact_value(self):
        selector = Selector('aaa', '=', 'xxx')
        self.assertTrue(selector({'aaa': 'xxx'}))
        self.assertFalse(selector({'aaa': 'YYY'}))  # <- Value does not match.
        self.assertFalse(selector({'BBB': 'xxx'}))  # <- No attribute 'aaa'.
        self.assertFalse(selector({'aaa': 'XXX'}))  # <- Matching is case-sensitive.

    def test_match_whitespace_separated_list(self):
        selector = Selector('aaa', '~=', 'xxx')
        self.assertTrue(selector({'aaa': 'ZZZ xxx YYY'}))
        self.assertTrue(selector({'aaa': 'xxx'}))  # <- Exact value should match, too.
        self.assertFalse(selector({'aaa': 'ZZZ-xxx-YYY'}))  # <- Not whitespace separated.
        self.assertFalse(selector({'aaa': 'ZZZ wxxx YYY'}))  # <- Substring won't match (must be exact).

        # Check irregular whitespace.
        self.assertTrue(selector({'aaa': 'UUU\tVVV\fWWW\r\nxxx\nYYY   ZZZ'}))

    def test_match_starts_with_value_and_hyphen(self):
        selector = Selector('aaa', '|=', 'xxx')
        self.assertTrue(selector({'aaa': 'xxx-YYY'}))
        self.assertTrue(selector({'aaa': 'xxx'}))  # <- Exact value should match, too.
        self.assertFalse(selector({'aaa': 'xxx YYY'}))  # <- Cannot be followed by any char except "-".
        self.assertFalse(selector({'aaa': 'ZZZ-xxx-YYY'}))  # <- Does not start with "xxx".

    def test_match_starts_with_value(self):
        selector = Selector('aaa', '^=', 'xxx')
        self.assertTrue(selector({'aaa': 'xxxYYY'}))
        self.assertTrue(selector({'aaa': 'xxx-YYY'}))
        self.assertTrue(selector({'aaa': 'xxx YYY'}))
        self.assertTrue(selector({'aaa': 'xxx'}))  # <- Exact value should match, too.
        self.assertFalse(selector({'aaa': 'Zxxx'}))  # <- Does not start with "xxx".

    def test_match_ends_with_value(self):
        selector = Selector('aaa', '$=', 'xxx')
        self.assertTrue(selector({'aaa': 'ZZZxxx'}))
        self.assertTrue(selector({'aaa': 'ZZZ-xxx'}))
        self.assertTrue(selector({'aaa': 'ZZZ xxx'}))
        self.assertTrue(selector({'aaa': 'xxx'}))  # <- Exact value should match, too.
        self.assertFalse(selector({'aaa': 'xxxZ'}))  # <- Does not end with "xxx".

    def test_match_substring_value(self):
        selector = Selector('aaa', '*=', 'xxx')
        self.assertTrue(selector({'aaa': 'ZZZxxxYYY'}))
        self.assertTrue(selector({'aaa': 'ZZZ-xxx-YYY'}))
        self.assertTrue(selector({'aaa': 'xxx'}))  # <- Exact value should match, too.
        self.assertTrue(selector({'aaa': 'ZZZ xxx YYY'}))
        self.assertFalse(selector({'aaa': 'ZZZ XXX YYY'}))  # <- Matching is case-sensitive.
        self.assertFalse(selector({'aaa': 'ZZZx-xx-YYY'}))  # <- No matching substring.

    def test_unknown_operator(self):
        regex = r"unknown operator: '//"
        with self.assertRaisesRegex(ValueError, regex):
            Selector('aaa', '//', 'xxx')

    def test_ignore_case(self):
        selector = Selector('aaa', '=', 'xxx', ignore_case=True)
        self.assertTrue(selector({'aaa': 'XXX'}))

        selector = Selector('aaa', '~=', 'xxx', ignore_case=True)
        self.assertTrue(selector({'aaa': 'zzz XXX yyy'}))

        selector = Selector('aaa', '|=', 'xxx', ignore_case=True)
        self.assertTrue(selector({'aaa': 'XXX-YYY'}))

        selector = Selector('aaa', '^=', 'xxx', ignore_case=True)
        self.assertTrue(selector({'aaa': 'XXXyyy'}))

        selector = Selector('aaa', '$=', 'xxx', ignore_case=True)
        self.assertTrue(selector({'aaa': 'zzzXXX'}))

        selector = Selector('aaa', '*=', 'xxx', ignore_case=True)
        self.assertTrue(selector({'aaa': 'zzzXXXyyy'}))

    def test_repr(self):
        sel_repr = "Selector('aaa')"
        self.assertEqual(repr(eval(sel_repr)), sel_repr)

        sel_repr = "Selector('aaa', '=', 'xxx')"
        self.assertEqual(repr(eval(sel_repr)), sel_repr)

        sel_repr = "Selector('aaa', '=', 'xxx', ignore_case=True)"
        self.assertEqual(repr(eval(sel_repr)), sel_repr)

    def test_str(self):
        selector = Selector('aaa')
        self.assertEqual(str(selector), '[aaa]')

        selector = Selector('aaa', '=', 'xxx')
        self.assertEqual(str(selector), '[aaa="xxx"]')

        selector = Selector('aaa', '=', 'xxx', ignore_case=True)
        self.assertEqual(str(selector), '[aaa="xxx" i]')

    def test_eq_and_hash(self):
        equal_values = [
            (Selector('aaa'),
             Selector('aaa')),

            (Selector('aaa', '=', 'xxx'),
             Selector('aaa', '=', 'xxx')),

            (Selector('aaa', '=', 'xxx', ignore_case=True),
             Selector('aaa', '=', 'xxx', ignore_case=True)),

            (Selector('aaa', '=', 'xxx', ignore_case=False),
             Selector('aaa', '=', 'xxx', ignore_case=None)),

            (Selector('aaa', '=', 'qqq', ignore_case=True),
             Selector('aaa', '=', 'QQQ', ignore_case=True)),
        ]
        for a, b in equal_values:
            with self.subTest(a=a, b=b):
                self.assertEqual(a, b)
                self.assertEqual(hash(a), hash(b))

        not_equal_values = [
            (Selector('aaa'),
             Selector('bbb')),

            (Selector('aaa', '=', 'xxx'),
             Selector('aaa', '^=', 'xxx')),

            (Selector('aaa', '=', 'xxx'),
             Selector('aaa', '=', 'yyy')),

            (Selector('aaa', '=', 'xxx'),
             Selector('bbb', '=', 'xxx')),

            (Selector('aaa', '=', 'xxx', ignore_case=True),
             Selector('AAA', '=', 'xxx', ignore_case=True)),
        ]
        for a, b in not_equal_values:
            with self.subTest(a=a, b=b):
                self.assertNotEqual(a, b)
                self.assertNotEqual(hash(a), hash(b))

    def test_specificity(self):
        """Specificity is modeled after CSS specificity but it's not
        the same. To see how specificity is determined in CSS, see:

            https://www.w3.org/TR/selectors-4/#specificity
        """
        selector = Selector('aaa')
        self.assertEqual(selector.specificity, (1, 0))

        selector = Selector('aaa', '=', 'xxx')
        self.assertEqual(selector.specificity, (1, 1))

        selector = Selector('aaa', '=', 'xxx', ignore_case=True)
        self.assertEqual(selector.specificity, (1, 1))


class TestSelectorComparisonKey(unittest.TestCase):
    def test_simple_key(self):
        result = _selector_comparison_key(Selector('aaa'))
        expected = ('simple', ('aaa', '', '', ''))
        self.assertEqual(result, expected)

        result = _selector_comparison_key(Selector('aaa', '=', 'Qqq'))
        expected = ('simple', ('aaa', '=', 'Qqq', ''))
        self.assertEqual(result, expected)

        result = _selector_comparison_key(Selector('aaa', '=', 'Qqq', ignore_case=True))
        expected = ('simple', ('aaa', '=', 'qqq', 'i'))
        self.assertEqual(result, expected)

    def test_simple_sort(self):
        selectors = [Selector('bbb'), Selector('aaa', '=', 'qqq')]
        result = sorted(selectors, key=_selector_comparison_key)
        expected = [Selector('aaa', '=', 'qqq'), Selector('bbb')]
        self.assertEqual(result, expected)


class TestCompoundSelector(unittest.TestCase):
    def test_simple_selector(self):
        """When given a single item list, should return the item itself."""
        selector = Selector('aaa')
        result = CompoundSelector([selector])
        self.assertIs(selector, result)

    def test_multiple_selectors(self):
        """All selectors must match to return True."""
        selector = CompoundSelector([Selector('aaa', '=', 'xxx'), Selector('bbb')])
        self.assertTrue(selector({'aaa': 'xxx', 'bbb': 'yyy'}))
        self.assertTrue(selector({'aaa': 'xxx', 'bbb': 'zzz'}))
        self.assertFalse(selector({'aaa': 'yyy', 'bbb': 'yyy'}))  # <- value of 'aaa' does not match
        self.assertFalse(selector({'aaa': 'xxx', 'ccc': 'zzz'}))  # <- no key matching 'bbb'

    def test_repr(self):
        repr_list = [
            "CompoundSelector([Selector('aaa'), Selector('bbb'), Selector('ccc')])",
            "CompoundSelector([Selector('aaa', '=', 'xxx'), Selector('bbb')])",
            "CompoundSelector([Selector('aaa', '=', 'xxx', ignore_case=True), Selector('bbb')])",
        ]
        for r in repr_list:
            with self.subTest(r=r):
                self.assertEqual(repr(eval(r)), r)

    def test_str(self):
        selector = CompoundSelector([Selector('aaa'), Selector('bbb'), Selector('ccc')])
        self.assertEqual(str(selector), '[aaa][bbb][ccc]')

        selector = CompoundSelector([Selector('aaa', '=', 'xxx'), Selector('bbb')])
        self.assertEqual(str(selector), '[aaa="xxx"][bbb]')

        selector = CompoundSelector([Selector('aaa', '=', 'xxx', ignore_case=True), Selector('bbb')])
        self.assertEqual(str(selector), '[aaa="xxx" i][bbb]')

    def test_eq(self):
        sel_a = CompoundSelector([Selector('aaa'), Selector('bbb')])
        sel_b = CompoundSelector([Selector('bbb'), Selector('aaa')])
        self.assertEqual(sel_a, sel_b)

        sel_a = CompoundSelector([Selector('aaa'), Selector('bbb')])
        sel_b = CompoundSelector([Selector('ccc'), Selector('aaa')])
        self.assertNotEqual(sel_a, sel_b)


class TestParseSelector(unittest.TestCase):
    def test_matches_any(self):
        result = parse_selector('[aaa]')
        expected = Selector('aaa')
        self.assertEqual(result, expected)

    def test_matches_value(self):
        result = parse_selector('[aaa="xxx"]')
        expected = Selector('aaa', '=', 'xxx')
        self.assertEqual(result, expected)

