"""Tests for toron._selectors module."""

import unittest

from toron._selectors import SimpleSelector
from toron._selectors import MatchesAnySelector
from toron._selectors import NegationSelector
from toron._selectors import SpecificityAdjustmentSelector
from toron._selectors import CompoundSelector
from toron._selectors import _get_comparison_key
from toron._selectors import parse_selector


class TestSimpleSelector(unittest.TestCase):
    def test_instantiation(self):
        SimpleSelector('aaa')
        SimpleSelector('aaa', '=', 'xxx')
        SimpleSelector('aaa', '=', 'xxx', ignore_case=True)

        with self.assertRaises(TypeError):
            SimpleSelector()

        with self.assertRaises(TypeError):
            SimpleSelector('aaa', '=', None)

        with self.assertRaises(TypeError):
            SimpleSelector('aaa', None, 'xxx')

        with self.assertRaises(TypeError):
            SimpleSelector('aaa', ignore_case=True)

    def test_match_any_value(self):
        selector = SimpleSelector('aaa')
        self.assertTrue(selector({'aaa': 'xxx'}))
        self.assertTrue(selector({'aaa': 'yyy'}))
        self.assertFalse(selector({'BBB': 'yyy'}))  # <- No attribute 'aaa'.
        self.assertFalse(selector({'aaa': ''}))  # <- Value is not truthy.

    def test_match_exact_value(self):
        selector = SimpleSelector('aaa', '=', 'xxx')
        self.assertTrue(selector({'aaa': 'xxx'}))
        self.assertFalse(selector({'aaa': 'YYY'}))  # <- Value does not match.
        self.assertFalse(selector({'BBB': 'xxx'}))  # <- No attribute 'aaa'.
        self.assertFalse(selector({'aaa': 'XXX'}))  # <- Matching is case-sensitive.

    def test_match_whitespace_separated_list(self):
        selector = SimpleSelector('aaa', '~=', 'xxx')
        self.assertTrue(selector({'aaa': 'ZZZ xxx YYY'}))
        self.assertTrue(selector({'aaa': 'xxx'}))  # <- Exact value should match, too.
        self.assertFalse(selector({'aaa': 'ZZZ-xxx-YYY'}))  # <- Not whitespace separated.
        self.assertFalse(selector({'aaa': 'ZZZ wxxx YYY'}))  # <- Substring won't match (must be exact).

        # Check irregular whitespace.
        self.assertTrue(selector({'aaa': 'UUU\tVVV\fWWW\r\nxxx\nYYY   ZZZ'}))

    def test_match_starts_with_value_and_hyphen(self):
        selector = SimpleSelector('aaa', '|=', 'xxx')
        self.assertTrue(selector({'aaa': 'xxx-YYY'}))
        self.assertTrue(selector({'aaa': 'xxx'}))  # <- Exact value should match, too.
        self.assertFalse(selector({'aaa': 'xxx YYY'}))  # <- Cannot be followed by any char except "-".
        self.assertFalse(selector({'aaa': 'ZZZ-xxx-YYY'}))  # <- Does not start with "xxx".

    def test_match_starts_with_value(self):
        selector = SimpleSelector('aaa', '^=', 'xxx')
        self.assertTrue(selector({'aaa': 'xxxYYY'}))
        self.assertTrue(selector({'aaa': 'xxx-YYY'}))
        self.assertTrue(selector({'aaa': 'xxx YYY'}))
        self.assertTrue(selector({'aaa': 'xxx'}))  # <- Exact value should match, too.
        self.assertFalse(selector({'aaa': 'Zxxx'}))  # <- Does not start with "xxx".

    def test_match_ends_with_value(self):
        selector = SimpleSelector('aaa', '$=', 'xxx')
        self.assertTrue(selector({'aaa': 'ZZZxxx'}))
        self.assertTrue(selector({'aaa': 'ZZZ-xxx'}))
        self.assertTrue(selector({'aaa': 'ZZZ xxx'}))
        self.assertTrue(selector({'aaa': 'xxx'}))  # <- Exact value should match, too.
        self.assertFalse(selector({'aaa': 'xxxZ'}))  # <- Does not end with "xxx".

    def test_match_substring_value(self):
        selector = SimpleSelector('aaa', '*=', 'xxx')
        self.assertTrue(selector({'aaa': 'ZZZxxxYYY'}))
        self.assertTrue(selector({'aaa': 'ZZZ-xxx-YYY'}))
        self.assertTrue(selector({'aaa': 'xxx'}))  # <- Exact value should match, too.
        self.assertTrue(selector({'aaa': 'ZZZ xxx YYY'}))
        self.assertFalse(selector({'aaa': 'ZZZ XXX YYY'}))  # <- Matching is case-sensitive.
        self.assertFalse(selector({'aaa': 'ZZZx-xx-YYY'}))  # <- No matching substring.

    def test_unknown_operator(self):
        regex = r"unknown operator: '//"
        with self.assertRaisesRegex(ValueError, regex):
            SimpleSelector('aaa', '//', 'xxx')

    def test_ignore_case(self):
        selector = SimpleSelector('aaa', '=', 'xxx', ignore_case=True)
        self.assertTrue(selector({'aaa': 'XXX'}))

        selector = SimpleSelector('aaa', '~=', 'xxx', ignore_case=True)
        self.assertTrue(selector({'aaa': 'zzz XXX yyy'}))

        selector = SimpleSelector('aaa', '|=', 'xxx', ignore_case=True)
        self.assertTrue(selector({'aaa': 'XXX-YYY'}))

        selector = SimpleSelector('aaa', '^=', 'xxx', ignore_case=True)
        self.assertTrue(selector({'aaa': 'XXXyyy'}))

        selector = SimpleSelector('aaa', '$=', 'xxx', ignore_case=True)
        self.assertTrue(selector({'aaa': 'zzzXXX'}))

        selector = SimpleSelector('aaa', '*=', 'xxx', ignore_case=True)
        self.assertTrue(selector({'aaa': 'zzzXXXyyy'}))

    def test_repr(self):
        sel_repr = "SimpleSelector('aaa')"
        self.assertEqual(repr(eval(sel_repr)), sel_repr)

        sel_repr = "SimpleSelector('aaa', '=', 'xxx')"
        self.assertEqual(repr(eval(sel_repr)), sel_repr)

        sel_repr = "SimpleSelector('aaa', '=', 'xxx', ignore_case=True)"
        self.assertEqual(repr(eval(sel_repr)), sel_repr)

    def test_str(self):
        selector = SimpleSelector('aaa')
        self.assertEqual(str(selector), '[aaa]')

        selector = SimpleSelector('aaa', '=', 'xxx')
        self.assertEqual(str(selector), '[aaa="xxx"]')

        selector = SimpleSelector('aaa', '=', 'xxx', ignore_case=True)
        self.assertEqual(str(selector), '[aaa="xxx" i]')

    def test_eq_and_hash(self):
        equal_values = [
            (SimpleSelector('aaa'),
             SimpleSelector('aaa')),

            (SimpleSelector('aaa', '=', 'xxx'),
             SimpleSelector('aaa', '=', 'xxx')),

            (SimpleSelector('aaa', '=', 'xxx', ignore_case=True),
             SimpleSelector('aaa', '=', 'xxx', ignore_case=True)),

            (SimpleSelector('aaa', '=', 'xxx', ignore_case=False),
             SimpleSelector('aaa', '=', 'xxx', ignore_case=None)),

            (SimpleSelector('aaa', '=', 'qqq', ignore_case=True),
             SimpleSelector('aaa', '=', 'QQQ', ignore_case=True)),
        ]
        for a, b in equal_values:
            with self.subTest(a=a, b=b):
                self.assertEqual(a, b)
                self.assertEqual(hash(a), hash(b))

        not_equal_values = [
            (SimpleSelector('aaa'),
             SimpleSelector('bbb')),

            (SimpleSelector('aaa', '=', 'xxx'),
             SimpleSelector('aaa', '^=', 'xxx')),

            (SimpleSelector('aaa', '=', 'xxx'),
             SimpleSelector('aaa', '=', 'yyy')),

            (SimpleSelector('aaa', '=', 'xxx'),
             SimpleSelector('bbb', '=', 'xxx')),

            (SimpleSelector('aaa', '=', 'xxx', ignore_case=True),
             SimpleSelector('AAA', '=', 'xxx', ignore_case=True)),
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
        selector = SimpleSelector('aaa')
        self.assertEqual(selector.specificity, (1, 0))

        selector = SimpleSelector('aaa', '=', 'xxx')
        self.assertEqual(selector.specificity, (1, 1))

        selector = SimpleSelector('aaa', '=', 'xxx', ignore_case=True)
        self.assertEqual(selector.specificity, (1, 1))


class TestGetComparisonKey(unittest.TestCase):
    def test_simple_key(self):
        result = _get_comparison_key(SimpleSelector('aaa'))
        expected = (SimpleSelector, ('aaa', None, None, False))
        self.assertEqual(result, expected)

        result = _get_comparison_key(SimpleSelector('aaa', '=', 'Qqq'))
        expected = (SimpleSelector, ('aaa', '=', 'Qqq', False))
        self.assertEqual(result, expected)

        result = _get_comparison_key(SimpleSelector('aaa', '=', 'Qqq', ignore_case=True))
        expected = (SimpleSelector, ('aaa', '=', 'qqq', True))
        self.assertEqual(result, expected)

    def test_simple_sort(self):
        selectors = [SimpleSelector('bbb'), SimpleSelector('aaa', '=', 'qqq')]
        result = sorted(selectors, key=_get_comparison_key)
        expected = [SimpleSelector('aaa', '=', 'qqq'), SimpleSelector('bbb')]
        self.assertEqual(result, expected)

    def test_compound_selector(self):
        compound = CompoundSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        result = _get_comparison_key(compound)
        expected = (
            CompoundSelector,
            frozenset({
                (SimpleSelector, ('aaa', None, None, False)),
                (SimpleSelector, ('bbb', None, None, False)),
            }),
        )
        self.assertEqual(result, expected)

    def test_matches_any_selector(self):
        compound = MatchesAnySelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        result = _get_comparison_key(compound)
        expected = (
            MatchesAnySelector,
            frozenset({
                (SimpleSelector, ('aaa', None, None, False)),
                (SimpleSelector, ('bbb', None, None, False)),
            }),
        )
        self.assertEqual(result, expected)

    def test_compound_and_matches_any_selector(self):
        nested = CompoundSelector([
            SimpleSelector('aaa'),
            MatchesAnySelector([SimpleSelector('bbb'), SimpleSelector('ccc')]),
        ])

        result = _get_comparison_key(nested)

        expected = (
            CompoundSelector,
            frozenset({
                (SimpleSelector, ('aaa', None, None, False)),
                (
                    MatchesAnySelector,
                    frozenset({
                        (SimpleSelector, ('bbb', None, None, False)),
                        (SimpleSelector, ('ccc', None, None, False)),
                    }),
                ),
            }),
        )
        self.assertEqual(result, expected)


class TestMatchesAnySelector(unittest.TestCase):
    def test_single_selector(self):
        """When given a single item list, should return the item itself."""
        selector = SimpleSelector('aaa')
        result = MatchesAnySelector([selector])
        self.assertIs(selector, result)

    def test_matches_any(self):
        """Returns True if one or more selectors match."""
        selector = MatchesAnySelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        self.assertTrue(selector({'aaa': 'xxx', 'bbb': 'yyy'}))
        self.assertTrue(selector({'aaa': 'xxx', 'ccc': 'zzz'}))
        self.assertTrue(selector({'bbb': 'yyy', 'ccc': 'zzz'}))
        self.assertFalse(selector({'ccc': 'zzz'}))

    def test_repr(self):
        repr_list = [
            "MatchesAnySelector([SimpleSelector('aaa'), SimpleSelector('bbb'), SimpleSelector('ccc')])",
            "MatchesAnySelector([SimpleSelector('aaa', '=', 'xxx'), SimpleSelector('bbb')])",
            "MatchesAnySelector([SimpleSelector('aaa', '=', 'xxx', ignore_case=True), SimpleSelector('bbb')])",
        ]
        for r in repr_list:
            with self.subTest(r=r):
                self.assertEqual(repr(eval(r)), r)

    def test_str(self):
        selector = MatchesAnySelector([SimpleSelector('aaa'), SimpleSelector('bbb'), SimpleSelector('ccc')])
        self.assertEqual(str(selector), ':is([aaa], [bbb], [ccc])')

        selector = MatchesAnySelector([SimpleSelector('aaa', '=', 'xxx'), SimpleSelector('bbb')])
        self.assertEqual(str(selector), ':is([aaa="xxx"], [bbb])')

        selector = MatchesAnySelector([SimpleSelector('aaa', '=', 'xxx', ignore_case=True), SimpleSelector('bbb')])
        self.assertEqual(str(selector), ':is([aaa="xxx" i], [bbb])')

    def test_eq(self):
        sel_a = MatchesAnySelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = MatchesAnySelector([SimpleSelector('bbb'), SimpleSelector('aaa')])
        self.assertEqual(sel_a, sel_b)

        sel_a = MatchesAnySelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = MatchesAnySelector([SimpleSelector('ccc'), SimpleSelector('aaa')])
        self.assertNotEqual(sel_a, sel_b)

    def test_hash(self):
        sel_a = MatchesAnySelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = MatchesAnySelector([SimpleSelector('bbb'), SimpleSelector('aaa')])
        self.assertEqual(hash(sel_a), hash(sel_b))

        sel_a = MatchesAnySelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = MatchesAnySelector([SimpleSelector('ccc'), SimpleSelector('aaa')])
        self.assertNotEqual(hash(sel_a), hash(sel_b))

    def test_specificity(self):
        """Specificity is modeled after CSS specificity but it's not
        the same. To see how specificity is determined in CSS, see:

            https://www.w3.org/TR/selectors-4/#specificity
        """
        sel = MatchesAnySelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        self.assertEqual(sel.specificity, (1, 0))

        sel = MatchesAnySelector([SimpleSelector('aaa'), SimpleSelector('bbb', '=', 'yyy')])
        self.assertEqual(sel.specificity, (1, 1))

        sel = MatchesAnySelector([SimpleSelector('aaa', '=', 'xxx'), SimpleSelector('bbb', '=', 'yyy')])
        self.assertEqual(sel.specificity, (1, 1))

        sel = MatchesAnySelector([SimpleSelector('aaa'), SimpleSelector('bbb'), SimpleSelector('ccc', '=', 'zzz')])
        self.assertEqual(sel.specificity, (1, 1))


class TestNegationSelector(unittest.TestCase):
    def test_single_selector(self):
        """Single item lists should not receive special handling."""
        selector = SimpleSelector('aaa')
        result = NegationSelector([selector])
        self.assertIsNot(selector, result)

    def test_negation(self):
        """Returns True if one or more selectors match."""
        selector = NegationSelector([SimpleSelector('aaa', '=', 'qqq'), SimpleSelector('ccc')])
        self.assertTrue(selector({'aaa': 'xxx', 'bbb': 'yyy'}))
        self.assertTrue(selector({'aaa': 'xxx'}))
        self.assertFalse(selector({'aaa': 'xxx', 'ccc': 'zzz'}))  # <- Key 'ccc' matches.
        self.assertFalse(selector({'aaa': 'qqq', 'bbb': 'yyy'}))  # <- Key 'aaa' has matching value 'qqq'.
        self.assertFalse(selector({'ccc': 'zzz'}))  # <- Key 'ccc' matches.

    def test_repr(self):
        repr_list = [
            "NegationSelector([SimpleSelector('aaa'), SimpleSelector('bbb'), SimpleSelector('ccc')])",
            "NegationSelector([SimpleSelector('aaa', '=', 'xxx'), SimpleSelector('bbb')])",
            "NegationSelector([SimpleSelector('aaa', '=', 'xxx', ignore_case=True), SimpleSelector('bbb')])",
        ]
        for r in repr_list:
            with self.subTest(r=r):
                self.assertEqual(repr(eval(r)), r)

    def test_str(self):
        selector = NegationSelector([SimpleSelector('aaa'), SimpleSelector('bbb'), SimpleSelector('ccc')])
        self.assertEqual(str(selector), ':not([aaa], [bbb], [ccc])')

        selector = NegationSelector([SimpleSelector('aaa', '=', 'xxx'), SimpleSelector('bbb')])
        self.assertEqual(str(selector), ':not([aaa="xxx"], [bbb])')

        selector = NegationSelector([SimpleSelector('aaa', '=', 'xxx', ignore_case=True), SimpleSelector('bbb')])
        self.assertEqual(str(selector), ':not([aaa="xxx" i], [bbb])')

    def test_eq(self):
        sel_a = NegationSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = NegationSelector([SimpleSelector('bbb'), SimpleSelector('aaa')])
        self.assertEqual(sel_a, sel_b)

        sel_a = NegationSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = NegationSelector([SimpleSelector('ccc'), SimpleSelector('aaa')])
        self.assertNotEqual(sel_a, sel_b)

    def test_hash(self):
        sel_a = NegationSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = NegationSelector([SimpleSelector('bbb'), SimpleSelector('aaa')])
        self.assertEqual(hash(sel_a), hash(sel_b))

        sel_a = NegationSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = NegationSelector([SimpleSelector('ccc'), SimpleSelector('aaa')])
        self.assertNotEqual(hash(sel_a), hash(sel_b))

    def test_specificity(self):
        """Specificity is modeled after CSS specificity but it's not
        the same. To see how specificity is determined in CSS, see:

            https://www.w3.org/TR/selectors-4/#specificity
        """
        sel = NegationSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        self.assertEqual(sel.specificity, (1, 0))

        sel = NegationSelector([SimpleSelector('aaa'), SimpleSelector('bbb', '=', 'yyy')])
        self.assertEqual(sel.specificity, (1, 1))

        sel = NegationSelector([SimpleSelector('aaa', '=', 'xxx'), SimpleSelector('bbb', '=', 'yyy')])
        self.assertEqual(sel.specificity, (1, 1))

        sel = NegationSelector([SimpleSelector('aaa'), SimpleSelector('bbb'), SimpleSelector('ccc', '=', 'zzz')])
        self.assertEqual(sel.specificity, (1, 1))


class TestSpecificityAdjustmentSelector(unittest.TestCase):
    def test_single_selector(self):
        """Single item lists should not receive special handling."""
        selector = SimpleSelector('aaa')
        result = SpecificityAdjustmentSelector([selector])
        self.assertIsNot(selector, result)

    def test_matches_any(self):
        """Returns True if one or more selectors match."""
        selector = SpecificityAdjustmentSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        self.assertTrue(selector({'aaa': 'xxx', 'bbb': 'yyy'}))
        self.assertTrue(selector({'aaa': 'xxx', 'ccc': 'zzz'}))
        self.assertTrue(selector({'bbb': 'yyy', 'ccc': 'zzz'}))
        self.assertFalse(selector({'ccc': 'zzz'}))

    def test_repr(self):
        repr_list = [
            "SpecificityAdjustmentSelector([SimpleSelector('aaa'), SimpleSelector('bbb'), SimpleSelector('ccc')])",
            "SpecificityAdjustmentSelector([SimpleSelector('aaa', '=', 'xxx'), SimpleSelector('bbb')])",
            "SpecificityAdjustmentSelector([SimpleSelector('aaa', '=', 'xxx', ignore_case=True), SimpleSelector('bbb')])",
        ]
        for r in repr_list:
            with self.subTest(r=r):
                self.assertEqual(repr(eval(r)), r)

    def test_str(self):
        selector = SpecificityAdjustmentSelector([SimpleSelector('aaa'), SimpleSelector('bbb'), SimpleSelector('ccc')])
        self.assertEqual(str(selector), ':where([aaa], [bbb], [ccc])')

        selector = SpecificityAdjustmentSelector([SimpleSelector('aaa', '=', 'xxx'), SimpleSelector('bbb')])
        self.assertEqual(str(selector), ':where([aaa="xxx"], [bbb])')

        selector = SpecificityAdjustmentSelector([SimpleSelector('aaa', '=', 'xxx', ignore_case=True), SimpleSelector('bbb')])
        self.assertEqual(str(selector), ':where([aaa="xxx" i], [bbb])')

    def test_eq(self):
        sel_a = SpecificityAdjustmentSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = SpecificityAdjustmentSelector([SimpleSelector('bbb'), SimpleSelector('aaa')])
        self.assertEqual(sel_a, sel_b)

        sel_a = SpecificityAdjustmentSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = SpecificityAdjustmentSelector([SimpleSelector('ccc'), SimpleSelector('aaa')])
        self.assertNotEqual(sel_a, sel_b)

    def test_hash(self):
        sel_a = SpecificityAdjustmentSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = SpecificityAdjustmentSelector([SimpleSelector('bbb'), SimpleSelector('aaa')])
        self.assertEqual(hash(sel_a), hash(sel_b))

        sel_a = SpecificityAdjustmentSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = SpecificityAdjustmentSelector([SimpleSelector('ccc'), SimpleSelector('aaa')])
        self.assertNotEqual(hash(sel_a), hash(sel_b))

    def test_specificity(self):
        """Specificity is modeled after CSS specificity but it's not
        the same. To see how specificity is determined in CSS, see:

            https://www.w3.org/TR/selectors-4/#specificity
        """
        sel = SpecificityAdjustmentSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        self.assertEqual(sel.specificity, (0, 0))

        sel = SpecificityAdjustmentSelector([SimpleSelector('aaa'), SimpleSelector('bbb', '=', 'yyy')])
        self.assertEqual(sel.specificity, (0, 0))

        sel = SpecificityAdjustmentSelector([SimpleSelector('aaa', '=', 'xxx'), SimpleSelector('bbb', '=', 'yyy')])
        self.assertEqual(sel.specificity, (0, 0))

        sel = SpecificityAdjustmentSelector([SimpleSelector('aaa'), SimpleSelector('bbb'), SimpleSelector('ccc', '=', 'zzz')])
        self.assertEqual(sel.specificity, (0, 0))


class TestCompoundSelector(unittest.TestCase):
    def test_single_selector(self):
        """When given a single item list, should return the item itself."""
        selector = SimpleSelector('aaa')
        result = CompoundSelector([selector])
        self.assertIs(selector, result)

    def test_multiple_selectors(self):
        """All selectors must match to return True."""
        selector = CompoundSelector([SimpleSelector('aaa', '=', 'xxx'), SimpleSelector('bbb')])
        self.assertTrue(selector({'aaa': 'xxx', 'bbb': 'yyy'}))
        self.assertTrue(selector({'aaa': 'xxx', 'bbb': 'zzz'}))
        self.assertFalse(selector({'aaa': 'yyy', 'bbb': 'yyy'}))  # <- value of 'aaa' does not match
        self.assertFalse(selector({'aaa': 'xxx', 'ccc': 'zzz'}))  # <- no key matching 'bbb'

    def test_repr(self):
        repr_list = [
            "CompoundSelector([SimpleSelector('aaa'), SimpleSelector('bbb'), SimpleSelector('ccc')])",
            "CompoundSelector([SimpleSelector('aaa', '=', 'xxx'), SimpleSelector('bbb')])",
            "CompoundSelector([SimpleSelector('aaa', '=', 'xxx', ignore_case=True), SimpleSelector('bbb')])",
        ]
        for r in repr_list:
            with self.subTest(r=r):
                self.assertEqual(repr(eval(r)), r)

    def test_str(self):
        selector = CompoundSelector([SimpleSelector('aaa'), SimpleSelector('bbb'), SimpleSelector('ccc')])
        self.assertEqual(str(selector), '[aaa][bbb][ccc]')

        selector = CompoundSelector([SimpleSelector('aaa', '=', 'xxx'), SimpleSelector('bbb')])
        self.assertEqual(str(selector), '[aaa="xxx"][bbb]')

        selector = CompoundSelector([SimpleSelector('aaa', '=', 'xxx', ignore_case=True), SimpleSelector('bbb')])
        self.assertEqual(str(selector), '[aaa="xxx" i][bbb]')

    def test_eq(self):
        sel_a = CompoundSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = CompoundSelector([SimpleSelector('bbb'), SimpleSelector('aaa')])
        self.assertEqual(sel_a, sel_b)

        sel_a = CompoundSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = CompoundSelector([SimpleSelector('ccc'), SimpleSelector('aaa')])
        self.assertNotEqual(sel_a, sel_b)

    def test_hash(self):
        sel_a = CompoundSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = CompoundSelector([SimpleSelector('bbb'), SimpleSelector('aaa')])
        self.assertEqual(hash(sel_a), hash(sel_b))

        sel_a = CompoundSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        sel_b = CompoundSelector([SimpleSelector('ccc'), SimpleSelector('aaa')])
        self.assertNotEqual(hash(sel_a), hash(sel_b))

    def test_specificity(self):
        """Specificity is modeled after CSS specificity but it's not
        the same. To see how specificity is determined in CSS, see:

            https://www.w3.org/TR/selectors-4/#specificity
        """
        sel = CompoundSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        self.assertEqual(sel.specificity, (2, 0))

        sel = CompoundSelector([SimpleSelector('aaa'), SimpleSelector('bbb', '=', 'yyy')])
        self.assertEqual(sel.specificity, (2, 1))

        sel = CompoundSelector([SimpleSelector('aaa', '=', 'xxx'), SimpleSelector('bbb', '=', 'yyy')])
        self.assertEqual(sel.specificity, (2, 2))

        sel = CompoundSelector([SimpleSelector('aaa'), SimpleSelector('bbb'), SimpleSelector('ccc', '=', 'zzz')])
        self.assertEqual(sel.specificity, (3, 1))


class TestParseSelector(unittest.TestCase):
    def test_simple_selector(self):
        result = parse_selector('[aaa]')
        expected = SimpleSelector('aaa')
        self.assertEqual(result, expected)

        result = parse_selector('[aaa="xxx"]')
        expected = SimpleSelector('aaa', '=', 'xxx')
        self.assertEqual(result, expected)

    def test_compound_selector(self):
        result = parse_selector('[aaa="xxx"][bbb]')
        expected = CompoundSelector([SimpleSelector('aaa', '=', 'xxx'), SimpleSelector('bbb')])
        self.assertEqual(result, expected)

    def test_matches_any(self):
        result = parse_selector(':is([aaa], [bbb])')
        expected = MatchesAnySelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        self.assertEqual(result, expected)

    def test_negation(self):
        result = parse_selector(':not([aaa], [bbb])')
        expected = NegationSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        self.assertEqual(result, expected)

    def test_specificity_adjustment(self):
        result = parse_selector(':where([aaa], [bbb])')
        expected = SpecificityAdjustmentSelector([SimpleSelector('aaa'), SimpleSelector('bbb')])
        self.assertEqual(result, expected)

