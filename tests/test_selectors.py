"""Tests for toron._selectors module."""

import json
import unittest

from toron._selectors import (
    SelectorBase,
    SimpleSelector,
    MatchesAnySelector,
    NegationSelector,
    SpecificityAdjustmentSelector,
    CompoundSelector,
    _get_comparison_key,
    accepts_json_input,
    parse_selector,
    convert_text_selectors,
    SelectorSyntaxError,
    GetMatchingKey,
)


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

    def test_bad_input(self):
        selector = SimpleSelector('aaa', '=', 'xxx')

        regex = r"expected mapping, got <class 'list'>: \['xxx', 'yyy'\]"
        with self.assertRaisesRegex(TypeError, regex):
            selector(['xxx', 'yyy'])  # <- Expects dict type, gets list instead.


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

    def test_non_selector_type(self):
        result = _get_comparison_key('abc')
        self.assertEqual(result, 'abc', msg='should be returned unchanged')

        result = _get_comparison_key(123)
        self.assertEqual(result, 123, msg='should be returned unchanged')

    def test_unhandled_selector(self):
        class DummySelector(SelectorBase):
            __init__ = lambda self: None
            __call__ = lambda self, _: None
            __eq__ = lambda self, _: None
            __hash__ = lambda self: NotImplemented
            __repr__ = lambda self: None
            __str__ = lambda self: None
            specificity = property(lambda self: (0, 0))

        dummy_selector = DummySelector()

        regex = 'comparison key not implemented for type: DummySelector'
        with self.assertRaisesRegex(ValueError, regex):
            _get_comparison_key(dummy_selector)


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


class TestAcceptsJsonInput(unittest.TestCase):
    def test_wrapping(self):
        """Should accept JSON object strings not Python dict objects."""
        selector = SimpleSelector('aaa', '=', 'xxx')
        wrapped = accepts_json_input(selector)  # <- Wrap selector.

        json_string = '{"aaa": "xxx"}'
        self.assertTrue(wrapped(json_string), msg='should accept JSON string')

        with self.assertRaises(TypeError, msg='should fail if given dict'):
            row_dict = {'aaa': 'xxx'}
            self.assertTrue(wrapped(row_dict))

    def test_hash(self):
        """Functionally equivalent behavior should, ideally, have the
        same hash.
        """
        selector1 = SimpleSelector('aaa', '=', 'xxx')
        selector2 = SimpleSelector('aaa', '=', 'xxx')
        wrapped1 = accepts_json_input(selector1)
        wrapped2 = accepts_json_input(selector2)

        self.assertEqual(hash(wrapped1), hash(wrapped2))
        self.assertNotEqual(hash(wrapped1), hash(selector1))
        self.assertNotEqual(hash(wrapped2), hash(selector2))

    def test_eq(self):
        wrapped1 = accepts_json_input(SimpleSelector('aaa', '=', 'xxx'))
        wrapped2 = accepts_json_input(SimpleSelector('aaa', '=', 'xxx'))
        self.assertEqual(wrapped1, wrapped2)

    def test_hash_table_behavior(self):
        """Equivalent objects should be indistinguishable from each
        other by `set` and `dict` hash-table handling.
        """
        wrapped1 = accepts_json_input(SimpleSelector('aaa', '=', 'xxx'))
        wrapped2 = accepts_json_input(SimpleSelector('aaa', '=', 'xxx'))

        self.assertNotEqual(id(wrapped1), id(wrapped2))
        self.assertEqual(hash(wrapped1), hash(wrapped2))
        self.assertEqual(wrapped1, wrapped2)

        my_set = {wrapped1, wrapped2}
        self.assertEqual(
            my_set,
            {wrapped1},
            msg='only one item because sets cannot contain duplicates',
        )
        self.assertEqual(
            my_set,
            {wrapped2},
            msg='wrapped2 should be indistinguishable',
        )

        my_dict = {wrapped1: 'x', wrapped2: 'y'}
        self.assertEqual(
            my_dict,
            {wrapped2: 'y'},
            msg='only one item because dictionaries cannot contain duplicate keys',
        )
        self.assertEqual(
            my_dict,
            {wrapped1: 'y'},
            msg='wrapped2 should be indistinguishable',
        )

    def test_repr_and_str(self):
        """Repr should be eval-able and str should match eval."""
        wrapped = accepts_json_input(SimpleSelector('aaa', '=', 'xxx'))
        expected = "accepts_json_input(SimpleSelector('aaa', '=', 'xxx'))"
        self.assertEqual(repr(wrapped), expected)
        self.assertEqual(str(wrapped), expected)


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


class TestConvertTextSelectors(unittest.TestCase):
    def test_single_selector(self):
        json_string = '["[aaa]"]'
        result = convert_text_selectors(json_string)
        expected = [SimpleSelector('aaa')]
        self.assertEqual(result, expected)

    def test_json_bytes_input(self):
        json_bytes = b'["[aaa]"]'
        result = convert_text_selectors(json_bytes)
        expected = [SimpleSelector('aaa')]
        self.assertEqual(result, expected)

    def test_multiple_single_selectors(self):
        json_string = '["[aaa]", "[bbb]"]'
        result = convert_text_selectors(json_string)
        expected = [SimpleSelector('aaa'), SimpleSelector('bbb')]
        self.assertEqual(result, expected)

    def test_compound_selector(self):
        json_string = '["[aaa][bbb]"]'
        result = convert_text_selectors(json_string)
        expected = [
            CompoundSelector([
                SimpleSelector('aaa'),
                SimpleSelector('bbb'),
            ]),
        ]
        self.assertEqual(result, expected)

    def test_multiple_mixed_selectors(self):
        json_string = r'["[aaa][bbb]", "[ccc=\"zzz\"]:not([ddd])", "[eee]"]'
        result = convert_text_selectors(json_string)
        expected = [
            CompoundSelector([
                SimpleSelector('aaa'),
                SimpleSelector('bbb'),
            ]),
            CompoundSelector([
                SimpleSelector('ccc', '=', 'zzz'),
                NegationSelector([SimpleSelector('ddd')]),
            ]),
            SimpleSelector('eee'),
        ]
        self.assertEqual(result, expected)

    def test_json_syntax_error(self):
        """JSON syntax errors are not handled, should raise normally."""
        json_string = '["[aaa=]"'
        with self.assertRaises(json.JSONDecodeError):
            convert_text_selectors(json_string)

    def test_selector_syntax_error(self):
        """Selector grammar errors should raise SelectorSyntaxError."""
        json_string = '["[aaa=]"]'
        regex = (
            '\\[aaa=\\]\n'
            '     \\^\n'
        )
        with self.assertRaisesRegex(SelectorSyntaxError, regex):
            convert_text_selectors(json_string)


class TestParserSelectorIntegration(unittest.TestCase):
    """Check parser and selector integration with common use cases."""
    def test_simple_selector(self):
        selector = parse_selector('[aaa]')
        self.assertTrue(selector({'aaa': 'xxx'}))
        self.assertFalse(selector({'bbb': 'yyy'}))

        selector = parse_selector('[aaa="xxx"]')
        self.assertTrue(selector({'aaa': 'xxx'}))
        self.assertFalse(selector({'aaa': 'zzz'}))

    def test_compound_selector(self):
        selector = parse_selector('[aaa="xxx"][bbb]')
        self.assertTrue(selector({'aaa': 'xxx', 'bbb': 'yyy'}))
        self.assertFalse(selector({'aaa': 'xxx', 'ccc': 'zzz'}))

    def test_matches_any(self):
        selector = parse_selector(':is([aaa], [bbb])')
        self.assertTrue(selector({'aaa': 'xxx', 'ccc': 'zzz'}))
        self.assertTrue(selector({'bbb': 'yyy', 'ccc': 'zzz'}))
        self.assertFalse(selector({'ccc': 'zzz'}))

    def test_negation(self):
        selector = parse_selector(':not([aaa], [bbb])')
        self.assertTrue(selector({'ccc': 'zzz'}))
        self.assertFalse(selector({'aaa': 'xxx', 'ccc': 'zzz'}))
        self.assertFalse(selector({'bbb': 'yyy', 'ccc': 'zzz'}))

    def test_specificity_adjustment(self):
        selector = parse_selector(':where([aaa], [bbb])')
        self.assertEqual(selector.specificity, (0, 0))

        self.assertTrue(selector({'aaa': 'xxx', 'ccc': 'zzz'}))
        self.assertTrue(selector({'bbb': 'yyy', 'ccc': 'zzz'}))
        self.assertFalse(selector({'ccc': 'zzz'}))

    def test_mixed_types(self):
        selector = parse_selector('[aaa="xxx"]:is([bbb], [ccc]):not([ddd], [eee="qqq" i])')
        self.assertEqual(selector.specificity, (3, 2))

        self.assertTrue(selector({'aaa': 'xxx', 'bbb': 'yyy'}))
        self.assertTrue(selector({'aaa': 'xxx', 'ccc': 'zzz'}))
        self.assertTrue(selector({'aaa': 'xxx', 'ccc': 'zzz', 'eee': 'rrr'}))
        self.assertFalse(selector({'aaa': 'xxx'}))  # <- Needs [bbb] or [ccc]
        self.assertFalse(selector({'aaa': 'qqq', 'bbb': 'yyy'}))  # <- Needs [aaa="xxx"]
        self.assertFalse(selector({'aaa': 'xxx', 'ccc': 'zzz', 'eee': 'QQq'}))  # <- Cannot have [eee="qqq" i]
        self.assertFalse(selector({'aaa': 'xxx', 'bbb': 'yyy', 'ddd': 'www'}))  # <- Cannot have [ddd]

    def test_nested_mixed_types(self):
        selector = parse_selector('[aaa="xxx"]:is([bbb], [ccc], :not([ddd], :where([eee="qqq" i])))')
        self.assertEqual(selector.specificity, (2, 1))

        self.assertTrue(selector({'aaa': 'xxx', 'bbb': 'yyy'}))
        self.assertTrue(selector({'aaa': 'xxx', 'ccc': 'zzz'}))
        self.assertTrue(selector({'aaa': 'xxx'}))
        self.assertTrue(selector({'aaa': 'xxx', 'bbb': 'yyy', 'ddd': 'www'}))
        self.assertFalse(selector({'aaa': 'xxx', 'ddd': 'www'}))  # <- Needs [bbb] or [ccc] or cannot have [ddd]
        self.assertFalse(selector({'aaa': 'qqq', 'bbb': 'yyy'}))  # <- Needs [aaa="xxx"]
        self.assertFalse(selector({'aaa': 'xxx', 'eee': 'QQq'}))  # <- Cannot have [eee="qqq" i]
        self.assertFalse(selector({'aaa': 'xxx', 'ddd': 'www'}))  # <- Cannot have [ddd]

    def test_hash_table_behavior(self):
        """Selectors with equivalent behavior should be indistinguishable
        from each other by `set` and `dict` hash-table handling.
        """
        selector1 = parse_selector('[aaa][bbb="ccc"]')
        selector2 = parse_selector('[bbb="ccc"][aaa]')

        self.assertNotEqual(id(selector1), id(selector2))
        self.assertEqual(hash(selector1), hash(selector2))
        self.assertEqual(selector1, selector2)

        my_set = {selector1, selector2}
        self.assertEqual(
            my_set,
            {selector1},
            msg='only one item because sets cannot contain duplicates',
        )
        self.assertEqual(
            my_set,
            {selector2},
            msg='selector2 should be indistinguishable',
        )

        my_dict = {selector1: 'x', selector2: 'y'}
        self.assertEqual(
            my_dict,
            {selector2: 'y'},
            msg='only one item because dictionaries cannot contain duplicate keys',
        )
        self.assertEqual(
            my_dict,
            {selector1: 'y'},
            msg='selector2 should be indistinguishable',
        )


class TestGetMatchingKey(unittest.TestCase):
    def test_instantiation(self):
        """Should accept dict or item-pairs/2-tuples as input."""
        key_selector_dict = {
            1: [SimpleSelector('A', '=', 'xxx')],
            2: [SimpleSelector('B', '=', 'yyy')],
        }
        get_matching_key = GetMatchingKey(key_selector_dict, default=1)

        key_selector_tuples = iter([
            (1, [SimpleSelector('A', '=', 'xxx')]),
            (2, [SimpleSelector('B', '=', 'yyy')]),
        ])
        get_matching_key = GetMatchingKey(key_selector_tuples, default=1)

    def test_simple_match(self):
        selector_dict = {
            1: [SimpleSelector('A', '=', 'xxx')],
            2: [SimpleSelector('B', '=', 'yyy')],
        }
        get_matching_key = GetMatchingKey(selector_dict, default=1)

        # Check JSON strings.
        self.assertEqual(get_matching_key('{"A": "xxx"}'), 1)
        self.assertEqual(get_matching_key('{"B": "yyy"}'), 2)
        self.assertEqual(get_matching_key('{"C": "zzz"}'), 1, msg='should get default')

        # Check Python dict.
        self.assertEqual(get_matching_key({'A': 'xxx'}), 1)
        self.assertEqual(get_matching_key({'B': 'yyy'}), 2)
        self.assertEqual(get_matching_key({'C': 'zzz'}), 1, msg='should get default')

        with self.assertRaises(TypeError, msg='string must be valid JSON'):
            get_matching_key('xyz')

        with self.assertRaises(TypeError, msg='string should be a JSON Object type'):
            get_matching_key('["xxx", "yyy"]')

        with self.assertRaises(TypeError, msg='value should be a dict object'):
            get_matching_key(['xxx', 'yyy'])

    def test_max_specificity(self):
        selector_dict = {
            1: [SimpleSelector('A')],
            2: [SimpleSelector('A', '=', 'xxx')],
            3: [SimpleSelector('B')],
            4: [SimpleSelector('B', '=', 'yyy')],
        }
        get_matching_key = GetMatchingKey(selector_dict, default=1)
        self.assertEqual(get_matching_key({'A': 'qqq'}), 1, msg='specificity: (0, 1)')
        self.assertEqual(get_matching_key({'A': 'xxx'}), 2, msg='specificity: (1, 1)')
        self.assertEqual(get_matching_key({'B': 'qqq'}), 3, msg='specificity: (0, 1)')
        self.assertEqual(get_matching_key({'B': 'yyy'}), 4, msg='specificity: (1, 1)')

    def test_greatest_unique_specificity(self):
        selector_dict = {
            1: [SimpleSelector('A')],
            2: [SimpleSelector('A', '=', 'xxx')],
            3: [CompoundSelector([SimpleSelector('B', '=', 'yyy'), SimpleSelector('C')])],
            4: [CompoundSelector([SimpleSelector('B'), SimpleSelector('C', '=', 'zzz')])],
        }
        get_matching_key = GetMatchingKey(selector_dict, default=1)

        # Check basic matches and default.
        self.assertEqual(get_matching_key({'A': 'qqq'}), 1)
        self.assertEqual(get_matching_key({'A': 'xxx'}), 2)
        self.assertEqual(get_matching_key({'A': 'xxx', 'B': 'yyy', 'C': 'qqq'}), 3)
        self.assertEqual(get_matching_key({'A': 'xxx', 'B': 'qqq', 'C': 'zzz'}), 4)
        self.assertEqual(get_matching_key({'D': 'qqq'}), 1, msg='default')

        # Check greatest-unique specificity.
        msg = (
            'The `row_dict` matches both 3 and 4 with a specificity '
            'of `(2, 1)` so they are not unique. But 2 matches with '
            'a specificity of `(1, 1)` and it *is* unique, therefore '
            'get_matching_key() should return 2.'
        )
        row_dict = {'A': 'xxx', 'B': 'yyy', 'C': 'zzz'}
        self.assertEqual(get_matching_key(row_dict), 2, msg=msg)

        # Check fall-back to default.
        get_matching_key = GetMatchingKey(selector_dict, default=1)
        msg = (
            'The `row_dict` matches both 3 and 4 with a specificity '
            'of `(2, 1)` so they are not unique. And since there is '
            'no other matching selector, get_matching_key() should '
            'return 1 (the default key).'
        )
        row_dict = {'D': 'qqq', 'B': 'yyy', 'C': 'zzz'}
        self.assertEqual(get_matching_key(row_dict), 1, msg=msg)

    def test_hash_and_eq_equal(self):
        """Check when hashes and objects should test as equal."""
        # Same arguments, different objects.
        matcher1 = GetMatchingKey({1: [SimpleSelector('A')]}, default=1)
        matcher2 = GetMatchingKey({1: [SimpleSelector('A')]}, default=1)
        self.assertEqual(hash(matcher1), hash(matcher2))
        self.assertEqual(matcher1, matcher2)

        # Same arguments, but list is ordered differently.
        matcher1 = GetMatchingKey(
            {1: [SimpleSelector('A'), SimpleSelector('B')]},
            default=1,
        )
        matcher2 = GetMatchingKey(
            {1: [SimpleSelector('B'), SimpleSelector('A')]},
            default=1,
        )
        self.assertEqual(hash(matcher1), hash(matcher2))
        self.assertEqual(matcher1, matcher2)

    def test_hash_and_eq_not_equal(self):
        """Check when hashes and objects should not be equal."""
        # Different default argument.
        matcher1 = GetMatchingKey(
            {1: [SimpleSelector('A')], 2: [SimpleSelector('B')]},
            default=1,
        )
        matcher2 = GetMatchingKey(
            {1: [SimpleSelector('A')], 2: [SimpleSelector('B')]},
            default=2,
        )
        self.assertNotEqual(hash(matcher1), hash(matcher2))
        self.assertNotEqual(matcher1, matcher2)

        # Swapped key-and-selector associations.
        matcher1 = GetMatchingKey(
            {1: [SimpleSelector('A')], 2: [SimpleSelector('B')]},
            default=1,
        )
        matcher2 = GetMatchingKey(
            {1: [SimpleSelector('B')], 2: [SimpleSelector('A')]},
            default=1,
        )
        self.assertNotEqual(hash(matcher1), hash(matcher2))
        self.assertNotEqual(matcher1, matcher2)

        # Check for equality against non-hashable object.
        matcher = GetMatchingKey({1: [SimpleSelector('A')]}, default=1)
        self.assertNotEqual(matcher, [1, 2, 3])

