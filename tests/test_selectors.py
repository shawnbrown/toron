"""Tests for toron._selectors module."""

import unittest

from toron._selectors import Selector


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

