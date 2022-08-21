"""Tests for toron._selectors module."""

import unittest

from toron._selectors import Selector


class TestSelector(unittest.TestCase):
    def test_instantiation(self):
        Selector('abc')
        Selector('abc', '=', 'xyz')
        Selector('abc', '=', 'xyz', ignore_case=True)

        with self.assertRaises(TypeError):
            Selector()

        with self.assertRaises(TypeError):
            Selector('abc', '=', None)

        with self.assertRaises(TypeError):
            Selector('abc', None, 'xyz')

        with self.assertRaises(TypeError):
            Selector('abc', ignore_case=True)

    def test_match_any_value(self):
        selector = Selector('abc')
        self.assertTrue(selector({'abc': 'xyz'}))
        self.assertTrue(selector({'abc': 'qrs'}))
        self.assertFalse(selector({'jkl': 'qrs'}))  # <- No attribute 'abc'.
        self.assertFalse(selector({'abc': ''}))  # <- Value is not truthy.

    def test_match_exact_value(self):
        selector = Selector('abc', '=', 'xyz')
        self.assertTrue(selector({'abc': 'xyz'}))
        self.assertFalse(selector({'abc': 'qrs'}))  # <- Value does not match.
        self.assertFalse(selector({'jkl': 'xyz'}))  # <- No attribute 'abc'.
        self.assertFalse(selector({'abc': 'XYZ'}))  # <- Matching is case-sensitive.

    def test_match_whitespace_separated_list(self):
        selector = Selector('abc', '~=', 'xyz')
        self.assertTrue(selector({'abc': 'ghi xyz qrs'}))
        self.assertTrue(selector({'abc': 'xyz'}))  # <- Exact value should match, too.
        self.assertFalse(selector({'abc': 'ghi-xyz-qrs'}))  # <- Not whitespace separated.
        self.assertFalse(selector({'abc': 'ghi wxyz qrs'}))  # <- Substring won't match (must be exact).

        # Check irregular whitespace.
        self.assertTrue(selector({'abc': 'ijk\tlmn\fopq\r\nxyz\nrst   uvw'}))

    def test_unknown_operator(self):
        regex = r"unknown operator: '//"
        with self.assertRaisesRegex(ValueError, regex):
            Selector('abc', '//', 'xyz')

