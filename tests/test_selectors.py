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

