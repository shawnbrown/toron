"""Tests for toron._selectors module."""

import unittest

from toron._selectors import Selector


class TestSelector(unittest.TestCase):
    def test_init(self):
        selector = Selector('abc')

