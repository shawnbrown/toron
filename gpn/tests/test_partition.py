# -*- coding: utf-8 -*-
import unittest

from gpn.partition import Partition


class TestInstantiation(unittest.TestCase):
    def test_basic(self):
        ptn = Partition()


if __name__ == '__main__':
    unittest.main()
