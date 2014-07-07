# -*- coding: utf-8 -*-
"""Granular Partition Network"""

__version__ = '0.01'

from gpn.partition import Partition
from gpn.partition import READ_ONLY
from gpn.partition import OUT_OF_MEMORY

__all__ = [
    'Partition',
    'READ_ONLY',
    'OUT_OF_MEMORY',
]
