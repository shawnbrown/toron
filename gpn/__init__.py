# -*- coding: utf-8 -*-
"""Granular Partition Network"""

__version__ = '0.01'

from gpn.partition import Partition
from gpn.connector import IN_MEMORY
from gpn.connector import TEMP_FILE
from gpn.connector import READ_ONLY

__all__ = [
    'Partition',
    'IN_MEMORY',
    'TEMP_FILE',
    'READ_ONLY',
]
