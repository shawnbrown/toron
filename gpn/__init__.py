# -*- coding: utf-8 -*-
"""Granular Partition Network"""

__version__ = '0.01'

from gpn.partition import Partition
from gpn.partition import IN_MEMORY
from gpn.partition import TEMP_FILE
from gpn.partition import READ_ONLY

__all__ = [
    'Partition',
    'IN_MEMORY',
    'TEMP_FILE',
    'READ_ONLY',
]
