# -*- coding: utf-8 -*-
"""Granular Partition Network"""

__version__ = '0.01'

from toron._gpn_node import Node
from toron.connector import IN_MEMORY
from toron.connector import TEMP_FILE
from toron.connector import READ_ONLY

__all__ = [
    'Node',
    'IN_MEMORY',
    'TEMP_FILE',
    'READ_ONLY',
]
