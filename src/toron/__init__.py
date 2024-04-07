# -*- coding: utf-8 -*-
"""Toron is tool kit for managing data joinability and ecological
inference problems.
"""

__version__ = '0.1.0'

from .node import xNode
from ._utils import (
    ToronError,
    ToronWarning,
    wide_to_narrow,
)

ToronError.__module__ = 'toron'

__all__ = [
    'xNode',
    'ToronError',
    'ToronWarning',
    'wide_to_narrow',
]
