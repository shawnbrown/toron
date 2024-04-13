"""Data structures to refine granularity and translate crosswalks."""

__all__ = [
    # PACKAGE CONTENTS
    'dal1',

    # CLASSES
    'Node',
    'ToronError',
    'ToronWarning',
    'xNode',

    # FUNCTIONS
    'wide_to_narrow',
]
__version__ = '0.1.0'
__author__ = 'Shawn Brown <shawnbrown@users.noreply.github.com>'

def __dir__():  # Customize module attribute list (PEP 562).
    special_attrs = [x for x in globals().keys() if x.startswith('__')]
    return __all__ + special_attrs

from . import dal1
from .node import Node
from .xnode import xNode
from ._utils import (
    ToronError,
    ToronWarning,
    wide_to_narrow,
)

ToronError.__module__ = 'toron'
