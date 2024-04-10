"""A tool kit for granularity refinement and crosswalk translation."""

__all__ = [
    # PACKAGE CONTENTS
    '_dal1',

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

from ._node import Node
from .node import xNode
from ._utils import (
    ToronError,
    ToronWarning,
    wide_to_narrow,
)

ToronError.__module__ = 'toron'
