"""Data structures to refine granularity and translate crosswalks."""

__all__ = [
    # PACKAGE CONTENTS
    #<modules or subpackages>,

    # CLASSES
    'Node',
    'ToronError',
    'ToronWarning',
    'xNode',

    # FUNCTIONS
    'wide_to_narrow',
    'wide_to_long',
]
__version__ = '0.1.0'
__author__ = 'Shawn Brown <shawnbrown@users.noreply.github.com>'

def __dir__():  # Customize module attribute list (PEP 562).
    special_attrs = [x for x in globals().keys() if x.startswith('__')]
    return __all__ + special_attrs

from .node import Node
from .xnode import xNode
from ._utils import (
    ToronError,
    ToronWarning,
    wide_to_narrow,
    wide_to_long,
)

ToronError.__module__ = 'toron'

# Define 'app-toron' logger and set level to INFO (20) but leave
# the handler unspecified--defaults to "handler of last resort".
#
# This logger will receive messages intended for an application
# enduser. The application should add its own handler to display
# these messages in a context-appropriate format (CLI message,
# GUI pop-up notification, etc.).
__import__('logging').getLogger(f'app-{__name__}').setLevel(level=20)
