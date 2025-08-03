"""Data structures to refine granularity and translate crosswalks."""

__all__ = [
    # PACKAGE CONTENTS
    #<modules or subpackages>,

    # CLASSES
    'TopoNode',
    'ToronError',
    'ToronWarning',
    'xNode',

    # FUNCTIONS
    'read_file',
    'bind_node',
    'wide_to_narrow',
    'wide_to_long',
]
__version__ = '0.1.0'
__author__ = 'Shawn Brown <shawnbrown@users.noreply.github.com>'

def __dir__():  # Customize module attribute list (PEP 562).
    special_attrs = [x for x in globals().keys() if x.startswith('__')]
    return __all__ + special_attrs

from .node import (
    TopoNode,
    read_file,
    bind_node,
)
from .xnode import xNode
from ._utils import (
    ToronError,
    ToronWarning,
    wide_to_narrow,
    wide_to_long,
)

# Set class modules to 'toron' to keep interface tidy.
TopoNode.__module__ = 'toron'
ToronError.__module__ = 'toron'
ToronWarning.__module__ = 'toron'
xNode.__module__ = 'toron'

# Define 'app-toron' logger and set level to INFO (20) but leave
# the handler unspecified--defaults to "handler of last resort".
#
# This logger will receive messages intended for an application
# enduser. The application should add its own handler to display
# these messages in a context-appropriate format (CLI message,
# GUI pop-up notification, etc.).
__import__('logging').getLogger(f'app-{__name__}').setLevel(level=20)
