"""compatibility layer for `typing` (Python standard library)"""
import sys
from typing import *


if sys.version_info < (3, 9):
    from typing import TextIO  # Not included in `__all__`.


if sys.version_info < (3, 8):
    from typing_extensions import Final
    from typing_extensions import Literal


if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias


if sys.version_info < (3, 11):
    from typing_extensions import Self
