"""compatibility layer for `typing` (Python standard library)"""
import sys
from typing import *


if sys.version_info < (3, 10):
    from typing_extensions import TypeAlias


if sys.version_info < (3, 11):
    from typing_extensions import Never
    from typing_extensions import Self
