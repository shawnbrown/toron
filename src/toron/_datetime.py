"""compatibility layer for `datetime` (Python standard library)"""
import sys
from datetime import *


if sys.version_info < (3, 11):
    UTC = timezone.utc  # Alias for the UTC time zone singleton.
