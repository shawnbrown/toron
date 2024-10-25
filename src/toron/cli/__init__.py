"""Command-line interface for the Toron project."""

from .._typing import (
    Final,
)

from .. import (
    __version__,
)


EXITCODE_OK: Final[int] = 0
EXITCODE_ERR: Final[int] = 1


def main() -> int:
    print(f'Toron {__version__}')
    return EXITCODE_OK
