"""Command-line interface for the Toron project."""

from enum import IntEnum

from . import (
    __version__,
)


class ExitCode(IntEnum):
    OK: int = 0
    ERR: int = 1


def main() -> ExitCode:
    print(f'Toron {__version__}')
    return ExitCode.OK
