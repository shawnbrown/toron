"""Command-line interface for the Toron project."""

import logging
from .._typing import (
    Final,
)
from .. import (
    __version__,
)
from .loggerconfig import configure_applogger


applogger = logging.getLogger('app-toron')
configure_applogger(applogger)


EXITCODE_OK: Final[int] = 0
EXITCODE_ERR: Final[int] = 1


def main() -> int:
    applogger.info(f'Toron {__version__}')
    return EXITCODE_OK
