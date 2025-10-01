"""Resources to configure CLI application logger."""

import logging
import logging.config
import os
import sys
from dataclasses import dataclass
from enum import IntEnum
from .._typing import (
    Any,
    Dict,
    Literal,
    Optional,
)


class ExitCode(IntEnum):
    OK = 0
    ERR = 1


if sys.platform == 'win32':
    from colorama import just_fix_windows_console
    just_fix_windows_console()


@dataclass(frozen=True)
class TerminalStyle:
    """ANSI escape codes for terminal styles."""
    info: str
    warning: str
    error: str
    critical: str
    reset: str
    bright: str


COLOR_STYLES = TerminalStyle(
    info='\33[38;5;33m',                   # blue
    warning='\33[38;5;214m',               # yellow
    error='\33[38;5;196m',                 # red
    critical='\33[48;5;196m\33[38;5;16m',  # red background
    reset='\33[0m',                        # reset styles
    bright='\33[1m',                       # bright text
)


NO_COLOR_STYLES = TerminalStyle(
    info='',
    warning='',
    error='',
    critical='',
    reset='',
    bright='',
)


if os.environ.get('NO_COLOR') or os.environ.get('TERM') == 'dumb':
    # Disable color and styles.
    stdout_styles = NO_COLOR_STYLES
    stderr_styles = NO_COLOR_STYLES
else:
    # Set color and styles if stream uses interactive terminal (a TTY).
    stdout_styles = COLOR_STYLES if sys.stdout.isatty() else NO_COLOR_STYLES
    stderr_styles = COLOR_STYLES if sys.stderr.isatty() else NO_COLOR_STYLES


if sys.version_info < (3, 8):
    # Prior to 3.8, `Formatter` did not support *validate* or *defaults* args.
    class _Formatter(logging.Formatter):
        def __init__(
            self,
            fmt: Optional[str] = None,
            datefmt: Optional[str] = None,
            style: Literal['%', '{', '$'] = '%',
            validate: bool = True,
            *,
            defaults: Optional[Dict[str, Any]] = None,
        ) -> None:
            """Initialize, discarding *validate* and *defaults* arguments."""
            super().__init__(fmt, datefmt, style)  # <- no validate, no defaults
elif sys.version_info < (3, 10):
    # Prior to 3.10, `Formatter` did not support *defaults* argument.
    class _Formatter(logging.Formatter):
        def __init__(
            self,
            fmt: Optional[str] = None,
            datefmt: Optional[str] = None,
            style: Literal['%', '{', '$'] = '%',
            validate: bool = True,
            *,
            defaults: Optional[Dict[str, Any]] = None,
        ) -> None:
            """Initialize, discarding *defaults* argument."""
            super().__init__(fmt, datefmt, style, validate)  # <- no defaults
else:
    # For Python 3.10 and newer.
    _Formatter = logging.Formatter  # <- accepts validate and defaults


class ColorFormatter(_Formatter):
    """Formatter to convert LogRecord into ANSI color terminal text."""
    def __init__(
        self,
        fmt: str,
        datefmt: Optional[str] = None,
        style: Literal['%', '{', '$'] = '%',
        validate: bool = True,
        *,
        defaults: Optional[Dict[str, Any]] = None,
    ) -> None:
        common_args = (datefmt, style, validate)

        # Initialize self as un-colored formatter (for fall-back).
        super().__init__(fmt, *common_args, defaults=defaults)

        # Define short alias for styles dataclass (used in f-strings).
        s = stderr_styles

        # Instantiate color formatters.
        self.color_formatters = {
            logging.INFO: _Formatter(
                f'{s.info}{fmt}{s.reset}', *common_args, defaults=defaults
            ),
            logging.WARNING: _Formatter(
                f'{s.warning}{fmt}{s.reset}', *common_args, defaults=defaults
            ),
            logging.ERROR: _Formatter(
                f'{s.error}{fmt}{s.reset}', *common_args, defaults=defaults
            ),
            logging.CRITICAL: _Formatter(
                f'{s.critical}{fmt}{s.reset}', *common_args, defaults=defaults
            ),
        }

    def format(self, record: logging.LogRecord) -> str:
        formatter = self.color_formatters.get(record.levelno)
        if formatter:
            return formatter.format(record)
        return super().format(record)  # <- Use un-colored fall-back.


def configure_applogger(applogger: logging.Logger) -> None:
    """Configure handler and formatter for given *applogger*."""
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'cli_formatter': {
                '()': ColorFormatter,
                'fmt': '%(levelname)s: %(message)s',
            },
        },
        'handlers': {
            'cli_handler': {
                'class': 'logging.StreamHandler',
                'formatter': 'cli_formatter',
                'stream': 'ext://sys.stderr',
            },
        },
        'loggers': {
            applogger.name: {
                'handlers': ['cli_handler'],
                'propagate': False,
            },
        },
    })
