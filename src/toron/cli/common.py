"""Common resources for Toron CLI application."""

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
    Mapping,
    Optional,
    TextIO,
)


class ExitCode(IntEnum):
    """Status code (errorlevel) to return when program exits."""
    OK = 0
    ERR = 1


# =====================================================================
# Terminal Colors
# =====================================================================

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

# Global (module-level) variables--set by `configure_terminalstyles()`.
_stdout_styles: Optional[TerminalStyle] = None
_stderr_styles: Optional[TerminalStyle] = None


def configure_terminalstyles(
    *,
    environ: Optional[Mapping] = None,
    stdout: Optional[TextIO] = None,
    stderr: Optional[TextIO] = None,
) -> None:
    """Configure terminal styles for stdout and stderr streams.

    Call without arguments for normal operation::

        >>> configure_terminalstyles()

    For testing, provide keyword arguments `environ`, `stdout`, and
    `stderr` as needed::

        >>> class FakeTTY(io.StringIO):
        ...     def isatty(self):
        ...         return True
        ...
        >>> fake_tty = FakeTTY()
        >>> configure_terminalstyles(environ={'TERM': 'dumb'}, stderr=fake_tty)
    """
    global _stdout_styles
    global _stderr_styles

    if not environ:
        environ = os.environ

    # If user has disabled colors or terminal is 'dumb', set styles and exit.
    if environ.get('NO_COLOR') or environ.get('TERM') == 'dumb':
        _stdout_styles = NO_COLOR_STYLES
        _stderr_styles = NO_COLOR_STYLES
        return  # <- EXIT!

    if not stdout:
        stdout = sys.stdout
    if not stderr:
        stderr = sys.stderr

    # Set color and styles if stream uses interactive terminal (a TTY).
    _stdout_styles = COLOR_STYLES if stdout.isatty() else NO_COLOR_STYLES
    _stderr_styles = COLOR_STYLES if stderr.isatty() else NO_COLOR_STYLES

    # If using color on Windows, enable ANSI color support.
    if sys.platform == 'win32' and (
        (_stderr_styles is COLOR_STYLES) or (_stdout_styles is COLOR_STYLES)
    ):
        import colorama
        colorama.just_fix_windows_console()


def get_stdout_styles() -> TerminalStyle:
    """Return configured styles for stdout, or no-color as fallback."""
    return _stdout_styles or NO_COLOR_STYLES


def get_stderr_styles() -> TerminalStyle:
    """Return configured styles for stderr, or no-color as fallback."""
    return _stderr_styles or NO_COLOR_STYLES


# =====================================================================
# Logging
# =====================================================================

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
        s = get_stderr_styles()

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
