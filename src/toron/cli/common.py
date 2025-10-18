"""Common resources for Toron CLI application."""
import csv
import io
import logging
import logging.config
import os
import sys
from contextlib import contextmanager
from dataclasses import astuple, dataclass
from enum import IntEnum
from .._typing import (
    Any,
    Dict,
    Generator,
    Literal,
    Mapping,
    Optional,
    TextIO,
    Tuple,
    Type,
    TYPE_CHECKING,
)


if TYPE_CHECKING:
    from _csv import _writer as WriterType


class ExitCode(IntEnum):
    """Status code (errorlevel) to return when program exits."""
    OK = 0     # Success.
    ERR = 1    # General error.
    USAGE = 2  # Incorrect usage (invalid options or missing args).


@contextmanager
def csv_stdout_writer(
    stdout: Optional[TextIO] = None
) -> Generator['WriterType', None, None]:
    """Context manager to yield a ``csv.writer()`` to stdout."""
    if not stdout:
        stdout = sys.stdout

    stdout_wrapper = io.TextIOWrapper(
        stdout.buffer,
        encoding='utf-8',
        newline='', # Disable universal newline translation.
        line_buffering=True, # Flush stream after every newline character.
    )
    writer = csv.writer(stdout_wrapper, lineterminator='\n')
    try:
        yield writer
    finally:
        stdout_wrapper.flush()  # Ensure all output is written.


# ============================
# Terminal Color Configuration
# ============================

@dataclass(frozen=True)
class StyleCodes:
    """Style codes to use for application output.

    The given codes are used in-line with text output to implement a
    procedural markup scheme. ANSI-style terminal control codes can
    be provided to provide colors and styles for stream output.
    """
    info: str = ''
    warning: str = ''
    error: str = ''
    critical: str = ''
    reset: str = ''
    bright: str = ''


# ANSI terminal control codes.
ansi_codes = {
    'info': '\33[38;5;33m',                   # blue
    'warning': '\33[38;5;214m',               # yellow
    'error': '\33[38;5;196m',                 # red
    'critical': '\33[48;5;196m\33[38;5;16m',  # red background
    'reset': '\33[0m',                        # reset styles
    'bright': '\33[1m',                       # bright text
}


def get_stream_styles(
    *,
    environ: Optional[Mapping] = None,
    stdout: Optional[TextIO] = None,
    stderr: Optional[TextIO] = None,
) -> Tuple[StyleCodes, StyleCodes]:
    """Get terminal styles for ``stdout`` and ``stderr`` streams.

    .. code-block:: python

        >>> stdout_style, stderr_style = get_stream_styles()
    """
    if environ is None:
        environ = os.environ

    if environ.get('NO_COLOR') or environ.get('TERM') == 'dumb':
        no_style = StyleCodes()
        return (no_style, no_style)

    if stdout is None:
        stdout = sys.stdout
    if stderr is None:
        stderr = sys.stderr

    # Set ANSI styles if a stream is connected to a terminal (a TTY).
    ansi_style = StyleCodes(**ansi_codes)
    no_style = StyleCodes()
    stdout_style = ansi_style if stdout.isatty() else no_style
    stderr_style = ansi_style if stderr.isatty() else no_style

    # If using ANSI styles on Windows, enable ANSI color support.
    if sys.platform == 'win32' and (
        (stderr_style is ansi_style) or (stdout_style is ansi_style)
    ):
        import colorama
        colorama.just_fix_windows_console()

    return (stdout_style, stderr_style)


# ====================
# Logger Configuration
# ====================

def get_formatter_class(style_codes: StyleCodes) -> Type[logging.Formatter]:
    """Return a logging `Formatter` class that optionally uses styled text."""
    # If no styles are defined, return built-in `logging.Formatter`.
    if not any(astuple(style_codes)):
        return logging.Formatter  # <- EXIT!

    # Define short alias to use in f-strings.
    s = style_codes  # <- This gets closed-over by AnsiStyleFormatter.

    class AnsiStyleFormatter(logging.Formatter):
        """Formatter to convert LogRecord into ANSI styled text."""
        def __init__(self, *args, **kwds) -> None:
            # Initialize self as un-styled formatter (for fall-back).
            super().__init__(*args, **kwds)

            # Get `fmt` argument.
            if 'fmt' in kwds:
                fmt = kwds.pop('fmt')
            elif args:
                fmt, args = args[0], args[1:]
            else:
                fmt = '%(message)s'

            # Instantiate ANSI style formatters.
            self.color_formatters = {
                logging.INFO: logging.Formatter(
                    f'{s.info}{fmt}{s.reset}', *args, **kwds
                ),
                logging.WARNING: logging.Formatter(
                    f'{s.warning}{fmt}{s.reset}', *args, **kwds
                ),
                logging.ERROR: logging.Formatter(
                    f'{s.error}{fmt}{s.reset}', *args, **kwds
                ),
                logging.CRITICAL: logging.Formatter(
                    f'{s.critical}{fmt}{s.reset}', *args, **kwds
                ),
            }

        def format(self, record: logging.LogRecord) -> str:
            formatter = self.color_formatters.get(record.levelno)
            if formatter:
                return formatter.format(record)
            return super().format(record)  # <- Use un-styled fall-back.

    return AnsiStyleFormatter


def configure_applogger(
    applogger: logging.Logger, style_codes: StyleCodes
) -> None:
    """Configure handler and formatter for given *applogger*."""
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'cli_formatter': {
                '()': get_formatter_class(style_codes),
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
