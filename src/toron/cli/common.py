"""Common resources for Toron CLI application."""
import argparse
import csv
import io
import logging
import logging.config
import os
import sys
from binascii import crc32
from collections import Counter
from contextlib import contextmanager
from dataclasses import astuple, dataclass
from enum import IntEnum
from struct import Struct
from .._typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    TextIO,
    Tuple,
    Type,
    Union,
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
    if stdout is None:
        stdout = sys.stdout

    stdout_wrapper = io.TextIOWrapper(
        stdout.buffer,
        encoding='utf-8',
        newline='', # Disable universal newline translation.
        line_buffering=True, # Flush stream after every newline character.
    )
    try:
        yield csv.writer(stdout_wrapper, lineterminator='\n')
    finally:
        stdout_wrapper.flush()  # Ensure all output is written.
        try:
            stdout_wrapper.detach()  # Keep `stdout.buffer` open.
        except Exception:
            pass


def process_backup_option(
    args: argparse.Namespace,
    node_args: Union[str, List[str]] = 'node',
) -> None:
    """Make a backup copy of node args if `args.backup` is True.

    The backup file name is the same as the node's `path_hint` but
    with the prefix 'backup-'. When a backup file of the same name
    already exists, it is overwritten. If the path hint is None, a
    FileNotFoundError is raised.

    Multiple files can be backed up by providing a list of argument
    names as *node_args*::

        process_backup_option(args, node_args=['node1', 'node2'])
    """
    if not getattr(args, 'backup', False):
        return  # Exit without making backups if `args.backup` is not True.

    if isinstance(node_args, str):
        node_args = [node_args]  # Single-item list.

    nodes = []
    for node_arg in node_args:
        node = getattr(args, node_arg)
        if node.path_hint is None:
            raise FileNotFoundError(
                f'{node_arg} is not associated with a file path '
                f'(has no `path_hint`)'
            )
        nodes.append(node)

    for node in nodes:
        dir_name, base_name = os.path.split(node.path_hint)
        backup_path = os.path.join(dir_name, f'backup-{base_name}')
        node.to_file(backup_path)


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
        return (no_style, no_style)  # <- EXIT!

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
            # Get `fmt` as keyword argument or as first positional argument.
            if 'fmt' in kwds:
                fmt = kwds.pop('fmt')
            elif args:
                fmt, args = args[0], args[1:]
            else:
                fmt = '%(message)s'  # Same default as `logging.Formatter`.

            # Initialize self as un-styled formatter (for fall-back).
            super().__init__(fmt, *args, **kwds)

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
    applogger: logging.Logger,
    style_codes: StyleCodes,
    *,
    stream: Optional[TextIO] = None,
) -> None:
    """Configure handler and formatter for given *applogger*.

    If *stream* is not given, ``sys.stderr`` is used by default.
    """
    # Get `AnsiStyleFormatter` or `logging.Formatter`.
    formatter_class = get_formatter_class(style_codes)

    # Configure `applogger` to write formatted output to stderr.
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'cli_formatter': {
                '()': formatter_class,  # Special key '()' to call Formatter factory.
                'fmt': '%(levelname)s: %(message)s',
            },
        },
        'handlers': {
            'cli_handler': {
                'class': 'logging.StreamHandler',  # Needs string value in 3.11 and earlier.
                'formatter': 'cli_formatter',
                'stream': stream or sys.stderr,
            },
        },
        'loggers': {
            applogger.name: {
                'handlers': ['cli_handler'],
                'propagate': False,
            },
        },
    })


# ==================
# Handle Index Codes
# ==================

# Serialize `int` as signed 64-bit big-endian ('>q' format).
pack_i64_be = Struct('>q').pack


def index_id_to_code(
    index_id: int, unique_id_bytes: bytes, pad_len: int = 0
) -> str:
    """Convert ``index_id`` into an ``index_code``."""
    # Using crc32 to verify IDs and distinguish datasets; not for security.
    checksum = crc32(pack_i64_be(index_id) + unique_id_bytes) & 0xffffffff
    return f'{index_id:0{pad_len}}X{checksum:08X}'


def index_code_to_id(
    index_code: str, unique_id_bytes: bytes
) -> int:
    """Verify ``index_code`` checksum and return ``index_id``."""
    try:
        index_id_dec, _, checksum_hex = index_code.partition('X')
    except AttributeError as e:
        msg = f'{e}; index_code must be a str, got {index_code!r}'
        raise AttributeError(msg)

    try:
        index_id = int(index_id_dec)
        checksum = int(checksum_hex, 16)
    except ValueError:
        raise ValueError(f'badly formatted index code: {index_code}')

    # Using crc32 to verify IDs and distinguish datasets; not for security.
    if checksum != crc32(pack_i64_be(index_id) + unique_id_bytes) & 0xffffffff:
        raise ValueError(f'checksum mismatch for index code: {index_code}')

    return index_id


def is_index_code(
    value: str, unique_id_bytes: bytes
) -> bool:
    """Returns True if ``value`` is a valid index code for the given id
    bytes, else return False.
    """
    try:
        index_code_to_id(value, unique_id_bytes)
    except (ValueError, AttributeError):
        return False
    return True


def get_index_code_position(
    sample_rows: Iterable[Sequence], unique_id_bytes: bytes
) -> int:
    """Get the column position containing a node's index codes.

    Return the 0-based position of the column associated with
    a node's *unique_id_bytes*. If a single column cannot be
    identified, a ``RuntimeError`` is raised.
    """
    counter: Counter[int] = Counter()
    for row in sample_rows:
        for pos, cell in enumerate(row):
            counter[pos] += is_index_code(cell, unique_id_bytes)

    counter = +counter  # Keep only positive counts (n >= 1).

    if len(counter) == 1:
        position = counter.most_common(1)[0][0]
        return position  # <- EXIT!

    if len(counter) == 0:
        msg = 'no column found with matching index codes'
    else:
        *nonfinal, final = (str(x) for x in sorted(counter))  # Unpack conjuncts.
        msg = (f"found multiple columns with matching index codes "
               f"at positions: {', '.join(nonfinal)} and {final}")

    raise RuntimeError(msg)


def remap_index_codes_to_index_ids(
    data: Iterable[Sequence], unique_id_bytes: bytes, position: int
) -> Generator[List, None, None]:
    """Change the data's index code column into an "index_id" column.

    Accepts an iterable of lists (*data*, usually a CSV reader). Yields
    an updated header with the index column name changed to "index_id",
    and yields rows with the converted index_id values.
    """
    iterator = iter(data)

    header = list(next(iterator))
    header[position] = 'index_id'
    yield header

    for row in iterator:
        row = list(row)
        index_code = row[position]
        if index_code:
            row[position] = index_code_to_id(index_code, unique_id_bytes)
        yield row


def make_index_code_header(domain: Union[str, Dict[str, str]]) -> str:
    """Make an "index code" column name prefixed with domain text."""
    # If using an old-style domain (dict), process values.
    if isinstance(domain, dict):
        # TODO: Remove `dict` handling once domain is properly updated.
        values = [v.strip() for _, v in sorted(domain.items())]
        values = [v.replace(' ', '_') for v in values if v]
        if values:
            return f"{'_'.join(values)}_index_code"  # <- EXIT!
        return 'index_code'  # <- EXIT!

    domain = domain.strip()
    if domain:
        return f"{domain.replace(' ', '_')}_index_code"
    return 'index_code'
