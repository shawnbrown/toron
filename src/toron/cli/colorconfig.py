"""Configure CLI application color and style codes."""

import os
import sys
from dataclasses import dataclass


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
