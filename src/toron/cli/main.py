"""Main command line application function."""

import argparse
import logging
import sys
from pathlib import Path
from shutil import get_terminal_size
from .._typing import (
    Final,
)
from .. import (
    __version__,
    Node,
)
from .loggerconfig import configure_applogger


parser = argparse.ArgumentParser(
    prog='toron',
    epilog=f'Version: Toron {__version__}',
)
parser.add_argument('path', help='path to file')


applogger = logging.getLogger('app-toron')
configure_applogger(applogger)


EXITCODE_OK: Final[int] = 0
EXITCODE_ERR: Final[int] = 1


def main() -> int:
    args = parser.parse_args()

    if Path(args.path).is_file():
        path = args.path
    elif Path(f'{args.path}.toron').is_file():
        path = f'{args.path}.toron'
    else:
        applogger.error(f'file not found {args.path!r}')
        return EXITCODE_ERR  # <- EXIT!

    filename = Path(path).name  # File only, no parent directory text.

    try:
        node = Node.from_file(path)
    except Exception as err:
        applogger.error(str(err))
        return EXITCODE_ERR  # <- EXIT!

    # Define horizontal rule `hr` made from "Box Drawings" character.
    hr = '─' * min(len(filename), (get_terminal_size()[0] - 1))

    sys.stdout.write(
        f'{hr}\n{filename}\n{hr}\n'
        f'{repr(node)}\n'
    )
    return EXITCODE_OK
