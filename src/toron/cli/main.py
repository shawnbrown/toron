"""Main command line application function."""
import argparse
import logging
from .. import __version__
from .common import (
    ExitCode,
    configure_styles,
    configure_applogger,
)


def get_parser() -> argparse.ArgumentParser:
    """Get argument parser for Toron command line interface."""
    parser = argparse.ArgumentParser(
        prog='toron',
        description='Show and edit Toron node file properties.',
        epilog=f'Version: Toron {__version__}',
    )
    parser.add_argument('path', help='path to file', metavar='PATH')
    return parser


applogger = logging.getLogger('app-toron')
configure_styles()
configure_applogger(applogger)


def main() -> ExitCode:
    parser = get_parser()
    args = parser.parse_args()

    from .command_info import command
    return command(args)
