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
    # Define main parser.
    parser = argparse.ArgumentParser(
        prog='toron',
        description='Show and edit Toron node file properties.',
        epilog=f'Version: Toron {__version__}',
    )

    # Define subparsers for COMMAND.
    subparsers = parser.add_subparsers(
        dest='command',
        required=True,
        metavar='COMMAND',
        prog='toron',
    )

    # Info command.
    parser_info = subparsers.add_parser(
        'info',
        help='show file info',
        description='Show file information.',
    )
    parser_info.add_argument('path', help='Toron node file', metavar='PATH')

    return parser


applogger = logging.getLogger('app-toron')
configure_styles()
configure_applogger(applogger)


def main() -> ExitCode:
    parser = get_parser()
    args = parser.parse_args()

    if args.command == 'info':
        from .command_info import command
        return command(args)

    parser.error('unable to process command')  # Exits with error code 2.
