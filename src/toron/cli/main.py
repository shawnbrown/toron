"""Main command line application function."""
import argparse
import logging
import os
from os.path import isfile
from .. import __version__
from .common import (
    ExitCode,
    configure_styles,
    configure_applogger,
)


def existing_file(path):
    """Checks if path (or path plus ".toron") exists and returns it
    or raises an ``ArgumentTypeError``.

    This function is used when adding arguments to an ArgumentParser
    instance::

        parser = argparse.ArgumentParser()
        parser.add_argument('file', type=existing_file)
    """
    if isfile(path):
        return path

    path_and_extension = f'{path}.toron'
    if isfile(path_and_extension):
        return path_and_extension

    raise argparse.ArgumentTypeError(f'no such file: {path}')


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

    # Index command.
    parser_index = subparsers.add_parser(
        name='index',
        help='select index records',
        description='Select index records in CSV format.',
    )
    parser_index.add_argument('file', type=existing_file,
                              help='Toron node file', metavar='FILE')

    # Info command.
    parser_info = subparsers.add_parser(
        'info',
        help='show file info',
        description='Show file information.',
    )
    parser_info.add_argument('file', type=existing_file,
                             help='Toron node file', metavar='FILE')

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

    if args.command == 'index':
        from .command_index import command
        try:
            return command(args)
        except BrokenPipeError:
            os._exit(ExitCode.OK)  # Downstream stopped early; exit with OK.

    parser.error('unable to process command')  # Exits with error code 2.
