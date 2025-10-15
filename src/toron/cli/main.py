"""Main command line application function."""
import argparse
import logging
import os
import sys
from os.path import isfile
from .._typing import Literal, Optional, Set
from .. import __version__, bind_node, TopoNode
from .common import (
    ExitCode,
    configure_applogger,
    get_stream_styles,
)


class TopoNodeType(object):
    """Factory for creating TopoNode object types.

    This class is used when adding arguments to an ArgumentParser
    instance::

        parser = argparse.ArgumentParser()
        parser.add_argument('file', type=TopoNodeType())
    """
    def __init__(
        self, mode: Optional[Literal['ro', 'rw', 'rwc']] = None
    ) -> None:
        self._mode = mode

    def __call__(self, string: str) -> TopoNode:
        try:
            if self._mode:
                # If mode was explicitly provided, use it as-is.
                node = bind_node(string, mode=self._mode)
            elif sys.stdin.isatty():
                # If input is a terminal device (a TTY), use read-only mode.
                node = bind_node(string, mode='ro')
            else:
                raise NotImplementedError('stream input untested')
                if not sys.stdout.isatty():
                    msg = 'cannot insert and select records at the same time'
                    raise argparse.ArgumentTypeError(msg)

                # If input is redirected from a file or pipe, use read-write mode.
                node = bind_node(string, mode='rw')

        except Exception as e:
            msg = f"can't open {string!r}: {e}"
            raise argparse.ArgumentTypeError(msg)

        return node


def get_parser() -> argparse.ArgumentParser:
    """Get argument parser for Toron command line interface."""

    # Local variable to hold subparser choices (once parser is defined).
    valid_choices: Set[str] = set()  # Closed-over by parser instance.

    # Define nested class to close over `valid_choices`.
    class ToronArgumentParser(argparse.ArgumentParser):
        def parse_args(self, args=None, namespace=None):
            if args is None:
                args = sys.argv[1:]  # Default to system args.

            if not args:
                self.print_help(sys.stderr)
                self.exit(ExitCode.USAGE)  # <- EXIT!

            if args[0] not in valid_choices and isfile(args[0]):
                if '-h' in args or '--help' in args:
                    args = ['-h']  # Invoke main "help".
                else:
                    args = ['info'] + args  # If arg is not a command but matches
                                            # an existing filename, invoke "info".
            return super().parse_args(args, namespace)

    # Define main parser.
    parser = ToronArgumentParser(
        prog='toron',
        description='Show and edit Toron node file properties.',
        usage='%(prog)s (COMMAND ... | filename) [-h] [--version]',
    )
    parser.add_argument('--version', action='version',
                        version=f'%(prog)s {__version__}')

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
    parser_index.add_argument('file', type=TopoNodeType(),
                              help='Toron node file', metavar='FILE')

    # Info command.
    parser_info = subparsers.add_parser(
        'info',
        help='show file info (default if filename given)',
        description='Show file information.',
    )
    parser_info.add_argument('file', type=TopoNodeType(mode='ro'),
                             help='Toron node file', metavar='FILE')

    # Add subparser choices to local variable.
    valid_choices.update(subparsers.choices)

    return parser


def main() -> ExitCode:
    applogger = logging.getLogger('app-toron')
    stdout_style, stderr_style = get_stream_styles()
    configure_applogger(applogger, stderr_style)

    parser = get_parser()
    args = parser.parse_args()

    if args.command == 'info':
        from .command_info import print_info
        return print_info(args, stdout_style)

    if args.command == 'index':
        from .command_index import print_index
        try:
            return print_index(args)
        except BrokenPipeError:
            os._exit(ExitCode.OK)  # Downstream stopped early; exit with OK.

    parser.error('unable to process command')  # Exits with error code 2.
