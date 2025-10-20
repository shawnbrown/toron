"""Main command line application function."""
import argparse
import logging
import os
import sys
from os.path import isfile
from .._typing import List, Literal, Optional, Set
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
                # Mode is given, use it as-is regardless of input.
                node = bind_node(string, mode=self._mode)
            else:
                if not sys.stdin.isatty():
                    # Input is redirected from a file or pipe, use read-write mode.
                    node = bind_node(string, mode='rw')
                else:
                    # Input is a terminal device (a TTY), use read-only mode.
                    node = bind_node(string, mode='ro')

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
    parser_index.add_argument('node', type=TopoNodeType(),
                              help='Toron node file', metavar='FILE')

    # Info command.
    parser_info = subparsers.add_parser(
        'info',
        help='show file info (default if filename given)',
        description='Show file information.',
    )
    parser_info.add_argument('node', type=TopoNodeType(mode='ro'),
                             help='Toron node file', metavar='FILE')

    # Add subparser choices to local variable.
    valid_choices.update(subparsers.choices)

    return parser


def main(argv: Optional[List[str]] = None) -> ExitCode:
    applogger = logging.getLogger('app-toron')
    stdout_style, stderr_style = get_stream_styles()
    configure_applogger(applogger, stderr_style)

    input_streamed = not sys.stdin.isatty()  # stdin is piped or redirected.

    parser = get_parser()
    if argv is None:
        argv = sys.argv[1:]  # Default to command line arguments.
    args = parser.parse_args(argv)

    if args.command == 'info':
        from . import command_info
        return command_info.write_to_stdout(args, stdout_style)

    if args.command == 'index':
        from . import command_index
        if input_streamed:
            return command_index.read_from_stdin(args)
        else:
            try:
                return command_index.write_to_stdout(args)
            except BrokenPipeError:
                os._exit(ExitCode.OK)  # Downstream stopped early; exit with OK.

    parser.error('unable to process command')  # Exits with error code 2.
