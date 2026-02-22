"""Main command line application function."""
import argparse
import logging
import sys
from os.path import isfile
from .._typing import (
    List,
    Literal,
    Optional,
    Set,
    TextIO,
)
from .. import (
    __version__,
    bind_node,
    TopoNode,
    ToronError,
)
from . import (
    command_add,
    command_info,
    command_index,
    command_new,
)
from .common import (
    ExitCode,
    process_backup_option,
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

    # New command.
    parser_new = subparsers.add_parser(
        'new',
        help='create a new node file',
        description='Create a new node file.',
    )
    parser_new.add_argument('node_path', type=str,
                            help='name of file to create', metavar='FILE')
    parser_new.add_argument('--domain',
                            help='define a domain (defaults to FILE without extension)')
    parser_new.set_defaults(func=command_new.create_file)

    # Add command.
    parser_add = subparsers.add_parser(
        'add',
        help='add properties to node file',
        description='Add properties to an existing node file.',
    )
    parser_add_subparsers = parser_add.add_subparsers(
        dest='element',
        required=True,
        metavar='ELEMENT',
        prog='toron add',
    )

    # Add label command.
    parser_add_label = parser_add_subparsers.add_parser(
        'label',
        aliases=['labels'],
        help='add index labels to node file',
        description='Add index labels to an existing node file.',
    )
    parser_add_label.add_argument('node', type=TopoNodeType(mode='rw'),
                                  help='name of file to modify', metavar='FILE')
    parser_add_label.add_argument('labels', nargs='+',
                                  help='index label to add', metavar='LABEL')
    parser_add_label.add_argument('--no-backup', action='store_false',
                                  dest='backup',
                                  help='do not make a backup file')
    parser_add_label.set_defaults(func=command_add.add_label)

    # Add weight command.
    parser_add_weight = parser_add_subparsers.add_parser(
        'weight',
        help='add index weight group to node file',
        description='Add index weight groups to an existing node file.',
    )
    parser_add_weight.add_argument('node', type=TopoNodeType(mode='rw'),
                                   help='name of file to modify',
                                   metavar='FILE')
    parser_add_weight.add_argument('weight',
                                   help='name of index weight to add',
                                   metavar='WEIGHT')
    parser_add_weight.add_argument('--description',
                                   help='description of weight group')
    parser_add_weight.add_argument('--selectors', nargs='+',
                                   help='attribute selectors')
    parser_add_weight.add_argument('--default', action='store_true',
                                   dest='make_default',
                                   help='set as the default weight group')
    parser_add_weight.add_argument('--no-backup', action='store_false',
                                   dest='backup',
                                   help='do not make a backup file')
    parser_add_weight.set_defaults(func=command_add.add_weight)

    # Index command.
    parser_index = subparsers.add_parser(
        name='index',
        help='select index records, or insert records from input',
        description=('Select index records and print them as CSV, or '
                     'insert records supplied as input.'),
    )
    parser_index.add_argument('node', type=TopoNodeType(),
                              help='Toron node file', metavar='FILE')
    parser_index.add_argument('--on-label-conflict',
                              default='abort',
                              choices=['ignore', 'replace', 'abort'],
                              dest='on_label_conflict',
                              help='how to handle label conflicts (default: %(default)s)')
    parser_index.add_argument('--on-weight-conflict',
                              default='abort',
                              choices=['ignore', 'replace', 'abort'],
                              dest='on_weight_conflict',
                              help='how to handle weight conflicts (default: %(default)s)')
    parser_index.add_argument('--no-backup', action='store_false',
                              dest='backup',
                              help='do not make a backup file')
    parser_index.set_defaults(func=command_index.process_index_action)

    # Info command.
    parser_info = subparsers.add_parser(
        'info',
        help='show file info (default if filename given)',
        description='Show file information.',
    )
    parser_info.add_argument('node', type=TopoNodeType(mode='ro'),
                             help='Toron node file', metavar='FILE')
    parser_info.set_defaults(func=command_info.write_to_stdout)

    # Add subparser choices to local variable.
    valid_choices.update(subparsers.choices)

    return parser


def main(
    argv: Optional[List[str]] = None,
    *,
    stdin: Optional[TextIO] = None,
    stdout: Optional[TextIO] = None,
    stderr: Optional[TextIO] = None,
) -> ExitCode:
    applogger = logging.getLogger('app-toron')
    stdout_style, stderr_style = get_stream_styles(stdout=stdout, stderr=stderr)
    configure_applogger(applogger, stderr_style, stream=stderr)

    parser = get_parser()
    if argv is None:
        argv = sys.argv[1:]  # Default to command line arguments.
    args = parser.parse_args(argv)

    args.stdin = stdin or sys.stdin
    args.stdin_is_streamed = not args.stdin.isatty()  # True if redirected or piped.

    args.stdout = stdout or sys.stdout
    args.stdout_style = stdout_style

    try:
        return args.func(args)
    except ToronError as e:
        applogger.error(str(e))
        return ExitCode.ERR
