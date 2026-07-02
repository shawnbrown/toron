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
    command_update,
    command_quantity,
    command_crosswalk,
    command_init,
)
from .common import (
    ExitCode,
    configure_applogger,
    get_stream_styles,
)


def get_parser() -> argparse.ArgumentParser:
    """Get argument parser for Toron command line interface."""

    class ToronArgumentParser(argparse.ArgumentParser):
        def __init__(self, *args, **kwds):
            super().__init__(*args, **kwds)
            self._choices_ref: Mapping[str, argparse.ArgumentParser] = {}

        def add_subparsers(self, *args, **kwds):
            subparsers = super().add_subparsers(*args, **kwds)
            self._choices_ref = subparsers.choices  # Assign ref to dict.
            return subparsers

        def _preprocess_args(self, args: List[str]) -> List[str]:
            """Preprocess arguments to support custom behavior.

            This method modifies the *args* list to support two
            user-friendly shorthands:

            1. Allows subcommand help invocation without requiring a
               FILE argument (e.g., "toron init -h").
            2. Defaults to the "info" command when FILE is provided
               but COMMAND is omitted (e.g., 'toron myfile.toron').
            """
            if '-h' in args or '--help' in args:
                # Get first two positional args before help flag.
                help_index = next(i for i, arg in enumerate(args)
                                  if arg in ('-h', '--help'))
                positionals = (a for a in args[:help_index] if not a.startswith('-'))
                first = next(positionals, None)
                second = next(positionals, None)

                # Parser expects FILE before COMMAND. But we want to support
                # calling help with COMMAND alone, so we insert a dummy value
                # if FILE is missing.
                if second not in self._choices_ref and (
                    first in self._choices_ref or (first and not isfile(first))
                ):
                    return ['<dummy-filename>'] + args
                return args

            # If FILE is given but COMMAND is missing, default to "info".
            if len(args) == 1:
                return args + ['info']

            return args

        def parse_args(self, args=None, namespace=None):
            """Parse ``args`` into a ``argparse.Namespace`` object."""
            if args is None:
                args = sys.argv[1:]  # Default to system args.
            else:
                args = list(args)

            if not args:
                self.print_help(sys.stderr)  # Print full help.
                self.exit(ExitCode.USAGE)  # <- EXIT!

            args = self._preprocess_args(args)
            return super().parse_args(args, namespace)

    # Main parser
    parser = ToronArgumentParser(
        prog='toron',
        description='Show and edit Toron node file properties.',
    )
    parser.add_argument('--version',
                        action='version',
                        version=f'%(prog)s {__version__}')
    parser.add_argument('filepath',
                        type=str,
                        help='name of node file',
                        metavar='FILE')
    subparsers = parser.add_subparsers(
        dest='command',
        required=True,
        metavar='COMMAND',
    )

    # Subcommand: init
    parser_init = subparsers.add_parser(
        'init',
        help='create a new node file',
        description='Create a new node file.',
    )
    parser_init.add_argument('--domain',
                             help='define a domain (defaults to FILE without extension)')
    parser_init.set_defaults(func=command_init.create_file)

    # Subcommand: add
    parser_add = subparsers.add_parser(
        'add',
        help='add properties to node file',
        description='Add properties to an existing node file.',
    )
    parser_add_subparsers = parser_add.add_subparsers(
        dest='element',
        required=True,
        metavar='ELEMENT',
    )

    # Subcommand: add label
    parser_add_label = parser_add_subparsers.add_parser(
        'label',
        help='add index label to node file',
        description=('Add index label to an existing node file. Labels may be '
                     'provided as separate arguments or as a comma-separated '
                     'list.'),
    )
    parser_add_label.add_argument('labels', nargs='+',
                                  help='index label to add', metavar='LABEL')
    parser_add_label.add_argument('--no-backup', action='store_false',
                                  dest='backup',
                                  help='do not make a backup file')
    parser_add_label.set_defaults(func=command_add.add_label)

    # Subcommand: add weight
    parser_add_weight = parser_add_subparsers.add_parser(
        'weight',
        help='add index weight group to node file',
        description='Add index weight groups to an existing node file.',
    )
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

    # Subcommand: add category
    parser_add_category = parser_add_subparsers.add_parser(
        'category',
        help='add a discrete category to node file',
        description=('Add a discrete category to an existing node file. '
                     'Labels may be provided as separate arguments or as '
                     'a comma-separated list.'),
    )
    parser_add_category.add_argument('labels', nargs='+',
                                     help='index labels that define a category',
                                     metavar='LABEL')
    parser_add_category.add_argument('--no-backup', action='store_false',
                                     dest='backup',
                                     help='do not make a backup file')
    parser_add_category.set_defaults(func=command_add.add_category)

    # Subcommand: add attribute
    parser_add_attribute = parser_add_subparsers.add_parser(
        'attribute',
        help='add attribute columns to node file',
        description=('Add attribute columns to an existing node file. '
                     'Attributes may be provided as separate arguments '
                     'or as a comma-separated list.'),
    )
    parser_add_attribute.add_argument('attributes', nargs='+',
                                      help='attribute column to add', metavar='ATTRIBUTE')
    parser_add_attribute.add_argument('--no-backup', action='store_false',
                                      dest='backup',
                                      help='do not make a backup file')
    parser_add_attribute.set_defaults(func=command_add.add_attribute)

    # Subcommand: add link
    parser_add_link = parser_add_subparsers.add_parser(
        'link',
        help='add a link between two node files',
        description='Add a link between two existing node files.',
        prog='toron FILE1 add link',  # <- Replaces "FILE" with "FILE1".
    )
    parser_add_link.add_argument('filepath2',
                                 help='name of second (right) node file',
                                 metavar='FILE2')
    parser_add_link.add_argument('link',
                                 help='name of the link to add',
                                 metavar='LINK')
    parser_add_link_group = parser_add_link.add_mutually_exclusive_group()
    parser_add_link_group.add_argument(
        '--right',
        action='store_const',
        const='right',
        dest='direction',
        help='add single direction: FILE1 -> FILE2',
    )
    parser_add_link_group.add_argument(
        '--left',
        action='store_const',
        const='left',
        dest='direction',
        help='add single direction: FILE1 <- FILE2',
    )
    parser_add_link.add_argument('--description',
                                 help='description of the link')
    parser_add_link.add_argument('--selectors', nargs='+',
                                 help='attribute selectors')
    parser_add_link.add_argument('--default', action='store_true',
                                 dest='make_default',
                                 help='set as the default link')
    parser_add_link.add_argument('--no-backup', action='store_false',
                                 dest='backup',
                                 help='do not make backup files')
    parser_add_link.set_defaults(
        func=command_add.add_link,
        direction='both',
    )

    # Subcommand: update
    parser_update = subparsers.add_parser(
        'update',
        help='update properties in node file',
        description='Update properties in an existing node file.',
    )
    parser_update_subparsers = parser_update.add_subparsers(
        dest='element',
        required=True,
        metavar='ELEMENT',
    )

    # Subcommand: update label
    parser_update_label = parser_update_subparsers.add_parser(
        'label',
        help='update index label in node file',
        description='Update an index label in an existing node file.',
    )
    parser_update_label.add_argument('label',
                                     help='index label to update', metavar='LABEL')
    parser_update_label_group = parser_update_label.add_mutually_exclusive_group(required=True)
    parser_update_label_group.add_argument('--move-left', action='count',
                                           default=0,
                                           help='move label to the left one position')
    parser_update_label_group.add_argument('--move-right', action='count',
                                           default=0,
                                           help='move label to the right one position')
    parser_update_label.add_argument('--no-backup', action='store_false',
                                     dest='backup',
                                     help='do not make a backup file')
    parser_update_label.set_defaults(func=command_update.update_label)

    # Subcommand: index
    parser_index = subparsers.add_parser(
        name='index',
        help='write index to stdout or load index from stdin',
        description=('Write index records to stdout or load index records '
                     'from stdin (CSV format).'),
    )
    parser_index.add_argument('--on-label-conflict',
                              default='abort',
                              choices=['ignore', 'replace', 'abort'],
                              dest='on_label_conflict',
                              help='strategy for label conflicts (default: %(default)s)')
    parser_index.add_argument('--on-weight-conflict',
                              default='abort',
                              choices=['ignore', 'replace', 'abort'],
                              dest='on_weight_conflict',
                              help='strategy for weight conflicts (default: %(default)s)')
    parser_index.add_argument('--no-backup', action='store_false',
                              dest='backup',
                              help='do not make a backup file')
    parser_index.set_defaults(func=command_index.process_index_action)

    # Subcommand: quantity
    parser_quantity = subparsers.add_parser(
        name='quantity',
        help='write quantities to stdout or load quantities from stdin',
        description=('Write quantity records to stdout or load quantity '
                     'records from stdin (CSV format).'),
    )
    parser_quantity.add_argument('--column',
                                 default='quantity',
                                 dest='value_column',
                                 help='name of column containing values (default: %(default)s)',
                                 metavar='NAME')
    parser_quantity.add_argument('--allow-invalid-label', action='store_true',
                                 dest='allow_invalid_label',
                                 help='allow quantities without matching index labels')
    parser_quantity.add_argument('--allow-invalid-category', action='store_true',
                                 dest='allow_invalid_category',
                                 help='allow quantities without matching categories')
    parser_quantity.add_argument('--on-existing',
                                 default='abort',
                                 choices=['ignore', 'replace', 'sum', 'abort'],
                                 dest='on_existing',
                                 help='strategy for existing quantities (default: %(default)s)')
    parser_quantity.add_argument('--no-backup', action='store_false',
                                 dest='backup',
                                 help='do not make a backup file')
    parser_quantity.set_defaults(func=command_quantity.process_quantity_action)

    # Subcommand: crosswalk
    parser_crosswalk = subparsers.add_parser(
        name='crosswalk',
        help='write relations to stdout or load relations from stdin',
        description=('Write crosswalk relations to stdout or load crosswalk '
                     'relations from stdin (CSV format).'),
        prog='toron FILE1 crosswalk',  # <- Replaces "FILE" with "FILE1".
    )
    parser_crosswalk.add_argument('filepath2',
                                  help='second (right) filename',
                                  metavar='FILE2')
    parser_crosswalk.add_argument('link',
                                  help='name of the link associated with the mapping',
                                  metavar='LINK')
    parser_crosswalk_group = parser_crosswalk.add_mutually_exclusive_group()
    parser_crosswalk_group.add_argument(
        '--right',
        action='store_const',
        const='right',
        dest='direction',
        help='add single direction: FILE1 -> FILE2',
    )
    parser_crosswalk_group.add_argument(
        '--left',
        action='store_const',
        const='left',
        dest='direction',
        help='add single direction: FILE1 <- FILE2',
    )
    parser_crosswalk.add_argument('--match-limit',
                                  default=1,
                                  type=int,
                                  help='exclude matches exceeding one-to-LIMIT (default 1)',
                                  metavar='LIMIT')
    parser_crosswalk.add_argument('--allow-overlapping',
                                  action='store_true',
                                  help='allow ambiguous matches to overlap')
    parser_crosswalk.add_argument('--allow-incomplete',
                                  action='store_true',
                                  help='allow loading even when matches are incomplete')
    parser_crosswalk.add_argument('--no-backup', action='store_false',
                                  dest='backup',
                                  help='do not make a backup file')
    parser_crosswalk.set_defaults(
        func=command_crosswalk.process_crosswalk_action,
        direction='both',
    )

    # Subcommand: info
    parser_info = subparsers.add_parser(
        'info',
        help='show file info (default if COMMAND omitted)',
        description='Show file information.',
    )
    parser_info.set_defaults(func=command_info.write_to_stdout)

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
    args = parser.parse_args(argv)

    args.stdin = stdin or sys.stdin
    args.stdout = stdout or sys.stdout

    args.stdout_style = stdout_style

    try:
        return args.func(args)
    except ToronError as e:
        applogger.error(str(e))
        return ExitCode.ERR
