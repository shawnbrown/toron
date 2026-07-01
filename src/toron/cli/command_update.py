"""Implementation for "update" command."""
import argparse
import logging

from .common import (
    cli_bind_node,
    process_backup_option,
    ExitCode,
)


applogger = logging.getLogger('app-toron')


def update_label(args: argparse.Namespace) -> ExitCode:
    """Update index label column in the given node file."""
    node = cli_bind_node(args.filepath, mode='rw')
    process_backup_option(args, node)

    if args.move_left and not args.move_right:
        node.change_label_order(args.label, offset=-args.move_left)
        applogger.info(f'moved label {args.label!r} to the left')
    elif args.move_right and not args.move_left:
        node.change_label_order(args.label, offset=args.move_right)
        applogger.info(f'moved label {args.label!r} to the right')
    else:
        raise Exception

    return ExitCode.OK
