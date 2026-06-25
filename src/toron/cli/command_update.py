"""Implementation for "update" command."""
import argparse
import logging

from .common import (
    process_backup_option,
    ExitCode,
)


applogger = logging.getLogger('app-toron')


def update_label(args: argparse.Namespace) -> ExitCode:
    """Update index label column in the given node file."""
    process_backup_option(args)

    if args.move_left and not args.move_right:
        args.node.change_label_order(args.label, offset=-args.move_left)
        applogger.info(f'moved label {args.label!r} to the left')
    elif args.move_right and not args.move_left:
        args.node.change_label_order(args.label, offset=args.move_right)
        applogger.info(f'moved label {args.label!r} to the right')
    else:
        raise Exception

    return ExitCode.OK
