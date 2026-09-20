"""Implementation for "label" command."""
import argparse
import logging

from .common import (
    cli_bind_file,
    process_backup_option,
    normalize_arg_list,
    ExitCode,
)

applogger = logging.getLogger('app-toron')


def add(args: argparse.Namespace) -> ExitCode:
    """Add index label columns to the given data-space file."""
    ds = cli_bind_file(args.filepath, mode='rw')
    process_backup_option(args, ds)
    normalized = normalize_arg_list(args.names)

    ds.add_index_columns(*normalized)

    formatted_names = ', '.join(repr(x) for x in normalized)
    applogger.info(f'added label names: {formatted_names}')

    return ExitCode.OK


def update(args: argparse.Namespace) -> ExitCode:
    """Update index label column in the given data-space file."""
    ds = cli_bind_file(args.filepath, mode='rw')
    process_backup_option(args, ds)

    if args.move_left and not args.move_right:
        ds.change_label_order(args.name, offset=-args.move_left)
        applogger.info(f'moved label {args.name!r} to the left')
    elif args.move_right and not args.move_left:
        ds.change_label_order(args.name, offset=args.move_right)
        applogger.info(f'moved label {args.name!r} to the right')
    else:
        raise Exception

    return ExitCode.OK
