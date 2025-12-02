"""Implementation for "add" command."""
import argparse
import logging

from .common import (
    process_backup_option,
    ExitCode,
)


applogger = logging.getLogger('app-toron')


def add_label(args: argparse.Namespace) -> ExitCode:
    """Add index label columns to the given node file."""
    process_backup_option(args)
    args.node.add_index_columns(*args.labels)

    formatted_labels = ', '.join(repr(x) for x in args.labels)
    applogger.info(f'added index label columns: {formatted_labels}')

    return ExitCode.OK


def add_weight(args: argparse.Namespace) -> ExitCode:
    """Add index weight groups to the given node file."""
    process_backup_option(args)

    if args.make_default:
        make_default = True
    else:
        make_default = None  # Use `None` instead of `False` for appropriate
                             # `add_weight_group()` behavior.

    args.node.add_weight_group(
        name=args.weight,
        description=args.description,
        selectors=args.selectors,
        make_default=make_default,
    )

    msg = f'added index weight group {args.weight!r} to {args.node.path_hint}'
    applogger.info(msg)

    return ExitCode.OK
