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
