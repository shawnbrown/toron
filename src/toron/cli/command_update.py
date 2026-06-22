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
    #process_backup_option(args)

    print(args)

    #args.node.add_index_columns(*normalized)

    #formatted_labels = ', '.join(repr(x) for x in normalized)
    #applogger.info(f'added index label columns: {formatted_labels}')

    return ExitCode.OK
