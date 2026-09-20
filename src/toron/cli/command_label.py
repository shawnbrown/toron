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
