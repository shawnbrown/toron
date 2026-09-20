"""Implementation for "weight" command."""
import argparse
import logging

from .common import (
    cli_bind_file,
    process_backup_option,
    ExitCode,
)


applogger = logging.getLogger('app-toron')


def add(args: argparse.Namespace) -> ExitCode:
    """Add index weight groups to the given data-space file."""
    ds = cli_bind_file(args.filepath, mode='rw')
    process_backup_option(args, ds)

    ds.add_weight_group(
        name=args.name,
        description=args.description,
        selectors=args.selectors,
        make_default=args.make_default or None,  # Use `None` instead of `False`.
    )

    msg = f'added index weight group {args.name!r} to {ds.path_hint}'
    applogger.info(msg)

    return ExitCode.OK
