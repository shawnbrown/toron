"""Implementation for ":" (relation operator) command."""
import argparse
import logging
import os

from .common import (
    ExitCode,
    cli_bind_file,
    process_backup_option,
)


applogger = logging.getLogger('app-toron')


def create_link(args: argparse.Namespace) -> ExitCode:
    """Create a link between two node files."""
    if args.direction not in {'both', 'right', 'left'}:
        raise RuntimeError(f'unhandled direction: {args.direction!r}')

    ds1 = cli_bind_file(args.filepath, mode='rw')
    ds2 = cli_bind_file(args.filepath2, mode='rw')
    process_backup_option(args, ds1, ds2)

    basename1 = os.path.basename(args.filepath)
    basename2 = os.path.basename(args.filepath2)

    if args.direction == 'both' or args.direction == 'right':
        applogger.info(f'adding link {basename1!r} -> {basename2}')
        ds2.add_link(
            space=ds1,
            link_name=args.link,
            other_filename_hint=ds1.path_hint,
            description=args.description,
            selectors=args.selectors,
            is_default=args.make_default or None,  # Use `None` instead of `False`.
        )

    if args.direction == 'both' or args.direction == 'left':
        applogger.info(f'adding link {basename1!r} <- {basename2}')
        ds1.add_link(
            space=ds2,
            link_name=args.link,
            other_filename_hint=ds2.path_hint,
            description=args.description,
            selectors=args.selectors,
            is_default=args.make_default or None,  # Use `None` instead of `False`.
        )

    return ExitCode.OK
