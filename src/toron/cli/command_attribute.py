"""Implementation for "attribute" command."""
import argparse
import logging

from .common import (
    cli_bind_file,
    process_backup_option,
    normalize_arg_list,
    ExitCode,
)
from .._utils import ToronError


applogger = logging.getLogger('app-toron')


def add(args: argparse.Namespace) -> ExitCode:
    """Add attribute columns to the given data-space file."""
    ds = cli_bind_file(args.filepath, mode='rw')
    process_backup_option(args, ds)

    attribute_columns = ds.get_registered_attributes()

    new_attributes = []
    for attr in normalize_arg_list(args.names):
        if attr not in attribute_columns:
            new_attributes.append(attr)
        else:
            applogger.warning(f'skipping {attr!r} (already registered)')

    if new_attributes:
        try:
            ds.set_registered_attributes(attribute_columns + new_attributes)
        except ValueError as e:
            raise ToronError(str(e))
        formatted_attrs = ', '.join(repr(x) for x in new_attributes)
        applogger.info(f'added attribute columns: {formatted_attrs}')
    else:
        applogger.info(f'no attributes added')

    return ExitCode.OK


def update(args: argparse.Namespace) -> ExitCode:
    """Update quantity attribute in the given data-space file."""
    ds = cli_bind_file(args.filepath, mode='rw')
    process_backup_option(args, ds)

    if args.move_left and not args.move_right:
        ds.change_attribute_order(args.name, offset=-args.move_left)
        applogger.info(f'moved attribute {args.name!r} to the left')
    elif args.move_right and not args.move_left:
        ds.change_attribute_order(args.name, offset=args.move_right)
        applogger.info(f'moved attribute {args.name!r} to the right')
    else:
        raise Exception

    return ExitCode.OK
