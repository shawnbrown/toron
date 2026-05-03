"""Implementation for "add" command."""
import argparse
import logging

from .common import (
    process_backup_option,
    ExitCode,
)
from .._utils import (
    ToronError,
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

    args.node.add_weight_group(
        name=args.weight,
        description=args.description,
        selectors=args.selectors,
        make_default=args.make_default or None,  # Use `None` instead of `False`.
    )

    msg = f'added index weight group {args.weight!r} to {args.node.path_hint}'
    applogger.info(msg)

    return ExitCode.OK


def add_attribute(args: argparse.Namespace) -> ExitCode:
    """Add attribute columns to the given node file."""
    process_backup_option(args)

    attribute_columns = args.node.get_registered_attributes()

    new_attributes = []
    for attr in args.attributes:
        if attr not in attribute_columns:
            new_attributes.append(attr)
        else:
            applogger.warning(f'skipping {attr!r} (already registered)')

    if new_attributes:
        try:
            args.node.set_registered_attributes(attribute_columns + new_attributes)
        except ValueError as e:
            raise ToronError(str(e))
        formatted_attrs = ', '.join(repr(x) for x in new_attributes)
        applogger.info(f'added attribute columns: {formatted_attrs}')
    else:
        applogger.info(f'no attributes added')

    return ExitCode.OK


def add_crosswalk(args: argparse.Namespace) -> ExitCode:
    """Add crosswalks between two node files."""
    process_backup_option(args, node_args=['node1', 'node2'])

    do_add = lambda tail, head, args: head.add_crosswalk(
        node=tail,
        crosswalk_name=args.crosswalk,
        other_filename_hint=tail.path_hint,
        description=args.description,
        selectors=args.selectors,
        is_default=args.make_default or None,  # Use `None` instead of `False`.
    )

    if args.direction == 'both':
        do_add(args.node1, args.node2, args)  # node1 -> node2
        do_add(args.node2, args.node1, args)  # node1 <- node2
    elif args.direction == 'right':
        do_add(args.node1, args.node2, args)  # node1 -> node2
    elif args.direction == 'left':
        do_add(args.node2, args.node1, args)  # node1 <- node2
    else:
        raise RuntimeError(f'unhandled direction: {args.direction!r}')

    return ExitCode.OK
