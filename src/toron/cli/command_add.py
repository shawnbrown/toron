"""Implementation for "add" command."""
import argparse
import logging

from .common import (
    process_backup_option,
    ExitCode,
)


applogger = logging.getLogger('app-toron')


def add_labels(args: argparse.Namespace) -> ExitCode:
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


def add_crosswalk(args: argparse.Namespace) -> ExitCode:
    """Add crosswalks between two node files."""
    process_backup_option(args, node_args=['node1', 'node2'])

    if args.make_default:
        make_default = True
    else:
        make_default = None  # Use `None` instead of `False` for appropriate
                             # `add_crosswalk()` behavior.

    do_add = lambda tail, head, args: head.add_crosswalk(
        node=tail,
        crosswalk_name=args.crosswalk,
        other_filename_hint=tail.path_hint,
        description=args.description,
        selectors=args.selectors,
        is_default=make_default,
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
