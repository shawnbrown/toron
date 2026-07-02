"""Implementation for "add" command."""
import argparse
import logging

from .common import (
    cli_bind_node,
    process_backup_option,
    normalize_arg_list,
    ExitCode,
)
from .._utils import (
    ToronError,
)


applogger = logging.getLogger('app-toron')


def add_label(args: argparse.Namespace) -> ExitCode:
    """Add index label columns to the given node file."""
    node = cli_bind_node(args.filepath, mode='rw')
    process_backup_option(args, node)
    normalized = normalize_arg_list(args.labels)

    node.add_index_columns(*normalized)

    formatted_labels = ', '.join(repr(x) for x in normalized)
    applogger.info(f'added index label columns: {formatted_labels}')

    return ExitCode.OK


def add_weight(args: argparse.Namespace) -> ExitCode:
    """Add index weight groups to the given node file."""
    node = cli_bind_node(args.filepath, mode='rw')
    process_backup_option(args, node)

    node.add_weight_group(
        name=args.weight,
        description=args.description,
        selectors=args.selectors,
        make_default=args.make_default or None,  # Use `None` instead of `False`.
    )

    msg = f'added index weight group {args.weight!r} to {node.path_hint}'
    applogger.info(msg)

    return ExitCode.OK


def add_category(args: argparse.Namespace) -> ExitCode:
    """Add a discrete category to an existing node file."""
    node = cli_bind_node(args.filepath, mode='rw')
    process_backup_option(args, node)

    normalized = normalize_arg_list(args.labels)
    try:
        node.add_discrete_category(set(normalized))
    except (ValueError, RuntimeError) as e:
        raise ToronError(str(e))

    return ExitCode.OK


def add_attribute(args: argparse.Namespace) -> ExitCode:
    """Add attribute columns to the given node file."""
    node = cli_bind_node(args.filepath, mode='rw')
    process_backup_option(args, node)

    attribute_columns = node.get_registered_attributes()

    new_attributes = []
    for attr in normalize_arg_list(args.attributes):
        if attr not in attribute_columns:
            new_attributes.append(attr)
        else:
            applogger.warning(f'skipping {attr!r} (already registered)')

    if new_attributes:
        try:
            node.set_registered_attributes(attribute_columns + new_attributes)
        except ValueError as e:
            raise ToronError(str(e))
        formatted_attrs = ', '.join(repr(x) for x in new_attributes)
        applogger.info(f'added attribute columns: {formatted_attrs}')
    else:
        applogger.info(f'no attributes added')

    return ExitCode.OK


def add_link(args: argparse.Namespace) -> ExitCode:
    """Add link between two node files."""
    node1 = cli_bind_node(args.filepath, mode='rw')
    node2 = cli_bind_node(args.filepath2, mode='rw')
    process_backup_option(args, node1, node2)

    do_add = lambda tail, head, args: head.add_link(
        node=tail,
        link_name=args.link,
        other_filename_hint=tail.path_hint,
        description=args.description,
        selectors=args.selectors,
        is_default=args.make_default or None,  # Use `None` instead of `False`.
    )

    if args.direction == 'both':
        do_add(node1, node2, args)  # node1 -> node2
        do_add(node2, node1, args)  # node1 <- node2
    elif args.direction == 'right':
        do_add(node1, node2, args)  # node1 -> node2
    elif args.direction == 'left':
        do_add(node2, node1, args)  # node1 <- node2
    else:
        raise RuntimeError(f'unhandled direction: {args.direction!r}')

    return ExitCode.OK
