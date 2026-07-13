"""Implementation for "update" command."""
import argparse
import logging
from dataclasses import replace
from .._typing import cast, TYPE_CHECKING

from .common import (
    cli_bind_node,
    process_backup_option,
    ExitCode,
)

if TYPE_CHECKING:
    from toron.data_models import WeightGroup


applogger = logging.getLogger('app-toron')


def update_label(args: argparse.Namespace) -> ExitCode:
    """Update index label column in the given node file."""
    node = cli_bind_node(args.filepath, mode='rw')
    process_backup_option(args, node)

    if args.move_left and not args.move_right:
        node.change_label_order(args.label, offset=-args.move_left)
        applogger.info(f'moved label {args.label!r} to the left')
    elif args.move_right and not args.move_left:
        node.change_label_order(args.label, offset=args.move_right)
        applogger.info(f'moved label {args.label!r} to the right')
    else:
        raise Exception

    return ExitCode.OK


def update_weight(args: argparse.Namespace) -> ExitCode:
    """Update index weight group in the given node file."""
    node = cli_bind_node(args.filepath, mode='rw')
    process_backup_option(args, node)

    group = node.get_weight_group(args.weight)
    if not group:
        applogger.error(f'no weight group named {args.weight!r}')
        return ExitCode.ERR

    if args.description:
        node.edit_weight_group(args.weight, description=args.description)
        applogger.info(f'changed description: {args.description!r}')

    if args.add_selector or args.remove_selector:
        selectors = list(group.selectors) if group.selectors else []
        if args.add_selector:
            selectors.extend(args.add_selector)
            applogger.info(f"added selectors: "
                           f"{', '.join(repr(x) for x in args.add_selector)}")

        if args.remove_selector:
            for sel in args.remove_selector:
                if sel in selectors:
                    selectors.remove(sel)
            applogger.info(f"removed selectors: "
                           f"{', '.join(repr(x) for x in args.remove_selector)}")

        selectors = sorted(set(selectors))  # Should be unique.

        # Remove any line-breaks in selector text.
        func = lambda x: x.replace('\n', ' ').replace('\r\n', ' ')
        selectors = [func(sel) for sel in selectors]

        node.edit_weight_group(args.weight, selectors=selectors)

    if args.make_default:
        group = cast('WeightGroup', node.get_weight_group(args.weight))
        node.set_default_weight_group(group)  # Change default WeightGroup.
        applogger.info(f'set weight {args.weight!r} as the default')

    return ExitCode.OK
