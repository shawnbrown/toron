"""Implementation for "weight" command."""
import argparse
import logging
from .._typing import TYPE_CHECKING, cast
from .common import (
    cli_bind_file,
    process_backup_option,
    ExitCode,
)

if TYPE_CHECKING:
    from ..data_models import WeightGroup


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


def update(args: argparse.Namespace) -> ExitCode:
    """Update index weight group in the given data-space file."""
    ds = cli_bind_file(args.filepath, mode='rw')
    process_backup_option(args, ds)

    group = ds.get_weight_group(args.name)
    if not group:
        applogger.error(f'no weight group named {args.name!r}')
        return ExitCode.ERR

    if args.description:
        ds.edit_weight_group(args.name, description=args.description)
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
        selectors = [' '.join(sel.splitlines()) for sel in selectors]  # Remove newlines.
        ds.edit_weight_group(args.name, selectors=selectors)

    if args.make_default:
        group = cast('WeightGroup', ds.get_weight_group(args.name))
        ds.set_default_weight_group(group)  # Change default WeightGroup.
        applogger.info(f'set weight {args.name!r} as the default')

    return ExitCode.OK
