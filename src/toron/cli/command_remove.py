"""Implementation for "remove" command."""
import argparse
import logging

from .common import (
    cli_bind_file,
    process_backup_option,
    ExitCode,
)
from .._utils import (
    ToronError,
)


applogger = logging.getLogger('app-toron')


def remove_link(args: argparse.Namespace) -> ExitCode:
    """Remove a link from between two files."""
    node1 = cli_bind_file(args.filepath, mode='rw')
    node2 = cli_bind_file(args.filepath2, mode='rw')
    process_backup_option(args, node1, node2)

    do_remove = lambda tail, head, link_name: head.drop_link(tail, link_name)

    if args.direction == 'both':
        try:  # Remove left-side mapping.
            do_remove(node2, node1, args.link)  # node1 <- node2
            left_link_removed = True
        except ToronError:
            left_link_removed = False

        try:  # Remove right-side mapping.
            do_remove(node1, node2, args.link)  # node1 -> node2
            right_link_removed = True
        except ToronError:
            right_link_removed = False

        # Write action to applogger or raise error.
        if left_link_removed and right_link_removed:
            applogger.info(f'removed {args.link!r} link from FILE1 and FILE2')
        elif left_link_removed and not right_link_removed:
            applogger.info(f'removed {args.link!r} link from FILE1')
            applogger.info(f'no {args.link!r} link found in FILE2')
        elif not left_link_removed and right_link_removed:
            applogger.info(f'no {args.link!r} link found in FILE1')
            applogger.info(f'removed {args.link!r} link from FILE2')
        else:
            raise ToronError(f'no {args.link!r} link in FILE1 or FILE2')

    elif args.direction == 'left':
        try:
            do_remove(node2, node1, args.link)  # node1 <- node2
            applogger.info(f'removed {args.link!r} link from FILE1')
        except ToronError:
            raise ToronError(f'no {args.link!r} link found in FILE1')

    elif args.direction == 'right':
        try:
            do_remove(node1, node2, args.link)  # node1 -> node2
            applogger.info(f'removed {args.link!r} link from FILE2')
        except ToronError:
            raise ToronError(f'no {args.link!r} link found in FILE2')

    else:
        raise RuntimeError(f'unhandled direction: {args.direction!r}')

    return ExitCode.OK
