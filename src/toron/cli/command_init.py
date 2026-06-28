"""Implementation for "init" command."""
import argparse
import logging
import os

from .common import ExitCode
from .. import TopoNode


applogger = logging.getLogger('app-toron')


def create_file(args: argparse.Namespace) -> ExitCode:
    """Create a new TopoNode and save it to the given 'filepath'."""
    if not os.path.basename(args.filepath).strip():        # Must first check for
        applogger.error(f'filename cannot be whitespace')  # whitespace for proper
        return ExitCode.ERR                                # behavior on Windows.

    if os.path.exists(args.filepath):
        applogger.error(f'cancelled: {args.filepath!r} already exists')
        return ExitCode.ERR

    node = TopoNode()

    if args.domain:
        node.set_domain(args.domain)
    else:
        node.set_domain(os.path.splitext(os.path.basename(args.filepath))[0])

    try:
        node.to_file(args.filepath)
    except OSError as e:
        applogger.error(f'cancelled: {e}')
        return ExitCode.ERR

    applogger.info(f'created file {args.filepath!r}')
    if args.domain is None:
        applogger.info(f'domain set to {node.domain!r}')
    return ExitCode.OK
