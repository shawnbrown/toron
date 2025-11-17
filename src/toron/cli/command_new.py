"""Implementation for "new" command."""
import argparse
import logging
import os

from .common import ExitCode
from .. import TopoNode


applogger = logging.getLogger('app-toron')


def create_file(args: argparse.Namespace) -> ExitCode:
    """Create a new TopoNode and save it to the given 'node_path'."""
    if os.path.exists(args.node_path):
        applogger.error(f'cancelled: {args.node_path!r} already exists')
        return ExitCode.ERR

    if not os.path.basename(args.node_path).strip():
        applogger.error(f'filename cannot be whitespace')
        return ExitCode.ERR

    node = TopoNode()
    try:
        node.to_file(args.node_path)
    except OSError as e:
        applogger.error(f'cancelled: {e}')
        return ExitCode.ERR

    applogger.info(f'created file: {args.node_path!r}')
    return ExitCode.OK
