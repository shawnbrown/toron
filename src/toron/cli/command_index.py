"""Implementation for "index" command."""
import argparse
import csv
import sys

from .common import ExitCode
from .. import bind_node
from ..graph import get_weights


def command(args: argparse.Namespace) -> ExitCode:
    """Stream node index in CSV format."""
    path = args.file
    try:
        node = bind_node(path, mode='ro')
    except Exception as err:
        import logging
        applogger = logging.getLogger('app-toron')
        applogger.error(str(err))
        return ExitCode.ERR  # <- EXIT!

    writer = csv.writer(sys.stdout, lineterminator='\n')

    weights = get_weights(node=node, weights=None, header=True)
    try:
        for row in weights:
            writer.writerow(row)
    except BrokenPipeError:
        pass  # Downstream process closed pipe early--no action needed.

    return ExitCode.OK
