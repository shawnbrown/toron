"""Implementation for "index" command."""
import argparse
import csv
import sys

from .common import ExitCode, csv_stdout_writer
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

    weights = get_weights(node=node, weights=None, header=True)

    with csv_stdout_writer() as writer:
        for row in weights:
            writer.writerow(row)

    return ExitCode.OK
