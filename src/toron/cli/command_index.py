"""Implementation for "index" command."""
import argparse
import logging

from .common import ExitCode, csv_stdout_writer
from .. import bind_node
from ..graph import get_weights


applogger = logging.getLogger('app-toron')


def command(args: argparse.Namespace) -> ExitCode:
    """Stream node index in CSV format."""
    path = args.file
    try:
        node = bind_node(path, mode='ro')
    except Exception as err:
        applogger.error(str(err))
        return ExitCode.ERR  # <- EXIT!

    weights = get_weights(node=node, weights=None, header=True)

    row_count = 0
    with csv_stdout_writer() as writer:
        for row in weights:
            writer.writerow(row)
            row_count += 1

    applogger.info(f"written {row_count} row{'s' if row_count != 1 else ''}")

    return ExitCode.OK
