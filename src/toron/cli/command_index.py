"""Implementation for "index" command."""
import argparse
import logging

from .common import ExitCode, csv_stdout_writer
from .. import bind_node
from ..graph import get_weights


applogger = logging.getLogger('app-toron')


def command(args: argparse.Namespace) -> ExitCode:
    """Stream node index in CSV format."""
    weights = get_weights(node=args.file, weights=None, header=True)

    row_count = 0
    with csv_stdout_writer() as writer:
        header = next(weights)
        writer.writerow(header)

        for row in weights:
            writer.writerow(row)
            row_count += 1

    applogger.info(f"written {row_count} record{'s' if row_count != 1 else ''}")

    return ExitCode.OK
