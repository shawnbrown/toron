"""Implementation for "index" command."""
import argparse
import csv
import logging
import sys

from .common import ExitCode, csv_stdout_writer
from .. import bind_node
from ..graph import get_weights


applogger = logging.getLogger('app-toron')


def write_to_stdout(args: argparse.Namespace) -> ExitCode:
    """Print node index in CSV format to stdout stream."""
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


def read_from_stdin(args: argparse.Namespace) -> ExitCode:
    """Insert index records read from stdin stream."""
    reader = csv.reader(sys.stdin)
    args.file.insert_index(reader)

    return ExitCode.OK
