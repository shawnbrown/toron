"""Implementation for "index" command."""
import argparse
import csv
import logging
import sys
from .._typing import Optional, TextIO

from .common import ExitCode, csv_stdout_writer
from .. import bind_node
from ..graph import get_weights


applogger = logging.getLogger('app-toron')


def write_to_stdout(
    args: argparse.Namespace,
    *,
    stdout: Optional[TextIO] = None,
) -> ExitCode:
    """Print node index in CSV format to stdout stream."""
    if stdout is None:
        stdout = sys.stdout

    weights = get_weights(node=args.node, weights=None, header=True)

    row_count = 0
    with csv_stdout_writer(stdout) as writer:
        header = next(weights)
        writer.writerow(header)

        for row in weights:
            writer.writerow(row)
            row_count += 1

    applogger.info(f"written {row_count} record{'s' if row_count != 1 else ''}")

    return ExitCode.OK


def read_from_stdin(
    args: argparse.Namespace,
    *,
    stdin: Optional[TextIO] = None,
) -> ExitCode:
    """Insert index records read from stdin stream."""
    if stdin is None:
        stdin = sys.stdin
    reader = csv.reader(stdin)
    args.node.insert_index(reader)

    return ExitCode.OK
