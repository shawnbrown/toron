"""Implementation for "index" command."""
import argparse
import csv
import logging
import os
import sys
from .._typing import Optional, TextIO

from .common import (
    ExitCode,
    csv_stdout_writer,
    process_backup_option,
)
from .. import bind_node
from ..graph import get_weights


applogger = logging.getLogger('app-toron')


def write_to_stdout(args: argparse.Namespace) -> ExitCode:
    """Print node index in CSV format to stdout stream."""
    weights = get_weights(node=args.node, weights=None, header=True)

    row_count = 0
    with csv_stdout_writer(args.stdout) as writer:
        header = next(weights)
        writer.writerow(header)

        for row in weights:
            writer.writerow(row)
            row_count += 1

    applogger.info(f"written {row_count} record{'s' if row_count != 1 else ''}")

    return ExitCode.OK


def read_from_stdin(args: argparse.Namespace) -> ExitCode:
    """Insert index records read from stdin stream."""
    reader = csv.reader(args.stdin)
    try:
        args.node.insert_index(
            reader,
            on_label_conflict=args.on_label_conflict,
            on_weight_conflict=args.on_weight_conflict,
        )
    except ValueError as e:
        msg = (f'{e}\n  load behavior can be changed using '
               f'--on-label-conflict and --on-weight-conflict')
        applogger.error(msg)
        return ExitCode.ERR

    return ExitCode.OK


def process_index_action(args: argparse.Namespace) -> ExitCode:
    """Write index to ``args.stdout`` or read from ``args.stdin``."""
    if args.stdin_is_streamed:
        process_backup_option(args)
        return read_from_stdin(args)
    else:
        try:
            return write_to_stdout(args)
        except BrokenPipeError:
            os._exit(ExitCode.OK)  # Downstream stopped early; exit with OK.
