"""Implementation for "quantity" command."""
import argparse
import csv
import logging
import os
from .._typing import TYPE_CHECKING

from .common import (
    ExitCode,
    is_streamed,
    csv_stdout_writer,
    cli_bind_node,
    process_backup_option,
)

if TYPE_CHECKING:
    from .. import TopoNode


applogger = logging.getLogger('app-toron')


def read_from_stdin(args: argparse.Namespace, node: 'TopoNode') -> ExitCode:
    """Load quantity records read from stdin stream."""
    reader = csv.reader(args.stdin)

    node.insert_quantities2(
        value_column=args.value_column,
        data=reader,
        allow_invalid_label=args.allow_invalid_label,
        allow_invalid_category=args.allow_invalid_category,
        on_existing=args.on_existing,
    )

    return ExitCode.OK


def write_to_stdout(args: argparse.Namespace, node: 'TopoNode') -> ExitCode:
    """Write quantity records to stdout stream in CSV format."""
    row_count = 0
    with csv_stdout_writer(args.stdout) as writer:
        data = node.select_quantities2(header=True)

        header = next(data)
        writer.writerow(header)

        for row in data:
            writer.writerow(row)
            row_count += 1

    applogger.info(f"written {row_count} record{'s' if row_count != 1 else ''}")
    return ExitCode.OK


def process_quantity_action(args: argparse.Namespace) -> ExitCode:
    """Write quantities to ``args.stdout`` or read from ``args.stdin``."""
    if is_streamed(args.stdin):
        node = cli_bind_node(args.filepath, mode='rw')
        process_backup_option(args, node)
        return read_from_stdin(args, node)
    else:
        # Open in read-only mode and skip processing the backup option.
        node = cli_bind_node(args.filepath, mode='ro')
        try:
            return write_to_stdout(args, node)
        except BrokenPipeError:
            os._exit(ExitCode.OK)  # Downstream stopped early; exit with OK.
