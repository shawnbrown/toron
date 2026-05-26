"""Implementation for "quantity" command."""
import argparse
import csv
import logging
import os

from .common import (
    ExitCode,
    is_streamed,
    csv_stdout_writer,
    process_backup_option,
)


applogger = logging.getLogger('app-toron')


def read_from_stdin(args: argparse.Namespace) -> ExitCode:
    """Load quantity records read from stdin stream."""
    reader = csv.reader(args.stdin)

    args.node.insert_quantities2(
        value_column=args.value_column,
        data=reader,
        allow_invalid_label=args.allow_invalid_label,
        allow_invalid_category=args.allow_invalid_category,
        on_existing=args.on_existing,
    )

    return ExitCode.OK


def write_to_stdout(args: argparse.Namespace) -> ExitCode:
    """Write quantity records to stdout stream in CSV format."""
    node = args.node
    domain_value = node.domain

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
        process_backup_option(args)
        return read_from_stdin(args)
    else:
        try:
            return write_to_stdout(args)
        except BrokenPipeError:
            os._exit(ExitCode.OK)  # Downstream stopped early; exit with OK.
