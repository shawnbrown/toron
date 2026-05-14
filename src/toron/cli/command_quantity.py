"""Implementation for "quantity" command."""
import argparse
import csv
import logging
import os

from .common import (
    ExitCode,
    csv_stdout_writer,
    process_backup_option,
)


applogger = logging.getLogger('app-toron')


def read_from_stdin(args: argparse.Namespace) -> ExitCode:
    """Load quantity records read from stdin stream."""
    raise NotImplementedError


def write_to_stdout(args: argparse.Namespace) -> ExitCode:
    """Write quantity records to stdout stream."""
    raise NotImplementedError


def process_quantity_action(args: argparse.Namespace) -> ExitCode:
    """Write quantities to ``args.stdout`` or read from ``args.stdin``."""
    if args.stdin_is_streamed:
        process_backup_option(args)
        return read_from_stdin(args)
    else:
        try:
            return write_to_stdout(args)
        except BrokenPipeError:
            os._exit(ExitCode.OK)  # Downstream stopped early; exit with OK.
