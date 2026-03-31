"""Implementation for "index" command."""
import argparse
import csv
import logging
import os
import re
import sys
import uuid
from itertools import chain, islice
from .._typing import Iterator, List, Optional, TextIO

from .common import (
    ExitCode,
    csv_stdout_writer,
    process_backup_option,
    index_id_to_code,
    get_index_code_position,
    remap_index_codes_to_index_ids,
)
from .. import bind_node
from ..graph import get_weights


applogger = logging.getLogger('app-toron')


def write_to_stdout(args: argparse.Namespace) -> ExitCode:
    """Print node index in CSV format to stdout stream."""
    weights = get_weights(node=args.node, weights=None, header=True)
    weights_header = next(weights)

    domain_formatted = args.node.domain.get('domain', '')
    if domain_formatted:
        domain_formatted = domain_formatted.replace(' ', '_') + '_'
    header = [f'{domain_formatted}index_code'] + weights_header[1:]

    unique_id_bytes = uuid.UUID(args.node.unique_id).bytes

    row_count = 0
    with csv_stdout_writer(args.stdout) as writer:
        writer.writerow(header)

        for index_id, *row in weights:
            index_code = index_id_to_code(index_id, unique_id_bytes)
            writer.writerow([index_code] + row)
            row_count += 1

    applogger.info(f"written {row_count} record{'s' if row_count != 1 else ''}")

    return ExitCode.OK


def read_from_stdin(args: argparse.Namespace) -> ExitCode:
    """Insert index records read from stdin stream."""
    reader = csv.reader(args.stdin)
    sample_rows = list(islice(reader, 10))
    iterator: Iterator[List] = chain(sample_rows, reader)

    unique_id_bytes = uuid.UUID(args.node.unique_id).bytes
    try:
        position = get_index_code_position(sample_rows, unique_id_bytes)
        iterator = remap_index_codes_to_index_ids(iterator, unique_id_bytes, position)
    except RuntimeError as e:
        # If raw 'index_id' is given (instead of index code), raise error.
        # But if no index is given at all, continue (for loading new index
        # records).
        header = sample_rows[0]
        if 'index_id' in header:
            msg = f"{e}; found unexpected column 'index_id'"
            raise RuntimeError(msg) from None

    try:
        args.node.insert_index(
            iterator,
            on_label_conflict=args.on_label_conflict,
            on_weight_conflict=args.on_weight_conflict,
        )
    except ValueError as e:
        e_str = str(e)
        match = re.search(r'index_id (\d+)\b', e_str)
        if match:
            # Replace index_id with index code in error message.
            index_id = int(match.group(1))
            index_code = index_id_to_code(index_id, unique_id_bytes)
            e_str = e_str.replace(match.group(0), f'index code {index_code}')

        msg = (f'{e_str}\n  load behavior can be changed using '
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
