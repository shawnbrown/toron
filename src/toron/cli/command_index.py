"""Implementation for "index" command."""
import argparse
import csv
import logging
import os
import re
import uuid
from itertools import chain, islice
from .._typing import Iterator, List

from .common import (
    ExitCode,
    csv_stdout_writer,
    process_backup_option,
    index_id_to_code,
    get_index_code_position,
    remap_index_codes_to_index_ids,
)


applogger = logging.getLogger('app-toron')


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
            pad_len = len(str(args.node.max_index_id))
            index_code = index_id_to_code(index_id, unique_id_bytes, pad_len)
            e_str = e_str.replace(match.group(0), f'index code {index_code}')

        msg = (f'{e_str}\n  load behavior can be changed using '
               f'--on-label-conflict and --on-weight-conflict')
        applogger.error(msg)
        return ExitCode.ERR

    return ExitCode.OK


def write_to_stdout(args: argparse.Namespace) -> ExitCode:
    """Print node index in CSV format to stdout stream."""
    node = args.node

    domain_value = node.domain
    unique_id_bytes = uuid.UUID(node.unique_id).bytes
    pad_len = len(str(node.max_index_id))

    with node._managed_cursor(n=2) as (cur1, cur2):
        index_repo = node._dal.IndexRepository(cur1)

        # Get groups and sort (start with default group then order by name).
        try:
            default_id = node._dal.PropertyRepository(cur1).get('default_weight_group_id')
        except KeyError:
            default_id = None
        groups = node._dal.WeightGroupRepository(cur1).get_all()
        groups = sorted(groups, key=lambda g: (g.id!=default_id, g.name))
        weight_group_names = [group.name for group in groups]
        weight_group_ids = [group.id for group in groups]

        # Define a helper function to get weight values (needs separate cursor).
        _get_weight_obj = node._dal.WeightRepository(cur2).get_by_weight_group_id_and_index_id
        def get_weight_value(group_id, index_id):
            try:
                return _get_weight_obj(group_id, index_id).value
            except KeyError:
                return 0.0 if index_id == 0 else None

        # Prepare domain text for header row.
        if domain_value:
            domain_value = domain_value.replace(' ', '_') + '_'

        row_count = 0
        with csv_stdout_writer(args.stdout) as writer:
            # Write header row.
            writer.writerow(chain(
                [f'{domain_value}index_code'],
                index_repo.get_label_names(),
                weight_group_names,
            ))

            # Write data rows.
            for index in index_repo.find_all():
                writer.writerow(chain(
                    [index_id_to_code(index.id, unique_id_bytes, pad_len)],
                    index.labels,
                    (get_weight_value(grp_id, index.id) for grp_id in weight_group_ids),
                ))
                row_count += 1

        applogger.info(f"written {row_count} record{'s' if row_count != 1 else ''}")

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
