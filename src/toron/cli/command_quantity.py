"""Implementation for "quantity" command."""
import argparse
import csv
import logging
import os
from .._typing import (
    Iterator,
    List,
    Literal,
    Sequence,
    Union,
    TYPE_CHECKING,
    cast,
)

from .common import (
    ExitCode,
    is_streamed,
    csv_stdout_writer,
    open_target_file,
    cli_bind_file,
    process_backup_option,
)
from .._utils import (
    ToronError,
)

if TYPE_CHECKING:
    from .. import DataSpace


applogger = logging.getLogger('app-toron')


def _import_records(
    ds: 'DataSpace',
    reader: Iterator[Sequence[Union[str, float]]],
    value_column: str,
    allow_invalid_label: bool,
    allow_invalid_partition: bool,
    on_existing: Literal['abort', 'sum', 'replace', 'ignore'],
) -> ExitCode:
    """Load quantity records from `csv.reader`-like object."""
    try:
        ds.insert_quantities2(
            value_column=value_column,
            data=reader,
            allow_invalid_label=allow_invalid_label,
            allow_invalid_partition=allow_invalid_partition,
            on_existing=on_existing,
        )
    except ValueError as err:
        with ds._managed_cursor() as (cur):
            index_repo = ds._dal.IndexRepository(cur)
            cardinality = index_repo.get_cardinality(include_undefined=False)

        if cardinality == 0:
            raise ToronError('operation cancelled, file contains no index records')
        else:
            raise  # Or else use original error as-is.

    return ExitCode.OK


def import_records(args: argparse.Namespace) -> ExitCode:
    """Load quantity records from source CSV file."""
    with open(args.source) as f_source:
        reader = csv.reader(f_source)

        ds = cli_bind_file(args.filepath, mode='rw')
        process_backup_option(args, ds)

        return _import_records(
            ds=ds,
            reader=reader,
            value_column=args.value_column,
            allow_invalid_label=args.allow_invalid_label,
            allow_invalid_partition=args.allow_invalid_partition,
            on_existing=args.on_existing,
        )


def _export_records(ds: 'DataSpace') -> Iterator[List[Union[str, float]]]:
    """Yield quantity record rows."""
    data = ds.select_quantities2(header=True)

    # Cast type until return hint for `select_quantities2()` is improved.
    # TODO: Update `select_quantities2()` to use better type hint.
    data = cast(Iterator[List[Union[str, float]]], data)

    header = next(data)
    yield header

    row_count = 0
    for row in data:
        yield row
        row_count += 1

    applogger.info(f"written {row_count} record{'s' if row_count != 1 else ''}")


def export_records(args: argparse.Namespace) -> ExitCode:
    """Write quantity records to target CSV file."""
    # Bind DataSpace (to make sure it exists) before opening output file.
    ds = cli_bind_file(args.filepath, mode='ro')

    with open_target_file(
        src_path=args.filepath,
        trg_path=args.target,
        auto_prefix='quantity-',
        force=args.force,
    ) as f:
        writer = csv.writer(f, lineterminator='\n')
        for row in _export_records(ds):
            writer.writerow(row)

    applogger.info(f'saved to {f.name!r}')
    return ExitCode.OK


def read_from_stdin(args: argparse.Namespace, node: 'DataSpace') -> ExitCode:
    """Load quantity records read from stdin stream."""
    reader = csv.reader(args.stdin)

    try:
        node.insert_quantities2(
            value_column=args.value_column,
            data=reader,
            allow_invalid_label=args.allow_invalid_label,
            allow_invalid_partition=args.allow_invalid_partition,
            on_existing=args.on_existing,
        )
    except ValueError as err:
        with node._managed_cursor() as (cur):
            index_repo = node._dal.IndexRepository(cur)
            cardinality = index_repo.get_cardinality(include_undefined=False)

        if cardinality == 0:
            msg = 'operation cancelled, file contains no index records'
        else:
            msg = f'operation cancelled, {err}'

        applogger.error(msg)
        return ExitCode.ERR

    return ExitCode.OK


def write_to_stdout(args: argparse.Namespace, node: 'DataSpace') -> ExitCode:
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
        node = cli_bind_file(args.filepath, mode='rw')
        process_backup_option(args, node)
        return read_from_stdin(args, node)
    else:
        # Open in read-only mode and skip processing the backup option.
        node = cli_bind_file(args.filepath, mode='ro')
        try:
            return write_to_stdout(args, node)
        except BrokenPipeError:
            os._exit(ExitCode.OK)  # Downstream stopped early; exit with OK.
