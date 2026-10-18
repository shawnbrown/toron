"""Implementation for "quantity" command."""
import argparse
import csv
import logging
import os
from contextlib import AbstractContextManager, nullcontext
from .._typing import (
    IO,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Union,
    TYPE_CHECKING,
    cast,
)

from .common import (
    ExitCode,
    is_streamed,
    csv_stdout_writer,
    open_file_for_writing,
    open_stream_for_writing,
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
    ignore_missing_column: Optional[List[str]],
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
            ignore_missing_columns=ignore_missing_column,
            on_existing=on_existing,
        )
    except ValueError as err:
        with ds._managed_cursor() as (cur):
            index_repo = ds._dal.IndexRepository(cur)
            cardinality = index_repo.get_cardinality(include_undefined=False)

        if cardinality == 0:
            raise ToronError('operation cancelled, file contains no index records')
        else:
            raise ToronError(err)

    return ExitCode.OK


def import_records(args: argparse.Namespace) -> ExitCode:
    """Load quantity records from source CSV file."""
    file_cm: AbstractContextManager[IO[str]]
    if args.source == '-':
        file_cm = nullcontext(args.stdin)
    else:
        file_cm = open(args.source)

    with file_cm as f_source:
        reader = csv.reader(f_source)

        ds = cli_bind_file(args.filepath, mode='rw')
        process_backup_option(args, ds)

        return _import_records(
            ds=ds,
            reader=reader,
            value_column=args.value_column,
            ignore_missing_column=args.ignore_missing_column,
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

    f: IO[str]
    if args.target == '-':
        try:
            # Write to stdout.
            with open_stream_for_writing(args.stdout) as f:
                writer = csv.writer(f, lineterminator='\n')
                for row in _export_records(ds):
                    writer.writerow(row)
        except BrokenPipeError:
            os._exit(ExitCode.OK)  # Downstream stopped early; exit with OK.
    else:
        # Write to file.
        with open_file_for_writing(
            target_path=args.target,
            autoname_parts=['quantity', args.filepath],
            autoname_ext='csv',
            overwrite=args.force,
        ) as f:
            writer = csv.writer(f, lineterminator='\n')
            for row in _export_records(ds):
                writer.writerow(row)

        applogger.info(f'saved to {f.name!r}')

    return ExitCode.OK
