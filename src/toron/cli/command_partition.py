"""Implementation for "partition" command."""
import argparse
import logging

from .common import (
    cli_bind_file,
    process_backup_option,
    normalize_arg_list,
    ExitCode,
)
from .._utils import ToronError


applogger = logging.getLogger('app-toron')


def add(args: argparse.Namespace) -> ExitCode:
    """Add a partition definition to the given data-space file."""
    ds = cli_bind_file(args.filepath, mode='rw')
    process_backup_option(args, ds)

    normalized = normalize_arg_list(args.names)
    try:
        ds.add_partition_definition(set(normalized))
    except (ValueError, RuntimeError) as e:
        raise ToronError(str(e))

    applogger.info(f"added partition definition: "
                   f"{{{', '.join(repr(x) for x in normalized)}}}")
    return ExitCode.OK
