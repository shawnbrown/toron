"""Implementation for "rename" command."""
import argparse
import logging

from .common import (
    cli_bind_node,
    process_backup_option,
    ExitCode,
)


applogger = logging.getLogger('app-toron')


def rename_label(args: argparse.Namespace) -> ExitCode:
    """Rename an index label column."""
    node = cli_bind_node(args.filepath, mode='rw')
    process_backup_option(args, node)

    node.rename_label_column(args.old_label, args.new_label)
    applogger.info(f'renamed label {args.old_label!r} -> {args.new_label!r}')

    return ExitCode.OK


def rename_domain(args: argparse.Namespace) -> ExitCode:
    """Change the given node's domain."""
    node = cli_bind_node(args.filepath, mode='rw')
    process_backup_option(args, node)

    node.set_domain(args.new_domain)
    applogger.info(f'domain set to {args.new_domain!r}')

    return ExitCode.OK
