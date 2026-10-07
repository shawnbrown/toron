"""Implementation for "info" command."""
import argparse
import sys
from pathlib import Path
from shutil import get_terminal_size

from .. import bind_file
from ..data_service import (
    get_registered_attributes,
    get_loaded_attributes,
    get_dataspace_info_text,
)
from .common import (
    ExitCode,
    StyleCodes,
    cli_bind_file,
)


def write_to_stdout(args: argparse.Namespace) -> ExitCode:
    """Show information for Toron data-space file."""
    # Check if there are info options to be written.
    updatable_info = ['set_domain']
    info_to_update = {}
    for option in updatable_info:
        value = getattr(args, option, None)
        if value is not None:
            info_to_update[option] = value

    # Open file and make updates if needed.
    if info_to_update:
        ds = cli_bind_file(args.filepath, mode='rw')  # Read-write mode.
        if 'set_domain' in info_to_update:
            ds.set_domain(info_to_update['set_domain'])
    else:
        ds = cli_bind_file(args.filepath, mode='ro')  # Read-only mode.

    # Get dictionary of DataSpace info values.
    with ds._managed_cursor() as cursor:
        property_repo = ds._dal.PropertyRepository(cursor)
        attribute_repo = ds._dal.AttributeGroupRepository(cursor)

        info_dict = get_dataspace_info_text(
            property_repo=property_repo,
            index_repo=ds._dal.IndexRepository(cursor),
            structure_repo=ds._dal.StructureRepository(cursor),
            weight_group_repo=ds._dal.WeightGroupRepository(cursor),
            attribute_repo=attribute_repo,
            link_repo=ds._dal.LinkRepository(cursor),
        )
        registered_attributes = get_registered_attributes(property_repo)
        loaded_attributes = \
            set(get_loaded_attributes(registered_attributes, attribute_repo))

    # Define short alias for style values (used in f-string).
    bright = args.stdout_style.bright
    dim = args.stdout_style.dim
    reset = args.stdout_style.reset

    # Get file name only, no parent directory text.
    filename = Path(args.filepath).name

    # Define horizontal rule `hr` made from "Box Drawings" character.
    hr = '─' * min(len(filename), (get_terminal_size()[0] - 1))

    # Format attributes (using dim style for unloaded ones).
    formatted_attributes = [
        attr if (attr in loaded_attributes) else f'{dim}{attr}{reset}'
        for attr in registered_attributes
    ]

    # When dropping support for Python 3.11, move these into f-string.
    partitions_formatted = '\n  '.join(info_dict['partition_list'])
    links_str = '\n  '.join(info_dict['links_list'])

    # Prepare and write output.
    sys.stdout.write(
        f"{hr}\n{filename}\n{hr}\n"
        f"{bright}domain:{reset}\n"
        f"  {info_dict['domain_str']}\n"
        f"{bright}partitions:{reset}\n"
        f"  {partitions_formatted}\n"
        f"{bright}weights:{reset}\n"
        f"  {', '.join(info_dict['weights_list'])}\n"
        f"{bright}attributes:{reset}\n"
        f"  {', '.join(formatted_attributes) or 'None'}\n"
        f"{bright}incoming links:{reset}\n"
        f"  {links_str}\n"
        f"{bright}created:{reset}\n"
        f"  {info_dict['created_date']}\n"
    )
    return ExitCode.OK
