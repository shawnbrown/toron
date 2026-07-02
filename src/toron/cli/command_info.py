"""Implementation for "info" command."""
import argparse
import sys
from pathlib import Path
from shutil import get_terminal_size

from .. import bind_node
from ..data_service import (
    get_registered_attributes,
    get_loaded_attributes,
    get_node_info_text,
)
from .common import (
    ExitCode,
    StyleCodes,
    cli_bind_node,
)


def write_to_stdout(args: argparse.Namespace) -> ExitCode:
    """Show information for Toron node file."""
    node = cli_bind_node(args.filepath, mode='ro')

    # Get dictionary of node info values.
    with node._managed_cursor() as cursor:
        property_repo = node._dal.PropertyRepository(cursor)
        attribute_repo = node._dal.AttributeGroupRepository(cursor)

        info_dict = get_node_info_text(
            property_repo=property_repo,
            index_repo=node._dal.IndexRepository(cursor),
            structure_repo=node._dal.StructureRepository(cursor),
            weight_group_repo=node._dal.WeightGroupRepository(cursor),
            attribute_repo=attribute_repo,
            crosswalk_repo=node._dal.LinkRepository(cursor),
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
    categories_formatted = '\n  '.join(info_dict['category_list'])
    crosswalks_str = '\n  '.join(info_dict['crosswalks_list'])

    # Prepare and write output.
    sys.stdout.write(
        f"{hr}\n{filename}\n{hr}\n"
        f"{bright}domain:{reset}\n"
        f"  {info_dict['domain_str']}\n"
        f"{bright}categories:{reset}\n"
        f"  {categories_formatted}\n"
        f"{bright}weights:{reset}\n"
        f"  {', '.join(info_dict['weights_list'])}\n"
        f"{bright}attributes:{reset}\n"
        f"  {', '.join(formatted_attributes) or 'None'}\n"
        f"{bright}incoming crosswalks:{reset}\n"
        f"  {crosswalks_str}\n"
    )
    return ExitCode.OK
