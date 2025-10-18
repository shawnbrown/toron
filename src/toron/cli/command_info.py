"""Implementation for "info" command."""
import argparse
import sys
from pathlib import Path
from shutil import get_terminal_size

from .. import bind_node
from ..data_service import get_node_info_text
from .common import ExitCode, StyleCodes


def print_info(
    args: argparse.Namespace, stdout_style: StyleCodes
) -> ExitCode:
    """Show information for Toron node file."""
    node = args.file

    # Get dictionary of node info values.
    with node._managed_cursor() as cursor:
        info_dict = get_node_info_text(
            property_repo=node._dal.PropertyRepository(cursor),
            column_manager=node._dal.ColumnManager(cursor),
            structure_repo=node._dal.StructureRepository(cursor),
            weight_group_repo=node._dal.WeightGroupRepository(cursor),
            attribute_repo=node._dal.AttributeGroupRepository(cursor),
            crosswalk_repo=node._dal.CrosswalkRepository(cursor),
        )

    # Define short alias for style values (used in f-string).
    bright = stdout_style.bright
    reset = stdout_style.reset

    # Get file name only, no parent directory text.
    filename = Path(node.path_hint).name

    # Define horizontal rule `hr` made from "Box Drawings" character.
    hr = '─' * min(len(filename), (get_terminal_size()[0] - 1))

    # Prepare and write output.
    domain_str = '\n  '.join(info_dict['domain_list'])
    crosswalks_str = '\n  '.join(info_dict['crosswalks_list'])
    sys.stdout.write(
        f"{hr}\n{filename}\n{hr}\n"
        f"{bright}domain:{reset}\n"
        f"  {domain_str}\n"
        f"{bright}index:{reset}\n"
        f"  {', '.join(info_dict['index_list'])}\n"
        f"{bright}granularity:{reset}\n"
        f"  {info_dict['granularity_str']}\n"
        f"{bright}weights:{reset}\n"
        f"  {', '.join(info_dict['weights_list'])}\n"
        f"{bright}attributes:{reset}\n"
        f"  {', '.join(info_dict['attribute_list'])}\n"
        f"{bright}incoming crosswalks:{reset}\n"
        f"  {crosswalks_str}\n"
    )
    return ExitCode.OK
