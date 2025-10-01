"""Main command line application function."""

import argparse
import logging
import sys
from pathlib import Path
from shutil import get_terminal_size
from .._typing import (
    Final,
)
from ..data_service import get_node_info_text
from .. import (
    __version__,
    bind_node,
)
from .common import configure_applogger, stdout_styles, ExitCode


def get_parser() -> argparse.ArgumentParser:
    """Get argument parser for Toron command line interface."""
    parser = argparse.ArgumentParser(
        prog='toron',
        description='Show and edit Toron node file properties.',
        epilog=f'Version: Toron {__version__}',
    )
    parser.add_argument('path', help='path to file', metavar='PATH')
    return parser


applogger = logging.getLogger('app-toron')
configure_applogger(applogger)


def main() -> ExitCode:
    parser = get_parser()
    args = parser.parse_args()

    if Path(args.path).is_file():
        path = args.path
    elif Path(f'{args.path}.toron').is_file():
        path = f'{args.path}.toron'
    else:
        applogger.error(f'file not found {args.path!r}')
        return ExitCode.ERR  # <- EXIT!

    filename = Path(path).name  # File only, no parent directory text.

    try:
        node = bind_node(path, mode='ro')
    except Exception as err:
        applogger.error(str(err))
        return ExitCode.ERR  # <- EXIT!

    # Define horizontal rule `hr` made from "Box Drawings" character.
    hr = '─' * min(len(filename), (get_terminal_size()[0] - 1))

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
    bright = stdout_styles.bright
    reset = stdout_styles.reset

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
