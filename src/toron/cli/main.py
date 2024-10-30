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
    Node,
)
from .loggerconfig import configure_applogger


parser = argparse.ArgumentParser(
    prog='toron',
    epilog=f'Version: Toron {__version__}',
)
parser.add_argument('path', help='path to file')


applogger = logging.getLogger('app-toron')
configure_applogger(applogger)


EXITCODE_OK: Final[int] = 0
EXITCODE_ERR: Final[int] = 1


def main() -> int:
    args = parser.parse_args()

    if Path(args.path).is_file():
        path = args.path
    elif Path(f'{args.path}.toron').is_file():
        path = f'{args.path}.toron'
    else:
        applogger.error(f'file not found {args.path!r}')
        return EXITCODE_ERR  # <- EXIT!

    filename = Path(path).name  # File only, no parent directory text.

    try:
        node = Node.from_file(path)
    except Exception as err:
        applogger.error(str(err))
        return EXITCODE_ERR  # <- EXIT!

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

    # ANSI style codes.
    bright = '\33[1m'
    reset = '\33[0m'

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
    return EXITCODE_OK
