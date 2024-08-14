"""Graph implementation and functions for the Toron project."""
import sqlite3
from json import (
    dumps as _dumps,
    loads as _loads,
)
from itertools import (
    compress,
    groupby,
    product,
)
from ._typing import (
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeAlias,
    Union,
)

from ._utils import (
    TabularData,
    make_readerlike,
    NOVALUE,
    ToronWarning,
    BitFlags,
)
from .node import Node
from .mapper import Mapper
from ._xmapper import xMapper
from .xnode import xNode


NoValueType: TypeAlias = NOVALUE.__class__
Direction: TypeAlias = Literal['->', '-->', '<->', '<-->', '<-', '<--']


def load_mapping(
    left_node : Node,
    direction : Direction,
    right_node : Node,
    crosswalk_name: str,
    data: Union[Iterable[Sequence], Iterable[Dict]],
    columns: Optional[Sequence[str]] = None,
    selectors: Optional[Union[List[str], str]] = None,
    match_limit: int = 1,
    allow_overlapping: bool = False,
) -> None:
    """Use mapping data to build a crosswalk between two nodes."""
    mapper = Mapper(crosswalk_name, data, columns)
    mapper.match_records(left_node, 'left', match_limit, allow_overlapping)
    mapper.match_records(right_node, 'right', match_limit, allow_overlapping)

    if '->' in direction:
        right_node.add_crosswalk(
            other_unique_id=left_node.unique_id,
            other_filename_hint=None,
            name=crosswalk_name,
            selectors=selectors,
        )
        right_node.insert_relations2(
            node_reference=left_node,
            crosswalk_name=crosswalk_name,
            data=mapper.get_relations('->'),
            columns=['other_index_id', crosswalk_name, 'index_id', 'mapping_level'],
        )

    if '<-' in direction:
        left_node.add_crosswalk(
            other_unique_id=right_node.unique_id,
            other_filename_hint=None,
            name=crosswalk_name,
            selectors=selectors,
        )
        left_node.insert_relations2(
            node_reference=right_node,
            crosswalk_name=crosswalk_name,
            data=mapper.get_relations('<-'),
            columns=['other_index_id', crosswalk_name, 'index_id', 'mapping_level'],
        )


def xadd_edge(
    data : TabularData,
    name : str,
    left_node : xNode,
    direction : Direction,
    right_node : xNode,
    selectors: Union[Iterable[str], None, NoValueType] = NOVALUE,
    match_limit: Union[int, float] = 1,
    weight_name: Optional[str] = None,
    allow_overlapping: bool = False,
) -> None:
    mapper = xMapper(data, name)
    try:
        mapper.find_matches(left_node, 'left', match_limit, weight_name, allow_overlapping)
        mapper.find_matches(right_node, 'right', match_limit, weight_name, allow_overlapping)

        # NOTE: `type: ignore` comments added for refactoring--remove when finished.
        if '<' in direction:
            relations = mapper.get_relations('left')
            left_node._dal.add_incoming_edge(
                unique_id=right_node._dal.unique_id,
                name=name,
                relations=relations,  # type: ignore [arg-type]
                selectors=selectors,
                filename_hint=right_node._dal.data_source or NOVALUE,
            )

        if '>' in direction:
            relations = mapper.get_relations('right')
            right_node._dal.add_incoming_edge(
                unique_id=left_node._dal.unique_id,
                name=name,
                relations=relations,  # type: ignore [arg-type]
                selectors=selectors,
                filename_hint=left_node._dal.data_source or NOVALUE,
            )

    finally:
        mapper.close()
