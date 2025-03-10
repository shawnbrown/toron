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
    Generator,
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
    check_type,
    TabularData,
    make_readerlike,
    normalize_tabular,
    NOVALUE,
    ToronWarning,
    BitFlags,
)
from .data_models import (
    Index,
    AttributesDict,
    QuantityIterator,
)
from .data_service import (
    find_crosswalks_by_ref,
)
from .node import TopoNode
from .mapper import (
    find_relation_value_index,
    Mapper,
)
from .selectors import (
    parse_selector,
    get_greatest_unique_specificity,
)
from ._xmapper import xMapper
from .xnode import xNode


NoValueType: TypeAlias = NOVALUE.__class__
Direction: TypeAlias = Literal['->', '-->', '<->', '<-->', '<-', '<--']


def normalize_mapping_data(
    data: Iterator[Sequence],
    columns: Sequence[str],
    crosswalk_name: str,
    left_domain: Dict[str, str],
    right_domain: Dict[str, str],
) -> Tuple[Iterator[Sequence], Sequence]:
    """Validate domain and format *data* stream and *columns*."""
    value_pos = find_relation_value_index(columns, crosswalk_name)

    domain_indexes: Dict[int, Tuple[str, str]] = {}

    for key, val in left_domain.items():
        if key in columns[:value_pos]:  # Search left-side only.
            pos = columns[:value_pos].index(key)
            domain_indexes[pos] = (key, val)

    for key, val in right_domain.items():
        if key in columns[value_pos + 1:]:  # Search right-side only.
            pos = columns.index(key, value_pos + 1)
            domain_indexes[pos] = (key, val)

    def validate_and_parse(row):
        """Verify domain if given, return rows without domain items."""
        # This function closes over `domain_indexes` and `value_pos`.
        for i, (key, val) in domain_indexes.items():
            if row[i] != val:
                side = 'left' if i < value_pos else 'right'
                msg = (
                    f'error in {side}-side domain: {key!r} should be '
                    f'{val!r}, got {row[i]!r}'
                )
                raise ValueError(msg)
        return [x for i, x in enumerate(row) if i not in domain_indexes]

    data = (validate_and_parse(row) for row in data)
    columns = [x for i, x in enumerate(columns) if i not in domain_indexes]

    return data, columns


def load_mapping(
    left_node : TopoNode,
    direction : Direction,
    right_node : TopoNode,
    crosswalk_name: str,
    data: Union[Iterable[Sequence], Iterable[Dict]],
    columns: Optional[Sequence[str]] = None,
    selectors: Optional[Union[List[str], str]] = None,
    is_default: Optional[bool] = None,
    match_limit: int = 1,
    allow_overlapping: bool = False,
) -> None:
    """Use mapping data to build a crosswalk between two nodes."""
    data, columns = normalize_tabular(data, columns)
    data, columns = normalize_mapping_data(
        data=data,
        columns=columns,
        crosswalk_name=crosswalk_name,
        left_domain=left_node.domain,
        right_domain=right_node.domain,
    )

    mapper = Mapper(crosswalk_name, data, columns)
    mapper.match_records(left_node, 'left', match_limit, allow_overlapping)
    mapper.match_records(right_node, 'right', match_limit, allow_overlapping)

    if '->' in direction:
        right_node.add_crosswalk(
            other_node=left_node,
            crosswalk_name=crosswalk_name,
            selectors=selectors,
            is_default=is_default,
        )
        right_node.insert_relations2(
            node_or_ref=left_node,
            crosswalk_name=crosswalk_name,
            data=mapper.get_relations('->'),
            columns=['other_index_id', crosswalk_name, 'index_id', 'mapping_level'],
        )

    if '<-' in direction:
        left_node.add_crosswalk(
            other_node=right_node,
            crosswalk_name=crosswalk_name,
            selectors=selectors,
            is_default=is_default,
        )
        left_node.insert_relations2(
            node_or_ref=right_node,
            crosswalk_name=crosswalk_name,
            data=mapper.get_relations('<-'),
            columns=['other_index_id', crosswalk_name, 'index_id', 'mapping_level'],
        )


def _translate(
    quantity_iterator: QuantityIterator, node: TopoNode
) -> Generator[Tuple[Index, AttributesDict, float], None, None]:
    """Generator to yield index, attribute, and quantity tuples."""
    with node._managed_cursor() as cursor:
        crosswalk_repo = node._dal.CrosswalkRepository(cursor)
        relation_repo = node._dal.RelationRepository(cursor)
        index_repo = node._dal.IndexRepository(cursor)

        # Get all crosswalks.
        crosswalks: List = find_crosswalks_by_ref(
            ref=quantity_iterator.unique_id,
            crosswalk_repo=crosswalk_repo,
        )

        # Get the default crosswalk and make sure it's locally complete.
        default_crosswalk_id = None
        for crosswalk in crosswalks:
            if crosswalk.is_default:
                if crosswalk.other_index_hash != quantity_iterator.index_hash \
                        or not crosswalk.is_locally_complete:
                    msg = f'default crosswalk {crosswalk.name!r} is not complete'
                    raise RuntimeError(msg)

                default_crosswalk_id = crosswalk.id
                break
        else:  # IF NO BREAK!
            msg = f'no default crosswalk found for node {node}'
            raise RuntimeError(msg)

        # Build dict of index id values and attribute selector objects.
        crosswalks = [x for x in crosswalks if x.is_locally_complete and x.selectors]
        func = lambda selectors: [parse_selector(s) for s in selectors]
        selector_dict = {x.id: func(x.selectors) for x in crosswalks}

        for index, attributes, quantity_value in quantity_iterator.data:
            if quantity_value is None:
                continue  # Skip to next relation.

            # Find crosswalk that matches with greated unique specificity.
            crosswalk_id = get_greatest_unique_specificity(
                row_dict=attributes,
                selector_dict=selector_dict,
                default=default_crosswalk_id,
            )

            # Get relations for matching crosswalk and other_index_id
            # (assign as a tuple to consume the iterator and free-up
            # the underlying cursor obj for the following yield-loop).
            relations = tuple(relation_repo.find_by_ids(
                crosswalk_id=crosswalk_id,
                other_index_id=index.id,
            ))

            # Yield translated results for each relation.
            for relation in relations:
                new_proportion = check_type(relation.proportion, float)
                new_index = check_type(index_repo.get(relation.index_id), Index)
                new_quantity_value = quantity_value * new_proportion
                yield (new_index, attributes, new_quantity_value)


def translate(
    quantity_iterator: QuantityIterator, node: TopoNode
) -> QuantityIterator:
    """Translate quantities to the index of the target *node*."""
    with node._managed_cursor() as cursor:
        property_repo = node._dal.PropertyRepository(cursor)
        new_unique_id = check_type(property_repo.get('unique_id'), str)
        new_index_hash = check_type(property_repo.get('index_hash'), str)
        new_label_names = node._dal.ColumnManager(cursor).get_columns()

    new_quantity_iter = QuantityIterator(
        unique_id=new_unique_id,
        index_hash=new_index_hash,
        domain=quantity_iterator.domain,
        data=_translate(quantity_iterator, node),
        label_names=new_label_names,
        attribute_keys=quantity_iterator.attribute_keys,
    )
    return new_quantity_iter


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
