"""Graph implementation and functions for the Toron project."""
import logging
import os
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
    cast,
)

from ._utils import (
    check_type,
    TabularData,
    make_readerlike,
    normalize_tabular,
    eagerly_initialize,
    NOVALUE,
    ToronWarning,
    BitFlags,
)
from .data_models import (
    Index,
    AttributesDict,
    QuantityIterator,
    Crosswalk,
)
from .data_service import (
    find_crosswalks_by_ref,
    get_domain,
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


applogger = logging.getLogger(f'app-{__name__}')


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


def normalize_filename_hints(
    left_path_hint: Optional[str],
    right_path_hint: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """Normalize filename hints (removes common directory prefix
    and ``.toron`` file extension).
    """
    # Make sure both are strings.
    left_filename_hint = left_path_hint or ''
    right_filename_hint = right_path_hint or ''

    # Normalize directory separators to POSIX-style slash.
    left_filename_hint = left_filename_hint.replace('\\', '/')
    right_filename_hint = right_filename_hint.replace('\\', '/')

    # Remove common directory prefix.
    try:
        common_prefix = os.path.commonpath([  # Raises ValueError if mixing
            left_filename_hint,               # absolute and relative paths
            right_filename_hint,              # or paths from different drives.
        ])
        if common_prefix:
            left_filename_hint = os.path.relpath(left_filename_hint, common_prefix)
            right_filename_hint = os.path.relpath(right_filename_hint, common_prefix)
            # Need to re-normalize separators if using Windows.
            if os.name == 'nt':
                left_filename_hint = left_filename_hint.replace('\\', '/')
                right_filename_hint = right_filename_hint.replace('\\', '/')
    except ValueError:
        pass  # If prefix removal fails, use paths as-is.

    # Remove `.toron` extension (change to removesuffix() method, new in 3.9,
    # when support for Python 3.8 is dropped).
    if left_filename_hint.endswith('.toron'):
        left_filename_hint = left_filename_hint[:-6]
    if right_filename_hint.endswith('.toron'):
        right_filename_hint = right_filename_hint[:-6]

    # Return string or None values.
    return (left_filename_hint or None, right_filename_hint or None)


def _get_mapping_stats(
    source_node: TopoNode,
    target_node: TopoNode,
    crosswalk: Crosswalk,
) -> Dict[str, int]:
    """Return a summary of mapping statistics for a given crosswalk.

    .. code-block::

        >>> _get_mapping_stats(node1, node2, crosswalk)
        {'src_cardinality': 10,
         'src_index_matched': 10,
         'src_index_missing': 0,
         'src_index_stale': 0,
         'trg_cardinality': 10,
         'trg_index_matched': 10,
         'trg_index_missing': 0}
    """
    with source_node._managed_cursor() as src_cur, \
            target_node._managed_cursor() as trg_cur:
        src_index_repo = source_node._dal.IndexRepository(src_cur)
        src_prop_repo = source_node._dal.PropertyRepository(src_cur)

        trg_index_repo = target_node._dal.IndexRepository(trg_cur)
        trg_rel_repo = target_node._dal.RelationRepository(trg_cur)

        if crosswalk.other_unique_id != src_prop_repo.get('unique_id'):
            msg = 'crosswalk does not match source node'
            raise Exception(msg)

        # Get source-side counts. If the hashes match, we know that all
        # records are matched and these matches are good. If the hashes
        # don't match, we need to verify each 'other_index_id' to
        # determine if it's good or stale.
        src_cardinality = src_index_repo.get_cardinality()
        src_index_matched = 0
        src_index_missing = 0
        src_index_stale = 0

        if crosswalk.other_index_hash == src_prop_repo.get('index_hash'):
            src_index_matched = src_cardinality
        else:
            other_index_ids = trg_rel_repo.find_distinct_other_index_ids(
                crosswalk.id,
            )
            for other_index_id in other_index_ids:
                try:
                    src_index_repo.get(other_index_id)
                    src_index_matched += 1
                except KeyError:
                    src_index_stale += 1

            src_index_missing = src_cardinality - src_index_matched

        # Get target-side counts. Note: There is no 'trg_index_stale'
        # because target index references should never go stale. They
        # are managed locally--the target node holds data for incoming
        # crosswalks so if an index is deleted from the target node,
        # it should also be deleted from crosswalks in that node.
        trg_cardinality = trg_index_repo.get_cardinality()
        trg_index_matched = trg_rel_repo.get_index_id_cardinality(crosswalk.id)
        trg_index_missing = trg_cardinality - trg_index_matched

        return {
            'src_cardinality': src_cardinality,
            'src_index_matched': src_index_matched,
            'src_index_missing': src_index_missing,
            'src_index_stale': src_index_stale,
            'trg_cardinality': trg_cardinality,
            'trg_index_matched': trg_index_matched,
            'trg_index_missing': trg_index_missing,
        }


def _log_load_mapping_stats(
    logger: logging.Logger,
    left_node : TopoNode,
    direction: Literal['<-', '->'],
    right_node : TopoNode,
    crosswalk_name: str,
) -> None:
    """Log mapping stats using given *logger* for specified crosswalk."""
    if direction == '<-':
        source_node = right_node
        target_node = left_node
        source_side = 'right-side'
        target_side = 'left-side'
    elif direction == '->':
        source_node = left_node
        target_node = right_node
        source_side = 'left-side'
        target_side = 'right-side'
    else:
        msg = f"direction must be '<-' or '->', got {direction!r}"
        raise ValueError(msg)

    crosswalk = target_node.get_crosswalk(source_node, crosswalk_name)
    mapping_stats = _get_mapping_stats(
        source_node, target_node, cast(Crosswalk, crosswalk)
    )
    if not any([mapping_stats['src_index_missing'],
                mapping_stats['src_index_stale'],
                mapping_stats['trg_index_missing']]):
        logger.info('mapping verified, cleanly matches both sides')
    else:
        if mapping_stats['src_index_missing']:
            logger.warning(
                f"missing {mapping_stats['src_index_missing']} indexes "
                f"on {source_side}"
            )

        if mapping_stats['src_index_stale']:
            logger.error(
                f"found {mapping_stats['src_index_stale']} indexes on "
                f"{source_side} that no longer exist"
            )

        if mapping_stats['trg_index_missing']:
            logger.warning(
                f"missing {mapping_stats['trg_index_missing']} indexes "
                f"on {target_side}"
            )


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

    left_filename_hint, right_filename_hint = normalize_filename_hints(
        left_node.path_hint,
        right_node.path_hint,
    )

    if '->' in direction:
        applogger.info('loading mapping from left to right')

        right_node.add_crosswalk(
            node=left_node,
            crosswalk_name=crosswalk_name,
            other_filename_hint=left_filename_hint,
            selectors=selectors,
            is_default=is_default,
        )
        right_node.insert_relations2(
            node_or_ref=left_node,
            crosswalk_name=crosswalk_name,
            data=mapper.get_relations('->'),
            columns=['other_index_id', crosswalk_name, 'index_id', 'mapping_level'],
        )

        _log_load_mapping_stats(
            logger=applogger,
            left_node=left_node,
            direction='->',
            right_node=right_node,
            crosswalk_name=crosswalk_name,
        )

    if '<-' in direction:
        applogger.info('loading mapping from right to left')

        left_node.add_crosswalk(
            node=right_node,
            crosswalk_name=crosswalk_name,
            other_filename_hint=right_filename_hint,
            selectors=selectors,
            is_default=is_default,
        )
        left_node.insert_relations2(
            node_or_ref=right_node,
            crosswalk_name=crosswalk_name,
            data=mapper.get_relations('<-'),
            columns=['other_index_id', crosswalk_name, 'index_id', 'mapping_level'],
        )

        _log_load_mapping_stats(
            logger=applogger,
            left_node=left_node,
            direction='<-',
            right_node=right_node,
            crosswalk_name=crosswalk_name,
        )


_MappingElementsTuple : TypeAlias = Union[
    Tuple[int, int, Optional[bytes], float],  # <- Matched elements.
    Tuple[None, int, None, None],  # <- Unmatched right-side elements.
    Tuple[int, None, None, None],  # <- Unmatched left-side elements.
]

def _get_mapping_elements(
    source_node: TopoNode,
    target_node: TopoNode,
    crosswalk_name: Optional[str] = None,
) -> Generator[_MappingElementsTuple, None, None]:
    """Get all mapped and disjoint elements involved in a mapping.

    When mapping elements are grouped by match-status, they should be
    given in the following order:

    * Matched records should be given first.
    * Unmatched right-side elements second.
    * Unmatched left-side elements last.

    This order is easier to work with in a spreadsheet program. It's
    best to avoid giving right-side elements last because it's very
    easy for users to overlook them when working on a mapping.
    """
    with target_node._managed_cursor() as trg_cursor:
        trg_index_repo = target_node._dal.IndexRepository(trg_cursor)
        trg_crosswalk_repo = target_node._dal.CrosswalkRepository(trg_cursor)
        trg_relation_repo = target_node._dal.RelationRepository(trg_cursor)

        crosswalk = target_node._get_crosswalk(
            source_node,
            crosswalk_name,
            trg_crosswalk_repo,
        )

        if not crosswalk:
            msg = f'no crosswalk named {crosswalk_name!r}'
            raise Exception(msg)

        # Yield matched records.
        relations = trg_relation_repo.find(crosswalk_id=crosswalk.id)
        for rel in relations:
            yield (rel.other_index_id, rel.index_id, rel.mapping_level, rel.value)

        # Yield unmatched right-side elements.
        if not crosswalk.is_locally_complete:
            # Only search for elements when crosswalk is not locally complete.
            unmatched_index_ids = trg_index_repo.find_unmatched_index_ids(crosswalk.id)
            for index_id in unmatched_index_ids:
                yield (None, index_id, None, None)

        # Yield unmatched left-side elements.
        with source_node._managed_cursor() as src_cur:
            src_prop_repo = source_node._dal.PropertyRepository(src_cur)

            # Only check source indexes if the index hash is different.
            if src_prop_repo.get('index_hash') != crosswalk.other_index_hash:
                src_index_repo = source_node._dal.IndexRepository(src_cur)

                # Check that each source index is matched to the target.
                for other_index_id in src_index_repo.find_all_index_ids():
                    matches = trg_relation_repo.find(
                        crosswalk_id=crosswalk.id,
                        other_index_id=other_index_id,
                    )
                    if next(matches, None) is None:  # Yield only if unmatched.
                        yield (other_index_id, None, None, None)


def _get_ambiguous_fields(
    mapping_level: Optional[bytes], column_names: Sequence[str]
) -> Optional[str]:
    """Return a formatted string of ambiguous field names.

    The mapping level ``b'\x80'`` represents (1, 0, 0). Given three
    fields, the second and third fields are marked as ambiguous::

        >>> _get_ambiguous_fields(b'\xc0', ['foo', 'bar', 'baz'])
        'bar, baz'

    The mapping level ``b'\xc0'`` represents (1, 1, 0). Given three
    fields, the last field is marked as ambiguous::

        >>> _get_ambiguous_fields(b'\xc0', ['foo', 'bar', 'baz'])
        'baz'

    The mapping level ``b'\xe0'`` represents (1, 1, 1). Given three
    columns, then they are all mapped and there are no ambiguous
    fields::

        >>> _get_ambiguous_fields(b'\xe0', ['foo', 'bar', 'baz'])
        ''

    When given ``None`` no fields are treated as ambiguous::

        >>> _get_ambiguous_fields(None, ['foo', 'bar', 'baz'])
        ''
    """
    if mapping_level is None:
        return None
    inverted_level = [(not bit) for bit in BitFlags(mapping_level)]
    ambiguous_fields = compress(column_names, inverted_level)
    return ', '.join(ambiguous_fields) or None


def get_mapping(
    source_node: TopoNode,
    target_node: TopoNode,
    crosswalk_name: Optional[str] = None,
    header: bool = True,
) -> Iterator[Tuple]:
    """Yield an index mapping from *source_node* to *target_node*
    for a particular crosswalk.
    """
    src_index_cols = tuple(source_node.index_columns)
    trg_index_cols = tuple(target_node.index_columns)

    src_domain = source_node.domain
    src_domain_keys = tuple(src_domain.keys())
    src_domain_vals = tuple(src_domain.values())

    trg_domain = target_node.domain
    trg_domain_keys = tuple(trg_domain.keys())
    trg_domain_vals = tuple(trg_domain.values())

    mapping_elements = _get_mapping_elements(
        source_node=source_node,
        target_node=target_node,
        crosswalk_name=crosswalk_name,
    )

    with source_node._managed_cursor() as src_cur, \
            target_node._managed_cursor() as trg_cur:
        src_index_repo = source_node._dal.IndexRepository(src_cur)
        trg_index_repo = target_node._dal.IndexRepository(trg_cur)

        if header:
            yield (
                ('index_id',)
                + src_domain_keys
                + src_index_cols
                + (crosswalk_name,)
                + ('index_id',)
                + trg_domain_keys
                + trg_index_cols
                + ('ambiguous_fields',)
            )

        src_domain_output: Tuple[Optional[str], ...]
        trg_domain_output: Tuple[Optional[str], ...]
        src_index_labels: Tuple[Optional[str], ...]
        trg_index_labels: Tuple[Optional[str], ...]

        for element in mapping_elements:
            src_index_id, trg_index_id, mapping_level, rel_value = element

            # Set domain output and get source node labels.
            if src_index_id is not None:
                src_domain_output = src_domain_vals
                try:
                    src_index = src_index_repo.get(src_index_id)
                    src_index_labels = src_index.labels
                except KeyError:
                    src_index_labels = (None,) * len(src_index_cols)
            else:
                src_domain_output = (None,) * len(src_domain_vals)
                src_index_labels = (None,) * len(src_index_cols)

            # Set domain output and get target node labels.
            if trg_index_id is not None:
                trg_domain_output = trg_domain_vals
                try:
                    trg_index = trg_index_repo.get(trg_index_id)
                    trg_index_labels = trg_index.labels
                except KeyError:
                    trg_index_labels = (None,) * len(trg_index_cols)
            else:
                trg_domain_output = (None,) * len(trg_domain_vals)
                trg_index_labels = (None,) * len(trg_index_cols)

            yield (
                (src_index_id,)
                + src_domain_output
                + src_index_labels
                + (rel_value,)
                + (trg_index_id,)
                + trg_domain_output
                + trg_index_labels
                + (_get_ambiguous_fields(mapping_level, trg_index_cols),)
            )


def get_mapping_info_str(
    source_node: TopoNode,
    target_node: TopoNode,
    crosswalk_name: Optional[str] = None,
) -> str:
    """Return a text description of information about a mapping."""
    crosswalk = target_node.get_crosswalk(
        node_or_ref=source_node,
        crosswalk_name=crosswalk_name,
    )

    if crosswalk is None:
        msg = f'no crosswalk named {crosswalk_name!r}'
        raise Exception(msg)

    if not crosswalk_name:
        crosswalk_name = crosswalk.name

    stats = _get_mapping_stats(
        source_node=source_node,
        target_node=target_node,
        crosswalk=crosswalk,
    )

    source_short_hint = source_node.path_hint
    if source_short_hint and source_short_hint.endswith('.toron'):
        source_short_hint = source_short_hint[:-6]
    else:
        source_short_hint = f'[{source_node.unique_id[:7]}]'

    target_short_hint = target_node.path_hint
    if target_short_hint and target_short_hint.endswith('.toron'):
        target_short_hint = target_short_hint[:-6]
    else:
        target_short_hint = f'[{target_node.unique_id[:7]}]'

    info = [
        f'{crosswalk_name}: {source_short_hint} -> {target_short_hint}',
        f'',
        f'  {source_short_hint}: matched {stats["src_index_matched"]} ' \
            f'of {stats["src_cardinality"]} indexes',
        f'  {target_short_hint}: matched {stats["trg_index_matched"]} ' \
            f'of {stats["trg_cardinality"]} indexes',
    ]

    if stats['src_index_stale']:
        info.extend([
            f'',
            f'  Mapping contains {stats["src_index_stale"]} indexes that ' \
                f'no longer exist in {source_node.path_hint}',
        ])

    return '\n'.join(info)


@eagerly_initialize
def get_weights(
    node: TopoNode,
    weights: Optional[Union[str, Iterable[str]]] = None,
    header: bool = True,
) -> Generator[List, None, None]:
    """Yield weight records from the given *node*."""
    with node._managed_cursor(n=2) as (cur1, cur2):
        group_repo = node._dal.WeightGroupRepository(cur1)
        index_repo = node._dal.IndexRepository(cur1)
        prop_repo = node._dal.PropertyRepository(cur1)
        weight_repo = node._dal.WeightRepository(cur2)

        domain = get_domain(prop_repo)
        domain_keys = list(domain.keys())
        domain_vals = list(domain.values())

        if weights is None:
            groups = group_repo.get_all()
        else:
            if isinstance(weights, str):
                weights = [weights]

            groups = []
            for name in weights:
                try:
                    groups.append(group_repo.get_by_name(name))
                except KeyError:
                    all_groups = ', '.join(repr(x.name) for x in group_repo.get_all())
                    msg = f'weight {name!r} not found, available weights: {all_groups}'
                    raise ValueError(msg)

        if header:
            # Make and yield header row.
            label_columns = node._dal.ColumnManager(cur1).get_columns()
            group_names = [grp.name for grp in groups]
            yield ['index_id'] + domain_keys + list(label_columns) + group_names

        # Assign shorter func name (also reduces dot lookups).
        get_weight = weight_repo.get_by_weight_group_id_and_index_id

        # Make and yield record rows.
        for index in index_repo.find_all():
            weight_vals: List[Optional[float]] = []
            for group in groups:
                try:
                    weight_value = get_weight(group.id, index.id).value
                except KeyError:
                    if index.id == 0:
                        weight_value = 0.0
                    else:
                        weight_value = None

                weight_vals.append(weight_value)

            yield [index.id] + domain_vals + list(index.labels) + weight_vals


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
            relations = tuple(relation_repo.find(
                crosswalk_id=crosswalk_id,
                other_index_id=index.id,
            ))

            # Yield translated results for each relation.
            for relation in relations:
                new_proportion = check_type(relation.proportion, float)
                new_index = index_repo.get(relation.index_id)
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
