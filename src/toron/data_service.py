"""Application logic functions that interact with repository objects."""

import logging
from collections import Counter
from itertools import chain, compress, groupby
from math import log2

from toron._typing import (
    Any,
    Callable,
    Collection,
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
    Union,
    cast,
    TypeAlias,
)

from .categories import (
    make_structure,
    minimize_discrete_categories,
)
from .data_models import (
    COMMON_RESERVED_IDENTIFIERS,
    EmptyCollectionError,
    BaseAttributeGroupRepository,
    BaseLabelManager,
    BaseIndexRepository,
    BaseLocationRepository,
    BaseWeightGroupRepository,
    BaseWeightRepository,
    BaseQuantityRepository,
    BaseRelationRepository,
    BaseLinkRepository,
    BasePropertyRepository,
    BaseStructureRepository,
    Index,
    Location,
    AttributeGroup,
    Weight,
    WeightGroup,
    Link,
    JsonTypes,
)
from .formatters import (
    format_granularity,
    sort_categories,
)
from .selectors import (
    parse_selector,
    get_greatest_unique_specificity,
)
from ._utils import (
    check_type,
    SequenceHash,
    ToronError,
    ToronWarning,
    BitFlags,
)


applogger = logging.getLogger('app-toron')


class IntegrityError(Exception):
    """Toron data model integrity error."""


def validate_new_index_columns(
    new_column_names: Iterable[str],
    reserved_identifiers: Set[str],
    label_manager: BaseLabelManager,
    property_repo: BasePropertyRepository,
    attribute_repo: BaseAttributeGroupRepository,
) -> None:
    """Raise a ToronError if a new column conflicts with an existing
    index column, the domain, or an attribute name.
    """
    all_reserved_identifiers = \
        reserved_identifiers.union(COMMON_RESERVED_IDENTIFIERS)
    existing_columns = set(label_manager.get_columns())
    attribute_names = set(attribute_repo.get_all_attribute_names())

    for col in new_column_names:
        if col in all_reserved_identifiers:
            raise ToronError(
                f'{col!r} is a reserved name'
            )

        if col in existing_columns:
            raise ToronError(
                f'index label column {col!r} already exists'
            )

        if col in attribute_names:
            raise ToronError(
                f'{col!r} is used as an attribute name'
            )


def refresh_index_hash_property(
    index_repo: BaseIndexRepository,
    prop_repo: BasePropertyRepository,
) -> None:
    """Update 'index_hash' property to reflect current index_id values."""
    sequence_hash = SequenceHash()
    for index_id in index_repo.find_all_index_ids(ordered=True):
        sequence_hash.add_value(index_id)

    index_hash = sequence_hash.get_hexdigest()
    try:
        prop_repo.add('index_hash', index_hash)
    except Exception:
        prop_repo.update('index_hash', index_hash)


def delete_index_record(
    index_id: int,
    index_repo: BaseIndexRepository,
    weight_repo: BaseWeightRepository,
    crosswalk_repo: BaseLinkRepository,
    relation_repo: BaseRelationRepository,
) -> None:
    """Delete index record and associated weights and relations."""
    # Remove associated weight records.
    weights = weight_repo.find_by_index_id(index_id)
    for weight in list(weights):
        weight_repo.delete(weight.id)

    # Remove associated relation records.
    relations = relation_repo.find(index_id=index_id)
    fully_specified_level = bytes(BitFlags([1] * len(index_repo.get_label_names())))
    other_index_ids = set()
    for relation in list(relations):
        if relation.mapping_level != fully_specified_level:
            # For now, prevent index deletion when there
            # are ambiguous relations. In the future, this
            # restriction should be removed by re-mapping
            # these relations using matching labels.
            raise ValueError(
                f'cannot delete index_id {index_id}, some associated '
                f'crosswalk relations are ambiguous\n'
                f'\n'
                f'Crosswalks with ambiguous relations must be removed '
                f'before deleting index records. Afterwards, these '
                f'crosswalks can be re-added.'
            )

        other_index_ids.add(relation.other_index_id)
        relation_repo.delete(relation.id)

    # Rebuild proportions for remaining relations.
    for other_index_id in other_index_ids:
        for crosswalk in crosswalk_repo.get_all():
            relation_repo.refresh_proportions(crosswalk.id, other_index_id)

    # Remove existing Index record.
    index_repo.delete(index_id)


def find_locations_without_index(
    location_repo: BaseLocationRepository,
    aux_index_repo: BaseIndexRepository,
) -> Iterator[Location]:
    """Find locations with no matching index records.

    The *location_repo* and *aux_index_repo* should use independent
    cursor instances.
    """
    label_names = location_repo.get_label_names()
    for location in location_repo.find_all():
        criteria = {k: v for k, v in zip(label_names, location.labels) if v}
        if not any(aux_index_repo.filter_index_ids_by_label(criteria)):
            yield location


def find_locations_without_structure(
    location_repo: BaseLocationRepository,
    structure_repo: BaseStructureRepository,
) -> Iterator[Location]:
    """Find locations with no matching structure records."""
    all_structure_bits = {BitFlags(x.bits) for x in structure_repo.get_all()}
    for location in location_repo.find_all():
        if BitFlags(location.labels) not in all_structure_bits:
            yield location


def find_nonmatching_locations(
    location_repo: BaseLocationRepository,
    structure_repo: BaseStructureRepository,
    aux_index_repo: BaseIndexRepository,
) -> Iterator[Location]:
    """Find locations with no matching structure or index records.

    The *location_repo* and *aux_index_repo* should use independent
    cursor instances.
    """
    all_structure_bits = {BitFlags(x.bits) for x in structure_repo.get_all()}
    label_names = location_repo.get_label_names()

    for location in location_repo.find_all():
        if BitFlags(location.labels) not in all_structure_bits:
            yield location
        else:
            criteria = {k: v for k, v in zip(label_names, location.labels) if v}
            if not any(aux_index_repo.filter_index_ids_by_label(criteria)):
                yield location


def count_nonmatching_locations(
    location_repo: BaseLocationRepository,
    structure_repo: BaseStructureRepository,
    aux_index_repo: BaseIndexRepository,
) -> Dict[str, int]:
    """Count the number of locations without a structure or index match.

    Returns a dictionary of counts::

        >>> count_nonmatching_locations(loc_repo, struct_repo, idx_repo)
        {'structure_and_index': 4, 'structure': 0, 'index': 3}

    The *location_repo* and *structure_repo* can use the same cursor
    but *aux_index_repo* should use an independent cursor instance.
    """
    all_structure_bits = {BitFlags(x.bits) for x in structure_repo.get_all()}
    label_names = location_repo.get_label_names()

    counter: Counter = Counter(structure_and_index=0, structure=0, index=0)

    for location in location_repo.find_all():
        nonmatching_structure = BitFlags(location.labels) not in all_structure_bits

        criteria = {k: v for k, v in zip(label_names, location.labels) if v}
        nonmatching_index = not any(aux_index_repo.filter_index_ids_by_label(criteria))

        if nonmatching_structure and nonmatching_index:
            counter['structure_and_index'] += 1
        elif nonmatching_structure:
            counter['structure'] += 1
        elif nonmatching_index:
            counter['index'] += 1

    return dict(counter)


def find_attribute_groups_without_quantity(
    attrib_repo: BaseAttributeGroupRepository,
    alt_quantity_repo: BaseQuantityRepository,
) -> Iterator[AttributeGroup]:
    """Find AttributeGroup records that have no matching Quantity."""
    for attr_group in attrib_repo.find_all():
        quantities = alt_quantity_repo.find(attribute_group_id=attr_group.id)
        if not any(quantities):
            yield attr_group


def find_locations_without_quantity(
    location_repo: BaseLocationRepository,
    alt_quantity_repo: BaseQuantityRepository,
) -> Iterator[Location]:
    """Find Location records that have no matching Quantity."""
    for location in location_repo.find_all():
        quantities = alt_quantity_repo.find(location_id=location.id)
        if not any(quantities):
            yield location


def get_quantity_value_sum(
    location_id: int,
    attribute_group_id: int,
    quantity_repo: BaseQuantityRepository,
) -> Optional[float]:
    """Return sum of quantities matching location_id and attribute_group_id."""
    quantities = quantity_repo.find(
        location_id=location_id,
        attribute_group_id=attribute_group_id,
    )
    try:
        return sum(x.value for x in chain([next(quantities)], quantities))
    except StopIteration:
        return None


def disaggregate_value(
    quantity_value: float,
    index_ids: Collection[int],
    weight_group_id: int,
    weight_repo: BaseWeightRepository,
) -> Iterator[Tuple[int, float]]:
    """Return disaggregated quantities for given index_id values.

    .. important::

        This is an internal (non-user-facing) function. It should only
        be used when certain conditions are met:

        * The *quantity_value* should represent an extensive property
          (not an intensive property). If an intensive property is
          given--like a percentage or temperature--the results will
          be nonsensical.
        * The weight group should exist and an associated weight should
          exist for every index record. If there is no matching weight,
          a KeyError will be raised.

        These conditions should be assured by the parent operation
        before calling this function.
    """
    # Assign shorter func name (also reduces dot lookups).
    get_weight = weight_repo.get_by_weight_group_id_and_index_id

    # Get sum of weight values associated with index_ids.
    group_weight = 0.0
    for index_id in index_ids:
        try:
            weight = get_weight(weight_group_id, index_id)
            group_weight += weight.value
        except KeyError:
            if index_id != 0:  # Re-raise if it's not the undefined record
                raise          # or continue to the next record if it is.

    # Yield disaggregated values for associated index records.
    if group_weight:
        for index_id in index_ids:
            try:
                weight = get_weight(weight_group_id, index_id)
                proportion = weight.value / group_weight
                yield (index_id, quantity_value * proportion)
            except KeyError:
                # Records with missing weights--except for the undefined
                # record--would have already raised an error during the
                # `group_weight` loop above. So we know this KeyError
                # can only be triggered by the undefined record itself.
                # And we also know that there are weights for other
                # records in this group, so we can yield this item with
                # a weight of zero.
                yield (index_id, 0.0)
    else:
        # When `group_weight` is 0.0, distribute quantity evenly--but
        # only to appropriate index records.
        index_ids_len = len(index_ids)
        if index_ids_len > 1:
            if 0 not in index_ids:
                # When the undefined record (index_id 0) is not in *index_ids*,
                # then we distribute the quantity evenly across all records.
                proportion = 1 / index_ids_len
                disaggregated_value = quantity_value * proportion
                for index_id in index_ids:
                    yield (index_id, disaggregated_value)
            else:
                # When there are multiple items and one of them is the
                # undefined record, the quantity is distributed evenly
                # among all of the items *except* the undefined record.
                proportion = 1 / (index_ids_len - 1)  # <- Subtract 1 for undefined record.
                disaggregated_value = quantity_value * proportion
                for index_id in index_ids:
                    if index_id == 0:
                        yield (index_id, 0.0)
                    else:
                        yield (index_id, disaggregated_value)
        elif index_ids_len == 1:
            # When there's only one item, there's nothing to disaggregate
            # so yield the quantity as-is, even for the undefined record.
            yield (next(iter(index_ids)), quantity_value)
        else:
            raise RuntimeError(
                f'unexpected condition when attempting to disaggregate quantity:\n'
                f'  quantity_value={quantity_value!r}\n'
                f'  index_ids={index_ids!r}\n'
                f'  weight_group_id={weight_group_id!r}'
            )


def find_crosswalks_by_ref(
    ref: str,
    crosswalk_repo: BaseLinkRepository,
) -> List[Link]:
    """Find crosswalks that match the given node reference."""
    # Try to match by exact 'other_unique_id'.
    matches = list(crosswalk_repo.find_by_other_unique_id(ref))
    if matches:
        return matches

    # Try to match by filename hints.
    matches = []
    matches.extend(crosswalk_repo.find_by_other_filename_hint(ref))  # Exact match.
    if isinstance(ref, str) and ref.endswith('.toron'):  # Without '.toron' extension.
        ref_truncated = ref[:-6]
        matches.extend(crosswalk_repo.find_by_other_filename_hint(ref_truncated))
    else:  # With '.toron' extension.
        matches.extend(crosswalk_repo.find_by_other_filename_hint(f'{ref}.toron'))
    if matches:
        return matches

    # Try to match by short code of 'other_unique_id'.
    if isinstance(ref, str) and len(ref) >= 7:
        matches = crosswalk_repo.get_all()
        return [x for x in matches if x.other_unique_id.startswith(ref)]

    return []  # Return empty list if no match.


def make_get_crosswalk_id_func(
    ref: str,
    crosswalk_repo: BaseLinkRepository,
    other_index_hash: str,
) -> Callable[[Dict[str, str]], int]:
    """Build a ``get_crosswalk_id()`` function that returns
    'crosswalk_id' values matched by the selector with the
    greatest unique specificity. When no match is found or
    when no unique match is found, the function will return
    the default ``crosswalk_id``.
    """
    # Get crosswalks with matching node.
    crosswalks = find_crosswalks_by_ref(
        ref=ref,
        crosswalk_repo=crosswalk_repo,
    )
    if not crosswalks:
        raise RuntimeError('no crosswalk found connecting nodes')

    # Verify that crosswalks are current.
    crosswalks = [x for x in crosswalks if x.other_index_hash == other_index_hash]
    if not crosswalks:
        raise RuntimeError('crosswalks are out of date, need to relink')

    # Get the default crosswalk and make sure it's locally complete.
    default_crosswalk_id = None
    for crosswalk in crosswalks:
        if crosswalk.is_default:
            default_crosswalk_id = crosswalk.id
            break
    else:  # IF NO BREAK!
        raise RuntimeError('no default crosswalk found for node')

    # Build dict of index id values and attribute selector objects.
    crosswalks = [x for x in crosswalks if x.is_locally_complete and x.selectors]
    func = lambda selectors: [parse_selector(s) for s in selectors]
    selector_dict = {x.id: func(x.selectors) for x in crosswalks}

    # Define function to match the crosswalk with the greatest unique
    # specificity (closes over selector_dict and default_crosswalk_id).
    def get_crosswalk_id_func(attributes: Dict[str, str]) -> int:
        """Return crosswalk_id that matches with the greatest unique specificity."""
        crosswalk_id = get_greatest_unique_specificity(
            row_dict=attributes,
            selector_dict=selector_dict,
            default=default_crosswalk_id,
        )
        return crosswalk_id

    return get_crosswalk_id_func


MappingElement : TypeAlias = Union[
    Tuple[int, int, Optional[bytes], float],  # <- Matched elements.
    Tuple[None, int, None, None],  # <- Unmatched right-side elements.
    Tuple[int, None, None, None],  # <- Unmatched left-side elements.
]

def generate_mapping_elements(
    crosswalk_name: Optional[str],
    trg_index_repo: BaseIndexRepository,
    trg_crosswalk_repo: BaseLinkRepository,
    trg_relation_repo: BaseRelationRepository,
    src_index_repo: BaseIndexRepository,
    src_prop_repo: BasePropertyRepository,
) -> Generator[MappingElement, None, None]:
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
    unique_id = cast(str, src_prop_repo.get('unique_id'))
    crosswalk = None
    for cw in trg_crosswalk_repo.find_by_other_unique_id(unique_id):
        if cw.name == crosswalk_name:
            crosswalk = cw

    if not crosswalk:
        msg = f'no crosswalk named {crosswalk_name!r}'
        raise Exception(msg)

    # Yield undefined-to-undefined record (always considered matched).
    yield (0, 0, None, 0.0)

    # Yield matched records.
    relations = trg_relation_repo.find(crosswalk_id=crosswalk.id)
    for rel in relations:
        yield (rel.other_index_id, rel.index_id, rel.mapping_level, rel.value)

    # If target is not complete, yield unmatched right-side elements.
    if not crosswalk.is_locally_complete:
        unmatched_index_ids = trg_index_repo.find_unmatched_index_ids(crosswalk.id)
        for index_id in unmatched_index_ids:
            yield (None, index_id, None, None)

    # If source index is different, yield unmatched left-side elements.
    if src_prop_repo.get('index_hash') != crosswalk.other_index_hash:
        for other_index_id in src_index_repo.find_all_index_ids():
            # In a mapping, an undefined record is always considered matched
            # to the other node's undefined record (can never be unmatched).
            if other_index_id == 0:
                continue

            matches = trg_relation_repo.find(
                crosswalk_id=crosswalk.id,
                other_index_id=other_index_id,
            )
            if not any(matches):  # Yield only if unmatched.
                yield (other_index_id, None, None, None)


def set_default_weight_group(
    weight_group: Union[WeightGroup, None],
    property_repo: BasePropertyRepository,
) -> None:
    """Sets the node's default weight group."""
    property_repo.add_or_update(
        key='default_weight_group_id',
        value=weight_group.id if weight_group else None,
    )


def get_default_weight_group(
    property_repo: BasePropertyRepository,
    weight_group_repo: BaseWeightGroupRepository,
) -> WeightGroup:
    """Return the node's default weight group."""
    try:
        weight_group_id = property_repo.get('default_weight_group_id')
    except KeyError:
        raise RuntimeError('no default weight group is defined')

    if not isinstance(weight_group_id, int):
        raise TypeError(
            f"'default_weight_group_id' property must be int, got "
            f"{weight_group_id.__class__.__qualname__}: {weight_group_id!r}"
        )

    return weight_group_repo.get(weight_group_id)


def find_matching_weight_groups(
    attribute_repo: BaseAttributeGroupRepository,
    weight_group_repo: BaseWeightGroupRepository,
    property_repo: BasePropertyRepository,
    attribute_ids: Optional[Iterable[int]] = None,
) -> Iterable[Tuple[AttributeGroup, WeightGroup]]:
    """Get selectors for *attribute_ids*. If *attribute_ids* is None,
    then this function will get selectors for all attribute groups.
    """
    # Build dicts of weight group id (keys) to weight group (values).
    all_weight_groups = {wg.id: wg for wg in weight_group_repo.get_all()}
    match_weight_groups = \
        {id: wg for id, wg in all_weight_groups.items() if (wg.is_complete and wg.selectors)}

    default_weight_group = get_default_weight_group(
        property_repo=property_repo,
        weight_group_repo=weight_group_repo,
    )

    # Build dict of weight group id (keys) to list of selectors (values).
    func = lambda selectors: [parse_selector(s) for s in selectors]
    selector_dict = {k: func(v.selectors) for k, v in match_weight_groups.items()}

    # Get attribute groups to match.
    attribute_groups: Iterable[AttributeGroup]
    if attribute_ids:
        attribute_groups = (attribute_repo.get(x) for x in attribute_ids)
    else:
        attribute_groups = attribute_repo.find_all()

    # Yield attribute group and matching weight group.
    for attribute_group in attribute_groups:
        weight_group_id = get_greatest_unique_specificity(
            row_dict=attribute_group.attributes,
            selector_dict=selector_dict,
            default=default_weight_group.id,
        )
        yield (attribute_group, all_weight_groups[weight_group_id])


def get_all_discrete_categories(
    property_repo: BasePropertyRepository
) -> List[Set[str]]:
    """Get all discrete categories defined for a node."""
    try:
        values = cast(List[List[str]], property_repo.get('discrete_categories'))
        return [set(x) for x in values]
    except KeyError:
        return []  # Empty list when no columns are defined.


def rename_discrete_categories(
    mapping: Dict[str, str],
    label_manager: BaseLabelManager,
    property_repo: BasePropertyRepository,
) -> None:
    categories = get_all_discrete_categories(property_repo)
    do_rename = lambda cat: {mapping.get(x, x) for x in cat}
    category_sets = [do_rename(cat) for cat in categories]
    category_lists: JsonTypes = [list(cat) for cat in category_sets]
    property_repo.update('discrete_categories', category_lists)


def calculate_granularity(
    columns: List[str],
    index_repo: BaseIndexRepository,
    aux_index_repo: BaseIndexRepository,
) -> Optional[float]:
    r"""Return the granularity of a partition (as given by *columns*).

    If *columns* list is empty or if the index contains no records
    (other than the "undefined" record), then ``None`` will be returned.

    .. code-block:: python

        >>> calculate_granularity(
        ...     ['county', 'town'],
        ...     index_repo,
        ...     aux_index_repo,
        ... )
        6.71556532205684

    This function implements a Shannon entropy based metric which
    was first proposed by Mark Wierman for the "granularity measure
    of a partition" on p. 293 of:

        MARK J. WIERMAN (1999) MEASURING UNCERTAINTY IN ROUGH SET
        THEORY, International Journal of General Systems, 28:4-5,
        283-297, DOI: 10.1080/03081079908935239

    The metric uses block cardinalities to derive relative frequencies,
    whose Shannon entropy serves as a measure of the partition's
    granularity.

    In PROBABILISTIC APPROACHES TO ROUGH SETS (Y. Y. Yao, 2003),
    Yiyu Yao presents the same metric in Eq. (6), using a form
    more useful for our implementation:

    .. code-block:: none

                   m
                  ___
                  \    |A_i|
        log |U| - /    ───── log |A_i|
                  ‾‾‾   |U|
                  i=1

        TeX notation:

            \[\log_{2}|U|-\sum_{i=1}^m \frac{|A_i|}{|U|}\log_{2}|A_i|\]
    """
    if not columns:
        return None  # <- EXIT!

    total_cardinality = index_repo.get_cardinality(include_undefined=False)
    if not total_cardinality:
        return None  # <- EXIT!

    block_labels = index_repo.find_distinct_labels(*columns, include_undefined=False)

    partition_coarseness = 0.0
    for labels in block_labels:
        criteria = dict(zip(columns, labels))
        records = aux_index_repo.filter_index_ids_by_label(criteria, include_undefined=False)
        block_cardinality = sum(1 for _ in records)
        partition_coarseness += (block_cardinality / total_cardinality) * log2(block_cardinality)

    return log2(total_cardinality) - partition_coarseness


def rebuild_structure_table(
    label_manager: BaseLabelManager,
    property_repo: BasePropertyRepository,
    structure_repo: BaseStructureRepository,
    index_repo: BaseIndexRepository,
    aux_index_repo: BaseIndexRepository,
    optimizations: Optional[Dict[str, Callable]] = None
) -> None:
    # Get granularity function (use optimized version when available).
    if optimizations and 'calculate_granularity' in optimizations:
        applogger.debug('using DAL optimized calculate_granularity()')
        granularity_func = optimizations['calculate_granularity']
    else:
        applogger.debug('using unoptimized calculate_granularity()')
        granularity_func = calculate_granularity

    # Remove existing structure.
    for structure in structure_repo.get_all():
        structure_repo.delete(structure.id)

    # Get columns and categories.
    columns = label_manager.get_columns()
    categories = get_all_discrete_categories(property_repo)
    if columns and not categories:
        raise RuntimeError("node has columns but no 'discrete_categories'")

    # Regenerate new structure.
    for cat in make_structure(categories):
        granularity = granularity_func(list(cat), index_repo, aux_index_repo)
        bits = [(x in cat) for x in columns]
        structure_repo.add(granularity, *bits)


def add_discrete_categories(
    categories: Iterable[Set[str]],
    label_manager: BaseLabelManager,
    property_repo: BasePropertyRepository,
) -> None:
    columns = label_manager.get_columns()
    if not columns:
        msg = 'must add index columns before defining categories'
        raise RuntimeError(msg)

    for field in set(chain(*categories)):
        if field not in columns:
            raise ValueError(
                f'invalid category value {field!r}, values '
                f'must be present in index columns'
            )

    existing_categories = get_all_discrete_categories(property_repo)

    whole_space = set(columns)
    category_sets: List[Set[str]] = minimize_discrete_categories(
        categories, existing_categories, [whole_space]
    )

    omitting = [cat for cat in categories if (cat not in category_sets)]
    if omitting:
        import warnings
        formatted = ', '.join(repr(cat) for cat in omitting)
        msg = f'omitting redundant categories: {formatted}'
        warnings.warn(msg, category=ToronWarning, stacklevel=2)

    category_lists = cast(JsonTypes, [list(cat) for cat in category_sets])
    try:
        property_repo.add('discrete_categories', category_lists)
    except Exception:
        property_repo.update('discrete_categories', category_lists)


def add_discrete_category(
    category: Collection[str],
    label_manager: BaseLabelManager,
    property_repo: BasePropertyRepository,
) -> None:
    """Add discrete category.

    Raises a ``ValueError`` if category uses index labels that do not
    exist. Raises a ``RuntimeError`` if category cannot be created for
    some other reason.
    """
    index_labels: Sequence[str] = label_manager.get_columns()

    if not index_labels:
        raise RuntimeError('must add index labels before defining a category')

    category = set(category)
    if category.difference(index_labels):
        invalid_labels = category.difference(index_labels)
        raise ValueError(
            f"invalid category, "
            f"no index label{'s' if len(invalid_labels) != 1 else ''} "
            f"{', '.join(repr(x) for x in sorted(invalid_labels))}"
        )

    # Make helper function for consistent error reporting.
    repr_sorted = lambda cat: f"""{{{
        repr(sorted(cat, key=lambda x: index_labels.index(x)))[1:-1]
    }}}"""

    existing_cats = get_all_discrete_categories(property_repo)

    if category in existing_cats:
        raise RuntimeError(f'category {repr_sorted(category)} is already defined')

    whole_space = set(index_labels)
    minimized_cats: List[Set[str]] = minimize_discrete_categories(
        [category], existing_cats, [whole_space]
    )

    if category not in minimized_cats:
        raise RuntimeError(
            f'category {repr_sorted(category)} is already covered by a union '
            f'of existing categories'
        )

    category_lists = cast(JsonTypes, [list(cat) for cat in minimized_cats])
    property_repo.add_or_update('discrete_categories', category_lists)


def remove_discrete_category(
    category: Collection[str],
    label_manager: BaseLabelManager,
    property_repo: BasePropertyRepository,
) -> None:
    """Remove a discrete category."""
    columns = label_manager.get_columns()
    whole_space = set(columns)

    if category == whole_space:
        formatted_category = f"{{{', '.join(repr(x) for x in sorted(whole_space))}}}"
        raise IntegrityError(f"cannot drop whole space: {formatted_category}")

    existing_cats = get_all_discrete_categories(property_repo)
    cats_to_keep = [x for x in existing_cats if x != category]

    category_sets = minimize_discrete_categories(
        cats_to_keep, [whole_space]
    )
    category_lists: JsonTypes = [list(cat) for cat in category_sets]
    property_repo.update('discrete_categories', category_lists)


def refresh_structure_granularity(
    label_manager: BaseLabelManager,
    structure_repo: BaseStructureRepository,
    index_repo: BaseIndexRepository,
    aux_index_repo: BaseIndexRepository,
    optimizations: Optional[Dict[str, Callable]] = None
) -> None:
    # Get granularity function (use optimized version when available).
    if optimizations and hasattr(optimizations, 'calculate_granularity'):
        granularity_func = optimizations.calculate_granularity
    else:
        granularity_func = calculate_granularity

    # Recalculate granularity and update structure records.
    label_columns = label_manager.get_columns()
    for structure in structure_repo.get_all():
        category = list(compress(label_columns, structure.bits))
        granularity = granularity_func(category, index_repo, aux_index_repo)
        structure.granularity = granularity
        structure_repo.update(structure)


def refresh_or_rebuild_structure_granularity(
    label_manager: BaseLabelManager,
    property_repo: BasePropertyRepository,
    structure_repo: BaseStructureRepository,
    index_repo: BaseIndexRepository,
    aux_index_repo: BaseIndexRepository,
    optimizations: Optional[Dict[str, Callable]] = None
) -> None:
    try:
        has_discrete_categories = bool(property_repo.get('discrete_categories'))
    except KeyError:
        has_discrete_categories = False

    if has_discrete_categories:
        # If categories already exist, then refresh granularity.
        refresh_structure_granularity(
            label_manager=label_manager,
            structure_repo=structure_repo,
            index_repo=index_repo,
            aux_index_repo=aux_index_repo,
            optimizations=optimizations,
        )
    else:
        # If no categories yet, add "whole space" and build structure.
        whole_space = set(index_repo.get_label_names())
        add_discrete_categories(
            categories=[whole_space],
            label_manager=label_manager,
            property_repo=property_repo,
        )
        rebuild_structure_table(
            label_manager=label_manager,
            property_repo=property_repo,
            structure_repo=structure_repo,
            index_repo=index_repo,
            aux_index_repo=aux_index_repo,
            optimizations=optimizations,
        )


def set_domain(domain: str, property_repo: BasePropertyRepository) -> None:
    """Set the node's domain."""
    check_type(domain, str)
    property_repo.add_or_update('domain', domain)


def get_domain(property_repo: BasePropertyRepository) -> str:
    """Return the node's domain."""
    try:
        domain = property_repo.get('domain')
        return check_type(domain, str)
    except KeyError:
        return ''


def set_registered_attributes(
    attribute_columns: Sequence[str],
    reserved_identifiers: Set[str],
    index_repo: BaseIndexRepository,
    property_repo: BasePropertyRepository,
) -> None:
    """Set the node's registered attribute columns in user-defined order."""
    all_reserved_identifiers = \
        reserved_identifiers.union(COMMON_RESERVED_IDENTIFIERS)

    index_labels = set(index_repo.get_label_names())

    unique_attribute_columns = []  # Assure uniqueness while keeping order.
    for attr in attribute_columns:
        if attr in all_reserved_identifiers:
            raise ValueError(f'{attr!r} is a reserved name')

        if attr in index_labels:
            raise ValueError(f'{attr!r} is already used as an index label')

        if attr in unique_attribute_columns:
            raise ValueError(
                f'{attr!r} appears more than once; attributes must be unique'
            )

        unique_attribute_columns.append(attr)

    # Save 'registered_attributes' value in property repository.
    property_repo.add_or_update(
        'registered_attributes', unique_attribute_columns
    )


def get_registered_attributes(
    property_repo: BasePropertyRepository,
) -> List[str]:
    """Get the node's registered attribute columns in user-defined order."""
    try:
        registered_attributes = property_repo.get('registered_attributes')
        return check_type(registered_attributes, required_type=list)
    except KeyError:
        return []


def get_loaded_attributes(
    registered_attributes: List[str],
    attribute_repo: BaseAttributeGroupRepository,
) -> List[str]:
    """Get the names of attributes that have been loaded.

    Raises a ``RuntimeError`` if node contains unregistered attributes.
    """
    loaded_attrs_set = set(attribute_repo.get_all_attribute_names())

    # Check for unregistered attributes.
    registered_attrs_set = set(registered_attributes)
    if not loaded_attrs_set.issubset(registered_attrs_set):
        unregistered_attrs = loaded_attrs_set.difference(registered_attrs_set)
        raise RuntimeError(
            f"node contains unregistered attributes: "
            f"{', '.join(repr(x) for x in sorted(unregistered_attrs))}"
        )

    # Return loaded attributes in `registered_attribute` order.
    return [attr for attr in registered_attributes if attr in loaded_attrs_set]


def set_labels_in_display_order(
    labels: List[str],
    index_repo: BaseIndexRepository,
    property_repo: BasePropertyRepository,
) -> None:
    """Save the list of *labels* as the specified display order."""
    all_labels = set(index_repo.get_label_names())

    unknown_labels = [x for x in labels if x not in all_labels]
    if unknown_labels:
        raise ValueError(
            f"cannot set display order for unknown labels: "
            f"{', '.join(repr(x) for x in unknown_labels)}"
        )
    property_repo.add_or_update('label_display_order', labels)


def get_labels_in_display_order(
    index_repo: BaseIndexRepository,
    property_repo: BasePropertyRepository,
) -> List[str]:
    """Return a list of labels in their specified display order.

    Any labels without a specified order will fall back to storage
    order and appear at the end of the list.
    """
    try:
        display_order = cast(List[str], property_repo.get('label_display_order'))
    except KeyError:
        display_order = []

    # Get any unspecified labels in storage order.
    storage_order = index_repo.get_label_names()
    unspecified_labels = [x for x in storage_order if x not in display_order]

    if unspecified_labels:
        display_order.extend(unspecified_labels)
    return display_order


def change_label_order(
    ordered_labels: Sequence[str], label: str, *, offset: int
) -> List[str]:
    """Move *label* by given *offset*, return reordered labels.

    Move label "B" one position to the right::

        >>> change_label_order(['B', 'A', 'C'], 'B', offset=1)
        ['A', 'B', 'C']

    Move label "A" two positions to the left::

        >>> change_label_order(['B', 'C', 'A'], 'A', offset=-2)
        ['A', 'B', 'C']
    """
    reordered_labels = list(ordered_labels)
    try:
        current_pos = reordered_labels.index(label)
    except ValueError:
        raise ToronError(f'no label named {label!r}')

    # Get target index position (keeping within valid bounds).
    new_pos = current_pos + offset
    new_pos = max(0, new_pos)
    new_pos = min(new_pos, len(reordered_labels) - 1)

    # Change position of label and save new display order.
    reordered_labels.insert(
        new_pos,
        reordered_labels.pop(current_pos),
    )
    return reordered_labels


def get_node_info_text(
    property_repo: BasePropertyRepository,
    index_repo: BaseIndexRepository,
    structure_repo: BaseStructureRepository,
    weight_group_repo: BaseWeightGroupRepository,
    attribute_repo: BaseAttributeGroupRepository,
    crosswalk_repo: BaseLinkRepository,
) -> Dict[str, Union[List[str], str]]:
    """Return dictionary of node information appropriate for repr."""
    # Get domain string.
    domain_str = get_domain(property_repo)
    if not domain_str:
        domain_str = 'None'

    # Get list of index column names.
    labels_in_display_order = get_labels_in_display_order(
        index_repo=index_repo,
        property_repo=property_repo,
    )

    # Get categories as an ordered list (granularity and labels).
    discrete_categories = get_all_discrete_categories(property_repo)
    sorted_categories = sort_categories(discrete_categories, labels_in_display_order)
    def _get_granularity(cat):
        try:
            return structure_repo.get_by_labels(cat).granularity
        except EmptyCollectionError:
            return None
    raw_granularity = [_get_granularity(cat) for cat in sorted_categories]
    formatted_granularity = format_granularity(raw_granularity)
    category_list = [
        f"{g}  {', '.join(c)}"
        for g, c in zip(formatted_granularity, sorted_categories)
    ]
    if not category_list:
        category_list = ['None']

    # Get list of weight group names.
    weight_groups = sorted(weight_group_repo.get_all(), key=lambda x: x.name)
    if weight_groups:
        try:
            default_group = get_default_weight_group(property_repo, weight_group_repo)
            default_group_id = default_group.id
        except RuntimeError:
            default_group_id = None

        def make_note(group):
            if group.id == default_group_id:
                return ' (default)' if group.is_complete else ' (default, incomplete)'
            return '' if group.is_complete else ' (incomplete)'

        weights_list = [f'{x.name}{make_note(x)}' for x in weight_groups]
    else:
        weights_list = ['None']

    # Get list of crosswalk nodes and names.
    crosswalks = crosswalk_repo.get_all()
    if crosswalks:
        def make_note(crosswalk):
            if crosswalk.is_default:
                if crosswalk.is_locally_complete:
                    return ' (default)'
                return ' (default, locally incomplete)'
            if crosswalk.is_locally_complete:
                return ''
            return ' (locally incomplete)'

        crosswalks = sorted(crosswalks, key=lambda x: (x.other_unique_id, x.id))
        crosswalks_list = []
        for key, grp in groupby(crosswalks, key=lambda x: x.other_unique_id):
            first_crosswalk = next(grp)
            node_ref = (first_crosswalk.other_filename_hint
                            or f'[{first_crosswalk.other_unique_id[:7]}]')
            sub_list = sorted(
                f'{x.name}{make_note(x)}' for x in chain([first_crosswalk], grp)
            )
            crosswalks_list.append(f'{node_ref}: {", ".join(sub_list)}')
    else:
        crosswalks_list = ['None']

    return {
        'domain_str': domain_str,
        'category_list': category_list,
        'weights_list': weights_list,
        'crosswalks_list': crosswalks_list,
    }
