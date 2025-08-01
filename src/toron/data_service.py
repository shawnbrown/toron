"""Application logic functions that interact with repository objects."""

from collections import Counter
from itertools import chain, compress, groupby
from math import log2

from toron._typing import (
    Any,
    Callable,
    Collection,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

from .categories import (
    make_structure,
    minimize_discrete_categories,
)
from .data_models import (
    COMMON_RESERVED_IDENTIFIERS,
    BaseAttributeGroupRepository,
    BaseColumnManager,
    BaseIndexRepository,
    BaseLocationRepository,
    BaseWeightGroupRepository,
    BaseWeightRepository,
    BaseQuantityRepository,
    BaseRelationRepository,
    BaseCrosswalkRepository,
    BasePropertyRepository,
    BaseStructureRepository,
    Index,
    Location,
    AttributeGroup,
    Weight,
    WeightGroup,
    Crosswalk,
    JsonTypes,
)
from .selectors import (
    parse_selector,
    get_greatest_unique_specificity,
)
from ._utils import (
    check_type,
    SequenceHash,
    ToronWarning,
    BitFlags,
)


def validate_new_index_columns(
    new_column_names: Iterable[str],
    reserved_identifiers: Set[str],
    column_manager: BaseColumnManager,
    property_repo: BasePropertyRepository,
    attribute_repo: BaseAttributeGroupRepository,
) -> None:
    """Raise a ValueError if a new column conflicts with an existing
    index column, the domain, or an attribute name.
    """
    all_reserved_identifiers = \
        reserved_identifiers.union(COMMON_RESERVED_IDENTIFIERS)
    existing_columns = set(column_manager.get_columns())
    domain_keys = set(get_domain(property_repo).keys())
    attribute_names = set(attribute_repo.get_all_attribute_names())

    for col in new_column_names:
        if col in all_reserved_identifiers:
            raise ValueError(
                f'cannot alter columns, {col!r} is a reserved identifier'
            )

        if col in existing_columns:
            raise ValueError(
                f'cannot alter columns, {col!r} is already an index column'
            )

        if col in domain_keys:
            raise ValueError(
                f'cannot alter columns, {col!r} is used in the domain'
            )

        if col in attribute_names:
            raise ValueError(
                f'cannot alter columns, {col!r} is used as an attribute '
                f'name'
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
    crosswalk_repo: BaseCrosswalkRepository,
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
        if not next(aux_index_repo.filter_index_ids_by_label(criteria), None):
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
            if not next(aux_index_repo.filter_index_ids_by_label(criteria), None):
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
        nonmatching_index = not next(aux_index_repo.filter_index_ids_by_label(criteria), None)

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
        if not next(quantities, None):
            yield attr_group


def find_locations_without_quantity(
    location_repo: BaseLocationRepository,
    alt_quantity_repo: BaseQuantityRepository,
) -> Iterator[Location]:
    """Find Location records that have no matching Quantity."""
    for location in location_repo.find_all():
        quantities = alt_quantity_repo.find(location_id=location.id)
        if not next(quantities, None):
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
    crosswalk_repo: BaseCrosswalkRepository,
) -> List[Crosswalk]:
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
    crosswalk_repo: BaseCrosswalkRepository,
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
        if not isinstance(weight_group_id, int):
            raise TypeError(
                f"'default_weight_group_id' property must be int, got "
                f"{weight_group_id.__class__.__qualname__}: {weight_group_id!r}"
            )
        return weight_group_repo.get(weight_group_id)
    except KeyError:
        raise RuntimeError('no default weight group is defined')


def get_all_discrete_categories(
    column_manager: BaseColumnManager,
    property_repo: BasePropertyRepository,
) -> List[Set[str]]:
    try:
        values = cast(List[List[str]], property_repo.get('discrete_categories'))
        return [set(x) for x in values]
    except KeyError:
        columns = column_manager.get_columns()
        if columns:
            return [set(columns)]  # Default to whole space.
        return []  # Empty when no columns defined.


def rename_discrete_categories(
    mapping: Dict[str, str],
    column_manager: BaseColumnManager,
    property_repo: BasePropertyRepository,
) -> None:
    categories = get_all_discrete_categories(column_manager, property_repo)
    do_rename = lambda cat: {mapping.get(x, x) for x in cat}
    category_sets = [do_rename(cat) for cat in categories]
    category_lists: JsonTypes = [list(cat) for cat in category_sets]
    property_repo.update('discrete_categories', category_lists)


def calculate_granularity(
    columns: List[str],
    index_repo: BaseIndexRepository,
    aux_index_repo: BaseIndexRepository,
) -> Optional[float]:
    r"""Return granularity of a given level--as defined by *columns*.

    If *columns* list is empty or if the index contains no records
    (other than the "undefined" record), then ``None`` will be returned.

    This function implements a Shannon entropy based metric for the
    "granularity measure of a partition" as described on p. 293 of:

        MARK J. WIERMAN (1999) MEASURING UNCERTAINTY IN ROUGH SET
        THEORY, International Journal of General Systems, 28:4-5,
        283-297, DOI: 10.1080/03081079908935239

    In PROBABILISTIC APPROACHES TO ROUGH SETS (Y. Y. Yao, 2003),
    Yiyu Yao presents the same equation in Eq. (6), using a form
    more useful for our implimentation:

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

    distinct_labels = index_repo.find_distinct_labels(
        *columns, include_undefined=False
    )

    total_uncertainty = 0.0
    for labels in distinct_labels:
        criteria = dict(zip(columns, labels))
        records = aux_index_repo.filter_index_ids_by_label(criteria, include_undefined=False)
        cardniality = sum(1 for x in records)
        total_uncertainty += (cardniality / total_cardinality) * log2(cardniality)

    return log2(total_cardinality) - total_uncertainty


def rebuild_structure_table(
    column_manager: BaseColumnManager,
    property_repo: BasePropertyRepository,
    structure_repo: BaseStructureRepository,
    index_repo: BaseIndexRepository,
    aux_index_repo: BaseIndexRepository,
    optimizations: Optional[Dict[str, Callable]] = None
) -> None:
    make_granularity = (
        optimizations.get('calculate_granularity', calculate_granularity)
        if optimizations
        else calculate_granularity
    )

    # Remove existing structure.
    for structure in structure_repo.get_all():
        structure_repo.delete(structure.id)

    # Regenerate new structure.
    categories = get_all_discrete_categories(column_manager, property_repo)
    columns = column_manager.get_columns()
    for cat in make_structure(categories):
        granularity = make_granularity(list(cat), index_repo, aux_index_repo)
        bits = [(x in cat) for x in columns]
        structure_repo.add(granularity, *bits)


def add_discrete_categories(
    categories: Iterable[Set[str]],
    column_manager: BaseColumnManager,
    property_repo: BasePropertyRepository,
) -> None:
    columns = column_manager.get_columns()
    if not columns:
        msg = 'must add index columns before defining categories'
        raise RuntimeError(msg)

    for field in set(chain(*categories)):
        if field not in columns:
            raise ValueError(
                f'invalid category value {field!r}, values '
                f'must be present in index columns'
            )

    existing_categories = get_all_discrete_categories(column_manager, property_repo)

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


def refresh_structure_granularity(
    column_manager: BaseColumnManager,
    structure_repo: BaseStructureRepository,
    index_repo: BaseIndexRepository,
    aux_index_repo: BaseIndexRepository,
    optimizations: Optional[Dict[str, Callable]] = None
) -> None:
    make_granularity = (
        optimizations.get('calculate_granularity', calculate_granularity)
        if optimizations
        else calculate_granularity
    )

    label_columns = column_manager.get_columns()
    for structure in structure_repo.get_all():
        category = list(compress(label_columns, structure.bits))
        granularity = make_granularity(category, index_repo, aux_index_repo)
        structure.granularity = granularity
        structure_repo.update(structure)


def set_domain(
    domain: Dict[str, str],
    column_manager: BaseColumnManager,
    attribute_repo: BaseAttributeGroupRepository,
    property_repo: BasePropertyRepository,
) -> None:
    """Set the node's domain."""
    # Check that domain does not conflict with index columns
    # or attribute names.
    columns = column_manager.get_columns()
    attribute_names = attribute_repo.get_all_attribute_names()
    for key in domain.keys():
        if key in columns:
            raise ValueError(
                f'cannot add domain, {key!r} is already used as '
                f'an index column'
            )

        if key in attribute_names:
            raise ValueError(
                f'cannot add domain, {key!r} is already used as '
                f'a quantity attribute'
            )

    # Save domain value in the property repository.
    property_repo.add_or_update(
        'domain',
        {check_type(k, str): check_type(v, str) for k, v in domain.items()},
    )


def get_domain(property_repo: BasePropertyRepository) -> Dict[str, str]:
    """Return the node's domain."""
    try:
        domain = property_repo.get('domain')
        return check_type(domain, dict)
    except KeyError:
        return {}


def get_node_info_text(
    property_repo: BasePropertyRepository,
    column_manager: BaseColumnManager,
    structure_repo: BaseStructureRepository,
    weight_group_repo: BaseWeightGroupRepository,
    attribute_repo: BaseAttributeGroupRepository,
    crosswalk_repo: BaseCrosswalkRepository,
) -> Dict[str, Union[List[str], str]]:
    """Return dictionary of node information appropriate for repr."""
    # Get list of domain items.
    domain = get_domain(property_repo)
    if domain:
        domain_list = [f'{k}: {v}' for k, v in sorted(domain.items())]
    else:
        domain_list = ['None']

    # Get list of index column names.
    index_columns = column_manager.get_columns()
    if index_columns:
        index_list = list(index_columns)
    else:
        index_list = ['None']

    # Get structure for highest granularity (whole space).
    bits = (1,) * len(index_columns)  # Bit pattern is all ones.
    try:
        structure = structure_repo.get_by_bits(cast(Tuple[Literal[0, 1]], bits))
        granularity_str = str(structure.granularity)
    except KeyError:
        granularity_str = 'None'

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

    attribute_list = sorted(attribute_repo.get_all_attribute_names())
    if not attribute_list:
        attribute_list = ['None']

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
        'domain_list': domain_list,
        'index_list': index_list,
        'granularity_str': granularity_str,
        'weights_list': weights_list,
        'attribute_list': attribute_list,
        'crosswalks_list': crosswalks_list,
    }
