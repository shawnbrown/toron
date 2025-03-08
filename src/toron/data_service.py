"""Application logic functions that interact with repository objects."""

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
    overload,
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
    BaseWeightGroupRepository,
    BaseWeightRepository,
    BaseQuantityRepository,
    BaseRelationRepository,
    BaseCrosswalkRepository,
    BasePropertyRepository,
    BaseStructureRepository,
    Index,
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
    for index_id in index_repo.get_index_ids(ordered=True):
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
    relations = relation_repo.find_by_ids(index_id=index_id)
    other_index_ids = set()
    for relation in list(relations):
        if relation.mapping_level:
            # For now, prevent index deletion when there
            # are ambiguous relations. In the future, this
            # restriction should be removed by re-mapping
            # these relations using matching labels.
            raise ValueError(
                f'cannot delete index_id {index_id}, '
                f'some associated crosswalk relations are '
                f'ambiguous\n'
                f'\n'
                f'Crosswalks with ambiguous relations must '
                f'be removed before deleting index records. '
                f'Afterwards, these crosswalks can be re-added.'
            )

        other_index_ids.add(relation.other_index_id)
        relation_repo.delete(relation.id)

    # Rebuild proportions for remaining relations.
    for other_index_id in other_index_ids:
        for crosswalk in crosswalk_repo.get_all():
            relation_repo.refresh_proportions(crosswalk.id, other_index_id)

    # Remove existing Index record.
    index_repo.delete(index_id)


def get_quantity_value_sum(
    location_id: int,
    attribute_group_id: int,
    quantity_repo: BaseQuantityRepository,
) -> Optional[float]:
    """Return sum of quantities matching location_id and attribute_group_id."""
    quantities = quantity_repo.find_by_ids(
        location_id=location_id,
        attribute_group_id=attribute_group_id,
    )
    quantity = next(quantities, None)
    if quantity is None:
        return None
    return quantity.value + sum(x.value for x in quantities)


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
          a RuntimeError is raised.

        These conditions should be assured by the parent operation
        before calling this function.
    """
    # Assign shorter func name and reduce dot-lookups.
    get_weight = weight_repo.get_by_weight_group_id_and_index_id

    # Get sum of weight values associated with index_ids.
    group_weight = 0.0
    for index_id in index_ids:
        weight = get_weight(weight_group_id, index_id)
        if weight:
            group_weight += weight.value
        else:
            raise RuntimeError(f'no weight value matching weight_group_id '
                               f'{weight_group_id} and index_id {index_id}')

    group_count = len(index_ids)  # Get count for zero-division handling.

    # Yield disaggregated values for associated index records.
    for index_id in index_ids:
        # OK to cast() since `group_weight` loop would have already errored.
        weight = cast(Weight, get_weight(weight_group_id, index_id))

        try:
            proportion = weight.value / group_weight
        except ZeroDivisionError:
            proportion = 1 / group_count

        yield (index_id, quantity_value * proportion)


def find_crosswalks_by_ref(
    ref: str,
    crosswalk_repo: BaseCrosswalkRepository,
) -> List[Crosswalk]:
    """Find crosswalks that match the given node reference."""
    # Try to match by exact 'other_unique_id'.
    matches = list(crosswalk_repo.find_by_other_unique_id(ref))
    if matches:
        return matches

    # Try to match by exact 'other_filename_hint'.
    matches = list(crosswalk_repo.find_by_other_filename_hint(ref))
    if matches:
        return matches

    # Try to match by stem of 'other_filename_hint' (name without '.toron' extension).
    matches = list(crosswalk_repo.find_by_other_filename_hint(f'{ref}.toron'))
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
    ``crosswalk_id``values matched by the selector with the
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


@overload
def get_default_weight_group(
    property_repo: BasePropertyRepository,
    weight_group_repo: BaseWeightGroupRepository,
    required: Literal[False] = False,
) -> Optional[WeightGroup]:
    ...
@overload
def get_default_weight_group(
    property_repo: BasePropertyRepository,
    weight_group_repo: BaseWeightGroupRepository,
    required: Literal[True],
) -> WeightGroup:
    ...
def get_default_weight_group(property_repo, weight_group_repo, required=False):
    """Return the node's default weight group (if any)."""
    weight_group_id = property_repo.get('default_weight_group_id')
    if isinstance(weight_group_id, int):
        weight_group = weight_group_repo.get(weight_group_id)
        if weight_group:
            return weight_group

    if required:
        raise RuntimeError('no default weight group is defined')
    return None


def get_all_discrete_categories(
    column_manager: BaseColumnManager,
    property_repo: BasePropertyRepository,
) -> List[Set[str]]:
    values = cast(Optional[List[List[str]]], property_repo.get('discrete_categories'))
    if values:
        return [set(x) for x in values]

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

    distinct_labels = index_repo.get_distinct_labels(
        *columns, include_undefined=False
    )

    total_uncertainty = 0.0
    for labels in distinct_labels:
        criteria = dict(zip(columns, labels))
        records = aux_index_repo.find_by_label(criteria, include_undefined=False)
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
    domain = property_repo.get('domain') or {}
    return check_type(domain, dict)


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
    structure = structure_repo.get_by_bits(cast(Tuple[Literal[0, 1]], bits))
    granularity_str = str(structure.granularity) if structure else 'None'

    # Get list of weight group names.
    weight_groups = sorted(weight_group_repo.get_all(), key=lambda x: x.name)
    if weight_groups:
        default_group = get_default_weight_group(property_repo, weight_group_repo)
        default_group_id = getattr(default_group, 'id', None)
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
