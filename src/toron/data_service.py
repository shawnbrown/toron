"""Application logic functions that interact with repository objects."""

from itertools import compress
from math import log2

from toron._typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    cast,
    overload,
)

from .categories import (
    make_structure,
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
from ._utils import (
    check_type,
    SequenceHash,
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
    index_criteria: Dict[str, str],
    weight_group_id: int,
    index_repo: BaseIndexRepository,
    weight_repo: BaseWeightRepository,
) -> Iterator[Tuple[Index, float]]:
    """Return Index records and disaggregated results for given value.

    .. important::

        This is an internal (non-user-facing) function. It should only
        be used when certain conditions are met:

        * The *quantity_value* should represent an extensive property
          (not an intensive property). If an intensive property is
          given--like a percentage or temperature--the results will
          be nonsensical.
        * The *index_criteria* should use keys that correspond with an
          existing structure record. If this condition is not satisfied,
          the level of granularity may be invalid and the disaggregation
          process could return misallocated results.
        * The *index_criteria* should match one or more index records.
          If there are no matching records, a RuntimeError is raised.
        * The weight group should exist and it should be complete--a
          weight should exist for every index record. If there is no
          matching weight, a RuntimeError is raised.
        * The given ``index_repo`` and ``weight_repo`` objects must
          use different cursor instances. If the same instance is used
          for both objects, the output could be incomplete. Many cursor
          implementations (like DBAPI2 cursors) return stateful
          iterators which will truncate the output of earlier results
          when given new queries to execute.

        These conditions should be assured by the parent operation
        before calling this function.
    """
    # NOTE: The following implementation calls find_by_label() twice.
    # This is done so that the entire collection of returned items is
    # not loaded into memory. It's possible that a very large number of
    # Index records match the *index_criteria*.

    # TODO: Once this code is in production, investigate ways to
    # optimize this function--there could be significant performance
    # improvements to be gained.

    # Get sum of index values and count of index records for location.
    group_weight = 0.0
    group_count = 0
    for index in index_repo.find_by_label(index_criteria):
        weight = weight_repo.get_by_weight_group_id_and_index_id(
            weight_group_id, index.id
        )
        if not weight:
            raise RuntimeError(
                f'no weight value matching weight_group_id {weight_group_id} '
                f'and index_id {index.id}'
            )
        group_weight += weight.value
        group_count += 1

    if group_count == 0:
        raise RuntimeError(f'no index matching {index_criteria!r}')

    # Yield disaggregated values for associated index records.
    for index in index_repo.find_by_label(index_criteria):
        # Using cast() because any missing `weight` values would have
        # already raised an error when summing `group_weight` above.
        weight = cast(
            Weight,
            weight_repo.get_by_weight_group_id_and_index_id(weight_group_id, index.id),
        )
        try:
            proportion = weight.value / group_weight
        except ZeroDivisionError:
            proportion = 1 / group_count

        yield (index, quantity_value * proportion)


def find_crosswalks_by_node_reference(
    node_reference: str,
    crosswalk_repo: BaseCrosswalkRepository,
) -> List[Crosswalk]:
    """Search repository and return crosswalks from matching nodes."""
    # Try to match by exact 'other_unique_id'.
    matches = list(crosswalk_repo.find_by_other_unique_id(node_reference))
    if matches:
        return matches

    # Try to match by exact 'other_filename_hint'.
    matches = list(crosswalk_repo.find_by_other_filename_hint(node_reference))
    if matches:
        return matches

    # Try to match by stem of 'other_filename_hint' (name without '.toron' extension).
    matches = list(crosswalk_repo.find_by_other_filename_hint(f'{node_reference}.toron'))
    if matches:
        return matches

    # Try to match by short code of 'other_unique_id'.
    if isinstance(node_reference, str) and len(node_reference) >= 7:
        matches = crosswalk_repo.get_all()
        return [x for x in matches if x.other_unique_id.startswith(node_reference)]

    return []  # Return empty list if no match.


def set_default_weight_group(
    weight_group: WeightGroup,
    property_repo: BasePropertyRepository,
) -> None:
    """Sets the node's default weight group."""
    property_repo.add('default_weight_group_id', weight_group.id)


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
    values: Optional[List[List[str]]]
    values = property_repo.get('discrete_categories')  # type: ignore [assignment]
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
) -> None:
    # Remove existing structure.
    for structure in structure_repo.get_all():
        structure_repo.delete(structure.id)

    # Regenerate new structure.
    categories = get_all_discrete_categories(column_manager, property_repo)
    columns = column_manager.get_columns()
    for cat in make_structure(categories):
        granularity = calculate_granularity(list(cat), index_repo, aux_index_repo)
        bits = [(x in cat) for x in columns]
        structure_repo.add(granularity, *bits)


def refresh_structure_granularity(
    column_manager: BaseColumnManager,
    structure_repo: BaseStructureRepository,
    index_repo: BaseIndexRepository,
    aux_index_repo: BaseIndexRepository,
) -> None:
    label_columns = column_manager.get_columns()
    for structure in structure_repo.get_all():
        category = list(compress(label_columns, structure.bits))
        granularity = calculate_granularity(category, index_repo, aux_index_repo)
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


def get_node_info(
    property_repo: BasePropertyRepository,
    #column_manager: BaseColumnManager,
    #structure_repo: BaseStructureRepository,
    #weight_group_repo: BaseWeightGroupRepository,
    #attribute_repo: BaseAttributeGroupRepository,
    #crosswalk_repo: BaseCrosswalkRepository,
) -> Dict[str, Any]:
    """Return dictionary of node information appropriate for repr."""
    domain = dict(sorted(get_domain(property_repo).items())) or None

    return {
        'domain': domain,
    }
