"""Application logic functions that interact with repository objects."""

from math import log2

from toron._typing import (
    Dict,
    List,
    Optional,
    Set,
)

from .categories import (
    make_structure,
)
from .data_models import (
    BaseColumnManager,
    BaseIndexRepository,
    BaseWeightRepository,
    BaseRelationRepository,
    BaseCrosswalkRepository,
    BasePropertyRepository,
    BaseStructureRepository,
    Crosswalk,
    JsonTypes,
)


def delete_index_record(
    index_id: int,
    index_repo: BaseIndexRepository,
    weight_repo: BaseWeightRepository,
    relation_repo: BaseRelationRepository,
) -> None:
    """Delete index record and associated weights and relations."""
    # Remove associated weight records.
    weights = weight_repo.find_by_index_id(index_id)
    for weight in list(weights):
        weight_repo.delete(weight.id)

    # Remove associated relation records.
    relations = relation_repo.find_by_index_id(index_id)
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
    relation_repo.refresh_proportions(other_index_ids)

    # Remove existing Index record.
    index_repo.delete(index_id)


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
    alt_index_repo: BaseIndexRepository,
) -> float:
    r"""Return the granularity of a given level (defined by *columns*).

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
    total_cardinality = index_repo.get_cardnality(include_undefined=False)
    if not total_cardinality:
        return 0.0  # <- EXIT!

    distinct_labels = index_repo.get_distinct_labels(
        *columns, include_undefined=False
    )

    total_uncertainty = 0.0
    for labels in distinct_labels:
        criteria = dict(zip(columns, labels))
        records = alt_index_repo.find_by_label(criteria, include_undefined=False)
        cardnality = sum(1 for x in records)
        total_uncertainty += (cardnality / total_cardinality) * log2(cardnality)

    return log2(total_cardinality) - total_uncertainty


def rebuild_structure_table(
    column_manager: BaseColumnManager,
    property_repo: BasePropertyRepository,
    structure_repo: BaseStructureRepository,
    index_repo: BaseIndexRepository,
    alt_index_repo: BaseIndexRepository,
) -> None:
    # Remove existing structure.
    for structure in structure_repo.get_all():
        structure_repo.delete(structure.id)

    # Regenerate new structure.
    categories = get_all_discrete_categories(column_manager, property_repo)
    columns = column_manager.get_columns()
    for cat in make_structure(categories):
        if cat:
            granularity = calculate_granularity(list(cat), index_repo, alt_index_repo)
        else:
            granularity = 0.0

        bits = [(x in cat) for x in columns]
        structure_repo.add(granularity, *bits)
