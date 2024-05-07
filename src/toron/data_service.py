"""Application logic functions that interact with repository objects."""

from toron._typing import (
    List,
)

from .data_models import (
    Crosswalk, BaseCrosswalkRepository,
)


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
