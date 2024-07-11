"""Tools for building weighted crosswalks between sets of labels."""

from ._typing import (
    Dict,
    Iterable,
    Optional,
    Sequence,
    Union,
)


class Mapper(object):
    """Class to build a weighted crosswalk between sets of labels."""
    def __init__(
        self,
        crosswalk_name: str,
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        pass
