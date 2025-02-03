"""NodeReader implementation for the Toron project."""

from toron._typing import (
    Dict,
    Optional,
    Self,
    Tuple,
)


class NodeReader(object):
    """An iterator for base level TopoNode data."""
    def __init__(self) -> None:
        self._data = iter([])

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Tuple[int, Dict[str, str], Optional[float]]:
        return next(self._data)
