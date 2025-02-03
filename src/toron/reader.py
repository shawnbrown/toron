"""NodeReader implementation for the Toron project."""

import os
import weakref
from contextlib import closing, suppress
from tempfile import NamedTemporaryFile
from toron._typing import (
    Dict,
    Optional,
    Self,
    Tuple,
)


class NodeReader(object):
    """An iterator for base level TopoNode data."""
    def __init__(self) -> None:
        # Create temp file and get its path (resolve symlinks with realpath).
        with closing(NamedTemporaryFile(delete=False)) as f:
            self._filepath = os.path.realpath(f.name)

        # Assign finalizer as a `close()` method.
        self.close = weakref.finalize(self, self._cleanup)

        self._data = iter([])

    def _cleanup(self):
        with suppress(FileNotFoundError):
            os.unlink(self._filepath)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Tuple[int, Dict[str, str], Optional[float]]:
        return next(self._data)
