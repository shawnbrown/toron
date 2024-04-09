"""Node implementation for the Toron project."""

from contextlib import contextmanager
from typing import (
    Any,
    Dict,
    Generator,
)

from . import _data_access


class Node(object):
    def __init__(
        self,
        *,
        backend: str = 'DAL1',
        **kwds: Dict[str, Any],
    ) -> None:
        self._dal = _data_access.get_data_access_layer(backend)
        self._connector = self._dal.DataConnector(**kwds)

    @contextmanager
    def _managed_resource(self) -> Generator[Any, None, None]:
        resource = self._connector.acquire_resource()
        try:
            yield resource
        finally:
            self._connector.release_resource(resource)
