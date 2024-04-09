"""Node implementation for the Toron project."""

from contextlib import contextmanager
from typing import (
    Any,
    Dict,
    Generator,
    Optional,
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

    @contextmanager
    def _managed_reader(
        self, data_resource: Optional[Any] = None
    ) -> Generator[Any, None, None]:
        if data_resource:
            reader = self._connector.acquire_data_reader(data_resource)
            try:
                yield reader
            finally:
                self._connector.release_data_reader(reader)
        else:
            with self._managed_resource() as data_resource:
                reader = self._connector.acquire_data_reader(data_resource)
                try:
                    yield reader
                finally:
                    self._connector.release_data_reader(reader)
