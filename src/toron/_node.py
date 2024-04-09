"""Node implementation for the Toron project."""

from contextlib import contextmanager
from typing import (
    Any,
    Dict,
    Generator,
    Optional,
    Tuple,
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

    def add_columns(self, column: str, *columns: str) -> None:
        with self._managed_reader() as data_reader:
            manager = self._dal.ColumnManager(data_reader)
            manager.add_columns(column, *columns)

    @property
    def columns(self) -> Tuple[str, ...]:
        with self._managed_reader() as data_reader:
            return self._dal.ColumnManager(data_reader).get_columns()

    def rename_columns(self, mapping: Dict[str, str]) -> None:
        with self._managed_reader() as data_reader:
            manager = self._dal.ColumnManager(data_reader)
            manager.update_columns(mapping)

    def delete_columns(self, column: str, *columns: str) -> None:
        with self._managed_reader() as data_reader:
            manager = self._dal.ColumnManager(data_reader)
            manager.delete_columns(column, *columns)
