"""Node implementation for the Toron project."""

from contextlib import contextmanager
from typing import (
    Any,
    Dict,
    Generator,
    Optional,
    Tuple,
)

from . import data_access


class Node(object):
    def __init__(
        self,
        *,
        backend: str = 'DAL1',
        **kwds: Dict[str, Any],
    ) -> None:
        self._dal = data_access.get_data_access_layer(backend)
        self._connector = self._dal.DataConnector(**kwds)

    @contextmanager
    def _managed_connection(self) -> Generator[Any, None, None]:
        connection = self._connector.acquire_connection()
        try:
            yield connection
        finally:
            self._connector.release_connection(connection)

    @contextmanager
    def _managed_cursor(
        self, connection: Optional[Any] = None
    ) -> Generator[Any, None, None]:
        if connection:
            cursor = self._connector.acquire_cursor(connection)
            try:
                yield cursor
            finally:
                self._connector.release_cursor(cursor)
        else:
            with self._managed_connection() as connection:
                cursor = self._connector.acquire_cursor(connection)
                try:
                    yield cursor
                finally:
                    self._connector.release_cursor(cursor)

    def add_columns(self, column: str, *columns: str) -> None:
        with self._managed_cursor() as cursor:
            manager = self._dal.ColumnManager(cursor)
            manager.add_columns(column, *columns)

    @property
    def columns(self) -> Tuple[str, ...]:
        with self._managed_cursor() as cursor:
            return self._dal.ColumnManager(cursor).get_columns()

    def rename_columns(self, mapping: Dict[str, str]) -> None:
        with self._managed_cursor() as cursor:
            manager = self._dal.ColumnManager(cursor)
            manager.update_columns(mapping)

    def delete_columns(self, column: str, *columns: str) -> None:
        with self._managed_cursor() as cursor:
            manager = self._dal.ColumnManager(cursor)
            manager.delete_columns(column, *columns)
