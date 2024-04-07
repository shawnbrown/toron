"""Node implementation for the Toron project."""

from typing import (
    Any,
    Dict,
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
