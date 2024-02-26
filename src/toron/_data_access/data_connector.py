"""DataConnector and related objects using SQLite."""

import os
import tempfile

from toron._typing import (
    Optional,
)

from .base_classes import BaseDataConnector


class DataConnector(BaseDataConnector):
    # Absolute path of class instance's database (None if file in memory).
    _current_working_path: Optional[str] = None

    def __init__(self, cache_to_drive: bool = False) -> None:
        """Initialize a new node instance."""
        if cache_to_drive:
            temp_f = tempfile.NamedTemporaryFile(suffix='.toron', delete=False)
            temp_f.close()
            database_path = os.path.abspath(temp_f.name)
            self._current_working_path = database_path
        else:
            database_path = ':memory:'
            self._current_working_path = None
