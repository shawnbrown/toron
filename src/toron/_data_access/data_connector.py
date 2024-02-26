"""DataConnector and related objects using SQLite."""

from .base_classes import BaseDataConnector


class DataConnector(BaseDataConnector):
    def __init__(self, cache_to_drive: bool = False) -> None:
        """Initialize a new node instance."""
