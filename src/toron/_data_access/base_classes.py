"""Abstract base classes for data access objects."""

from abc import ABC, abstractmethod


class BaseDataConnector(ABC):
    @abstractmethod
    def __init__(self, cache_to_drive: bool = False) -> None:
        """Initialize a new node instance."""
