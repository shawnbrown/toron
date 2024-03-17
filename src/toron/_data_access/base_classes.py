"""Abstract base classes for data access objects."""

import os
from abc import ABC, abstractmethod

from toron._typing import (
    Generic,
    Self,
    TypeVar,
    Union,
)


T = TypeVar('T')


class BaseDataConnector(ABC, Generic[T]):
    @abstractmethod
    def __init__(self) -> None:
        """Initialize a new node instance."""

    @property
    @abstractmethod
    def unique_id(self) -> str:
        """Unique identifier for the node object."""

    @abstractmethod
    def acquire_resource(self) -> T:
        """Return an appropriate object to interact with a node's data.

        If a node's storage backend is a database, the resource
        might be a DBAPI2 Connection. If other storage backends are
        implemented, the resource could be an HDF5 group object, a
        collection of Parquet tables, etc.
        """

    @abstractmethod
    def release_resource(self, resource: T) -> None:
        """Release the acquired data *resource*.

        This method should release the given data *resource* object
        if it's acceptable to do so. In the case of connections to
        a temporary database, this method may not do anything at all
        (since closing such a connection would delete the data). But
        if the resource is a connection to an on-drive file or other
        persistent storage, then this method should release it to
        free up system resources (file handles, network connections,
        etc.).
        """

    @abstractmethod
    def to_file(
        self, path: Union[str, bytes, os.PathLike], *, fsync: bool = True
    ) -> None:
        """Write node data to a file.

        Parameters
        ----------
        path : :py:term:`path-like-object`
            File path where the node data should be saved.
        fsync : bool, default True
            Immediately flush any cached data to drive storage.

            On systems where it's not possible to guarantee that data
            is flushed, this method should still make a best-effort
            attempt to do so.
        """

    @classmethod
    @abstractmethod
    def from_file(
        cls, path: Union[str, bytes, os.PathLike]
    ) -> Self:
        """Read a node file into a new data connector object.

        Parameters
        ----------
        path : :py:term:`path-like-object`
            File path containing the node data.
        """
