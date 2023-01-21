"""Graph implementation and functions for the Toron project."""

from ._typing import (
    Literal,
    Optional,
    TypeAlias,
)

from ._utils import (
    TabularData,
    make_readerlike,
)
from .node import Node


Direction: TypeAlias = Literal['->', '-->', '<->', '<-->', '<-', '<--']


def add_edge(
    data : TabularData,
    name : str,
    left_node : Node,
    direction : Direction,
    right_node : Node,
    selector : Optional[str] = None,
) -> None:
    raise NotImplementedError
