"""Handling for attribute selectors (using CSS-inspired syntax)."""

from ._typing import Literal, Optional


class Selector(object):
    def __init__(
        self,
        attr: str,
        op: Literal['=', '~=', '|=', '^=', '$=', '*=', None] = None,
        val: Optional[str] = None,
        ignore_case: Optional[bool] = None,
    ) -> None:
        self.attr = attr
        self.op = op
        self.val = val
        self.ignore_case = ignore_case

