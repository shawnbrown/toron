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
        """Initialize Selector instance."""
        if (op and not val) or (val and not op):
            raise TypeError('must provide `op` and `val` together')

        if ignore_case and not (op and val):
            raise TypeError('got `ignore_case`, must also provide `op` and `val`')

        self.attr = attr
        self.op = op
        self.val = val
        self.ignore_case = ignore_case

