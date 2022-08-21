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

        # Define appropriate match function.
        if op is None:  # Any truthy value.
            match_func = lambda a, b: bool(b)
        elif op == '=':  # Exact match.
            match_func = lambda a, b: a == b
        elif op == '~=':  # Contained in whitespace separated list.
            match_func = lambda a, b: a in b.split()
        else:
            raise ValueError(f'unknown operator: {op!r}')

        self._match_func = match_func

    def __call__(self, dict_row):
        return self._match_func(self.val, dict_row.get(self.attr, ''))

