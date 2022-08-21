"""Handling for attribute selectors (using CSS-inspired syntax)."""

from ._typing import Literal, Mapping, Optional, Tuple


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
        elif op == '|=':  # Starts with value followed by "-" or exact match.
            match_func = lambda a, b: b.startswith(f'{a}-') or a == b
        elif op == '^=':  # Starts with.
            match_func = lambda a, b: b.startswith(a)
        elif op == '$=':  # Ends with.
            match_func = lambda a, b: b.endswith(a)
        elif op == '*=':  # Matches value as substring.
            match_func = lambda a, b: a in b
        else:
            raise ValueError(f'unknown operator: {op!r}')

        # Assign match function to instance.
        if ignore_case:
            self._match_func = lambda a, b: match_func(a.upper(), b.upper())
        else:
            self._match_func = match_func

    def __call__(self, dict_row: Mapping[str, str]) -> bool:
        return self._match_func(self.val, dict_row.get(self.attr, ''))

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        if not self.op:
            return f'{cls_name}({self.attr!r})'
        if self.ignore_case:
            return f'{cls_name}({self.attr!r}, {self.op!r}, {self.val!r}, ignore_case=True)'
        return f'{cls_name}({self.attr!r}, {self.op!r}, {self.val!r})'

    def __str__(self) -> str:
        """Return CSS-like string of selector."""
        if not self.val:
            return f'[{self.attr}]'

        value = self.val.replace(r'"', r'\"')
        if self.ignore_case:
            return f'[{self.attr}{self.op}"{value}" i]'
        return f'[{self.attr}{self.op}"{value}"]'

    @property
    def specificity(self) -> Tuple[int, int]:
        """Return specificity value of selector.

        Selectors that match attributes of any value will have a
        specificity of (1, 0) and Selectors that match attributes
        of a specific value should have a specificity of (1, 1).
        The use of `ignore_case` has no effect on specificity.
        """
        if self.val:
            return (1, 1)
        return (1, 0)

