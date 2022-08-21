"""Handling for attribute selectors (using CSS-inspired syntax)."""

from ._typing import Literal, Mapping, Optional, Tuple


class Selector(object):
    """Callable (function-like) object to check for matching key/value
    pairs in a dictionary.

    Match when key 'A' is defined and its value is any non-empty
    string::

        >>> selector = Selector('A')
        >>> selector({'A': 'xyzzy'})
        True
        >>> selector({'A': 'plover'})
        True
        >>> selector({'B': 'plugh'})
        False
        >>> selector({'A': '', 'B': 'plugh'})
        False

    Match when key 'A' is defined and value is exactly 'xyzzy'::

        >>> selector = Selector('A', '=', 'xyzzy')
        >>> selector({'A': 'xyzzy', 'B': 'plugh'})
        True
        >>> selector({'A': 'plover', 'B': 'plugh'})
        False

    Match when key 'A' is defined and value is a case-insensitive
    match to 'Xyzzy'::

        >>> selector = Selector('A', '=', 'Xyzzy', ignore_case=True)
        >>> selector({'A': 'xyzzy'})
        True
        >>> selector({'A': 'XYZZY'})
        True
        >>> selector({'a': 'xyzzy'})
        False

    Match behavior can be changed by providing different *op* values:

    +----------+--------------------------------------------------+
    | ``op``   | matches                                          |
    +==========+==================================================+
    | ``'='``  | exact match                                      |
    +----------+--------------------------------------------------+
    | ``'~='`` | whitespace separated list containing *val*       |
    +----------+--------------------------------------------------+
    | ``'|='`` | string starting with *val* followed by "``-``"   |
    |          | or exact match                                   |
    +----------+--------------------------------------------------+
    | ``'^='`` | string starting with *val*                       |
    +----------+--------------------------------------------------+
    | ``'$='`` | string ending with *val*                         |
    +----------+--------------------------------------------------+
    | ``'*='`` | string containing *val* as a substring           |
    +----------+--------------------------------------------------+
    """
    def __init__(
        self,
        attr: str,
        op: Literal['=', '~=', '|=', '^=', '$=', '*=', None] = None,
        val: Optional[str] = None,
        ignore_case: Optional[bool] = None,
    ) -> None:
        """Initialize Selector instance."""
        if bool(op) != bool(val):
            raise TypeError('must use `op` and `val` together or not at all')

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
        """Return eval-able string representation of selector.

        .. code-block::

            >>> selector = Selector('A', '=', 'xyzzy')
            >>> repr(selector)
            "Selector('A', '=', 'xyzzy')"
        """
        cls_name = self.__class__.__name__
        if not self.op:
            return f'{cls_name}({self.attr!r})'
        if self.ignore_case:
            return f'{cls_name}({self.attr!r}, {self.op!r}, {self.val!r}, ignore_case=True)'
        return f'{cls_name}({self.attr!r}, {self.op!r}, {self.val!r})'

    def __str__(self) -> str:
        """Return CSS-like string of selector.

        .. code-block::

            >>> selector = Selector('A', '=', 'xyzzy')
            >>> str(selector)
            '[A="xyzzy"]'
        """
        if not self.val:
            return f'[{self.attr}]'

        value = self.val.replace(r'"', r'\"')
        if self.ignore_case:
            return f'[{self.attr}{self.op}"{value}" i]'
        return f'[{self.attr}{self.op}"{value}"]'

    @property
    def specificity(self) -> Tuple[int, int]:
        """Return specificity value of selector.

        Selectors that match attributes with any value will have a
        specificity of `(1, 0)` and Selectors that match attributes
        with a specific value will have a specificity of `(1, 1)`.
        The given `op` and use of `ignore_case` have no effect on
        specificity.
        """
        if self.val:
            return (1, 1)
        return (1, 0)

