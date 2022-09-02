"""Handling for attribute selectors (using CSS-inspired syntax)."""

from abc import ABC, abstractmethod
from ._typing import List, Literal, Mapping, Optional, Tuple, Union

from lark import Lark, Transformer, v_args


class SelectorBase(ABC):
    """Abstract base class for attribute selector objects."""
    @abstractmethod
    def __init__(self) -> None:
        """Initialize class instance."""
        raise NotImplementedError

    @abstractmethod
    def __call__(self, dict_row: Mapping[str, str]) -> bool:
        """Return True if selector matches values in *dict_row*."""
        raise NotImplementedError

    @abstractmethod
    def __repr__(self) -> str:
        """Return eval-able string representation of selector."""
        raise NotImplementedError

    @abstractmethod
    def __str__(self) -> str:
        """Return CSS-like string of selector."""
        raise NotImplementedError

    @abstractmethod
    def __eq__(self, other) -> bool:
        """Check if self is equal to other."""
        raise NotImplementedError

    @abstractmethod
    def __hash__(self) -> int:
        """Build and return the hash value of this instance."""
        raise NotImplementedError

    @property
    @abstractmethod
    def specificity(self) -> Tuple[int, int]:
        """Selector specificity value."""
        raise NotImplementedError


class SelectorContainer(SelectorBase):
    def __init__(
        self, selector_list: List[Union['SelectorContainer', 'SimpleSelector']]
    ) -> None:
        """Initialize class instance."""
        self.selector_list = selector_list

    def __contains__(self, item) -> bool:
        """Check if *item* is in `self.selector_list`."""
        return item in self.selector_list

    def __repr__(self) -> str:
        """Return eval-able string representation of selector."""
        cls_name = self.__class__.__name__
        selectors = ', '.join(repr(x) for x in self.selector_list)
        return f'{cls_name}([{selectors}])'

    def __eq__(self, other) -> bool:
        """Check if self is equal to other."""
        return _get_comparison_key(self) == _get_comparison_key(other)

    def __hash__(self) -> int:
        """Build and return the hash value of this instance."""
        return hash(_get_comparison_key(self))


class SimpleSelector(SelectorBase):
    """Callable (function-like) object to check for matching key/value
    pairs in a dictionary.

    Match when key 'A' is defined and its value is any non-empty
    string::

        >>> selector = SimpleSelector('A')
        >>> selector({'A': 'xyzzy'})
        True
        >>> selector({'A': 'plover'})
        True
        >>> selector({'B': 'plugh'})
        False
        >>> selector({'A': '', 'B': 'plugh'})
        False

    Match when key 'A' is defined and value is exactly 'xyzzy'::

        >>> selector = SimpleSelector('A', '=', 'xyzzy')
        >>> selector({'A': 'xyzzy', 'B': 'plugh'})
        True
        >>> selector({'A': 'plover', 'B': 'plugh'})
        False

    Match when key 'A' is defined and value is a case-insensitive
    match to 'Xyzzy'::

        >>> selector = SimpleSelector('A', '=', 'Xyzzy', ignore_case=True)
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
        """Initialize class instance."""
        if bool(op) != bool(val):
            raise TypeError('must use `op` and `val` together or not at all')

        if ignore_case and not (op and val):
            raise TypeError('got `ignore_case`, must also provide `op` and `val`')

        self._attr = attr
        self._op = op
        self._val = val
        self._ignore_case = bool(ignore_case)

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
        return self._match_func(self._val, dict_row.get(self._attr, ''))

    def __repr__(self) -> str:
        """Return eval-able string representation of selector.

        .. code-block::

            >>> selector = SimpleSelector('A', '=', 'xyzzy')
            >>> repr(selector)
            "SimpleSelector('A', '=', 'xyzzy')"
        """
        cls_name = self.__class__.__name__
        if not self._op:
            return f'{cls_name}({self._attr!r})'
        if self._ignore_case:
            return f'{cls_name}({self._attr!r}, {self._op!r}, {self._val!r}, ignore_case=True)'
        return f'{cls_name}({self._attr!r}, {self._op!r}, {self._val!r})'

    def __str__(self) -> str:
        """Return CSS-like string of selector.

        .. code-block::

            >>> selector = SimpleSelector('A', '=', 'xyzzy')
            >>> str(selector)
            '[A="xyzzy"]'
        """
        if not self._val:
            return f'[{self._attr}]'

        value = self._val.replace(r'"', r'\"')
        if self._ignore_case:
            return f'[{self._attr}{self._op}"{value}" i]'
        return f'[{self._attr}{self._op}"{value}"]'

    def __eq__(self, other) -> bool:
        """Check if self is equal to other."""
        return _get_comparison_key(self) == _get_comparison_key(other)

    def __hash__(self) -> int:
        """Build and return the hash value of this instance."""
        return hash(_get_comparison_key(self))

    @property
    def specificity(self) -> Tuple[int, int]:
        """SimpleSelector that match attributes with any value will
        have a specificity of `(1, 0)` and Selectors that match
        attributes with a specific value will have a specificity of
        `(1, 1)`. The given `op` and use of `ignore_case` have no
        effect on specificity.
        """
        if self._val:
            return (1, 1)
        return (1, 0)


def _get_comparison_key(
    selector: Union[SimpleSelector, SelectorContainer]
) ->  Tuple:
    """Returns a value suitable for comparing selectors for equality.

    .. code-block::

        >>> _get_comparison_key(SimpleSelector('aaa', '=', 'xxx'))
        (SimpleSelector, ('aaa', '=', 'xxx', False))
    """
    if isinstance(selector, SelectorContainer):
        cmp_keys = [_get_comparison_key(x) for x in selector.selector_list]
        return (selector.__class__, frozenset(cmp_keys))

    if isinstance(selector, SimpleSelector):
        val = selector._val
        ignore_case = selector._ignore_case
        if ignore_case and val:
            val = val.lower()
        return (selector.__class__, (selector._attr, selector._op, val, ignore_case))

    cls_name = selector.__class__.__name__
    raise ValueError(f'comparison key for {cls_name} type not implemented')


class MatchesAnySelector(SelectorContainer):
    """Callable (function-like) object to check that a dict_row
    contains at least one matching selector.

    This class is designed to mimic the "matches-any" selector--i.e.,
    the :is() pseudo-class. For details, see:

        https://www.w3.org/TR/selectors-4/#matches
    """
    def __new__(cls, selector_list):
        if len(selector_list) == 1:
            return selector_list[0]  # Return simple selector, if one item.
        return super().__new__(cls)

    def __call__(self, dict_row: Mapping[str, str]) -> bool:
        """Return True if selector matches values in *dict_row*."""
        for selector in self.selector_list:
            if selector(dict_row):
                return True
        return False

    def __str__(self) -> str:
        """Return CSS-like string of selector."""
        inner_str = ', '.join(str(x) for x in self.selector_list)
        return f':is({inner_str})'

    @property
    def specificity(self) -> Tuple[int, int]:
        """The specificity of a "matches-any" selector (i.e., the :is()
        pseudo-class) is the specificity of the most specific selector
        it contains.
        """
        return max(x.specificity for x in self.selector_list)


class NegationSelector(SelectorContainer):
    """
    This class is designed to mimic the "negation" selector--i.e.,
    the :not() pseudo-class. For details, see:

        https://www.w3.org/TR/selectors-4/#negation
    """
    def __call__(self, dict_row: Mapping[str, str]) -> bool:
        """Return True if selector matches values in *dict_row*."""
        for selector in self.selector_list:
            if selector(dict_row):
                return False
        return True

    def __str__(self) -> str:
        """Return CSS-like string of selector."""
        inner_str = ', '.join(str(x) for x in self.selector_list)
        return f':not({inner_str})'

    @property
    def specificity(self) -> Tuple[int, int]:
        """The specificity of a "negation" selector (i.e., the :is()
        pseudo-class) is the specificity of the most specific selector
        it contains.
        """
        return max(x.specificity for x in self.selector_list)


class SpecificityAdjustmentSelector(SelectorContainer):
    """
    This class is designed to mimic the "specificity-adjustment"
    selector--i.e., the :where() pseudo-class. For details, see:

        https://www.w3.org/TR/selectors-4/#zero-matches
    """
    def __new__(cls, selector_list):
        if len(selector_list) == 1:
            return selector_list[0]  # Return simple selector, if one item.
        return super().__new__(cls)

    def __call__(self, dict_row: Mapping[str, str]) -> bool:
        """Return True if selector matches values in *dict_row*."""
        for selector in self.selector_list:
            if selector(dict_row):
                return True
        return False

    def __str__(self) -> str:
        """Return CSS-like string of selector."""
        inner_str = ', '.join(str(x) for x in self.selector_list)
        return f':where({inner_str})'

    @property
    def specificity(self) -> Tuple[int, int]:
        """Specificity is always zero."""
        return (0, 0)


class CompoundSelector(SelectorContainer):
    def __new__(cls, selector_list):
        if len(selector_list) == 1:
            return selector_list[0]  # Return simple selector, if one item.
        return super().__new__(cls)

    def __call__(self, dict_row: Mapping[str, str]) -> bool:
        return all(selector(dict_row) for selector in self.selector_list)

    def __str__(self) -> str:
        """Return CSS-like string of selector."""
        return ''.join(str(selector) for selector in self.selector_list)

    @property
    def specificity(self) -> Tuple[int, int]:
        """The specificity of a compound selector is the element-wise
        sum of the specificity values of the selectors it contains.
        """
        specificity_values = [x.specificity for x in self.selector_list]
        return tuple(sum(tup) for tup in zip(*specificity_values))  # type: ignore[return-value]


selector_grammar = r"""
    // --------------------------------------------------------------------
    // Lark grammar for CSS-inspired attribute selectors in Toron.
    // --------------------------------------------------------------------
    //
    // For general information on CSS attribute selectors see:
    //
    //   https://www.w3.org/TR/selectors-4/#structure
    //   https://www.w3.org/TR/selectors-4/#attribute-selectors
    //   https://www.w3.org/TR/selectors-4/#matches
    //   https://www.w3.org/TR/selectors-4/#negation
    //   https://www.w3.org/TR/selectors-4/#zero-matches
    //
    // --------------------------------------------------------------------

    start: selector+ -> compound_selector

    selector : "[" attribute [operator value [ignore_case]] "]"
             | ":is(" selector ("," selector)* ")"    -> matches_any
             | ":not(" selector ("," selector)* ")"   -> negation
             | ":where(" selector ("," selector)* ")" -> specificity_adjustment

    attribute : IDENTIFIER

    !operator : "=" | "~=" | "|=" | "^=" | "$=" | "*="

    value : IDENTIFIER | DOUBLE_QUOTED_STRING | SINGLE_QUOTED_STRING

    ignore_case : "i" | "I"

    //
    // Define CSS 2.2-style identifier. Follows specification except that
    // escaped characters are not allowed. For details, see:
    //
    //   https://www.w3.org/TR/CSS22/syndata.html#value-def-identifier
    //
    _FIRST_CHAR : "a".."z" | "A".."Z" | "\u00A0".."\uFFFF" | "_"
    _OTHER_CHAR : _FIRST_CHAR | "0".."9" | "-"
    IDENTIFIER : "-"? _FIRST_CHAR _OTHER_CHAR* | "-"

    //
    // Adapted from "lark/grammars/common.lark" for single- and double-quoted
    // escaped strings.
    //
    _STRING_INNER : /.*?/
    _STRING_ESC_INNER : _STRING_INNER /(?<!\\)(\\\\)*?/
    DOUBLE_QUOTED_STRING : "\"" _STRING_ESC_INNER "\""
    SINGLE_QUOTED_STRING : "'" _STRING_ESC_INNER "'"

    %import common.WS  // import whitespace
    %ignore WS         // ignore whitespace between tokens
"""


class SelectorTransformer(Transformer):
    def compound_selector(self, args):
        return CompoundSelector(args)

    def matches_any(self, args):
        return MatchesAnySelector(args)

    def negation(self, args):
        return NegationSelector(args)

    def specificity_adjustment(self, args):
        return SpecificityAdjustmentSelector(args)

    @v_args(inline=True)
    def selector(self, attr, op=None, val=None, ignore_case=None):
        return SimpleSelector(attr, op, val, ignore_case)

    @v_args(inline=True)
    def attribute(self, token):
        return token.value

    @v_args(inline=True)
    def operator(self, token):
        return token.value

    @v_args(inline=True)
    def value(self, token):
        s = token.value
        if s.startswith('"') and s.endswith('"'):
            return s[1:-1].replace('\\"', '"')

        if s.startswith("'") and s.endswith("'"):
            return s[1:-1].replace("\\'", "'")

    def ignore_case(self, args):
        return True


parse_selector = Lark(selector_grammar,
                      parser='lalr',
                      transformer=SelectorTransformer()).parse

