"""Utility functions and classes for the Toron project.

This sub-module contains code that is used elsewhere in Toron to
handle data, raise exceptions, and issue warnings.

NOTE: Ideally, this module should not import code from within Toron
itself--it should only import from the Python Standard Library or
indepdendent, third-party packages. That said, Toron's compatibility
modules (like `_typing`) are treated as if they are part of the
Standard Library and may be imported.
"""

import csv
import hashlib
import re
import sqlite3
from functools import wraps
from itertools import chain, zip_longest
from json import (
    dumps as _dumps,
    loads as _loads,
)
from math import copysign, modf
from ._typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Hashable,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    overload,
    Self,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeAlias,
    TypeVar,
    Union,
    cast,
)


T = TypeVar('T')


def check_type(obj: Any, required_type: Type[T]) -> T:
    """Check that *obj* is instance of *required_type* and return value
    unchanged or else raise a TypeError.
    """
    if isinstance(obj, required_type):
        return obj  # <- EXIT, return unchanged.
    msg = f'required type {required_type}, but got {obj.__class__}'
    raise TypeError(msg)


@overload
def normalize_tabular(
    data: Iterable[Sequence], columns: Optional[Sequence[str]] = None,
) -> Tuple[Iterator[Sequence], Sequence]:
    ...
@overload
def normalize_tabular(
    data: Iterable[Mapping], columns: Optional[Sequence[str]] = None,
) -> Tuple[Iterator[Sequence], Sequence]:
    ...
def normalize_tabular(data, columns=None):
    """Return normalized *data* and column names or raise a TypeError
    if data cannot be normalized.

    Normalizing an iterable of sequence rows::

        >>> raw_data = [
        ...     ('A', 'B', 'C'),
        ...     (1, 11, 111),
        ...     (2, 22, 222)
        ... ]
        >>> data, cols = normalize_tabular(raw_data)
        >>> list(data)
        [(1, 11, 111), (2, 22, 222)]
        >>> cols
        ('A', 'B', 'C')

    Normalizing an iterable of dictionary rows::

        >>> raw_data = [
        ...     {'A': 1, 'B': 11, 'C': 111},
        ...     {'A': 2, 'B': 22, 'C': 222},
        ... ]
        >>> data, cols = normalize_tabular(raw_data)
        >>> list(data)
        [(1, 11, 111), (2, 22, 222)]
        >>> cols
        ('A', 'B', 'C')
    """
    try:
        data_iter = iter(data)
    except TypeError:
        qualname = data.__class__.__qualname__
        msg = f'data must be iterable, got {qualname!r}: {data!r}'
        raise TypeError(msg)

    try:
        first_row = next(data_iter)
    except StopIteration:
        return data_iter, columns or tuple()

    if isinstance(first_row, Sequence):
        if columns:
            return chain([first_row], data_iter), columns
        return data_iter, first_row

    if isinstance(first_row, Mapping):
        if not columns:
            columns = tuple(first_row.keys())
        func = lambda row: tuple(row.get(x) for x in columns)
        data_iter = (func(row) for row in chain([first_row], data_iter))
        return data_iter, columns

    qualname = first_row.__class__.__qualname__
    msg = f'rows must be sequence or mapping, got {qualname!r}: {first_row!r}'
    raise TypeError(msg)


def verify_columns_set(
    columns: Iterable,
    required_columns: Iterable,
    allow_extras: bool = False,
) -> None:
    """Raise error if columns do not match set of required columns."""
    if allow_extras:
        columns = set(columns)

        missing = [repr(x) for x in required_columns if (x not in columns)]

        if missing:
            raise ValueError(
                f'invalid column names\n'
                f'  missing required columns: {", ".join(missing)}'
            )
    else:
        columns = tuple(columns)
        required_columns = tuple(required_columns)

        missing = [repr(x) for x in required_columns if (x not in columns)]
        extra = [repr(x) for x in columns if (x not in required_columns)]

        if missing or extra:
            msg = ['invalid column names']
            if missing:
                msg.append(f'missing required columns: {", ".join(missing)}')
            if extra:
                msg.append(f'extra columns found: {", ".join(extra)}')
            raise ValueError('\n  '.join(msg))


TabularData : TypeAlias = Union[
    Iterable[Sequence],
    Iterable[Mapping],
]

TabularData.__doc__ = """
A type alias for objects that can represent tabular data.

Valid tabular data sources include:

* an iterable of sequences (uses first item as a "header" row)
* an iterable of dictionary rows (expects uniform dictionaries)

This includes ``csv.reader(...)`` (an iterable of sequences)
and ``csv.DictReader`` (an iterable of dictionary rows).
"""


_csv_reader_type = type(csv.reader([]))


def make_readerlike(data: TabularData) -> Iterator[Sequence]:
    """Normalize tabular data source as a csv.reader-like iterator.

    If *data* is an iterable of dictionary rows, this function assumes
    that all rows share a uniform set of keys.
    """
    # Return csv.reader(...) object unchanged.
    if isinstance(data, _csv_reader_type):
        return data

    # Build rows using DictReader.fieldnames attribute.
    if isinstance(data, csv.DictReader):
        fieldnames = list(data.fieldnames)  # type: ignore [arg-type]
        make_row = lambda dictrow: [dictrow.get(x, None) for x in fieldnames]
        return chain([fieldnames], (make_row(x) for x in data))

    try:
        iterator = iter(data)
    except TypeError:
        cls_name = data.__class__.__name__
        msg = f'cannot normalize object as tabular data, got {cls_name!r}: {data!r}'
        raise TypeError(msg)

    first_value = next(iterator, None)

    # Empty iterable.
    if first_value is None:
        return iter([])

    # Iterable of mappings (assumes uniform keys).
    if isinstance(first_value, Mapping):
        fieldnames = list(first_value.keys())
        make_row = lambda dictrow: [dictrow.get(x, None) for x in fieldnames]
        iterator = (make_row(x) for x in chain([first_value], iterator))
        return chain([fieldnames], iterator)  # type: ignore [arg-type]

    # Once all other cases are handled, remaining case should be
    # an iterable of sequences.
    if not isinstance(first_value, Sequence):
        cls_name = first_value.__class__.__name__
        msg = f'rows must be sequences, got {cls_name!r}: {first_value!r}'
        raise TypeError(msg)
    return chain([first_value], iterator)  # type: ignore [arg-type]


class ToronError(Exception):
    """Error in Toron usage or invocation."""


class ToronWarning(UserWarning):
    """Base class for warnings generated by Toron."""


def make_dictreaderlike(data: TabularData) -> Iterator[Mapping]:
    """Normalize tabular data source as a DictReader-like iterator."""
    # If csv.DictReader(...), return original object.
    if isinstance(data, csv.DictReader):
        return data

    try:
        iterator = iter(data)
    except TypeError:
        cls_name = data.__class__.__name__
        msg = f'cannot make iterator of dictrows, got {cls_name!r}: {data!r}'
        raise TypeError(msg)

    first_value = next(iterator, None)
    if first_value is None:
        return iter([])  # Return if iterator is empty.

    iterator = chain([first_value], iterator)

    # If iterable of mappings, return without further changes.
    if isinstance(first_value, Mapping):
        return iterator  # type: ignore [return-value]

    # Normalize as reader-like object and return dictrow generator.
    reader = make_readerlike(iterator)  # type: ignore [arg-type]
    fieldnames = next(reader)
    return (dict(zip(fieldnames, row)) for row in reader)


def wide_to_narrow(
    data: TabularData,
    cols_to_stack: Sequence[str],
    var_name: Hashable = 'variable',
    value_name: Hashable = 'value',
) -> Generator[Mapping, None, None]:
    """A generator function that takes an iterable of wide-format
    records and yields narrow-format dictionary rows.

    Parameters
    ----------

    data : Iterable[Sequence] | Iterable[Mapping]
        Wide-format tabular data.
    cols_to_stack : sequence of str
        Name of column(s) to stack.
    var_name : hashable, default 'variable'
        Name to use for the variable column.
    value_name : hashable, default 'value'
        Name to use for the value column.

    Returns
    -------

    Generator
        Narrow-format dictionary rows.

    Examples
    --------

    .. code-block::

        >>> from toron import wide_to_narrow
        >>> wide_data = [
        ...     ('A', 'B', 'C', 'D'),
        ...     ('x', 10,  20,  30),
        ...     ('y', 40,  50,  60),
        ...     ('z', 70,  80,  90),
        ... ]

    Stack columns ``'B'``, ``'C'``, and ``'D'``:

    .. code-block::

        >>> long_data = wide_to_narrow(wide_data, ['B', 'C', 'D'])
        >>> list(long_data)
        [{'A': 'x', 'variable': 'B', 'value': 10},
         {'A': 'x', 'variable': 'C', 'value': 20},
         {'A': 'x', 'variable': 'D', 'value': 30},
         {'A': 'y', 'variable': 'B', 'value': 40},
         {'A': 'y', 'variable': 'C', 'value': 50},
         {'A': 'y', 'variable': 'D', 'value': 60},
         {'A': 'x', 'variable': 'B', 'value': 70},
         {'A': 'x', 'variable': 'C', 'value': 80},
         {'A': 'x', 'variable': 'D', 'value': 90}]

    Because column ``'A'`` (above) was left unstacked, its values are
    repeated for each associated item.

    Specify different names for the variable and value items:

    .. code-block::

        >>> long_data = wide_to_narrow(wide_data, ['B', 'C', 'D'], 'myvar', 'myval')
        >>> list(long_data)
        [{'A': 'x', 'myvar': 'B', 'myval': 10},
         {'A': 'x', 'myvar': 'C', 'myval': 20},
         {'A': 'x', 'myvar': 'D', 'myval': 30},
         {'A': 'y', 'myvar': 'B', 'myval': 40},
         {'A': 'y', 'myvar': 'C', 'myval': 50},
         {'A': 'y', 'myvar': 'D', 'myval': 60},
         {'A': 'x', 'myvar': 'B', 'myval': 70},
         {'A': 'x', 'myvar': 'C', 'myval': 80},
         {'A': 'x', 'myvar': 'D', 'myval': 90}]
    """
    dict_rows = make_dictreaderlike(data)

    for input_row in dict_rows:
        if var_name in input_row:
            msg = f'must provide alternate name for variable column: ' \
                  f'{var_name!r} already present in {input_row!r}'
            raise ValueError(msg)

        if value_name in input_row:
            msg = f'must provide alternate name for value column: ' \
                  f'{value_name!r} already present in {input_row!r}'
            raise ValueError(msg)

        unstacked_cols = [k for k in input_row if k not in cols_to_stack]

        for var in cols_to_stack:
            try:
                value = input_row[var]
            except KeyError:
                msg = f'wide_to_narrow column not found: {var!r} not in ' \
                      f'{list(input_row.keys())!r}'
                generator_error = ToronError(msg)
                generator_error.__cause__ = None
                raise generator_error

            output_row = {k: input_row[k] for k in unstacked_cols}
            output_row[var_name] = var
            output_row[value_name] = value
            yield output_row


def parse_edge_shorthand(string):
    """Parse a string containing a special shorthand syntax used to
    describe edges between nodes. If the given syntax is valid, its
    contents are parsed and returned as a dictionary of strings. When
    a string does not contain valid shorthand syntax, an empty dict
    is returned.

    Sample of the edge-description shorthand:

    .. code-block:: text

        edge_name: node_file1 <--> node_file2

    Edge-description with an optional attribute selector:

    .. code-block:: text

        edge_name: node_file1 <--> node_file2 : [selector]

    Code example::

        >>> parse_edge_shorthand('population: mynode1 <--> mynode2')
        {'edge_name': 'population',
         'node_file1': 'mynode1',
         'direction': '<-->',
         'node_file2': 'mynode2',
         'selector': None}

    Code example with an attribute selector::

        >>> parse_edge_shorthand('population: mynode1 <--> mynode2 : [age="20to34"]')
        {'edge_name': 'population',
         'node_file1': 'mynode1',
         'direction': '<-->',
         'node_file2': 'mynode2',
         'selector': '[age="20to34"]'}

    **EDGE DESCRIPTION PARTS**

    edge_name:
        Name of the edge to add.
    node_file1:
        Filename of the left-hand node in the mapping data. The
        ``.toron`` suffix can be omitted in the shorthand syntax.
    direction (``->``, ``-->``, ``<-``, ``<--``, ``<->`` or ``<-->``):
        Indicates the direction of the edge to be added. Directions
        can be left-to-right, right-to-left, or indicate that edges
        should be added in both directions (left-to-right *and*
        right-to-left).
    node_file2:
        Filename of the right-hand node in the mapping data. The
        ``.toron`` suffix can be omitted in the shorthand syntax.
    selector:
        An optional attribute selector to associate with the
        edge.

    Note: This function will not match filenames that include any
    of the following characters: ``<``, ``>``, ``:``, ``"``, ``/``,
    ``\\``, ``|``, ``?``, and ``*``.
    """
    pattern = r"""
        ^                                     # Start of string.
        \s*                                   # Zero or more whitespace.
        (?P<edge_name>[^<>:"/\\|?*]+?)        # GROUP 1 (EDGE NAME)
        \s*                                   # Zero or more whitespace.
        :                                     # Colon/separator.
        \s*                                   # Zero or more whitespace.
        (?P<node_file1>[^<>:"/\\|?*]+?)       # GROUP 2 (FIRST NODE FILENAME)
        \s+                                   # One or more whitespace.
        (?P<direction>->|-->|<->|<-->|<-|<--) # GROUP 3 (EDGE DIRECTION)
        \s+                                   # One or more whitespace.
        (?P<node_file2>[^<>:"/\\|?*]+?)       # GROUP 4 (SECOND NODE FILENAME)
        \s*                                   # Zero or more whitespace.
        (?:                                   # Start of non-capturing group:
        :                                     # - Colon/separator.
        \s*                                   # - Zero or more whitespace.
        (?P<selector>\[.*\])?                 # - GROUP 5 (AN ATTRIBUTE SELECTOR)
        )?                                    # End of non-capturing group (zero or one).
        \s*                                   # Zero or more whitespace.
        $                                     # End of string.
    """
    matched = re.match(pattern, string, re.VERBOSE)
    if matched:
        return matched.groupdict()
    return {}


def make_hash(values: Iterable, sep: str = '|') -> Optional[str]:
    """Hashes an iterable of values returning a message digest string
    or None if the given iterable is empty.

    Before hashing, values are converted into strings and separated
    with ``sep`` (defaults to pipe, ``'|'``). So, given the list
    ``[1, 2, 3]``, this function will output the digest for the
    message ``"1|2|3"``.
    """
    values = iter(values)
    try:
        first_item = next(values)
    except StopIteration:
        return None

    values = chain([str(first_item)], (f'{sep}{x}' for x in values))

    sha256 = hashlib.sha256()
    for value in values:
        sha256.update(value.encode('utf-8'))

    return sha256.hexdigest()


class SequenceHash(object):
    """An object to calculate a checksum from a sequence of integers.

    Values must be a strictly increasing sequence of distinct integers
    that start at 0 or greater and do not exceed 8 bytes in length (the
    max integer value is 18446744073709551615, i.e., `2 ** 64 - 1`).

    This class can be used to generate hashes for a node's 'index_id'
    values::

        >>> index_ids = [0, 1, 2, 5, 6, ...]
        >>>
        >>> index_hash = SequenceHash()
        >>> for index_id in index_ids:
        ...     index_hash.add_value(index_id)
        >>>
        >>> index_hash.get_hexdigest()
        'efaaa8ba342b5791c9a5fc25ec1fbc3bb77c6a110364840ce1cca88c14a93872'

    Optionally, SequenceHash can be initialized with an iterable::

        >>> index_hash = SequenceHash([0, 1, 2, 5, 6, ...])
        >>> index_hash.get_hexdigest()
        'efaaa8ba342b5791c9a5fc25ec1fbc3bb77c6a110364840ce1cca88c14a93872'
    """
    def __init__(self, iterable: Optional[Iterable] = None) -> None:
        self.hash_obj = hashlib.sha256()
        self._prev_value = -1

        if iterable:
            for value in iterable:
                self.add_value(value)

    def add_value(self, value: int) -> None:
        """Update the current digest with an additional integer."""
        if not value > self._prev_value:
            raise ValueError(
                'illegal value - values must be a strictly increasing '
                'sequence starting at 0 or greater'
            )
        try:
            self.hash_obj.update(value.to_bytes(length=8, byteorder='big'))
        except OverflowError as e:  # <- Int too big (more than 8 bytes).
            raise ValueError(e)

        self._prev_value = value

    def get_hexdigest(self) -> str:
        """Return the current digest as a hexadecimal string."""
        return self.hash_obj.hexdigest()


def splitmix64(x: int) -> int:
    """Hash 64-bit *x* and return a pseudo-random 64-bit integer digest.

    This function implements a version of Sebastiano Vigna's SplitMix64
    (CC0 1.0 Public Domain) which is, itself, based on "Fast Splittable
    Pseudorandom Number Generators" by Steele, G. L., Lea, D., & Flood,
    C. H. (2014, doi:10.1145/2714064.2660195).

    .. important::

        This function is intended to operate on unsigned, 64-bit integer
        input (specifically "index_id" values). For speed, it does not
        perform checks on its input values. The calling parent context
        should assure that only 64-bit integers are used.

        This function *will* operate on negative values and integers
        that are larger than 64-bits. But that behavior is outside
        of the intended scope, can cause potential hash collisions,
        and is not being tested for.
    """
    x = (x + 0x9e3779b97f4a7c15) & 0xffffffffffffffff
    x = ((x ^ (x >> 30)) * 0xbf58476d1ce4e5b9) & 0xffffffffffffffff
    x = ((x ^ (x >> 27)) * 0x94d049bb133111eb) & 0xffffffffffffffff
    return x ^ (x >> 31)


def quantize_values(
    items: Iterator[Tuple[int, float]],
    sum_total: float,
) -> Iterator[Tuple[int, float]]:
    """Quantize item values using Largest Remainder Method (LRM).

    .. code-block::

        >>> list(quantize_values([(1, 3.75), (2, 5.25)], sum_total=9.0))
        [(1, 4.0), (2, 5.0)]

    .. note::

        This function is used to optionally quantize values during
        disaggregation and translation. In these contexts, the sum
        total is already known so it is used as a parameter rather
        than calculating it again.
    """
    # Accumulate sum of whole parts and format items as 3-tuples.
    sum_of_whole_parts = 0.0
    idx_frac_whole: List[Tuple[int, float, float]] = []
    for index_id, quantity_value in items:
        fractional_part, whole_part = modf(quantity_value)
        sum_of_whole_parts += whole_part
        idx_frac_whole.append((index_id, fractional_part, whole_part))

    # Sort items by largest to smallest magnitude of fractional parts.
    idx_frac_whole = sorted(idx_frac_whole, key=lambda x: abs(x[1]), reverse=True)

    # Get remainder from sum total (assign as fractional and whole parts).
    remainder_frac, remainder_whole = modf(sum_total - sum_of_whole_parts)

    # Create a consumable iterator from the sorted list of items.
    iterator = iter(idx_frac_whole)

    # Yield items with the highest fractional parts (incrementing whole
    # values by one) for a number of items equal to the whole remainder.
    increment = copysign(1, sum_total)  # Increment by 1 or -1.
    for _ in range(int(abs(remainder_whole))):
        index_id, _, whole_part = next(iterator)
        yield (index_id, whole_part + increment)

    # If there's a fractional remainder, distribute it to the next item.
    if remainder_frac:
        index_id, _, whole_part = next(iterator)
        yield (index_id, whole_part + remainder_frac)

    # Yield remaining items without their fractional parts.
    for index_id, _, whole_part in iterator:
        yield (index_id, whole_part)


@overload
def eagerly_initialize(func_or_iter: Callable[..., Generator]) -> Callable[..., Iterator]:
    ...
@overload
def eagerly_initialize(func_or_iter: Iterator) -> Iterator:
    ...
def eagerly_initialize(func_or_iter):
    """A decorator to eagerly initialize a generator object.

    On instantiation, this decorator will execute all code up to the
    first yield statement. This allows a generator to immediately
    perform any needed pre-iteration actions (like validation, error
    handling, logging, etc.) rather than passively waiting until the
    object is iterated over somewhere else in the code.
    """
    def do_initialize(generator: Iterator) -> Iterator:  # <-  Helper function.
        sentinel_value = object()
        first_item = next(generator, sentinel_value)
        if first_item is sentinel_value:
            return chain()  # <- Empty chain() for consistent return type.
        return chain([first_item], generator)

    # Decorate generator function.
    if isinstance(func_or_iter, Callable):
        @wraps(func_or_iter)
        def wrapped_genfunc(*args, **kwds) -> Iterator:
            generator = func_or_iter(*args, **kwds)
            return do_initialize(generator)
        return wrapped_genfunc  # <- EXIT!

    # Eagerly initialize generator instance.
    if isinstance(func_or_iter, Iterator):
        return do_initialize(func_or_iter)  # <- EXIT!

    msg = f'unhandled type: {func_or_iter.__class__.__name__}'
    raise Exception(msg)


@overload
def wide_to_long(
    data: Iterable[Sequence],
    cols_to_stack: Union[Iterable[Any], Any],
    var_name: Any = 'variable',
    val_name: Any = 'value',
) -> Iterator[Tuple]:
    ...
@overload
def wide_to_long(
    data: Iterable[Mapping],
    cols_to_stack: Union[Iterable[Hashable], Hashable],
    var_name: Hashable = 'variable',
    val_name: Hashable = 'value',
) -> Iterator[Dict]:
    ...
@eagerly_initialize
def wide_to_long(data, cols_to_stack, var_name='variable', val_name='value'):
    """A generator function that takes an iterable of wide-format
    records and yields long-format records.

    Parameters
    ----------

    data:
        Wide-format tabular data.
    cols_to_stack:
        Name of column(s) to stack.
    var_name:
        Name to use for the variable column (defaults to "variable").
        This is the new column that contains the stacked column names.
    val_name:
        Name to use for the value column (defaults to "value). This
        is the new column that contains the stacked cell values.

    Returns
    -------

    Generator
        Narrow-format records.

    Examples
    --------

    .. code-block::

        >>> from toron import wide_to_long
        >>> wide_data = [
        ...     ('A', 'B', 'C', 'D'),
        ...     ('x', 10,  20,  30),
        ...     ('y', 40,  50,  60),
        ...     ('z', 70,  80,  90),
        ... ]

    Stack columns ``'B'``, ``'C'``, and ``'D'``:

    .. code-block::

        >>> long_data = wide_to_long(wide_data, ['B', 'C', 'D'])
        >>> list(long_data)
        [('A', 'variable', 'value'),
         ('x', 'B', 10),
         ('x', 'C', 20),
         ('x', 'D', 30),
         ('y', 'B', 40),
         ('y', 'C', 50),
         ('y', 'D', 60),
         ('z', 'B', 70),
         ('z', 'C', 80),
         ('z', 'D', 90)]

    Because column ``'A'`` (above) was left unstacked, its values are
    repeated for each associated item.

    When given an iterable of dictionaries, an iterable of dictionaries
    is returned:

    .. code-block::

        >>> from toron import wide_to_long
        >>> wide_data = [
        ...     {'A': 'x', 'B': 10, 'C': 20, 'D': 30},
        ...     {'A': 'y', 'B': 40, 'C': 50, 'D': 60},
        ...     {'A': 'z', 'B': 70, 'C': 80, 'D': 90},
        ... ]
        >>> long_data = wide_to_long(wide_data, ['B', 'C', 'D'])
        >>> list(long_data)
        [{'A': 'x', 'variable': 'B', 'value': 10},
         {'A': 'x', 'variable': 'C', 'value': 20},
         {'A': 'x', 'variable': 'D', 'value': 30},
         {'A': 'y', 'variable': 'B', 'value': 40},
         {'A': 'y', 'variable': 'C', 'value': 50},
         {'A': 'y', 'variable': 'D', 'value': 60},
         {'A': 'z', 'variable': 'B', 'value': 70},
         {'A': 'z', 'variable': 'C', 'value': 80},
         {'A': 'z', 'variable': 'D', 'value': 90}]
    """
    data = iter(data)
    first_item = next(data)

    if not isinstance(cols_to_stack, Iterable) \
            or isinstance(cols_to_stack, str):
        cols_to_stack = [cols_to_stack]

    if isinstance(first_item, Sequence):
        header = tuple(first_item)

        if not all((col in header) for col in cols_to_stack):
            missing = [repr(x) for x in cols_to_stack if (x not in header)]
            raise ValueError(f'columns missing from data: {", ".join(missing)}')

        cols_to_keep = tuple(x for x in header if x not in cols_to_stack)

        # Yield header with variable and value columns.
        yield cols_to_keep + (var_name, val_name)

        # Yield rows with stacked values.
        for row in data:
            row_dict = dict(zip_longest(header, row))
            vals_to_keep = tuple(row_dict.get(x) for x in cols_to_keep)
            for col in cols_to_stack:
                yield vals_to_keep + (col, row_dict.get(col))

    elif isinstance(first_item, Mapping):
        data = chain([first_item], data)  # Add first item back to `data`.

        if not all((col in first_item) for col in cols_to_stack):
            missing = [repr(x) for x in cols_to_stack if (x not in first_item)]
            raise ValueError(f'columns missing from data: {", ".join(missing)}')

        cols_to_keep = tuple(x for x in first_item if x not in cols_to_stack)

        # Yield dictionary rows with stacked values.
        for row_dict in data:
            items_to_keep = {x: row_dict.get(x) for x in cols_to_keep}
            for col in cols_to_stack:
                new_row_dict = items_to_keep.copy()
                new_row_dict[var_name] = col
                new_row_dict[val_name] = row_dict.get(col)
                yield new_row_dict

    else:
        raise TypeError(
            f'iterable must contain sequences or mappings, '
            f'got {first_item.__class__.__qualname__}'
        )


# Define and instantiate NOVALUE inline (keeps class reference out of scope).
NOVALUE = type('NoValueType', (object,), {
    '__doc__': """
        Token to differentiate between ``None`` and no value given.

        This token is used by ``DataAccessLayer._add_edge()`` to
        differentiate between giving ``None`` as an argument and
        giving no value at all.
    """,
    '__repr__': lambda self: '<no value>',
    '__bool__': lambda self: False,  # Object is falsy.
})()


class BitFlags(Sequence[Literal[0, 1]]):
    """A sequence of 0s and 1s used to encode multiple true/false or
    on/off values. This class can be registered with SQLite to support
    a "BLOB_BITFLAGS" data type.

    Create a BitFlags object from arguments of 0 or 1 (bit sequences
    are padded to the nearest multiple of 8)::

        >>> BitFlags(1, 1, 0, 1)
        BitFlags(1, 1, 0, 1, 0, 0, 0, 0)

    Create a BitFlags object from a single iterable argument::

        >>> BitFlags([1, 1, 0, 1])
        BitFlags(1, 1, 0, 1, 0, 0, 0, 0)

    Create a BitFlags object from bytes::

        >>> BitFlags(b'\xd0')
        BitFlags(1, 1, 0, 1, 0, 0, 0, 0)

    Convert a BitFlags object into bytes::

        >>> bits = BitFlags(1, 1, 0, 1, 0, 0, 0, 0)
        >>> bytes(bits)
        b'\xd0'

    Other values are converted to 0 and 1 based on their truth value::

        >>> BitFlags('x', 'x', '', 'x', '', '', '', '')
        BitFlags(1, 1, 0, 1, 0, 0, 0, 0)

    When comparing BitFlags against other containers, trailing zeros
    are ignored and objects are compared by their truth values::

        >>> BitFlags(1, 1, 0, 1, 0, 0, 0, 0) == (1, 1, 0, 1)
        True

    Overlong bit sequences are truncated to the smallest multiple of 8
    that preserves the given bits::

        >>> BitFlags(1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        BitFlags(1, 1, 0, 1, 0, 0, 0, 0)

    Register the BitFlags type with SQLite::

        >>> import sqlite3
        >>> sqlite3.register_adapter(BitFlags, bytes)
        >>> sqlite3.register_converter('BLOB_BITFLAGS', BitFlags)
    """
    __slots__ = ('_bytes',)
    _bytes: bytes

    def __init__(self, *args: Any) -> None:
        """
        BitFlags(byte_string) -> None
        BitFlags(iterable) -> None
        BitFlags(bit1[, bit2[, ...]]) -> None

        Initialize a new BitFlags instance.
        """
        if len(args) == 1:
            if isinstance(args[0], bytes):
                self._bytes = args[0].rstrip(b'\x00')
            elif isinstance(args[0], Iterable):
                self._bytes = self._bitstream_to_bytes(args[0])
            else:
                self._bytes = self._bitstream_to_bytes(args)
        else:
            self._bytes = self._bitstream_to_bytes(args)

    @staticmethod
    def _bitstream_to_bytes(stream: Iterable[Any]) -> bytes:
        """Return a bytes object representing the *stream* of bits.

        .. code-block::

            >>> BitFlags._bitstream_to_bytes([1, 1, 1, 1, 0, 0, 0, 0])
            b'\xf0'

        Trailing bytes of zeros are removed from final byte string.

        Elements in the incoming stream are handled as zeros and ones
        based on their truth values (falsy elements as 0s and truthy
        elements as 1s).
        """
        normalized = ((1 if x else 0) for x in stream)  # Must be exhaustible.
        eight_bit_words = zip_longest(*([normalized] * 8), fillvalue=0)

        byte_list = []
        for binary_word in eight_bit_words:
            decimal_number = 0
            for bit in binary_word:
                # Shift left and set the right-most bit.
                decimal_number = (decimal_number << 1) | bit
            byte_list.append(decimal_number.to_bytes(1, 'big'))

        return b''.join(byte_list).rstrip(b'\x00')

    @staticmethod
    def _bytes_to_bitstream(
        byte_string: bytes
    ) -> Generator[Literal[0, 1], None, None]:
        """Generate a stream of bits (0s and 1s) from *byte_string*.

        .. code-block::

            >>> stream = BitFlags._bytes_to_bitstream(b'\xf0')
            >>> tuple(stream)
            (1, 1, 1, 1, 0, 0, 0, 0)

        Note: Unlike _bitstream_to_bytes() method, trailing bytes of
        zeros are *not* removed from the final bit stream. This method
        is intended to be used on byte strings that have already been
        normalized.
        """
        for byte in byte_string:
            for i in range (7, -1, -1):  # range() yields 7 thru 0.
                # Shift right and get the right-most bit.
                yield cast(Literal[0, 1], (byte >> i) & 1)

    def __bytes__(self) -> bytes:
        """Return a bytes object representing the sequence of bits."""
        return self._bytes

    @overload
    def __getitem__(self, index: int) -> Literal[0, 1]:
        ...
    @overload
    def __getitem__(self, index: slice) -> 'BitFlags':
        ...
    def __getitem__(self, index):
        """Return value at index position or slice."""
        if isinstance(index, int):
            len_self = len(self)

            if index < 0:
                index = len_self + index  # Convert negative index to positive.

            if not (0 <= index < len_self):
                raise IndexError('index out of range')

            byte = self._bytes[index // 8]  # Get byte containing requested bit.
            shift_count = 7 - (index % 8)  # Get offset minus bit position.
            return (byte >> shift_count) & 1  # Shift right and get right-most bit.

        if isinstance(index, slice):
            tuple_of_bits = tuple(self._bytes_to_bitstream(self._bytes))
            sliced_bits = tuple_of_bits[index]  # Get slice.
            return self.__class__(sliced_bits)

        slf_cls = self.__class__.__name__
        idx_cls = index.__class__.__name__
        msg = f'{slf_cls} indices must be integers or slices, not {idx_cls}'
        raise TypeError(msg)

    def __len__(self) -> int:
        """Return number of bits contained in instance."""
        return len(self._bytes) * 8

    def __iter__(self) -> Iterator[Literal[0, 1]]:
        """Return iterator of 0s and 1s."""
        return self._bytes_to_bitstream(self._bytes)

    def __repr__(self) -> str:
        """Return string representation of BitFlags object."""
        bitstream = self._bytes_to_bitstream(self._bytes)
        formatted = ', '.join(str(x) for x in bitstream)
        return f'{self.__class__.__name__}({formatted})'

    def __eq__(self, other: Any) -> bool:
        """Return True if BitFlags == other."""
        if isinstance(other, self.__class__):
            return self._bytes == other._bytes

        if isinstance(other, Iterable):
            other_bytes = self._bitstream_to_bytes(other)
            return self._bytes == other_bytes

        return NotImplemented

    def __or__(self, other: Any) -> Self:
        """Return the bitwise-or (``|``) of self and other."""
        if not isinstance(other, self.__class__):
            return NotImplemented

        zipped = zip_longest(self._bytes, other._bytes, fillvalue=0)
        return self.__class__(bytes(a | b for a, b in zipped))

    def __hash__(self) -> int:
        """Return hash integer of instance."""
        return hash((self.__class__, self._bytes))


class XQuantityIterator(object):
    """An iterator to temporarily store disaggregated quantity data.

    When consumed, the iterator returns reaggregated results sorted
    by index_id.

    This object is used to to store large amounts of quantity data
    on drive rather than in memory. Internally, the iterator opens a
    temporary file and stores its incoming data in a SQLite database.
    The temporary file remains open until the iterator is exhausted
    or until the close() method is called.

    When *unique_id* and *data* are given, the ``attribute_keys``
    property is automatically derived from the attributes dict
    of each row in *data*::

        data = [
            (1, {'key1': 'val1', 'key2': 'val2'}, 20.5),
            (2, {'key1': 'val1', 'key2': 'val2'}, 50.0),
            ...
        ]
        iterator = XQuantityIterator(unique_id, data)

    The keyword-only argument '_attribute_keys' is intended for
    internal use but can be provided directly::

        data = [
            (1, {'key1': 'val1', 'key2': 'val2'}, 20.5),
            (2, {'key1': 'val1', 'key2': 'val2'}, 50.0),
            (3, {'key3': 'val3'}, 42.0),
            (4, {'key4': 'val4'}, 71.0),
            ...
        ]
        iterator = XQuantityIterator(
            unique_id,
            data,
            _attribute_keys={'key1', 'key2', 'key3, 'key4'},
        )

    .. warning::

        When an *_attribute_keys* value is given, it MUST contain the
        set of dictionary keys from the 'attributes' column of every
        row in *data*. These values can be accessed later via the
        ``attribute_keys`` property which can be used to transform the
        data into a tabular format.
    """
    def __init__(self,
        unique_id: str,
        data: Iterable[Tuple[int, Dict[str, str], float]],
        *,
        _attribute_keys: Optional[Iterable[str]] = None,
    ) -> None:
        """Initialize iterator (create and populate temp database).

        The temporary database consists of a single table:

        .. code-block:: text

            +-----------------+
            | temp_quantities |
            +-----------------+
            | index_id        |
            | attributes      |
            | quantity_value  |
            +-----------------+
        """
        self.unique_id = unique_id

        # Connect to private, on-drive temp file (using '' makes temp file).
        self._connection = sqlite3.connect('')
        self._cursor = self._connection.cursor()

        # Create a temporary table.
        self._cursor.execute("""
            CREATE TEMP TABLE temp_quantities (
                index_id INTEGER NOT NULL,
                attributes TEXT NOT NULL,
                quantity_value REAL NOT NULL
            )
        """)

        # Populate the temp table with the given data and set attr keys.
        sql = 'INSERT INTO temp.temp_quantities VALUES(?, ?, ?)'
        if _attribute_keys:
            iterator = ((a, _dumps(b, sort_keys=True), c) for a, b, c in data)
            self._cursor.executemany(sql, iterator)
            self._attribute_keys = set(_attribute_keys)
        else:
            # If no *_attribute_keys* given, get them when inserting rows.
            _attribute_keys = set()
            for a, b, c in data:
                _attribute_keys.update(b.keys())  # Accumulate keys.
                self._cursor.execute(sql, (a, _dumps(b, sort_keys=True), c))
            self._attribute_keys = _attribute_keys

        # Run query to group and order quantity data.
        self._cursor.execute("""
            SELECT
                index_id,
                attributes,
                SUM(quantity_value) AS quantity_value
            FROM temp.temp_quantities
            GROUP BY index_id, attributes
            ORDER BY index_id
        """)

    def close(self) -> None:
        """Close iterator (removes data from drive).

        Drive space used by the iterator is reclaimed after it is
        closed--the associated temporary file gets automatically
        removed some moments later.
        """
        try:
            self._cursor.close()
        except sqlite3.ProgrammingError:
            pass
        self._connection.close()

    def __next__(self) -> Tuple[int, Dict[str, str], float]:
        try:
            index_id, attributes, quantity_value = next(self._cursor)
        except sqlite3.ProgrammingError:
            raise StopIteration  # Raise StopIteration if cursor is closed.
        return index_id, _loads(attributes), quantity_value

    def __iter__(self):
        return self

    @property
    def attribute_keys(self) -> Set[str]:
        """The keys used by 'attributes' values contained in data."""
        return self._attribute_keys

    def __del__(self) -> None:
        self.close()
