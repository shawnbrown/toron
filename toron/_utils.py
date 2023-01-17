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
from itertools import chain
import re
from ._typing import (
    Generator,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    TypeAlias,
    Union,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    import pandas


TabularData : TypeAlias = Union[
    Iterable[Sequence],
    Iterable[Mapping],
    'pandas.DataFrame',
]

TabularData.__doc__ = """
A type alias for objects that can represent tabular data.

Valid tabular data sources include:

* an iterable of sequences (uses first item as a "header" row)
* an iterable of dictionary rows (expects uniform dictionaries)
* ``pandas.DataFrame``

This includes ``csv.reader(...)`` (an iterable of sequences)
and ``csv.DictReader`` (an iterable of dictionary rows).
"""


_csv_reader_type = type(csv.reader([]))


def normalize_tabular_data(data: TabularData) -> Iterator[Sequence]:
    """Normalize tabular data sources as an iterator of sequence rows.

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

    # Handle pandas.DataFrame() objects.
    if data.__class__.__name__ == 'DataFrame' \
            and data.__class__.__module__.partition('.')[0] == 'pandas':
        df_index = data.index            # type: ignore [union-attr]
        df_columns = data.columns        # type: ignore [union-attr]
        df_to_records = data.to_records  # type: ignore [union-attr]

        if df_index.names == [None]:
            fieldnames = list(df_columns)
            records = (list(x) for x in df_to_records(index=False))
            return chain([fieldnames], records)
        else:
            if any(x is None for x in df_index.names):
                type_name = df_index.__class__.__name__
                index_names = list(df_index.names)
                msg = f'{type_name} names must not be None, got {index_names!r}'
                raise ValueError(msg)
            fieldnames = list(df_index.names) + list(df_columns)
            records = (list(x) for x in df_to_records(index=True))
            return chain([fieldnames], records)

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

    # Handle pandas.DataFrame() objects.
    if data.__class__.__name__ == 'DataFrame' \
            and data.__class__.__module__.partition('.')[0] == 'pandas':
        reader = normalize_tabular_data(data)
        fieldnames = next(reader)
        return (dict(zip(fieldnames, row)) for row in reader)

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
    reader = normalize_tabular_data(iterator)  # type: ignore [arg-type]
    fieldnames = next(reader)
    return (dict(zip(fieldnames, row)) for row in reader)


def _data_to_dict_rows(
    data: Union[Iterable[Mapping], Iterable[Sequence]],
    columns: Optional[Sequence[str]] = None,
) -> Iterable[Mapping]:
    """Normalize data as an iterator of dictionary rows."""
    iter_data = iter(data)
    first_element = next(iter_data)
    if isinstance(first_element, Sequence):
        if not columns:
            columns = first_element
        else:
            iter_data = chain([first_element], iter_data)
        dict_rows = (dict(zip(columns, row)) for row in iter_data)
    elif isinstance(first_element, Mapping):
        dict_rows = chain([first_element], iter_data)  # type: ignore [assignment]
    else:
        msg = (f'data must contain mappings or sequences, '
               f'got type {type(first_element)}')
        raise TypeError(msg)
    return dict_rows


def wide_to_narrow(
    data: Union[Iterable[Mapping], Iterable[Sequence]],
    cols_to_stack: Sequence[str],
    var_name: Hashable = 'variable',
    value_name: Hashable = 'value',
    *,
    columns: Optional[Sequence[str]] = None,
) -> Generator[Mapping, None, None]:
    """A generator function that takes an iterable of wide-format
    records and yields narrow-format dictionary rows.

    Parameters
    ----------

    data : iterable of mappings (dict) or sequences
        Wide-format data.
    cols_to_stack : sequence of str
        Name of column(s) to stack.
    var_name : hashable, default 'variable'
        Name to use for the variable column.
    value_name : hashable, default 'value'
        Name to use for the value column.
    columns : sequence of str, optional
        Column names to use if data is a sequence with no header row.

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
    dict_rows = _data_to_dict_rows(data, columns)

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
    a string does not contain valid shorthand syntax, a ``None`` value
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
    return None
