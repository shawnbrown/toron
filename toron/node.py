"""Node implementation for the Toron project."""

from itertools import chain
from typing import List

from ._dal import dal_class
from ._dal import Strategy as _Strategy
from ._categories import make_structure
from ._categories import minimize_discrete_categories


class Node(object):
    def __init__(self, cache_to_drive: bool = False) -> None:
        self._dal = dal_class(cache_to_drive=cache_to_drive)

    @property
    def path(self):
        return self._dal.path

    @property
    def mode(self):
        return self._dal.mode

    def add_index_columns(self, columns: List[str]) -> None:
        """Add columns to node.

        .. code-block::

            >>> node = toron.Node()
            >>> node.add_index_columns(['state', 'county', 'mcd'])
        """
        data = self._dal.get_data(['discrete_categories', 'column_names'])

        minimized = minimize_discrete_categories(
            data['discrete_categories'],
            [set(columns).union(data['column_names'])],
        )
        structure = make_structure(minimized)

        self._dal.set_data({
            'add_index_columns': columns,
            'structure': structure,
        })

    def remove_columns(
        self, columns: List[str], strategy: _Strategy = 'preserve'
    ) -> None:
        """Remove columns from node.

        .. code-block::

            >>> node = toron.Node.from_file(...)
            >>> node.remove_columns(['C', 'D'])

        The following *strategy* values can be used when deleting
        columns:

        +--------------------------+----------------------------------+
        | Value                    | Meaning                          |
        +==========================+==================================+
        | ``'preserve'``           | Cancel the operation and Raise   |
        |                          | a ToronError if the node's       |
        |                          | granularity and category         |
        |                          | structure cannot be preserved.   |
        +--------------------------+----------------------------------+
        | ``'restructure'``        | Restructure existing categories  |
        |                          | as necessary or cancel the       |
        |                          | operation and raise a ToronError |
        |                          | if the node's granularity cannot |
        |                          | be preserved.                    |
        +--------------------------+----------------------------------+
        | ``'coarsen'``            | Coarsen the node's granularity   |
        |                          | as necessary or cancel the       |
        |                          | operation and raise a ToronError |
        |                          | if its category structure cannot |
        |                          | be preserved.                    |
        +--------------------------+----------------------------------+
        | ``'coarsenrestructure'`` | Coarsen the node's granularity   |
        |                          | and restructure its existing     |
        |                          | categories as necessary.         |
        +--------------------------+----------------------------------+
        """
        self._dal.remove_columns(columns, strategy=strategy)

    def rename_columns(self, mapper):
        self._dal.rename_columns(mapper)

    def add_elements(self, iterable, columns=None):
        self._dal.add_elements(iterable, columns)

    def add_weights(self, iterable, columns=None, *, name, selectors, description=None):
        self._dal.add_weights(iterable, columns,
                              name=name,
                              selectors=selectors,
                              description=description)

    def add_discrete_categories(self, discrete_categories):
        """Add discrete categories to the node's internal structure.

        .. code-block::

            >>> node = Node(...)
            >>> node.add_index_columns(['state', 'county', 'mcd'])
            >>> node.add_discrete_categories([{'state'}, {'state', 'county'}])

        **Understanding Discrete Categories**

        A dataset is used to model some external domain that we want
        to understand. For example, a dataset with the fields "state",
        "county", and "mcd" (minor civil division) can be used to model
        states, counties, and towns in the United States. Fields in the
        dataset contain labels that refer to entities in the domain.

        A category is said to be *discrete* if its values each contain
        enough information to identify single entities.

        In our example, "state" is a discrete category because--for
        any valid label--there exists a single entity being referred
        to. For instance, every time we see the state label
        "California", we know that the record refers to the state
        of California in the United States. There are not multiple
        states named California, so this value alone contains enough
        information to identify a single entity.

        On the other hand, "county" is a non-discrete category. While
        the label "Plymouth" is valid, it matches two different
        counties--one in Massachusetts and another in Iowa. This value
        alone does not identify a single entity. A discrete category
        for counties, would require a combination of "state" and
        "county" labels together.

        It is important to clarify that a category's discreteness is
        not determined by the uniqueness of its labels. Our example
        dataset would contain multiple records for which the state
        label is "California" so the labels are not unique despite the
        category being discrete.

        Even when a field's labels *are* unique, there is no guarantee
        that the field represents a discrete category. A category's
        discreteness is a property of the relationsip between the
        dataset and the domain it models. It's not a property that
        can be derived with certainty from the dataset alone.
        """
        self._dal.add_discrete_categories(discrete_categories)

    def remove_discrete_categories(self, discrete_categories):
        """Remove discrete categories from the node's internal
        structure.

        .. code-block::

            >>> node = Node(...)
            >>> node.remove_discrete_categories([{'county'}, {'state', 'mcd'}])
        """
        self._dal.remove_discrete_categories(discrete_categories)

