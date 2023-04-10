"""Node implementation for the Toron project."""

from itertools import chain
from typing import (
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from ._dal import dal_class
from ._categories import make_structure
from ._categories import minimize_discrete_categories
from ._utils import TabularData


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
        data = self._dal.get_data(['discrete_categories', 'index_columns'])

        minimized = minimize_discrete_categories(
            data['discrete_categories'],
            [set(columns).union(data['index_columns'])],
        )
        structure = make_structure(minimized)

        self._dal.set_data({
            'add_index_columns': columns,
            'structure': structure,
        })

    def remove_index_columns(
        self,
        columns: List[str],
        *,
        preserve_structure: bool = True,
        preserve_granularity: bool = True,
    ) -> None:
        """Remove columns from node.

        Args:
            columns (List[str]): A list of column names to remove.
            preserve_structure (bool): When True, the operation will
                raise a ToronError if the node's category structure
                cannot be preserved. When False, the categories will
                be restructured as necessary to satisfy the remaining
                columns.
            preserve_granularity (bool): When True, the operation will
                raise a ToronError if the node's granularity cannot be
                preserved. When False, the node's granularity will be
                coarsened as necessary to satisfy the remaining columns.

        .. code-block::

            >>> node = toron.Node.from_file(...)
            >>> node.remove_index_columns(['C', 'D'])
        """
        self._dal.remove_index_columns(
            columns,
            preserve_structure=preserve_structure,
            preserve_granularity=preserve_granularity,
        )

    def rename_index_columns(self, mapper):
        self._dal.rename_index_columns(mapper)

    def index_columns(self) -> Sequence[str]:
        return self._dal.index_columns()

    def add_index_records(self, data: TabularData) -> None:
        self._dal.add_index_records(data)

    def index_records(self, **where: Union[str, int]) -> Iterable[Sequence]:
        return self._dal.index_records(**where)

    def add_weights(
        self,
        data: TabularData,
        name: str,
        *,
        selectors: Optional[Sequence[str]],
        description: Optional[str] = None,
    ) -> None:
        self._dal.add_weights(data=data,
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
        states named California, so the value alone contains enough
        information to identify a single entity.

        On the other hand, "county" is a non-discrete category. While
        the label "Plymouth" is valid, it matches two different
        counties--one in Massachusetts and another in Iowa. This value
        alone does not identify a single entity. A discrete category
        for counties, would require a combination of "state" and
        "county" labels together.

        It is important to clarify that a category's discreteness is
        not determined by the uniqueness of its labels. Our example
        dataset would contain multiple town records for which the state
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

    def structure(self) -> Sequence[Tuple]:
        """Sequence of bitmask tuples representing the node structure."""
        return self._dal.structure()

    def add_quantities(
        self,
        data: TabularData,
        value: str,
        attributes: Optional[Iterable[str]] = None,
    ) -> None:
        self._dal.add_quantities(data=data,
                                 value=value,
                                 attributes=attributes)
