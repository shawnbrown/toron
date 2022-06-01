"""Node implementation for the Toron project."""

from itertools import chain
from itertools import combinations

from ._dal import dal_class
from ._exceptions import ToronWarning


class Node(object):
    def __init__(self, path, mode='rwc'):
        self._dal = dal_class(path, mode)

    @property
    def path(self):
        return self._dal.path

    @property
    def mode(self):
        return self._dal.mode

    def add_columns(self, columns):
        data = self._dal.get_data(['discrete_categories', 'column_names'])

        minimized = self._minimize_discrete_categories(
            data['discrete_categories'],
            [set(columns).union(data['column_names'])],
        )
        structure = self._make_structure(minimized)

        self._dal.set_data({
            'add_columns': columns,
            'structure': structure,
        })

    def add_elements(self, iterable, columns=None):
        self._dal.add_elements(iterable, columns)

    def add_weights(self, iterable, columns=None, *, name, type_info, description=None):
        self._dal.add_weights(iterable, columns,
                              name=name,
                              type_info=type_info,
                              description=description)

    def rename_columns(self, mapper):
        self._dal.rename_columns(mapper)

    @staticmethod
    def _make_structure(discrete_categories):
        """Returns a category structure generated from a base of
        discrete categories::

            >>> node._make_structure([{'A'}, {'B'}, {'B', 'C'}])
            [set(), {'A'}, {'B'}, {'B', 'C'}, {'A', 'B'}, {'A', 'B', 'C'}]

        The generated structure is almost always a topology but that
        is not necessarily the case. There are valid collections of
        discrete categories that do not result in a valid topology::

            >>> node._make_structure([{'A', 'B'}, {'B', 'C'}])
            [set(), {'A', 'B'}, {'B', 'C'}, {'A', 'B', 'C'}]

        The above result is not a topology because it's missing the
        intersection of {'A', 'B'} and {'B', 'C'}--the set {'B'}.
        """
        structure = []  # Use list to preserve lexical order of input.
        for length in range(len(discrete_categories) + 1):
            for subsequence in combinations(discrete_categories, length):
                unioned = set().union(*subsequence)
                if unioned not in structure:
                    structure.append(unioned)
        return structure

    @classmethod
    def _minimize_discrete_categories(cls, *bases):
        """Returns a minimal base of discrete categories that covers
        the same generated structure as all given bases combined::

            >>> base_a = [{'A'}, {'B'}, {'B', 'C'}]
            >>> base_b = [{'A', 'C'}, {'C'}, {'C', 'D'}]
            >>> Node._minimize_discrete_categories(base_a, base_b)
            [{'A'}, {'B'}, {'C'}, {'C', 'D'}]
        """
        base_categories = []
        for category in sorted(chain(*bases), key=len):
            structure = cls._make_structure(base_categories)
            if category not in structure:
                base_categories.append(category)

        return base_categories

    def add_discrete_categories(self, discrete_categories):
        """Add discrete categories to the node's internal structure.

        .. code-block::

            >>> node = Node(...)
            >>> node.add_columns(['state', 'county', 'mcd'])
            >>> node.add_discrete_categories([{'state'}, {'state', 'county'}])

        **Understanding Discrete Categories**

        A dataset is used to model some external domain that we want
        to understand. For example, a dataset with the fields "state",
        "county", and "mcd" (Minor Civil Division) can be used to model
        states, counties, and towns in the United States. Labels in the
        dataset refer to entities in the domain.

        A category is discrete if its values each contain enough
        information to identify single entities.

        In our example, "state" is a discrete category because--for
        any valid label--there exists a single entity being referred
        to. For instance, every time we see the state label
        "California", we know that the record refers to the state
        of California in the United States. There are not multiple
        states named California, so this value alone contains enough
        information to identify a single entity.

        On the other hand, "county" is a non-discrete category. The
        label "Plymouth" matches two different counties--one in Iowa
        and another in Massachusetts. To define a discrete category
        for counties, we need to use the combination of state and
        county labels together.

        It is important to clarify that a category's discreteness is
        not determined by the uniqueness of its labels. Our example
        dataset would contain multiple records for which the state
        label is "California" so the labels are not unique despite the
        category being discrete.

        Even when a field's labels *are* unique, we cannot know for
        sure if it represents a discrete category. A category's
        discreteness is a property of the external domain being
        modeled, not a property that can be reliably derived from the
        dataset itself.
        """
        data = self._dal.get_data(['discrete_categories', 'column_names'])
        minimized = self._minimize_discrete_categories(
            data['discrete_categories'],
            discrete_categories,
            [set(data['column_names'])],
        )

        omitted = [cat for cat in discrete_categories if (cat not in minimized)]
        if omitted:
            import warnings
            formatted = ', '.join(repr(cat) for cat in omitted)
            msg = f'omitting categories already covered: {formatted}'
            warnings.warn(msg, category=ToronWarning, stacklevel=2)

        structure = self._make_structure(minimized)
        self._dal.set_data({
            'discrete_categories': minimized,
            'structure': structure,
        })

    def remove_discrete_categories(self, discrete_categories):
        """Remove discrete categories from the node's internal
        structure.

        .. code-block::

            >>> node = Node(...)
            >>> node.remove_discrete_categories([{'county'}, {'state', 'mcd'}])
        """
        data = self._dal.get_data(['discrete_categories', 'column_names'])
        current_cats = data['discrete_categories']
        mandatory_cat = set(data['column_names'])

        if mandatory_cat in discrete_categories:
            import warnings
            formatted = ', '.join(repr(x) for x in data['column_names'])
            msg = f'cannot remove whole space: {{{mandatory_cat}}}'
            warnings.warn(msg, category=ToronWarning, stacklevel=2)
            discrete_categories.remove(mandatory_cat)  # <- Remove and continue.

        no_match = [x for x in discrete_categories if x not in current_cats]
        if no_match:
            import warnings
            formatted = ', '.join(repr(x) for x in no_match)
            msg = f'no match for categories, cannot remove: {formatted}'
            warnings.warn(msg, category=ToronWarning, stacklevel=2)

        remaining_cats = [x for x in current_cats if x not in discrete_categories]

        minimized = self._minimize_discrete_categories(
            remaining_cats,
            [mandatory_cat],
        )
        structure = self._make_structure(minimized)
        self._dal.set_data({
            'discrete_categories': minimized,
            'structure': structure,
        })

