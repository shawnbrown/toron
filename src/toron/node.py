"""Node implementation for the Toron project."""

from collections import Counter
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from itertools import chain

from toron._typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)

from . import data_access
from .data_models import (
    WeightGroup,
    BaseCrosswalkRepository,
    Crosswalk,
    JsonTypes,
)
from .data_service import (
    delete_index_record,
    find_crosswalks_by_node_reference,
)
from ._utils import (
    ToronWarning,
    normalize_tabular,
    verify_columns_set,
)


def warn_if_issues(
    counter: Counter,
    expected: str,
    stacklevel: int = 3,
    **extras: str,
) -> None:
    """Emit warning if counter contains more items than expected.

    Additional warning text items or replacement items can be passed
    as keyword arguments (**extras).
    """
    # If counter is empty or only contains expected item, exit early.
    if not counter or tuple(counter.keys()) == (expected,):
        return  # <- EXIT! (no warning)

    import warnings

    warning_text = {
        'dupe_or_empty_str': 'skipped {dupe_or_empty_str} rows with duplicate labels or empty strings',
        'empty_str': 'skipped {empty_str} rows with empty string values',
        'no_index': 'skipped {no_index} rows with non-matching index_id values',
        'mismatch': 'skipped {mismatch} rows with mismatched labels',
        'no_match': 'skipped {no_match} rows with labels that match no index',
        'no_weight': 'skipped {no_weight} rows with no matching weights',
        'merged': 'merged {merged} existing records with duplicate label values',
        'inserted': 'loaded {inserted} rows',
        'updated': 'updated {updated} rows',
        'deleted': 'deleted {deleted} rows',
    }
    warning_text.update(extras)

    msg = [v for k, v in warning_text.items() if k in counter and k != expected]
    msg.append(warning_text[expected])  # Make sure expected item is last.

    if expected not in counter:
        counter[expected] = 0  # Must add explicitly if 0 (for **kwds use).

    warnings.warn(
        message=', '.join(msg).format(**counter),
        category=ToronWarning,
        stacklevel=stacklevel,
    )


class Node(object):
    def __init__(
        self,
        *,
        backend: str = 'DAL1',
        **kwds: Any,
    ) -> None:
        self._dal = data_access.get_data_access_layer(backend)
        self._connector = self._dal.DataConnector(**kwds)

    @contextmanager
    def _managed_connection(self) -> Generator[Any, None, None]:
        connection = self._connector.acquire_connection()
        try:
            yield connection
        finally:
            self._connector.release_connection(connection)

    @contextmanager
    def _managed_cursor(
        self, connection: Optional[Any] = None, n: int = 1
    ) -> Generator[Any, None, None]:
        """A context manager to handle cursor objects from *connection*.

        When *n* is ``1``, a cursor object is created. When *n* is 2 or
        more, a tuple of *n* cursors is created.
        """
        cm = nullcontext(connection) if connection else self._managed_connection()

        with cm as connection:
            cursors = tuple(
                self._connector.acquire_cursor(connection) for _ in range(n)
            )
            try:
                yield cursors[0] if n == 1 else cursors
            finally:
                for cursor in cursors:
                    self._connector.release_cursor(cursor)

    @contextmanager
    def _managed_transaction(
        self, cursor: Optional[Any] = None
    ) -> Generator[Any, None, None]:
        cm = nullcontext(cursor) if cursor else self._managed_cursor()

        with cm as cursor:
            self._connector.transaction_begin(cursor)
            try:
                yield cursor
                self._connector.transaction_commit(cursor)
            except Exception:
                self._connector.transaction_rollback(cursor)
                raise

    @property
    def index_columns(self) -> Tuple[str, ...]:
        with self._managed_cursor() as cursor:
            return self._dal.ColumnManager(cursor).get_columns()

    def add_index_columns(self, column: str, *columns: str) -> None:
        with self._managed_transaction() as cursor:
            manager = self._dal.ColumnManager(cursor)
            manager.add_columns(column, *columns)

    def rename_index_columns(self, mapping: Dict[str, str]) -> None:
        with self._managed_transaction() as cursor:
            manager = self._dal.ColumnManager(cursor)
            manager.rename_columns(mapping)

    def drop_index_columns(self, column: str, *columns: str) -> None:
        with self._managed_transaction() as cursor:
            manager = self._dal.ColumnManager(cursor)

            if set(manager.get_columns()).issubset(chain([column], columns)):
                msg = (
                    'cannot remove all index columns\n'
                    '\n'
                    'Without at least one index column, a node cannot represent '
                    'any weights, quantities, or relations it might contain.'
                )
                raise RuntimeError(msg)

            manager.drop_columns(column, *columns)

    def insert_index(
        self,
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        data, columns = normalize_tabular(data, columns)

        counter: Counter = Counter()
        with self._managed_transaction() as cursor:
            manager = self._dal.ColumnManager(cursor)
            repository = self._dal.IndexRepository(cursor)

            index_columns = manager.get_columns()
            verify_columns_set(columns, index_columns)

            order_lookup = dict(enumerate(index_columns.index(x) for x in columns))
            sort_key = lambda item: order_lookup[item[0]]

            for row in data:
                row = [v for k, v in sorted(enumerate(row), key=sort_key)]
                try:
                    repository.add(*row)
                    counter['inserted'] += 1
                except ValueError:
                    counter['dupe_or_empty_str'] += 1

        warn_if_issues(counter, expected='inserted')

    def select_index(
        self, header: bool = False, **criteria: str
    ) -> Iterator[Sequence]:
        with self._managed_cursor() as cursor:
            if header:
                label_columns = self._dal.ColumnManager(cursor).get_columns()
                yield ('index_id',) + label_columns  # Yield header row.

            repository = self._dal.IndexRepository(cursor)
            if criteria:
                index_records = repository.find_by_label(criteria)
            else:
                index_records = repository.get_all()

            results = ((x.id,) + x.labels for x in index_records)
            for row in results:
                yield row

    def update_index(
        self,
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
        merge_on_conflict: bool = False,
    ) -> None:
        data, columns = normalize_tabular(data, columns)

        if 'index_id' not in columns:
            raise ValueError("column 'index_id' required to update records")

        counter: Counter = Counter()
        with self._managed_transaction() as cursor:
            index_repo = self._dal.IndexRepository(cursor)
            weight_repo = self._dal.WeightRepository(cursor)
            relation_repo = self._dal.RelationRepository(cursor)
            column_manager = self._dal.ColumnManager(cursor)

            label_columns = column_manager.get_columns()
            verify_columns_set(columns, label_columns, allow_extras=True)

            previously_merged = set()
            for updated_values in data:
                if '' in updated_values:
                    counter['empty_str'] += 1
                    continue  # <- Skip to next item.

                # Make a dictionary of updated labels and get existing record.
                updated_dict = dict(zip(columns, updated_values))
                index_record = index_repo.get(updated_dict['index_id'])

                if not index_record:
                    if updated_dict['index_id'] not in previously_merged:
                        counter['no_index'] += 1
                        continue  # <- Skip to next item.

                    raise ValueError(
                        f'cannot update index_id {updated_dict["index_id"]}, '
                        f'it was merged with another record on a previous '
                        f'row'
                    )

                # Make a dictionary of existing labels and apply new labels.
                label_dict = dict(zip(label_columns, index_record.labels))
                for key in label_dict.keys():
                    label_dict[key] = updated_dict[key]

                # Check for matching record, raise error or merge if exists.
                matching = next(index_repo.find_by_label(label_dict), None)
                if matching:
                    if not merge_on_conflict:
                        raise ValueError(
                            f"cannot update index_id {index_record.id}, new labels "
                            f"conflict with the existing index_id {matching.id}.\n"
                            f"\n"
                            f"To merge these records use 'node.update_index(..., "
                            f"merge_on_conflict=True)'."
                        )
                    weight_repo.merge_by_index_id(matching.id, index_record.id)
                    relation_repo.merge_by_index_id(matching.id, index_record.id)
                    index_repo.delete(matching.id)
                    counter['merged'] += 1
                    previously_merged.add(matching.id)

                # Assign updated label values and perform update action.
                index_record.labels = tuple(label_dict.values())
                index_repo.update(index_record)
                counter['updated'] += 1

            #if counter['merged']:
            #    self._dal.CrosswalkRepository(cursor).refresh_is_locally_complete()
            #    self._dal.WeightGroupRepository(cursor).refresh_is_complete()
            #    self._dal.StructureRepository(cursor).refresh_granularity()

        warn_if_issues(counter, expected='updated')

    @overload
    def delete_index(
        self,
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        ...
    @overload
    def delete_index(
        self,
        **criteria: str,
    ) -> None:
        ...
    def delete_index(self, data=None, columns=None, **criteria):
        if data and criteria:
            raise TypeError('must provide either data or keyword criteria')

        counter = Counter()
        with self._managed_connection() as connection, \
                self._managed_cursor(connection) as cursor, \
                self._managed_transaction(cursor) as cursor:
            # Line continuations (above) needed for Python 3.8 and earlier.

            index_repo = self._dal.IndexRepository(cursor)
            weight_repo = self._dal.WeightRepository(cursor)
            relation_repo = self._dal.RelationRepository(cursor)
            column_manager = self._dal.ColumnManager(cursor)

            if data:
                data, columns = normalize_tabular(data, columns)
                if 'index_id' not in columns:
                    raise ValueError("column 'index_id' required to delete records")

                label_columns = column_manager.get_columns()
                verify_columns_set(columns, label_columns, allow_extras=True)

                for row in data:
                    row_dict = dict(zip(columns, row))
                    existing_record = index_repo.get(row_dict['index_id'])

                    # Check that matching index_id exists.
                    if not existing_record:
                        counter['no_index'] += 1
                        continue  # <- Skip to next item.

                    # Check that existing labels match row labels.
                    row_labels = tuple(row_dict[k] for k in label_columns)
                    if existing_record.labels != row_labels:
                        counter['mismatch'] += 1
                        continue  # <- Skip to next item.

                    # Remove existing Index record.
                    delete_index_record(
                        existing_record.id,
                        index_repo,
                        weight_repo,
                        relation_repo,
                    )
                    counter['deleted'] += 1

            elif criteria:
                # Get a second cursor on the same connection to provide
                # matching records for the delete function.
                with self._managed_cursor(connection) as aux_cursor:
                    aux_index_repo = self._dal.IndexRepository(aux_cursor)

                    for index_record in aux_index_repo.find_by_label(criteria):
                        delete_index_record(
                            index_record.id,
                            index_repo,
                            weight_repo,
                            relation_repo,
                        )
                        counter['deleted'] += 1

            else:
                raise TypeError('expected data or keyword criteria, got neither')

            #if counter['deleted']:
            #    self._dal.CrosswalkRepository(cursor).refresh_is_locally_complete()
            #    self._dal.WeightGroupRepository(cursor).refresh_is_complete()
            #    self._dal.StructureRepository(cursor).refresh_granularity()

        warn_if_issues(counter, expected='deleted')

    @property
    def weight_groups(self) -> List[WeightGroup]:
        with self._managed_cursor() as cursor:
            return self._dal.WeightGroupRepository(cursor).get_all()

    def get_weight_group(self, name: str) -> Optional[WeightGroup]:
        with self._managed_cursor() as cursor:
            return self._dal.WeightGroupRepository(cursor).get_by_name(name)

    def add_weight_group(
        self,
        name: str,
        description: Optional[str] = None,
        selectors: Optional[Union[List[str], str]] = None,
        is_complete: bool = False,
    ) -> None:
        with self._managed_transaction() as cursor:
            self._dal.WeightGroupRepository(cursor).add(
                name=name,
                description=description,
                selectors=selectors,
                is_complete=is_complete
            )

    def edit_weight_group(self, existing_name: str, **changes: Any) -> None:
        with self._managed_transaction() as cursor:
            repository = self._dal.WeightGroupRepository(cursor)
            group = repository.get_by_name(existing_name)
            if not group:
                import warnings
                msg = f'no weight group named {existing_name!r}'
                warnings.warn(msg, category=ToronWarning, stacklevel=2)
                return  # <- EXIT!

            group = replace(group, **changes)
            repository.update(group)

    def drop_weight_group(self, existing_name: str) -> None:
        with self._managed_transaction() as cursor:
            repository = self._dal.WeightGroupRepository(cursor)
            group = repository.get_by_name(existing_name)
            if not group:
                import warnings
                msg = f'no weight group named {existing_name!r}'
                warnings.warn(msg, category=ToronWarning, stacklevel=2)
                return  # <- EXIT!

            repository.delete(group.id)

    def select_weights(
        self,
        weight_group_name: str,
        header: bool = False,
        **criteria: str,
    ) -> Iterator[Sequence]:
        with self._managed_cursor(n=2) as (cursor, aux_cursor):
            col_manager = self._dal.ColumnManager(cursor)
            group_repo = self._dal.WeightGroupRepository(cursor)
            index_repo = self._dal.IndexRepository(cursor)

            if header:
                label_columns = col_manager.get_columns()
                header_row = ('index_id',) + label_columns + (weight_group_name,)
                yield header_row

            weight_group = group_repo.get_by_name(weight_group_name)
            if not weight_group:
                import warnings
                msg = f'no weight group named {weight_group_name!r}'
                warnings.warn(msg, category=ToronWarning, stacklevel=2)
                return  # <- EXIT! (stops iteration)

            if criteria:
                index_records = index_repo.find_by_label(criteria, include_undefined=False)
            else:
                index_records = index_repo.get_all(include_undefined=False)

            weight_repo = self._dal.WeightRepository(aux_cursor)
            weight_group_id = weight_group.id
            for index in index_records:
                index_id = index.id
                weight = weight_repo.get_by_weight_group_id_and_index_id(
                    weight_group_id,
                    index_id,
                )
                weight_value = getattr(weight, 'value', None)
                yield (index.id,) + index.labels + (weight_value,)

    def insert_weights(
        self,
        weight_group_name: str,
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        data, columns = normalize_tabular(data, columns)

        counter: Counter = Counter()
        with self._managed_transaction() as cursor:
            col_manager = self._dal.ColumnManager(cursor)
            group_repo = self._dal.WeightGroupRepository(cursor)
            index_repo = self._dal.IndexRepository(cursor)
            weight_repo = self._dal.WeightRepository(cursor)

            label_columns = col_manager.get_columns()
            verify_columns_set(columns, label_columns, allow_extras=True)

            group = group_repo.get_by_name(weight_group_name)
            if group:
                weight_group_id = group.id
            else:
                group_repo.add(weight_group_name)
                group = group_repo.get_by_name(weight_group_name)
                weight_group_id = group.id  # type: ignore [union-attr]

                import warnings
                msg = f'weight_group {weight_group_name!r} created'
                warnings.warn(msg, category=ToronWarning, stacklevel=2)

            for row in data:
                row_dict = dict(zip(columns, row))
                weight_value = row_dict.pop(weight_group_name)

                if 'index_id' in row_dict:
                    index_record = index_repo.get(row_dict['index_id'])
                    if not index_record:
                        counter['no_index'] += 1
                        continue  # <- Skip to next item.

                    labels_dict = dict(zip(label_columns, index_record.labels))
                    if any(row_dict[k] != v for k, v in labels_dict.items()):
                        counter['mismatch'] += 1
                        continue  # <- Skip to next item.

                else:
                    index_records = index_repo.find_by_label(
                        {k: v for k, v in row_dict.items() if k in label_columns}
                    )
                    index_record = next(index_records, None)
                    if not index_record:
                        counter['no_match'] += 1
                        continue  # <- Skip to next item.

                weight_repo.add(
                    weight_group_id=weight_group_id,
                    index_id=index_record.id,
                    value=weight_value,
                )
                counter['inserted'] += 1

        warn_if_issues(counter, expected='inserted')

    def update_weights(
        self,
        weight_group_name: str,
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        data, columns = normalize_tabular(data, columns)

        if 'index_id' not in columns:
            raise ValueError("column 'index_id' required to update weights")
        elif weight_group_name not in columns:
            raise ValueError(f'no column named {weight_group_name!r} in  data')

        counter: Counter = Counter()
        with self._managed_transaction() as cursor:
            col_manager = self._dal.ColumnManager(cursor)
            group_repo = self._dal.WeightGroupRepository(cursor)
            index_repo = self._dal.IndexRepository(cursor)
            weight_repo = self._dal.WeightRepository(cursor)

            label_columns = col_manager.get_columns()
            verify_columns_set(columns, label_columns, allow_extras=True)

            weight_group = group_repo.get_by_name(weight_group_name)
            if not weight_group:
                msg = f'no weight group named {weight_group_name!r}'
                raise ValueError(msg)
            weight_group_id = weight_group.id

            for row in data:
                row_dict = dict(zip(columns, row))
                index_id = row_dict['index_id']

                index_record = index_repo.get(index_id)
                if not index_record:
                    counter['no_index'] += 1
                    continue  # <- Skip to next item.

                labels_dict = dict(zip(label_columns, index_record.labels))
                if any(row_dict[k] != v for k, v in labels_dict.items()):
                    counter['mismatch'] += 1
                    continue  # <- Skip to next item.

                weight_record = weight_repo.get_by_weight_group_id_and_index_id(
                    weight_group_id, index_id,
                )
                if weight_record:
                    # Update weight if it exists.
                    weight_record.value = row_dict[weight_group_name]
                    weight_repo.update(weight_record)
                    counter['updated'] += 1
                else:
                    # Add new weight if it does not exist.
                    weight_repo.add(
                        weight_group_id=weight_group_id,
                        index_id=index_id,
                        value=row_dict[weight_group_name],
                    )
                    counter['inserted'] += 1

        warn_if_issues(
            counter,
            expected='updated',
            inserted='inserted {inserted} rows that did not previously exist',
        )

    @overload
    def delete_weights(
        self,
        weight_group_name: str,
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        ...
    @overload
    def delete_weights(
        self,
        weight_group_name: str,
        **criteria: str,
    ) -> None:
        ...
    def delete_weights(
        self,
        weight_group_name,
        data=None,
        columns=None,
        **criteria,
    ):
        if data and criteria:
            raise TypeError('must provide either data or keyword criteria')

        counter = Counter()
        with self._managed_connection() as connection, \
                self._managed_cursor(connection) as cursor, \
                self._managed_transaction(cursor) as cursor:
            # Line continuations (above) needed for Python 3.8 and earlier.

            group_repo = self._dal.WeightGroupRepository(cursor)
            col_manager = self._dal.ColumnManager(cursor)
            index_repo = self._dal.IndexRepository(cursor)
            weight_repo = self._dal.WeightRepository(cursor)

            weight_group = group_repo.get_by_name(weight_group_name)
            if not weight_group:
                msg = f'no weight group named {weight_group_name!r}'
                raise ValueError(msg)
            weight_group_id = weight_group.id

            if data:
                data, columns = normalize_tabular(data, columns)
                if 'index_id' not in columns:
                    raise ValueError("column 'index_id' required to delete records")

                label_columns = col_manager.get_columns()
                verify_columns_set(columns, label_columns, allow_extras=True)

                for row in data:
                    row_dict = dict(zip(columns, row))

                    index_record = index_repo.get(row_dict['index_id'])
                    if not index_record:
                        counter['no_index'] += 1
                        continue  # <- Skip to next item.

                    index_id = index_record.id

                    labels_dict = dict(zip(label_columns, index_record.labels))
                    if any(row_dict[k] != v for k, v in labels_dict.items()):
                        counter['mismatch'] += 1
                        continue  # <- Skip to next item.

                    weight_record = weight_repo.get_by_weight_group_id_and_index_id(
                        weight_group_id, index_id,
                    )
                    if weight_record:
                        weight_repo.delete(weight_record.id)
                        counter['deleted'] += 1
                    else:
                        counter['no_weight'] += 1

            elif criteria:
                # Get a second cursor on the same connection to provide
                # matching records for the `get...()` function.
                with self._managed_cursor(connection) as aux_cursor:
                    aux_index_repo = self._dal.IndexRepository(aux_cursor)

                    for index_record in aux_index_repo.find_by_label(criteria):
                        weight_record = weight_repo.get_by_weight_group_id_and_index_id(
                            weight_group_id, index_record.id,
                        )
                        if weight_record:
                            weight_repo.delete(weight_record.id)
                            counter['deleted'] += 1
                        else:
                            counter['no_weight'] += 1

            else:
                raise TypeError('expected data or keyword criteria, got neither')

        warn_if_issues(counter, expected='deleted')

    @property
    def crosswalks(self) -> List[Crosswalk]:
        with self._managed_cursor() as cursor:
            return self._dal.CrosswalkRepository(cursor).get_all()

    @staticmethod
    def _get_crosswalk(
        node_reference: Union[str, 'Node'],
        name: Optional[str],
        crosswalk_repo: BaseCrosswalkRepository,
    ) -> Optional[Crosswalk]:
        """Get crosswalk by node reference and name."""
        if isinstance(node_reference, Node):  # If Node, find by 'unique_id' only.
            node_reference = node_reference._connector.unique_id
            matches = list(crosswalk_repo.find_by_other_unique_id(node_reference))
        else:
            matches = find_crosswalks_by_node_reference(node_reference, crosswalk_repo)

        if len({x.other_unique_id for x in matches}) > 1:
            node_info = {x.other_unique_id: x.other_filename_hint for x in matches}
            func = lambda a, b: f'{a} ({b or "<no filename>"})'
            formatted = '\n  '.join(func(k, v) for k, v in node_info.items())
            msg = f'node reference matches more than one node:\n  {formatted}'
            raise ValueError(msg)

        if name:
            filtered = [x for x in matches if x.name == name]
            if not filtered and matches:
                import warnings
                names = ', '.join(repr(x.name) for x in matches)
                msg = f'crosswalk {name!r} not found, can be: {names}'
                warnings.warn(msg, ToronWarning, stacklevel=2)
        else:
            filtered = matches

        if len(filtered) > 1:
            import warnings

            defaults = [x for x in filtered if x.is_default]
            if len(defaults) == 1:
                crosswalk = defaults[0]
                msg = f'found multiple crosswalks, using default: {crosswalk.name!r}'
                warnings.warn(msg, ToronWarning, stacklevel=2)
                return crosswalk
            else:
                names = ', '.join(repr(x.name) for x in filtered)
                msg = f'found multiple crosswalks, must specify name: {names}'
                raise ValueError(msg)

        if len(filtered) == 1:
            return filtered[0]

        return None

    def get_crosswalk(
        self, node_reference: Union[str, 'Node'], name: Optional[str] = None,
    ) -> Optional[Crosswalk]:
        with self._managed_cursor() as cursor:
            crosswalk_repo = self._dal.CrosswalkRepository(cursor)
            return self._get_crosswalk(node_reference, name, crosswalk_repo)

    def add_crosswalk(
        self,
        other_unique_id: str,
        other_filename_hint: Union[str, None],
        name: str,
        *,
        description: Optional[str] = None,
        selectors: Optional[Union[List[str], str]] = None,
        is_default: bool = False,
        user_properties: Optional[Dict[str, JsonTypes]] = None,
        other_index_hash: Optional[str] = None,
        is_locally_complete: bool = False,
    ) -> None:
        with self._managed_transaction() as cursor:
            self._dal.CrosswalkRepository(cursor).add(
                other_unique_id=other_unique_id,
                other_filename_hint=other_filename_hint,
                name=name,
                description=description,
                selectors=selectors,
                is_default=is_default,
                user_properties=user_properties,
                other_index_hash=other_index_hash,
                is_locally_complete=is_locally_complete,
            )

    def edit_crosswalk(
        self,
        node_reference: Union[str, 'Node'],
        current_name: Optional[str],
        **changes: Any,
    ) -> None:
        with self._managed_transaction() as cursor:
            crosswalk_repo = self._dal.CrosswalkRepository(cursor)
            crosswalk = self._get_crosswalk(node_reference, current_name, crosswalk_repo)

            if not crosswalk:
                import warnings
                msg = (
                    f'no crosswalk matching node_reference={node_reference!r} '
                    f'and name={current_name!r}'
                )
                warnings.warn(msg, category=ToronWarning, stacklevel=2)
                return  # <- EXIT!

            # If setting is_default=True, all other crosswalks coming from
            # the same node should be set to False.
            if changes.get('is_default') == True:
                matches = crosswalk_repo.find_by_other_unique_id(crosswalk.other_unique_id)
                for match in list(matches):  # Use list() to eagerly fetch all matches.
                    crosswalk_repo.update(replace(match, is_default=False))

            crosswalk = replace(crosswalk, **changes)
            crosswalk_repo.update(crosswalk)
