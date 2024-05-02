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
    delete_index_record,
)
from ._utils import (
    ToronWarning,
    normalize_tabular,
    verify_columns_set,
)


class Node(object):
    def __init__(
        self,
        *,
        backend: str = 'DAL1',
        **kwds: Dict[str, Any],
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
        self, connection: Optional[Any] = None
    ) -> Generator[Any, None, None]:
        cm = nullcontext(connection) if connection else self._managed_connection()

        with cm as connection:
            cursor = self._connector.acquire_cursor(connection)
            try:
                yield cursor
            finally:
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

        with self._managed_transaction() as cursor:
            manager = self._dal.ColumnManager(cursor)
            repository = self._dal.IndexRepository(cursor)

            index_columns = manager.get_columns()
            verify_columns_set(columns, index_columns)

            order_lookup = dict(enumerate(index_columns.index(x) for x in columns))
            sort_key = lambda item: order_lookup[item[0]]

            counter: Counter = Counter()
            for row in data:
                row = [v for k, v in sorted(enumerate(row), key=sort_key)]
                try:
                    repository.add(*row)
                    counter['loaded'] += 1
                except ValueError:
                    counter['skipped'] += 1

            if counter['skipped']:
                import warnings
                msg = (
                    f'skipped {counter["skipped"]} rows with duplicate '
                    f'labels or empty strings, loaded {counter["loaded"]} '
                    f'rows'
                )
                warnings.warn(msg, category=ToronWarning, stacklevel=2)

    def select_index(
        self, header: bool = False, **criteria: str
    ) -> Iterator[Sequence]:
        with self._managed_transaction() as cursor:
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
                        counter['no_match'] += 1
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

        # If counter includes items besides 'updated', emit a warning.
        if set(counter.keys()).difference({'updated'}):
            import warnings
            msg = []
            if counter['empty_str']:
                msg.append(f'skipped {counter["empty_str"]} rows with '
                           f'empty string values')
            if counter['no_match']:
                msg.append(f'skipped {counter["no_match"]} rows with '
                           f'non-matching index_id values')
            if counter['merged']:
                msg.append(f'merged {counter["merged"]} existing records '
                           f'with duplicate label values')
            msg.append(f'updated {counter["updated"]} rows')
            warnings.warn(', '.join(msg), category=ToronWarning, stacklevel=2)

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

        counter: Counter = Counter()
        with self._managed_transaction() as cursor:
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
                        counter['no_match'] += 1
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
                index_ids = [x.id for x in index_repo.find_by_label(criteria)]
                for index_id in index_ids:
                    delete_index_record(
                        index_id,
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

        # If counter includes items besides 'deleted', emit a warning.
        if set(counter.keys()).difference({'deleted'}):
            import warnings
            msg = []
            if counter['no_match']:
                msg.append(f'skipped {counter["no_match"]} rows with '
                           f'non-matching index_id values')
            if counter['mismatch']:
                msg.append(f'skipped {counter["mismatch"]} rows with '
                           f'mismatched labels')
            msg.append(f'deleted {counter["deleted"]} rows')
            warnings.warn(', '.join(msg), category=ToronWarning, stacklevel=2)

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
        with self._managed_cursor() as cursor:
            self._dal.WeightGroupRepository(cursor).add(
                name=name,
                description=description,
                selectors=selectors,
                is_complete=is_complete
            )

    def edit_weight_group(self, existing_name: str, **changes: Any) -> None:
        with self._managed_cursor() as cursor:
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
        with self._managed_cursor() as cursor:
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
        with self._managed_connection() as con, \
                self._managed_cursor(con) as cur1, \
                self._managed_cursor(con) as cur2:
            # Line continuations (above) needed for Python 3.8 and earlier.

            col_manager = self._dal.ColumnManager(cur1)
            group_repo = self._dal.WeightGroupRepository(cur1)
            index_repo = self._dal.IndexRepository(cur1)

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

            weight_repo = self._dal.WeightRepository(cur2)
            weight_group_id = weight_group.id
            for index in index_records:
                index_id = index.id
                weight = weight_repo.get_by_weight_group_id_and_index_id(
                    weight_group_id,
                    index_id,
                )
                yield (index.id,) + index.labels + (weight.value,)

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
            if not group:
                group_repo.add(weight_group_name)
                group = group_repo.get_by_name(weight_group_name)

                import warnings
                msg = f'weight_group {weight_group_name!r} created'
                warnings.warn(msg, category=ToronWarning, stacklevel=2)

            weight_group_id = group.id
            for row in data:
                row_dict = dict(zip(columns, row))
                weight_value = row_dict.pop(weight_group_name)

                if 'index_id' in row_dict:
                    index_record = index_repo.get(row_dict['index_id'])
                    if not index_record:
                        counter['no_match'] += 1
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

        # If counter includes items besides 'inserted', emit a warning.
        if set(counter.keys()).difference({'inserted'}):
            import warnings
            msg = []
            if counter['no_match']:
                msg.append(f'skipped {counter["no_match"]} rows that '
                           f'had no matching index record')
            if counter['mismatch']:
                msg.append(f'skipped {counter["mismatch"]} rows with '
                           f'mismatched labels')
            msg.append(f'inserted {counter["inserted"]} rows')
            warnings.warn(', '.join(msg), category=ToronWarning, stacklevel=2)

    def update_weights(
        self,
        weight_group_name: str,
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ):
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
                    counter['no_match'] += 1
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

        # If counter includes items besides 'inserted', emit a warning.
        if set(counter.keys()).difference({'updated'}):
            import warnings
            msg = []
            if counter['no_match']:
                msg.append(f'skipped {counter["no_match"]} rows that '
                           f'had no matching index record')
            if counter['mismatch']:
                msg.append(f'skipped {counter["mismatch"]} rows with '
                           f'mismatched labels')
            if counter['inserted']:
                msg.append(f'inserted {counter["inserted"]} rows that '
                           f'did not previously exist')
            msg.append(f'updated {counter["updated"]} rows')
            warnings.warn(', '.join(msg), category=ToronWarning, stacklevel=2)

    def delete_weights(
        self,
        weight_group_name: str,
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ):
        counter: Counter = Counter()
        with self._managed_transaction() as cursor:
            col_manager = self._dal.ColumnManager(cursor)
            group_repo = self._dal.WeightGroupRepository(cursor)
            index_repo = self._dal.IndexRepository(cursor)
            weight_repo = self._dal.WeightRepository(cursor)

            data, columns = normalize_tabular(data, columns)
            if 'index_id' not in columns:
                raise ValueError("column 'index_id' required to delete records")

            label_columns = col_manager.get_columns()
            verify_columns_set(columns, label_columns, allow_extras=True)

            weight_group = group_repo.get_by_name(weight_group_name)
            if not weight_group:
                msg = f'no weight group named {weight_group_name!r}'
                raise ValueError(msg)
            weight_group_id = weight_group.id

            for row in data:
                row_dict = dict(zip(columns, row))

                index_record = index_repo.get(row_dict['index_id'])
                if not index_record:
                    counter['no_match'] += 1
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

        # If counter includes items besides 'deleted', emit a warning.
        if set(counter.keys()).difference({'deleted'}):
            import warnings
            msg = []
            if counter['no_match']:
                msg.append(f'skipped {counter["no_match"]} rows that '
                           f'had no matching index record')
            if counter['mismatch']:
                msg.append(f'skipped {counter["mismatch"]} rows with '
                           f'mismatched labels')
            if counter['no_weight']:
                msg.append(f'skipped {counter["no_weight"]} rows with '
                           f'no matching weights')
            msg.append(f'deleted {counter["deleted"]} rows')
            warnings.warn(', '.join(msg), category=ToronWarning, stacklevel=2)
