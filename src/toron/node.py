"""Node implementation for the Toron project."""

from collections import Counter
from contextlib import contextmanager, nullcontext
from itertools import chain

from toron._typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    Iterator,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from . import data_access
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

    def delete_index(
        self,
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        data, columns = normalize_tabular(data, columns)
        if 'index_id' not in columns:
            raise ValueError("column 'index_id' required to delete records")

        counter: Counter = Counter()
        with self._managed_transaction() as cursor:
            index_repo = self._dal.IndexRepository(cursor)
            weight_repo = self._dal.WeightRepository(cursor)
            column_manager = self._dal.ColumnManager(cursor)

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

                # Remove associated weight records.
                weights = weight_repo.find_by_index_id(existing_record.id)
                for weight in list(weights):
                    weight_repo.delete(weight.id)

                # Remove existing Index record.
                index_repo.delete(existing_record.id)
                counter['deleted'] += 1

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
