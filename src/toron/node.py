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
                    f'values or empty strings, loaded {counter["loaded"]} '
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

            results = ((x.id,) + x.values for x in index_records)
            for row in results:
                yield row

    def update_index(
        self,
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        data, columns = normalize_tabular(data, columns)

        if 'index_id' not in columns:
            raise ValueError("column 'index_id' required to update records")

        with self._managed_transaction() as cursor:
            repository = self._dal.IndexRepository(cursor)
            label_columns = self._dal.ColumnManager(cursor).get_columns()

            counter: Counter = Counter()
            for new_vals in data:
                if '' in new_vals:
                    counter['empty_str'] += 1
                    continue  # <- Skip to next item.

                new_dict = dict(zip(columns, new_vals))
                index = repository.get(new_dict['index_id'])

                if index is None:
                    counter['no_match'] += 1
                    continue  # <- Skip to next item.

                index_dict = dict(zip(label_columns, index.values))
                for key in index_dict.keys():
                    if key in new_dict:
                        index_dict[key] = new_dict[key]
                index.values = tuple(index_dict.values())
                repository.update(index)
                counter['updated'] += 1

            if counter['no_match'] or counter['empty_str']:
                import warnings
                msg = []
                if counter['empty_str']:
                    msg.append(f'skipped {counter["empty_str"]} rows with '
                               f'empty string values')
                if counter['no_match']:
                    msg.append(f'skipped {counter["no_match"]} rows with '
                               f'non-matching index_id values')
                msg.append(f'updated {counter["updated"]} rows')
                warnings.warn(', '.join(msg), category=ToronWarning, stacklevel=2)
