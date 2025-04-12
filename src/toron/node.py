"""TopoNode implementation for the Toron project."""

import array
import os
from collections import Counter
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from itertools import chain, compress, groupby
from logging import getLogger
from math import isnan, isinf

from toron._typing import (
    Any,
    Collection,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Self,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
    overload,
)

from . import data_access
from .categories import (
    minimize_discrete_categories,
)
from .data_models import (
    COMMON_RESERVED_IDENTIFIERS,
    Index,
    Location,
    AttributesDict,
    AttributeGroup,
    WeightGroup,
    BaseCrosswalkRepository,
    Crosswalk,
    JsonTypes,
    QuantityIterator,
)
from .data_service import (
    validate_new_index_columns,
    refresh_index_hash_property,
    delete_index_record,
    get_quantity_value_sum,
    disaggregate_value,
    find_crosswalks_by_ref,
    set_default_weight_group,
    get_default_weight_group,
    get_all_discrete_categories,
    rename_discrete_categories,
    rebuild_structure_table,
    add_discrete_categories,
    refresh_structure_granularity,
    set_domain,
    get_domain,
    get_node_info_text,
)
from toron.reader import (
    NodeReader,
)
from .selectors import (
    parse_selector,
    get_greatest_unique_specificity,
)
from ._utils import (
    check_type,
    BitFlags,
    ToronWarning,
    normalize_tabular,
    verify_columns_set,
    SequenceHash,
    quantize_values,
)


applogger = getLogger(f'app-{__name__}')


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
        'empty_str': 'skipped {empty_str} rows with empty string values',
        'no_index': 'skipped {no_index} rows with non-matching index_id values',
        'mismatch': 'skipped {mismatch} rows with mismatched labels',
        'value_mismatch': 'skipped {value_mismatch} rows with mismatched values',
        'mapping_level_mismatch': 'skipped {mapping_level_mismatch} rows with mismatched mapping levels',
        'bad_mapping_level': 'skipped {bad_mapping_level} rows with invalid mapping levels',
        'approximate_relations': 'skipped {approximate_relations} approximate relations (reify to delete)',
        'no_match': 'skipped {no_match} rows with labels that match no index',
        'no_weight': 'skipped {no_weight} rows with no matching weights',
        'no_relation': 'skipped {no_relation} rows with no matching relations',
        'merged': 'merged {merged} existing records with duplicate label values',
        'inserted': 'loaded {inserted} rows',
        'updated': 'updated {updated} rows',
        'deleted': 'deleted {deleted} rows',
        'reified': 'reified {reified} records'
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


class TopoNode(object):
    """Topologically organized dataset of quantities, weights, and edges.

    This data structure's primary purpose is to:

    * Disaggregate coarse-grained data to a fine-grained base level.
    * Translate external data to use local, base level labels.

    This is the primary toron data structure.
    """
    def __init__(
        self,
        *,
        backend: str = 'DAL1',
        **kwds: Any,
    ) -> None:
        self._dal = data_access.get_data_access_layer(backend)
        self._connector = self._dal.DataConnector(**kwds)
        self._path_hint = None

        with self._managed_cursor() as cursor:
            refresh_index_hash_property(
                index_repo=self._dal.IndexRepository(cursor),
                prop_repo=self._dal.PropertyRepository(cursor),
            )

    @property
    def path_hint(self) -> Optional[str]:
        """The first known file path associated with this instance,
        if any.

        This property serves as a hint and is not guaranteed to be
        a valid or current file path. It is not persisted when the
        instance is saved to disk.

        * If the instance was loaded from a file, this is the source
          file path.
        * If the instance was created in memory and later saved, this
          is the first save path.
        * If the instance was created in memory but has never been
          saved, this will be ``None``.
        * This value can be manually overwritten at any time.
        """
        return self._path_hint

    @path_hint.setter
    def path_hint(self, value: Union[str, bytes, os.PathLike]) -> None:
        self._path_hint = os.fsdecode(value)

    @classmethod
    def from_file(cls, path: Union[str, bytes, os.PathLike], **kwds) -> Self:
        """Load a node from an existing file on drive."""
        backend = data_access.get_backend_from_path(path)
        if not backend:
            raise RuntimeError(f'invalid file format, cannot open {path!r}')

        obj = cls.__new__(cls)
        obj._dal = data_access.get_data_access_layer(backend)
        obj._connector = obj._dal.DataConnector.from_file(path, **kwds)
        obj._path_hint = os.fsdecode(path)
        return obj

    def to_file(
        self, path: Union[str, bytes, os.PathLike], *, fsync: bool = True
    ) -> None:
        """Write node data to a file."""
        self._connector.to_file(path=path, fsync=fsync)
        if not self._path_hint:
            self._path_hint = os.fsdecode(path)

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
            if self._connector.transaction_is_active(cursor):
                msg = 'cannot start a transaction within a transaction'
                raise Exception(msg)

            self._connector.transaction_begin(cursor)
            try:
                yield cursor
                self._connector.transaction_commit(cursor)
            except Exception:
                self._connector.transaction_rollback(cursor)
                raise
            finally:
                # If the transaction never completed, roll it back.
                if self._connector.transaction_is_active(cursor):
                    self._connector.transaction_rollback(cursor)

    @property
    def unique_id(self) -> str:
        """Unique identifier for the node."""
        return self._connector.unique_id

    @property
    def domain(self) -> Dict[str, str]:
        """The common set of attributes associated with all node data."""
        with self._managed_cursor() as cursor:
            return get_domain(self._dal.PropertyRepository(cursor))

    def set_domain(self, domain: Dict[str, str]) -> None:
        """Set the node's domain value."""
        with self._managed_cursor() as cursor:
            set_domain(
                domain=domain,
                column_manager=self._dal.ColumnManager(cursor),
                attribute_repo=self._dal.AttributeGroupRepository(cursor),
                property_repo=self._dal.PropertyRepository(cursor),
            )

    @property
    def discrete_categories(self) -> List[Set[str]]:
        with self._managed_cursor() as cursor:
            col_manager = self._dal.ColumnManager(cursor)
            prop_repo = self._dal.PropertyRepository(cursor)
            return get_all_discrete_categories(col_manager, prop_repo)

    def add_discrete_categories(
        self, category: Set[str], *categories: Set[str]
    ) -> None:
        categories_to_add = (category,) + categories

        with self._managed_cursor(n=2) as (cursor, aux_cursor), \
                self._managed_transaction(cursor):

            col_manager = self._dal.ColumnManager(cursor)
            prop_repo = self._dal.PropertyRepository(cursor)

            add_discrete_categories(
                categories=categories_to_add,
                column_manager=col_manager,
                property_repo=prop_repo,
            )

            rebuild_structure_table(
                column_manager=col_manager,
                property_repo=prop_repo,
                structure_repo=self._dal.StructureRepository(cursor),
                index_repo=self._dal.IndexRepository(cursor),
                aux_index_repo=self._dal.IndexRepository(aux_cursor),
                optimizations=self._dal.optimizations,
            )

    def drop_discrete_categories(
        self, category: Set[str], *categories: Set[str]
    ) -> None:
        cats_to_drop = list(chain((category,), categories))

        with self._managed_cursor(n=2) as (cursor, aux_cursor), \
                self._managed_transaction(cursor):

            col_manager = self._dal.ColumnManager(cursor)
            prop_repo = self._dal.PropertyRepository(cursor)

            columns = col_manager.get_columns()
            whole_space = set(columns)

            if whole_space in cats_to_drop:
                msg = f'cannot drop whole space: {whole_space!r}'
                raise ValueError(msg)

            existing_cats = get_all_discrete_categories(col_manager, prop_repo)
            cats_to_keep = [x for x in existing_cats if x not in cats_to_drop]

            category_sets = minimize_discrete_categories(
                cats_to_keep, [whole_space]
            )
            category_lists: JsonTypes = [list(cat) for cat in category_sets]
            prop_repo.update('discrete_categories', category_lists)

            rebuild_structure_table(
                column_manager=col_manager,
                property_repo=prop_repo,
                structure_repo=self._dal.StructureRepository(cursor),
                index_repo=self._dal.IndexRepository(cursor),
                aux_index_repo=self._dal.IndexRepository(aux_cursor),
                optimizations=self._dal.optimizations,
            )

    @property
    def index_columns(self) -> Tuple[str, ...]:
        with self._managed_cursor() as cursor:
            return self._dal.ColumnManager(cursor).get_columns()

    def add_index_columns(self, column: str, *columns: str) -> None:
        with self._managed_transaction() as cursor:
            column_manager = self._dal.ColumnManager(cursor)
            validate_new_index_columns(
                new_column_names=chain([column], columns),
                reserved_identifiers=self._dal.reserved_identifiers,
                column_manager=column_manager,
                property_repo=self._dal.PropertyRepository(cursor),
                attribute_repo=self._dal.AttributeGroupRepository(cursor),
            )
            column_manager.add_columns(column, *columns)

    def rename_index_columns(self, mapping: Dict[str, str]) -> None:
        with self._managed_transaction() as cursor:
            column_manager = self._dal.ColumnManager(cursor)
            property_repo = self._dal.PropertyRepository(cursor)

            # Check old column names.
            all_reserved_identifiers = \
                self._dal.reserved_identifiers.union(COMMON_RESERVED_IDENTIFIERS)
            for col in mapping.keys():
                if col in all_reserved_identifiers:
                    msg = f'cannot alter columns, {col!r} is a reserved identifier'
                    raise ValueError(msg)

            # Check new column names.
            validate_new_index_columns(
                new_column_names=mapping.values(),
                reserved_identifiers=self._dal.reserved_identifiers,
                column_manager=column_manager,
                property_repo=property_repo,
                attribute_repo=self._dal.AttributeGroupRepository(cursor),
            )

            # Rename columns and discrete categories.
            column_manager.rename_columns(mapping)
            rename_discrete_categories(
                mapping=mapping,
                column_manager=column_manager,
                property_repo=property_repo,
            )

    def drop_index_columns(self, column: str, *columns: str) -> None:
        with self._managed_transaction() as cursor:
            col_manager = self._dal.ColumnManager(cursor)

            all_reserved_identifiers = \
                self._dal.reserved_identifiers.union(COMMON_RESERVED_IDENTIFIERS)
            for col in chain([column], columns):
                if col in all_reserved_identifiers:
                    msg = f'cannot alter columns, {col!r} is a reserved identifier'
                    raise ValueError(msg)

            if set(col_manager.get_columns()).issubset(chain([column], columns)):
                msg = (
                    'cannot remove all index columns\n'
                    '\n'
                    'Without at least one index column, a node cannot represent '
                    'any weights, quantities, or relations it might contain.'
                )
                raise RuntimeError(msg)

            col_manager.drop_columns(column, *columns)

    def insert_index(
        self,
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        data, columns = normalize_tabular(data, columns)

        counter: Counter = Counter()
        with self._managed_cursor(n=2) as (cursor, aux_cursor), \
                self._managed_transaction(cursor):

            col_manager = self._dal.ColumnManager(cursor)
            index_repo = self._dal.IndexRepository(cursor)

            index_columns = col_manager.get_columns()
            verify_columns_set(columns, index_columns, allow_extras=True)
            extra_columns = [x for x in columns if x not in index_columns]

            label_positions = [
                columns.index(x) for x in index_columns if x in columns
            ]

            for row in data:
                # If row is empty, skip to next.
                if not row:
                    continue

                # Get label values by internal column order.
                labels = [row[pos] for pos in label_positions]

                # Insert new index record.
                try:
                    index_repo.add(*labels)
                    counter['inserted'] += 1
                except ValueError:
                    if '' in labels:
                        counter['empty_labels'] += 1
                    else:
                        counter['duplicate_labels'] += 1

            if counter['inserted']:
                prop_repo=self._dal.PropertyRepository(cursor)

                refresh_index_hash_property(
                    index_repo=index_repo,
                    prop_repo=prop_repo,
                )

                if prop_repo.get('discrete_categories'):
                    # If categories already exist, then refresh granularity.
                    refresh_structure_granularity(
                        column_manager=col_manager,
                        structure_repo=self._dal.StructureRepository(cursor),
                        index_repo=index_repo,
                        aux_index_repo=self._dal.IndexRepository(aux_cursor),
                        optimizations=self._dal.optimizations,
                    )
                else:
                    # If no categories yet, add "whole space" and build structure.
                    whole_space = set(index_columns)
                    add_discrete_categories(
                        categories=[whole_space],
                        column_manager=col_manager,
                        property_repo=prop_repo,
                    )
                    rebuild_structure_table(
                        column_manager=col_manager,
                        property_repo=prop_repo,
                        structure_repo=self._dal.StructureRepository(cursor),
                        index_repo=index_repo,
                        aux_index_repo=self._dal.IndexRepository(aux_cursor),
                        optimizations=self._dal.optimizations,
                    )

                # Existing groups will not include newly inserted indexes.
                group_repo = self._dal.WeightGroupRepository(cursor)
                for group in group_repo.get_all():
                    if group.is_complete:
                        group_repo.update(replace(group, is_complete=False))

                # Existing crosswalks will not include newly inserted indexes.
                crosswalk_repo = self._dal.CrosswalkRepository(cursor)
                for crosswalk in crosswalk_repo.get_all():
                    if crosswalk.is_locally_complete:
                        crosswalk_repo.update(replace(crosswalk, is_locally_complete=False))

        if counter['inserted']:
            applogger.info(f"loaded {counter['inserted']} index records")

        if extra_columns:
            extra_fmt = ', '.join(repr(x) for x in extra_columns)
            applogger.warning(f'ignored extra columns: {extra_fmt}')

        if counter['duplicate_labels']:
            applogger.warning(f"skipped {counter['duplicate_labels']} duplicate records")

        if counter['empty_labels']:
            applogger.warning(f"skipped {counter['empty_labels']} records having some empty string labels")

    def select_index(
        self, header: bool = False, **criteria: str
    ) -> Iterator[Sequence]:
        with self._managed_cursor() as cursor:
            if header:
                label_columns = self._dal.ColumnManager(cursor).get_columns()
                yield ('index_id',) + label_columns  # Yield header row.

            index_repo = self._dal.IndexRepository(cursor)
            if criteria:
                index_records = index_repo.find_by_label(criteria)
            else:
                index_records = index_repo.get_all()

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
        with self._managed_cursor(n=2) as (cursor, aux_cursor), \
                self._managed_transaction(cursor):

            index_repo = self._dal.IndexRepository(cursor)
            weight_repo = self._dal.WeightRepository(cursor)
            relation_repo = self._dal.RelationRepository(cursor)
            col_manager = self._dal.ColumnManager(cursor)

            label_columns = col_manager.get_columns()
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

            if counter['merged'] or counter['updated']:
                refresh_structure_granularity(
                    column_manager=col_manager,
                    structure_repo=self._dal.StructureRepository(cursor),
                    index_repo=index_repo,
                    aux_index_repo=self._dal.IndexRepository(aux_cursor),
                    optimizations=self._dal.optimizations,
                )

            if counter['merged']:
                refresh_index_hash_property(
                    index_repo=index_repo,
                    prop_repo=self._dal.PropertyRepository(cursor),
                )

                # Merges may have eliminated all unweighted indexes.
                group_repo = self._dal.WeightGroupRepository(cursor)
                for group in group_repo.get_all():
                    if (not group.is_complete
                            and weight_repo.weight_group_is_complete(group.id)):
                        group_repo.update(replace(group, is_complete=True))

                # Merges may have eliminated all unrelated indexes.
                crosswalk_repo = self._dal.CrosswalkRepository(cursor)
                for crosswalk in crosswalk_repo.get_all():
                    if (not crosswalk.is_locally_complete
                            and relation_repo.crosswalk_is_complete(crosswalk.id)):
                        crosswalk_repo.update(replace(crosswalk, is_locally_complete=True))

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
        with self._managed_cursor(n=2) as (cursor, aux_cursor), \
                self._managed_transaction(cursor):

            index_repo = self._dal.IndexRepository(cursor)
            aux_index_repo = self._dal.IndexRepository(aux_cursor)
            weight_repo = self._dal.WeightRepository(cursor)
            relation_repo = self._dal.RelationRepository(cursor)
            crosswalk_repo = self._dal.CrosswalkRepository(cursor)
            col_manager = self._dal.ColumnManager(cursor)

            if data:
                data, columns = normalize_tabular(data, columns)
                if 'index_id' not in columns:
                    raise ValueError("column 'index_id' required to delete records")

                label_columns = col_manager.get_columns()
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
                        crosswalk_repo,
                        relation_repo,
                    )
                    counter['deleted'] += 1

            elif criteria:
                for index_record in aux_index_repo.find_by_label(criteria):
                    delete_index_record(
                        index_record.id,
                        index_repo,
                        weight_repo,
                        crosswalk_repo,
                        relation_repo,
                    )
                    counter['deleted'] += 1

            else:
                raise TypeError('expected data or keyword criteria, got neither')

            if counter['deleted']:
                refresh_index_hash_property(
                    index_repo=index_repo,
                    prop_repo=self._dal.PropertyRepository(cursor),
                )

                refresh_structure_granularity(
                    column_manager=col_manager,
                    structure_repo=self._dal.StructureRepository(cursor),
                    index_repo=index_repo,
                    aux_index_repo=aux_index_repo,
                    optimizations=self._dal.optimizations,
                )

                # All unweighted indexes may have been deleted.
                group_repo = self._dal.WeightGroupRepository(cursor)
                for group in group_repo.get_all():
                    if (not group.is_complete
                            and weight_repo.weight_group_is_complete(group.id)):
                        group_repo.update(replace(group, is_complete=True))

                aux_relation_repo = self._dal.RelationRepository(aux_cursor)
                for crosswalk in crosswalk_repo.get_all():
                    # Rebuild 'other_index_hash'. When all occurances of an
                    # 'other_index_id' are associated with 'index_id' values
                    # that are deleted, this hash will change.
                    other_index_hash = SequenceHash(
                        aux_relation_repo.get_distinct_other_index_ids(
                            crosswalk.id,
                            ordered=True,
                        )
                    ).get_hexdigest()

                    # Check 'is_locally_complete' status. A crosswalk can
                    # become complete if all of the unmapped index_id values
                    # are deleted.
                    is_locally_complete = (
                        crosswalk.is_locally_complete
                        or relation_repo.crosswalk_is_complete(crosswalk.id)
                    )

                    # Assign new values and update crosswalk record.
                    crosswalk_repo.update(replace(
                        crosswalk,
                        other_index_hash=other_index_hash,
                        is_locally_complete=is_locally_complete,
                    ))

        warn_if_issues(counter, expected='deleted')

    @property
    def weight_groups(self) -> List[WeightGroup]:
        with self._managed_cursor() as cursor:
            return self._dal.WeightGroupRepository(cursor).get_all()

    def get_weight_group(self, name: str) -> Optional[WeightGroup]:
        with self._managed_cursor() as cursor:
            return self._dal.WeightGroupRepository(cursor).get_by_name(name)

    def set_default_weight_group(self, weight_group: WeightGroup) -> None:
        with self._managed_transaction() as cursor:
            set_default_weight_group(
                weight_group=weight_group,
                property_repo=self._dal.PropertyRepository(cursor),
            )

    def get_default_weight_group(self) -> Optional[WeightGroup]:
        with self._managed_transaction() as cursor:
            return get_default_weight_group(
                property_repo=self._dal.PropertyRepository(cursor),
                weight_group_repo=self._dal.WeightGroupRepository(cursor),
            )

    def add_weight_group(
        self,
        name: str,
        description: Optional[str] = None,
        selectors: Optional[Union[List[str], str]] = None,
        is_complete: bool = False,
        *,
        make_default: Optional[bool] = None,
    ) -> None:
        with self._managed_transaction() as cursor:
            weight_group_repo = self._dal.WeightGroupRepository(cursor)

            if make_default is None and not weight_group_repo.get_all():
                # If *make_default* is None and this is the first group,
                # then log a warning and make this group the default.
                applogger.warning(f'setting default weight group: {name!r}')
                make_default = True

            weight_group_repo.add(
                name=name,
                description=description,
                selectors=selectors,
                is_complete=is_complete
            )

            if make_default:
                weight_group = weight_group_repo.get_by_name(name)
                set_default_weight_group(
                    check_type(weight_group, WeightGroup),
                    self._dal.PropertyRepository(cursor)
                )

    def edit_weight_group(self, existing_name: str, **changes: Any) -> None:
        with self._managed_transaction() as cursor:
            group_repo = self._dal.WeightGroupRepository(cursor)
            group = group_repo.get_by_name(existing_name)
            if not group:
                applogger.warning(f'no weight group named {existing_name!r}')
                return  # <- EXIT!

            group = replace(group, **changes)
            group_repo.update(group)

    def drop_weight_group(self, existing_name: str) -> None:
        with self._managed_transaction() as cursor:
            group_repo = self._dal.WeightGroupRepository(cursor)
            property_repo = self._dal.PropertyRepository(cursor)

            group = group_repo.get_by_name(existing_name)
            if not group:
                applogger.warning(f'no weight group named {existing_name!r}')
                return  # <- EXIT!

            group_repo.delete(group.id)
            applogger.info(f'removed weight group {existing_name!r}')

            if group.id == property_repo.get('default_weight_group_id'):
                property_repo.delete('default_weight_group_id')
                applogger.warning(f'default weight group was removed')

    def _select_weights(
        self,
        weight_group_name: str,
        **criteria: str,
    ) -> Generator[Tuple[Index, AttributesDict, Optional[float]], None, None]:
        """Generator to yield index, attribute, and weight value tuples."""
        with self._managed_cursor(n=2) as (cursor, aux_cursor):
            index_repo = self._dal.IndexRepository(cursor)
            group_repo = self._dal.WeightGroupRepository(cursor)
            weight_repo = self._dal.WeightRepository(aux_cursor)

            weight_group = group_repo.get_by_name(weight_group_name)
            if not weight_group:
                applogger.warning(f'no weight group named {weight_group_name!r}')
                return  # <- EXIT! (stops iteration)

            if criteria:
                index_records = index_repo.find_by_label(criteria, include_undefined=False)
            else:
                index_records = index_repo.get_all(include_undefined=False)

            weight_group_id = weight_group.id
            for index in index_records:
                attributes_dict = {'weight': weight_group_name}

                weight = weight_repo.get_by_weight_group_id_and_index_id(
                    weight_group_id,
                    index.id,
                )
                weight_value = weight.value if weight else None

                yield (index, attributes_dict, weight_value)

    def select_weights(
        self,
        weight_group_name: str,
        **criteria: str,
    ) -> QuantityIterator:
        with self._managed_cursor() as cursor:
            property_repo = self._dal.PropertyRepository(cursor)
            unique_id = check_type(property_repo.get('unique_id'), str)
            index_hash = check_type(property_repo.get('index_hash'), str)
            domain = get_domain(property_repo)

            col_manager = self._dal.ColumnManager(cursor)
            label_names = col_manager.get_columns()

        quantity_iter = QuantityIterator(
            unique_id=unique_id,
            index_hash=index_hash,
            domain=domain,
            data=self._select_weights(weight_group_name, **criteria),
            label_names=label_names,
            attribute_keys=['weight'],
        )
        return quantity_iter

    def insert_weights(
        self,
        weight_group_name: str,
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
        value_column: Optional[str] = None,
        on_conflict: Literal['abort', 'skip', 'overwrite', 'sum'] = 'abort',
    ) -> None:
        if on_conflict not in ('abort', 'skip', 'overwrite', 'sum'):
            msg = (f"on_conflict must be 'abort', 'skip', 'overwrite', "
                   f"or 'sum'; got {on_conflict!r}")
            raise ValueError(msg)

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
                applogger.info(f'creating new weight group: {weight_group_name!r}')
                group_repo.add(weight_group_name)
                group = cast(WeightGroup, group_repo.get_by_name(weight_group_name))
                weight_group_id = group.id

            value_column = value_column or weight_group_name
            for row in data:
                row_dict = dict(zip(columns, row))
                weight_value = row_dict.pop(value_column)

                if not isinstance(weight_value, float):
                    try:
                        weight_value = float(weight_value)
                    except (ValueError, TypeError):
                        counter['not_realnum'] += 1
                        continue  # <- Skip to next item.

                if isnan(weight_value) or isinf(weight_value):
                    counter['not_realnum'] += 1
                    continue  # <- Skip to next item.

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

                if index_record.id == 0:
                    counter['undefined_record'] += 1
                    continue  # <- Skip to next item.

                try:
                    result_code = weight_repo.add_or_resolve(
                        weight_group_id=weight_group_id,
                        index_id=index_record.id,
                        value=weight_value,
                        on_conflict=on_conflict,
                    )
                except Exception as err:
                    msg = f'{err}; this error occured on record: {row!r}'
                    raise Exception(msg)

                counter[result_code] += 1

            group_is_complete = weight_repo.weight_group_is_complete(weight_group_id)
            if counter['inserted'] and group_is_complete:
                group_repo.update(replace(group, is_complete=True))

        applogger.info(
            f"loaded {counter['inserted']} new records into {group.name!r}"
            f"{', weight group is complete' if group_is_complete else ''}"
        )

        if counter['skipped']:
            applogger.warning(f"skipped {counter['skipped']} rows that match existing records")

        if counter['overwritten']:
            applogger.warning(f"replaced {counter['overwritten']} existing records with new weights")

        if counter['summed']:
            applogger.warning(f"combined sum of {counter['summed']} new weights together with existing records")

        if counter['not_realnum']:
            applogger.warning(f"skipped {counter['not_realnum']} rows without real number values")

        if counter['undefined_record']:
            applogger.warning(f"skipped {counter['undefined_record']} rows matching the undefined record")

        if counter['no_index']:
            applogger.warning(f"skipped {counter['no_index']} rows with no matching index_id")

        if counter['mismatch']:
            applogger.warning(f"skipped {counter['mismatch']} rows whose labels do not match the given index_id")

        if counter['no_match']:
            applogger.warning(f"skipped {counter['no_match']} rows whose labels do not match any existing index")

    def update_weights(
        self,
        weight_group_name: str,
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
        value_column: Optional[str] = None,
    ) -> None:
        data, columns = normalize_tabular(data, columns)
        value_column = value_column or weight_group_name

        if 'index_id' not in columns:
            raise ValueError("column 'index_id' required to update weights")
        elif value_column not in columns:
            raise ValueError(f'no column named {value_column!r} in data')

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
                msg = f'no weight group named {weight_group_name!r}'
                raise ValueError(msg)
            weight_group_id = group.id

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
                    # Update the weight if it exists.
                    weight_record.value = row_dict[value_column]
                    weight_repo.update(weight_record)
                    counter['updated'] += 1
                else:
                    # Add new weight if it does not exist.
                    weight_repo.add(
                        weight_group_id=weight_group_id,
                        index_id=index_id,
                        value=row_dict[value_column],
                    )
                    counter['inserted'] += 1

            group_is_complete = weight_repo.weight_group_is_complete(weight_group_id)
            if counter['inserted'] and group_is_complete:
                group_repo.update(replace(group, is_complete=True))

        applogger.info(
            f"updated {counter['updated']} existing records in {group.name!r}"
        )

        if counter['inserted']:
            applogger.warning(
                f"loaded {counter['inserted']} new records"
                f"{', weight group is complete' if group_is_complete else ''}"
            )

        if counter['no_index']:
            applogger.warning(
                f"skipped {counter['no_index']} rows with no matching index_id"
            )

        if counter['mismatch']:
            applogger.warning(
                f"skipped {counter['mismatch']} rows whose labels do not "
                f"match the given index_id"
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

            group = group_repo.get_by_name(weight_group_name)
            if not group:
                msg = f'no weight group named {weight_group_name!r}'
                raise ValueError(msg)
            weight_group_id = group.id

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

            if counter['deleted'] and group and group.is_complete:
                group_repo.update(replace(group, is_complete=False))

        applogger.info(f"deleted {counter['deleted']} weights from {group.name!r}")

        if counter['mismatch']:
            applogger.warning(f"skipped {counter['mismatch']} rows with mismatched labels")

        if counter['no_index']:
            applogger.warning(f"skipped {counter['no_index']} rows with no matching index_id")

        if counter['no_weight']:
            applogger.warning(f"skipped {counter['no_weight']} rows with no matching weight record")

    @property
    def crosswalks(self) -> List[Crosswalk]:
        with self._managed_cursor() as cursor:
            return self._dal.CrosswalkRepository(cursor).get_all()

    @staticmethod
    def _get_crosswalk(
        node_or_ref: Union['TopoNode', str],
        crosswalk_name: Optional[str],
        crosswalk_repo: BaseCrosswalkRepository,
    ) -> Optional[Crosswalk]:
        """Get crosswalk by node reference and name."""
        if isinstance(node_or_ref, TopoNode):  # If TopoNode, find by 'unique_id' only.
            matches = list(crosswalk_repo.find_by_other_unique_id(node_or_ref.unique_id))
        else:
            matches = find_crosswalks_by_ref(node_or_ref, crosswalk_repo)

        if len({x.other_unique_id for x in matches}) > 1:
            node_info = {x.other_unique_id: x.other_filename_hint for x in matches}
            func = lambda a, b: f'{a} ({b or "<no filename>"})'
            formatted = '\n  '.join(func(k, v) for k, v in node_info.items())
            msg = f'node reference matches more than one node:\n  {formatted}'
            raise ValueError(msg)

        if crosswalk_name:
            filtered = [x for x in matches if x.name == crosswalk_name]
            if not filtered and matches:
                names = ', '.join(repr(x.name) for x in matches)
                applogger.warning(
                    f'crosswalk {crosswalk_name!r} not found, can be: {names}'
                )
        else:
            filtered = matches

        if len(filtered) > 1:
            defaults = [x for x in filtered if x.is_default]
            if len(defaults) == 1:
                crosswalk = defaults[0]
                applogger.warning(
                    f'found multiple crosswalks, using default: {crosswalk.name!r}'
                )
                return crosswalk
            else:
                names = ', '.join(repr(x.name) for x in filtered)
                msg = f'found multiple crosswalks, must specify name: {names}'
                raise ValueError(msg)

        if len(filtered) == 1:
            return filtered[0]

        return None

    def get_crosswalk(
        self,
        node_or_ref: Union['TopoNode', str],
        crosswalk_name: Optional[str] = None,
    ) -> Optional[Crosswalk]:
        with self._managed_cursor() as cursor:
            crosswalk = self._get_crosswalk(
                node_or_ref,
                crosswalk_name,
                self._dal.CrosswalkRepository(cursor),
            )
            return crosswalk

    def add_crosswalk(
        self,
        node: 'TopoNode',
        crosswalk_name: str,
        *,
        description: Optional[str] = None,
        selectors: Optional[Union[List[str], str]] = None,
        is_default: Optional[bool] = None,
        user_properties: Optional[Dict[str, JsonTypes]] = None,
        other_index_hash: Optional[str] = None,
        is_locally_complete: bool = False,
    ) -> None:
        other_unique_id = node.unique_id

        with self._managed_transaction() as cursor:
            crosswalk_repo = self._dal.CrosswalkRepository(cursor)

            other_crosswalks = \
                list(crosswalk_repo.find_by_other_unique_id(other_unique_id))

            if is_default is None:
                if not other_crosswalks:
                    # If *is_default* is None and this is the first crosswalk
                    # from *other_unique_id*, then log a warning and make this
                    # crosswalk the default.
                    applogger.warning(f'setting default crosswalk: {crosswalk_name!r}')
                    is_default = True
                else:
                    is_default = False

            if is_default:
                # If *is_default* is True, all other crosswalks coming from
                # the same node should be set to False.
                for crosswalk in other_crosswalks:
                    if crosswalk.is_default:
                        crosswalk_repo.update(replace(crosswalk, is_default=False))

            crosswalk_repo.add(
                other_unique_id=other_unique_id,
                other_filename_hint=node.path_hint,
                name=crosswalk_name,
                description=description,
                selectors=selectors,
                is_default=is_default,
                user_properties=user_properties,
                other_index_hash=other_index_hash,
                is_locally_complete=is_locally_complete,
            )

    def edit_crosswalk(
        self,
        node_or_ref: Union['TopoNode', str],
        crosswalk_name: str,
        **changes: Any,
    ) -> None:
        with self._managed_transaction() as cursor:
            crosswalk_repo = self._dal.CrosswalkRepository(cursor)
            crosswalk = self._get_crosswalk(node_or_ref, crosswalk_name, crosswalk_repo)

            if not crosswalk:
                applogger.warning(
                    f'no crosswalk matching node reference {node_or_ref!r} '
                    f'and name {crosswalk_name!r}'
                )
                return  # <- EXIT!

            # If setting is_default=True, all other crosswalks coming from
            # the same node should be set to False.
            if changes.get('is_default') == True:
                matches = crosswalk_repo.find_by_other_unique_id(crosswalk.other_unique_id)
                for match in list(matches):  # Use list() to eagerly fetch all matches.
                    crosswalk_repo.update(replace(match, is_default=False))

            crosswalk = replace(crosswalk, **changes)
            crosswalk_repo.update(crosswalk)

    def drop_crosswalk(
        self,
        node_or_ref: Union['TopoNode', str],
        crosswalk_name: str,
    ) -> None:
        with self._managed_transaction() as cursor:
            crosswalk_repo = self._dal.CrosswalkRepository(cursor)
            crosswalk = self._get_crosswalk(node_or_ref, crosswalk_name, crosswalk_repo)
            if not crosswalk:
                applogger.warning(
                    f'no crosswalk matching node reference {node_or_ref!r} '
                    f'and name {crosswalk_name!r}'
                )
                return  # <- EXIT!

            crosswalk_repo.delete(crosswalk.id)

    def select_relations(
        self,
        node_or_ref: Union['TopoNode', str],
        crosswalk_name: Optional[str] = None,
        header: bool = False,
        **criteria: str,
    ) -> Iterator[Sequence]:
        with self._managed_cursor(n=2) as (cursor, aux_cursor):
            col_manager = self._dal.ColumnManager(cursor)
            index_repo = self._dal.IndexRepository(cursor)
            crosswalk_repo = self._dal.CrosswalkRepository(cursor)

            crosswalk = self._get_crosswalk(node_or_ref, crosswalk_name, crosswalk_repo)
            if not crosswalk:
                raise ValueError(
                    f'no crosswalk matching node reference {node_or_ref!r} '
                    f'and name {crosswalk_name!r}'
                )

            label_columns = col_manager.get_columns()

            if header:
                header_row = (('other_index_id', crosswalk.name, 'index_id')
                              + label_columns
                              + ('ambiguous_fields',))
                yield header_row

            if criteria:
                index_records = index_repo.find_by_label(criteria, include_undefined=True)
            else:
                index_records = index_repo.get_all(include_undefined=True)

            relation_repo = self._dal.RelationRepository(aux_cursor)
            crosswalk_id = crosswalk.id
            for index in index_records:
                index_id = index.id
                relations = list(relation_repo.find_by_ids(
                    crosswalk_id=crosswalk_id, index_id=index_id
                ))
                if relations:
                    for rel in relations:
                        if rel.mapping_level:
                            # Build a description of the columns that were
                            # left unspecified in the original mapping.
                            bit_flags = BitFlags(rel.mapping_level)
                            inverted_bits = [(not bit) for bit in bit_flags]
                            ambiguous_labels = compress(label_columns, inverted_bits)
                            ambiguous_desc = ', '.join(ambiguous_labels)
                        else:
                            ambiguous_desc = None

                        row_tuple = ((rel.other_index_id, rel.value, index_id)
                                     + index.labels
                                     + (ambiguous_desc,))
                        yield row_tuple
                else:
                    yield (None, None, index_id) + index.labels + (None,)

    def insert_relations2(
        self,
        node_or_ref: Union['TopoNode', str],
        crosswalk_name: Optional[str],
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        r"""Insert relations for specified crosswalk.

        .. code-block:: python

            >>> node.insert_relations2(
            ...     node_or_ref='myfile',
            ...     crosswalk_name='pop2000',
            ...     data=[
            ...         ('other_index_id', 'index_id', 'mapping_level', 'pop2000'),
            ...         (0, 0, b'\xe0',  0.0),
            ...         (1, 1, b'\xe0', 10.0),
            ...         (2, 2, b'\xe0', 20.0),
            ...         (3, 2, b'\xe0',  5.0),
            ...         (3, 3, b'\xe0', 15.0),
            ...     ]
            ... )

        .. note::

            This is a new implementation to use with the updated Mapper
            class. Once the use cases for relation handling are better
            defined, the existing methods should be revisited and
            finalized.
        """
        # TODO: Fix `columns` not being used in the function.
        data, columns = normalize_tabular(data, columns)

        counter: Dict[str, int] = Counter()
        with self._managed_cursor(n=2) as (cursor, aux_cursor), \
                self._managed_transaction(cursor):
            crosswalk_repo = self._dal.CrosswalkRepository(cursor)
            relation_repo = self._dal.RelationRepository(cursor)

            # Get crosswalk id.
            crosswalk = self._get_crosswalk(node_or_ref, crosswalk_name,
                                            crosswalk_repo)
            if not crosswalk:
                raise ValueError(
                    f'no crosswalk matching node reference {node_or_ref!r} '
                    f'and name {crosswalk_name!r}'
                )
            crosswalk_id = crosswalk.id

            # Get allowed structure values.
            structure = {bytes(BitFlags(x.bits)) if any(x.bits) else None
                         for x
                         in self._dal.StructureRepository(cursor).get_all()}

            # Insert records from given `data`.
            for row in data:
                other_index_id, index_id, mapping_level, value = row[:4]

                # Verify mapping level.
                if mapping_level not in structure:
                    counter['bad_mapping_level'] += 1
                    continue  # <- Skip to next item.

                relation_repo.add(
                    crosswalk_id=crosswalk_id,
                    other_index_id=other_index_id,
                    index_id=index_id,
                    mapping_level=mapping_level,
                    value=value,
                )
                counter['inserted'] += 1

            # If missing, add the "undefined" relation (0 -> 0).
            if next(relation_repo.find_by_ids(crosswalk_id=crosswalk_id,
                                              other_index_id=0,
                                              index_id=0), None) is None:
                relation_repo.add(
                    crosswalk_id=crosswalk_id,
                    other_index_id=0,  # <- Undefined record.
                    index_id=0,  # <- Undefined record.
                    mapping_level=None,
                    value=0.0,
                )

            if counter['inserted'] and crosswalk:
                applogger.info(f"loaded {counter['inserted']} relations")

                # Get ordered sequence of other_index_id values.
                aux_relation_repo = self._dal.RelationRepository(aux_cursor)
                other_index_ids = aux_relation_repo.get_distinct_other_index_ids(
                    crosswalk_id,
                    ordered=True,  # <- Must be ordered for `sequence_hash`.
                )

                # Build new hash and refresh proportion values.
                sequence_hash = SequenceHash()
                for other_index_id in other_index_ids:
                    sequence_hash.add_value(other_index_id)
                    relation_repo.refresh_proportions(crosswalk_id, other_index_id)

                # Assign new values and update crosswalk record.
                crosswalk_repo.update(replace(
                    crosswalk,
                    other_index_hash=sequence_hash.get_hexdigest(),
                    is_locally_complete=relation_repo.crosswalk_is_complete(crosswalk_id),
                ))
            else:
                applogger.warning('no relations loaded')

            if counter['bad_mapping_level']:
                applogger.warning(
                    f"skipped {counter['bad_mapping_level']} relations with "
                    f"invalid mapping levels"
                )

    def insert_relations(
        self,
        node_or_ref: Union['TopoNode', str],
        crosswalk_name: Optional[str],
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        data, columns = normalize_tabular(data, columns)

        if tuple(columns[:3]) != ('other_index_id', crosswalk_name, 'index_id'):
            raise ValueError(
                f"columns should be start with "
                f"('other_index_id', {crosswalk_name!r}, 'index_id', ...); "
                f"got ({columns[0]!r}, {columns[1]!r}, {columns[2]!r}, ...)"
            )

        counter: Counter = Counter()
        with self._managed_cursor(n=2) as (cursor, aux_cursor), \
                self._managed_transaction(cursor):
            col_manager = self._dal.ColumnManager(cursor)
            crosswalk_repo = self._dal.CrosswalkRepository(cursor)
            relation_repo = self._dal.RelationRepository(cursor)
            index_repo = self._dal.IndexRepository(cursor)
            struct_repo = self._dal.StructureRepository(cursor)

            label_columns = col_manager.get_columns()
            verify_columns_set(columns, label_columns, allow_extras=True)

            crosswalk = self._get_crosswalk(node_or_ref, crosswalk_name, crosswalk_repo)
            if not crosswalk:
                raise ValueError(
                    f'no crosswalk matching node reference {node_or_ref!r} '
                    f'and name {crosswalk_name!r}'
                )
            crosswalk_id = crosswalk.id

            structure = {BitFlags(x.bits) for x in struct_repo.get_all()}

            for row in data:
                # Get values for relation record.
                other_index_id, value, index_id = row[:3]
                row_dict = dict(zip(columns[3:], row[3:]))

                # Check for matching index_id.
                index_record = index_repo.get(index_id)
                if not index_record:
                    counter['no_index'] += 1
                    continue  # <- Skip to next item.

                # Check for matching index labels.
                row_labels = tuple(row_dict[x] for x in label_columns)
                if row_labels != index_record.labels:
                    counter['mismatch'] += 1
                    continue  # <- Skip to next item.

                # Verify mapping level if provided.
                mapping_level = row_dict.get('mapping_level') or None
                if mapping_level and BitFlags(mapping_level) not in structure:
                    counter['bad_mapping_level'] += 1
                    continue  # <- Skip to next item.

                # Add relation record.
                relation_repo.add(
                    crosswalk_id=crosswalk_id,
                    other_index_id=other_index_id,
                    index_id=index_id,
                    value=value,
                    mapping_level=mapping_level,
                )
                counter['inserted'] += 1

            if counter['inserted'] and crosswalk:
                # Get ordered sequence of other_index_id values.
                aux_relation_repo = self._dal.RelationRepository(aux_cursor)
                other_index_ids = aux_relation_repo.get_distinct_other_index_ids(
                    crosswalk_id,
                    ordered=True,  # <- Must be ordered for `sequence_hash`.
                )

                # Build new hash and refresh proportion values.
                sequence_hash = SequenceHash()
                for other_index_id in other_index_ids:
                    sequence_hash.add_value(other_index_id)
                    relation_repo.refresh_proportions(crosswalk_id, other_index_id)

                # Assign new values and update crosswalk record.
                crosswalk_repo.update(replace(
                    crosswalk,
                    other_index_hash=sequence_hash.get_hexdigest(),
                    is_locally_complete=relation_repo.crosswalk_is_complete(crosswalk_id),
                ))

        warn_if_issues(counter, expected='inserted')

    def update_relations(
        self,
        node_or_ref: Union['TopoNode', str],
        crosswalk_name: Optional[str],
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        data, columns = normalize_tabular(data, columns)

        if tuple(columns[:3]) != ('other_index_id', crosswalk_name, 'index_id'):
            raise ValueError(
                f"columns should be start with "
                f"('other_index_id', {crosswalk_name!r}, 'index_id', ...); "
                f"got ({columns[0]!r}, {columns[1]!r}, {columns[2]!r}, ...)"
            )

        counter: Counter = Counter()
        with self._managed_cursor(n=2) as (cursor, aux_cursor), \
                self._managed_transaction(cursor):
            col_manager = self._dal.ColumnManager(cursor)
            crosswalk_repo = self._dal.CrosswalkRepository(cursor)
            relation_repo = self._dal.RelationRepository(cursor)
            index_repo = self._dal.IndexRepository(cursor)
            struct_repo = self._dal.StructureRepository(cursor)

            label_columns = col_manager.get_columns()
            verify_columns_set(columns, label_columns, allow_extras=True)

            crosswalk = self._get_crosswalk(node_or_ref, crosswalk_name, crosswalk_repo)
            if not crosswalk:
                raise ValueError(
                    f'no crosswalk matching node reference {node_or_ref!r} '
                    f'and name {crosswalk_name!r}'
                )
            crosswalk_id = crosswalk.id

            structure = {BitFlags(x.bits) for x in struct_repo.get_all()}

            for row in data:
                # Get values for relation record.
                other_index_id, value, index_id = row[:3]
                row_dict = dict(zip(columns[3:], row[3:]))

                # Check for matching index_id.
                index_record = index_repo.get(index_id)
                if not index_record:
                    counter['no_index'] += 1
                    continue  # <- Skip to next item.

                # Check for matching index labels.
                row_labels = tuple(row_dict[x] for x in label_columns)
                if row_labels != index_record.labels:
                    counter['mismatch'] += 1
                    continue  # <- Skip to next item.

                # Verify mapping level if provided.
                mapping_level = row_dict.get('mapping_level') or None
                if mapping_level and BitFlags(mapping_level) not in structure:
                    counter['bad_mapping_level'] += 1
                    continue  # <- Skip to next item.

                # Find matching relation record (can only match one record).
                relation_match = relation_repo.find_by_ids(
                    crosswalk_id=crosswalk_id,
                    other_index_id=other_index_id,
                    index_id=index_id,
                )
                relation_record = next(relation_match, None)

                if relation_record:
                    # Update relation record if it exists.
                    relation_record.value = value
                    relation_record.mapping_level = mapping_level
                    relation_repo.update(relation_record)
                    counter['updated'] += 1
                else:
                    # Add new relation if it does not exist.
                    relation_repo.add(
                        crosswalk_id=crosswalk_id,
                        other_index_id=other_index_id,
                        index_id=index_id,
                        value=value,
                        mapping_level=mapping_level,
                    )
                    counter['inserted'] += 1

            aux_relation_repo = self._dal.RelationRepository(aux_cursor)
            if counter['inserted']:
                # Get ordered sequence of other_index_id values.
                other_index_ids = aux_relation_repo.get_distinct_other_index_ids(
                    crosswalk_id,
                    ordered=True,  # <- Must be ordered for `sequence_hash`.
                )

                # Build new hash and refresh proportion values.
                sequence_hash = SequenceHash()
                for other_index_id in other_index_ids:
                    sequence_hash.add_value(other_index_id)
                    relation_repo.refresh_proportions(crosswalk_id, other_index_id)

                # Update crosswalk's hash and is-complete status.
                crosswalk_repo.update(replace(
                    crosswalk,
                    other_index_hash=sequence_hash.get_hexdigest(),
                    is_locally_complete=relation_repo.crosswalk_is_complete(crosswalk_id),
                ))

            elif counter['updated']:
                # Get iterator of other_index_id values.
                other_index_ids = aux_relation_repo.get_distinct_other_index_ids(crosswalk_id)

                # Refresh proportion values.
                for other_index_id in other_index_ids:
                    relation_repo.refresh_proportions(crosswalk_id, other_index_id)

        warn_if_issues(
            counter,
            expected='updated',
            inserted='inserted {inserted} rows that did not previously exist',
        )

    @overload
    def delete_relations(
        self,
        node_or_ref: Union['TopoNode', str],
        crosswalk_name: Optional[str],
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        ...
    @overload
    def delete_relations(
        self,
        node_or_ref: Union['TopoNode', str],
        crosswalk_name: Optional[str],
        **criteria: str,
    ) -> None:
        ...
    def delete_relations(
        self,
        node_or_ref,
        crosswalk_name=None,
        data=None,
        columns=None,
        **criteria,
    ):
        if data and criteria:
            raise TypeError('must provide either data or keyword criteria')

        counter = Counter()
        with self._managed_cursor(n=2) as (cursor, aux_cursor), \
                self._managed_transaction(cursor):

            col_manager = self._dal.ColumnManager(cursor)
            crosswalk_repo = self._dal.CrosswalkRepository(cursor)
            relation_repo = self._dal.RelationRepository(cursor)
            aux_relation_repo = self._dal.RelationRepository(aux_cursor)
            index_repo = self._dal.IndexRepository(cursor)

            crosswalk = self._get_crosswalk(node_or_ref, crosswalk_name, crosswalk_repo)
            if not crosswalk:
                raise ValueError(
                    f'no crosswalk matching node reference {node_or_ref!r} '
                    f'and name {crosswalk_name!r}'
                )
            crosswalk_id = crosswalk.id

            if data:
                data, columns = normalize_tabular(data, columns)

                label_columns = col_manager.get_columns()
                verify_columns_set(columns, label_columns, allow_extras=True)

                if tuple(columns[:3]) != ('other_index_id', crosswalk_name, 'index_id'):
                    raise ValueError(
                        f"columns should be start with "
                        f"('other_index_id', {crosswalk_name!r}, 'index_id', ...); "
                        f"got ({columns[0]!r}, {columns[1]!r}, {columns[2]!r}, ...)"
                    )

                for row in data:
                    # Get values for relation record.
                    other_index_id, value, index_id = row[:3]
                    row_dict = dict(zip(columns[3:], row[3:]))

                    # Check for matching index_id.
                    index_record = index_repo.get(index_id)
                    if not index_record:
                        counter['no_index'] += 1
                        continue  # <- Skip to next item.

                    # Check for matching index labels.
                    row_labels = tuple(row_dict[x] for x in label_columns)
                    if row_labels != index_record.labels:
                        counter['mismatch'] += 1
                        continue  # <- Skip to next item.

                    # Find matching relation record (can only match one record).
                    relation_match = relation_repo.find_by_ids(
                        crosswalk_id=crosswalk_id,
                        other_index_id=other_index_id,
                        index_id=index_id,
                    )
                    relation_record = next(relation_match, None)

                    if relation_record:
                        # Check for matching value.
                        if relation_record.value != float(value):
                            counter['value_mismatch'] += 1
                            continue  # <- Skip to next item.

                        # Check for mapping level, skip if present.
                        if relation_record.mapping_level:
                            counter['approximate_relations'] += 1
                            continue  # <- Skip to next item.

                        relation_repo.delete(relation_record.id)
                        counter['deleted'] += 1
                    else:
                        counter['no_relation'] += 1

            elif criteria:
                label_columns = col_manager.get_columns()
                criteria_keys = set(criteria.keys())
                criteria_level = BitFlags(x in criteria_keys for x in label_columns)

                index_records = index_repo.find_by_label(criteria, include_undefined=True)
                for index in index_records:
                    relations = list(aux_relation_repo.find_by_ids(
                        crosswalk_id=crosswalk_id,
                        index_id=index.id,
                    ))
                    for rel in relations:
                        bitwise_or = criteria_level | BitFlags(rel.mapping_level)
                        if criteria_level == bitwise_or:
                            # If criteria uses the same or more columns than
                            # the relation's mapping level, then delete it.
                            aux_relation_repo.delete(rel.id)
                            counter['deleted'] += 1
                        else:
                            # If the relation's mapping level uses columns that
                            # aren't used in the criteria, then don't delete it.
                            counter['mapping_level_mismatch'] += 1

            else:
                raise TypeError('expected data or keyword criteria, got neither')

            if counter['deleted']:
                # Get ordered sequence of other_index_id values.
                other_index_ids = aux_relation_repo.get_distinct_other_index_ids(
                    crosswalk_id,
                    ordered=True,  # <- Must be ordered for `sequence_hash`.
                )

                # Build new hash and refresh proportion values.
                sequence_hash = SequenceHash()
                for other_index_id in other_index_ids:
                    sequence_hash.add_value(other_index_id)
                    relation_repo.refresh_proportions(crosswalk_id, other_index_id)

                # Update crosswalk's hash and is-complete status.
                crosswalk_repo.update(replace(
                    crosswalk,
                    other_index_hash=sequence_hash.get_hexdigest(),
                    is_locally_complete=relation_repo.crosswalk_is_complete(crosswalk_id),
                ))

        warn_if_issues(counter, expected='deleted')

    def reify_relations(
        self,
        node_or_ref: Union['TopoNode', str],
        crosswalk_name: str,
        **criteria: str,
    ) -> None:
        """Remove 'mapping_level' from approximate relations associated
        with the specified crosswalk (*node_or_ref* and *crosswalk_name*).

        A 'mapping_level' records the level of granularity at which an
        approximate relation (or partial relation) was established. By
        removing it, we indicate that the probabilistically assigned
        value associated with an approximate relation should now be
        considered as the actual value.

        Parameters
        ----------
        node_or_ref : Union[TopoNode, str]
            The node from which the crosswalk is coming.
        crosswalk_name : str
            The name of the crosswalk. This is needed because multiple
            crosswalks can come from the same node.
        **criteria : str
            Additional keyword arguments to select only those relations
            associated the given index labels for reification.
        """
        counter: Counter = Counter()
        with self._managed_cursor(n=2) as (cursor, aux_cursor):
            col_manager = self._dal.ColumnManager(cursor)
            index_repo = self._dal.IndexRepository(cursor)
            crosswalk_repo = self._dal.CrosswalkRepository(cursor)

            crosswalk = self._get_crosswalk(node_or_ref, crosswalk_name, crosswalk_repo)
            if not crosswalk:
                raise ValueError(
                    f'no crosswalk matching node reference {node_or_ref!r} '
                    f'and name {crosswalk_name!r}'
                )

            if criteria:
                # Reify selected relations in crosswalk.
                label_columns = col_manager.get_columns()
                criteria_keys = set(criteria.keys())
                criteria_level = BitFlags(x in criteria_keys for x in label_columns)

                index_records = index_repo.find_by_label(criteria, include_undefined=True)

                relation_repo = self._dal.RelationRepository(aux_cursor)
                crosswalk_id = crosswalk.id
                for index in index_records:
                    relations = list(relation_repo.find_by_ids(
                        crosswalk_id=crosswalk_id, index_id=index.id
                    ))
                    for rel in relations:
                        if rel.mapping_level:
                            bitwise_or = criteria_level | BitFlags(rel.mapping_level)
                            if criteria_level == bitwise_or:
                                relation_repo.update(replace(rel, mapping_level=None))
                                counter['reified'] += 1
                            else:
                                counter['mapping_level_mismatch'] += 1

            else:
                # Reify ALL relations in crosswalk.
                relation_repo = self._dal.RelationRepository(cursor)
                aux_relation_repo = self._dal.RelationRepository(aux_cursor)

                for rel in relation_repo.find_by_ids(crosswalk_id=crosswalk.id):
                    if rel.mapping_level:
                        aux_relation_repo.update(replace(rel, mapping_level=None))
                        counter['reified'] += 1

        warn_if_issues(counter, expected='reified')

    def insert_quantities(
        self,
        *,
        value: str,
        attributes: Union[str, Iterable[str]],
        data: Union[Iterable[Sequence], Iterable[Dict]],
        columns: Optional[Sequence[str]] = None,
    ) -> None:
        data, columns = normalize_tabular(data, columns)

        if isinstance(attributes, str):
            attributes = [attributes]
        else:
            attributes = list(attributes)

        counter: Counter = Counter()
        with self._managed_transaction() as cursor:
            property_repo = self._dal.PropertyRepository(cursor)
            location_repo = self._dal.LocationRepository(cursor)
            attribute_repo = self._dal.AttributeGroupRepository(cursor)
            quantity_repo = self._dal.QuantityRepository(cursor)

            domain_dict = get_domain(property_repo)

            if any((x in attributes) for x in domain_dict.keys()):
                applogger.warning('removing domain columns from attributes')
                attributes = [x for x in attributes if x not in domain_dict.keys()]

            label_columns = location_repo.get_label_columns()

            verify_columns_set(
                columns=columns,
                required_columns=chain(label_columns, domain_dict.keys(), attributes, [value]),
                allow_extras=True,
            )

            for row in data:
                row_dict = dict(zip(columns, row))

                # Check that domain columns contain required values.
                if not all((row_dict.get(k) == v) for k, v in domain_dict.items()):
                    counter['bad_domain'] += 1
                    continue

                # Parse row into separate label and attribute dictionaries.
                labels_dict = {k: row_dict[k] for k in label_columns}
                attr_dict = {k: row_dict[k] for k in attributes if row_dict[k]}

                # Skip record if it has no attribute values.
                if not attr_dict:
                    continue

                # Get `location` and `attribute_group` instances.
                location = location_repo.get_by_labels_add_if_missing(labels_dict)
                attribute_group = attribute_repo.get_by_value_add_if_missing(attr_dict)

                # Add quantity.
                quantity_repo.add(
                    location_id=location.id,
                    attribute_group_id=attribute_group.id,
                    value=row_dict[value],
                )
                counter['inserted'] += 1

        if counter['inserted']:
            applogger.info(f'loaded {counter["inserted"]} quantities')
        else:
            applogger.warning('no quantities loaded')

        if counter['bad_domain']:
            items = [f'{k} must be {v!r}' for k, v in domain_dict.items()]
            applogger.warning(
                f'skipped {counter["bad_domain"]} quantities with '
                f'bad domain values: {", ".join(items)}'
            )

    def _disaggregate(
        self,
        attribute_id_filter: Optional[List[int]] = None,
        quantize: bool = False,
    ) -> Generator[Tuple[int, AttributesDict, float], None, None]:
        """Generator to yield index, attribute, and quantity tuples."""
        domain = self.domain  # Assign locally to reduce dot-lookups.

        with self._managed_cursor(n=3) as (cur1, cur2, cur3):
            # These repository instances can share a single cursor.
            property_repo = self._dal.PropertyRepository(cur1)
            weight_group_repo = self._dal.WeightGroupRepository(cur1)
            structure_repo = self._dal.StructureRepository(cur1)
            attribute_repo = self._dal.AttributeGroupRepository(cur1)
            index_repo = self._dal.IndexRepository(cur1)
            weight_repo = self._dal.WeightRepository(cur1)

            # These repositories must have their own cursors.
            location_repo = self._dal.LocationRepository(cur2)
            quantity_repo = self._dal.QuantityRepository(cur3)

            # Get the default weight group and make sure it's complete.
            default_weight_group = get_default_weight_group(
                property_repo, weight_group_repo, required=True
            )
            if not default_weight_group.is_complete:
                msg = f'default weight group {default_weight_group.name!r} is not complete'
                raise RuntimeError(msg)

            # Build dict of index id values and attribute selector objects.
            weight_groups = weight_group_repo.get_all()
            weight_groups = [wg for wg in weight_groups if (wg.is_complete and wg.selectors)]
            func = lambda selectors: [parse_selector(s) for s in selectors]
            selector_dict = {wg.id: func(wg.selectors) for wg in weight_groups}

            # Get structure records (ordered from most- to least-granular).
            structures = structure_repo.get_all()
            finest_granularity = structures[0].granularity

            # Get label column names (in table definition order).
            label_columns = location_repo.get_label_columns()

            for structure in structures:
                quantities = quantity_repo.find_by_multiple(
                    structure=structure,
                    attribute_id_filter=attribute_id_filter,
                )
                grouped = groupby(quantities, key=lambda x: x.location_id)

                if structure.granularity == finest_granularity:
                    for location_id, group in grouped:
                        # Use location labels to make index search criteria.
                        location = cast(Location, location_repo.get(location_id))
                        zipped = zip(label_columns, location.labels)
                        criteria = {k: v for k, v in zipped if v != ''}

                        # Since we're at the finest granularity, there can
                        # only be one matching index record.
                        index_id = next(index_repo.find_index_ids_by_label(criteria))

                        # Yield whole quantity values (cannot be disaggregated
                        # further).
                        for quantity in group:
                            attribute_group = cast(
                                AttributeGroup,
                                attribute_repo.get(quantity.attribute_group_id),
                            )
                            attributes = attribute_group.attributes
                            attributes.update(domain)  # Add domain to attributes.
                            yield (index_id, attributes, quantity.value)
                else:
                    for location_id, group in grouped:
                        # Use location labels to make index search criteria.
                        location = cast(Location, location_repo.get(location_id))
                        zipped = zip(label_columns, location.labels)
                        criteria = {k: v for k, v in zipped if v != ''}

                        # Get all index records associated with the location
                        # (using `array` for smallest memory footprint).
                        index_ids = array.array(
                            'i', index_repo.find_index_ids_by_label(criteria)
                        )

                        for quantity in group:
                            attribute_group = cast(
                                AttributeGroup,
                                attribute_repo.get(quantity.attribute_group_id),
                            )
                            attributes = attribute_group.attributes
                            attributes.update(domain)  # Add domain to attributes.

                            weight_group_id = get_greatest_unique_specificity(
                                row_dict=attributes,
                                selector_dict=selector_dict,
                                default=default_weight_group.id,
                            )

                            # Split quantity into individual components (one
                            # record for each associated index).
                            disaggregated = disaggregate_value(
                                quantity.value,
                                index_ids,
                                weight_group_id,
                                weight_repo=weight_repo,
                            )

                            # Optionally, quantize results to whole values
                            # where possible.
                            if quantize:
                                disaggregated = quantize_values(
                                    items=disaggregated,
                                    sum_total=quantity.value,
                                )

                            # Yield disaggregated values.
                            for index_id, value in disaggregated:
                                yield (index_id, attributes, value)

    def __call__(
        self,
        *selectors: str,
        cache_to_drive: bool = False,
        quantize: bool = False,
        sum_by_attrs: Optional[Union[Collection[str], str]] = None,
    ) -> NodeReader:
        """Return rows with disaggregated quantity values."""
        with self._managed_cursor() as cursor:
            property_repo = self._dal.PropertyRepository(cursor)
            unique_id = check_type(property_repo.get('unique_id'), str)
            index_hash = check_type(property_repo.get('index_hash'), str)
            domain = get_domain(property_repo)

            col_manager = self._dal.ColumnManager(cursor)
            label_names = col_manager.get_columns()

            attribute_repo = self._dal.AttributeGroupRepository(cursor)

            if selectors:
                try:
                    selector_objs = [parse_selector(s) for s in selectors]
                except TypeError as e:
                    msg = f'{e}. Did you mean to use a keyword-only argument?'
                    raise TypeError(msg)
                attribute_id_filter = []
                for attr in attribute_repo.find_all():
                    for sel in selector_objs:
                        # If a selector matches, add id to filter,
                        # then break inner-loop and skip to next attr.
                        if sel(attr.attributes):
                            attribute_id_filter.append(attr.id)
                            break
            else:
                attribute_id_filter = None

        # Get disaggregated results generator.
        data = self._disaggregate(attribute_id_filter, quantize=quantize)

        # If *sum_by_attrs* is provided, only keep the specified attributes.
        if sum_by_attrs:
            if isinstance(sum_by_attrs, str):
                sum_by_attrs = [sum_by_attrs]
            sum_by_attrs = set(sum_by_attrs)
            sum_by_attrs = sum_by_attrs.union(domain)  # Domain always included.

            def filter_attrs(attrs):
                return {k: v for k, v in attrs.items() if k in sum_by_attrs}

            # Apply attribute filtering function to each item.
            data = ((idx, filter_attrs(attrs), quant) for idx, attrs, quant in data)

            # Note: Records are grouped and summed later--when iterating
            # over the returned `NodeReader` instance.

        # Build and return a reader instance.
        node_reader = NodeReader(
            data=data,
            node=self,
            cache_to_drive=cache_to_drive,
            quantize_default=quantize,
        )
        return node_reader

    def __repr__(self):
        """Return string representation of TopoNode object."""
        with self._managed_cursor() as cursor:
            info = get_node_info_text(
                property_repo=self._dal.PropertyRepository(cursor),
                column_manager=self._dal.ColumnManager(cursor),
                structure_repo=self._dal.StructureRepository(cursor),
                weight_group_repo=self._dal.WeightGroupRepository(cursor),
                attribute_repo=self._dal.AttributeGroupRepository(cursor),
                crosswalk_repo=self._dal.CrosswalkRepository(cursor),
            )

        domain_str = '\n  '.join(info['domain_list'])
        crosswalks_str = '\n  '.join(info['crosswalks_list'])

        return (
            f"{super().__repr__()}\n"  # Use default repr as a first line.
            f"domain:\n"
            f"  {domain_str}\n"
            f"index:\n"
            f"  {', '.join(info['index_list'])}\n"
            f"granularity:\n"
            f"  {info['granularity_str']}\n"
            f"weights:\n"
            f"  {', '.join(info['weights_list'])}\n"
            f"attributes:\n"
            f"  {', '.join(info['attribute_list'])}\n"
            f"incoming crosswalks:\n"
            f"  {crosswalks_str}"
        )
