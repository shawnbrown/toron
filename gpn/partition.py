# -*- coding: utf-8 -*-
import csv
import itertools
import sqlite3

from gpn.connector import _Connector
from gpn.connector import _create_triggers


class Partition(object):
    def __init__(self, path=None, mode=0):
        """Get existing Partition or create a new one."""
        self._connect = _Connector(path, mode=mode)

    def select_cell(self, **kwds):
        connection = self._connect()
        cursor1 = connection.cursor()
        cursor2 = connection.cursor()

        for cell_id in self._select_cell_id(cursor1, **kwds):
            query = """
                SELECT hierarchy_value, label_value
                FROM cell
                NATURAL JOIN cell_label
                NATURAL JOIN label
                NATURAL JOIN hierarchy
                WHERE cell_id=?
                ORDER BY hierarchy_level
            """
            cursor2.execute(query, (cell_id,))
            yield dict(cursor2.fetchall())

        connection.close()

    @staticmethod
    def _select_cell_id(cursor, **kwds):
        query = """
            SELECT cell_id
            FROM cell_label
            NATURAL JOIN label
            NATURAL JOIN hierarchy
            WHERE hierarchy_value=? AND label_value=?
        """
        operation = [query] * len(kwds)
        operation = '\nINTERSECT\n'.join(operation)
        params = itertools.chain.from_iterable(kwds.items())
        params = list(params)

        cursor.execute(operation, params)
        return (x[0] for x in cursor)

    def insert_cells(self, filename):
        """Insert cells from given CSV filename."""
        with open(filename, 'r') as fh:
            self._insert_cells(fh)

    def _insert_cells(self, fh):
        """Insert cells from given CSV file object."""
        reader = csv.reader(fh)
        fieldnames = next(reader)  # Use header row as fieldnames.

        connection = self._connect()
        connection.isolation_level = None
        cursor = connection.cursor()
        cursor.execute('BEGIN TRANSACTION')

        complete = False
        try:
            # Temporarily drop triggers (too slow for bulk insert).
            cursor.execute('DROP TRIGGER CheckUniqueLabels_ins')
            cursor.execute('DROP TRIGGER CheckUniqueLabels_upd')
            cursor.execute('DROP TRIGGER CheckUniqueLabels_del')

            self._insert_hierarchies(cursor, fieldnames)

            # Add "UNMAPPED" cell if not present.
            unmapped_items = [(x, 'UNMAPPED') for x in fieldnames]
            unmapped_dict = dict(unmapped_items)
            resultgen = self._select_cell_id(cursor, **unmapped_dict)
            if not list(resultgen):
                self._insert_one_cell(cursor, unmapped_items)

            # Add all other cells.
            for row in reader:
                items = zip(fieldnames, row)
                self._insert_one_cell(cursor, items)

            # Check for duplicate label combinations.
            cursor.execute("""
                SELECT 1
                FROM (SELECT GROUP_CONCAT(label_id) AS label_combo
                      FROM (SELECT cell_id, label_id
                            FROM cell_label
                            ORDER BY cell_id, label_id)
                      GROUP BY cell_id)
                GROUP BY label_combo
                HAVING COUNT(*) > 1
            """)
            if cursor.fetchone():
                raise sqlite3.IntegrityError('CHECK constraint failed: cell_label')

            # Re-create "CheckUniqueLabel" triggers.
            for operation in _create_triggers:
                cursor.execute(operation)

            connection.commit()
            complete = True

        finally:
            if not complete:
                connection.rollback()
            connection.close()

    @staticmethod
    def _insert_hierarchies(cursor, fieldnames):
        cursor.execute('SELECT hierarchy_value FROM hierarchy ORDER BY hierarchy_level')
        hierarchies = [x[0] for x in cursor.fetchall()]
        if not hierarchies:
            query = 'INSERT INTO hierarchy (hierarchy_level, hierarchy_value) VALUES (?, ?)'
            cursor.executemany(query, enumerate(fieldnames))
        else:
            msg = ('Fieldnames must match hierarchy values.\n'
                   ' Found: %s\n Required: %s') % (', '.join(fieldnames),
                                                   ', '.join(hierarchies))
            assert set(hierarchies) == set(fieldnames), msg

    @staticmethod
    def _insert_one_cell(cursor, items):
        """Performs insert-cell operation using given items."""
        items = list(items)

        # Insert cell record.
        cursor.execute('INSERT INTO cell DEFAULT VALUES')
        cell_id = cursor.lastrowid

        # Insert label records.
        operation = """
            INSERT OR IGNORE INTO label (hierarchy_id, label_value)
            SELECT hierarchy_id, ? AS label_value
            FROM hierarchy
            WHERE hierarchy_value=?
        """
        params = [(lbl, hrchy) for hrchy, lbl in items]
        cursor.executemany(operation, params)

        # Insert cell_label records.
        operation = """
            INSERT INTO cell_label (cell_id, hierarchy_id, label_id)
            SELECT ? as cell_id, hierarchy_id, label_id
            FROM label
            WHERE hierarchy_id IN (SELECT hierarchy_id
                                   FROM hierarchy
                                   WHERE hierarchy_value=?)
                  AND label_value=?
        """
        params = [(cell_id, hrchy, lbl) for hrchy, lbl in items]
        cursor.executemany(operation, params)
