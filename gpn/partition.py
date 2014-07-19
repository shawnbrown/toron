# -*- coding: utf-8 -*-
import csv

from gpn.connector import _Connector


class Partition(object):
    def __init__(self, path=None, mode=0):
        """Get existing Partition or create a new one."""
        self._connect = _Connector(path, mode=mode)

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
        cursor.execute('SAVEPOINT BeforeInsertCells')

        is_complete = False
        try:
            self._insert_hierarchies(cursor, fieldnames)

            if not self._get_unmapped_cell(cursor, fieldnames):
                items = [(x, 'UNMAPPED') for x in fieldnames]
                self._insert_one_cell(cursor, items)

            for row in reader:
                items = zip(fieldnames, row)
                self._insert_one_cell(cursor, items)

            cursor.execute('RELEASE SAVEPOINT BeforeInsertCells')
            connection.commit()

            is_complete = True

        finally:
            if not is_complete:
                cursor.execute('ROLLBACK TO SAVEPOINT BeforeInsertCells')
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
    def _get_unmapped_cell(cursor, fieldnames):
        select_one = ("SELECT cell_id FROM cell_label "
                      "NATURAL JOIN label NATURAL JOIN hierarchy "
                      "WHERE label_value='UNMAPPED' AND hierarchy_value=?")
        operation = [select_one] * len(fieldnames)
        operation = '\nINTERSECT\n'.join(operation)
        cursor.execute(operation, fieldnames)
        result = cursor.fetchone()
        return result[0] if result else None

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
