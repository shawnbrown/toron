# -*- coding: utf-8 -*-
import itertools
import hashlib
import os
import sqlite3
import textwrap

from gpn import _csv as csv
from gpn.connector import _Connector
from gpn.connector import _duplicate_label_sets
from gpn.connector import _invalid_unmapped_levels
from gpn.connector import _expensive_constraints


class Node(object):
    def __init__(self, path=None, mode=0, **kwds):
        """Get existing node or create a new one."""
        self._connect = _Connector(path, mode=mode)
        if path:
            assert 'name' not in kwds, 'Cannot specify both path and name.'
            self.name = path.rsplit('.', 1)[0]
        else:
            self.name = kwds.get('name')

    def __repr__(self):
        info = []
        info.append(repr(self.__class__))
        info.append('Name: ' + str(self.name))

        with self._connect() as connection:
            cursor = connection.cursor()

            # Get cell count.
            cursor.execute('SELECT COUNT(*) FROM cell WHERE partial=0')
            cellcount = cursor.fetchone()[0]
            if cellcount > 0:
                info.append('Cells: %s' % cellcount)

                # Get hierarchy list.
                cursor.execute('SELECT hierarchy_value FROM hierarchy '
                               'ORDER BY hierarchy_level')
                hierarchy = [x[0] for x in cursor.fetchall()]

                # Get root label (maximal cell).
                cursor.execute("""
                    SELECT label_value
                    FROM label
                    NATURAL JOIN hierarchy
                    WHERE hierarchy_id IN (SELECT hierarchy_id
                                           FROM hierarchy
                                           ORDER BY hierarchy_level
                                           LIMIT 1)
                          AND label_value!='UNMAPPED'
                """)
                root_label = cursor.fetchone()[0]

                # Append root label to root hierarchy.
                hierarchy[0] = hierarchy[0] + ' (%s)' % root_label
                info.append('Hierarchy: %s' % ', '.join(hierarchy))

                # Get edges ("None" for now).
                info.append('Edges: None')

            else:
                info.append('Cells: None')
                info.append('Hierarchy: None')
                info.append('Edges: None')

        # Format `info` list.
        def wrap(x):
            x = textwrap.wrap(x, width=70, subsequent_indent='  ')
            return '\n'.join(x)
        info = [wrap(line) for line in info]
        return '\n'.join(info)


    def export_cells(self, filename):
        assert not os.path.exists(filename), '%s already exists' % filename

        with open(filename, 'w') as fh:
            with self._connect() as connection:
                cursor = connection.cursor()

                # Get field names.
                cursor.execute('SELECT hierarchy_value FROM hierarchy '
                               'ORDER BY hierarchy_level')
                fieldnames = [x[0] for x in cursor]
                fieldnames.insert(0, 'cell_id')

                # Write output file.
                writer = csv.DictWriter(fh, fieldnames, lineterminator='\n')
                writer.writeheader()
                cursor.execute('SELECT cell_id from cell ORDER BY cell_id')
                cursor2 = connection.cursor()
                for cell_id in (x[0] for x in cursor):
                    row = self._select_cell(cursor2, cell_id)
                    row['cell_id'] = cell_id
                    writer.writerow(row)

    def select_cell(self, **kwds):
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor2 = connection.cursor()
            for cell_id in self._select_cell_id(cursor, **kwds):
                yield self._select_cell(cursor2, cell_id)

    @staticmethod
    def _select_cell(cursor, cell_id):
        query = """
            SELECT hierarchy_value, label_value
            FROM cell
            NATURAL JOIN cell_label
            NATURAL JOIN label
            NATURAL JOIN hierarchy
            WHERE cell_id=?
            ORDER BY hierarchy_level
        """
        cursor.execute(query, (cell_id,))
        return dict(cursor.fetchall())

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
        global _duplicate_label_sets
        global _invalid_unmapped_levels

        reader = csv.reader(fh)
        fieldnames = next(reader)  # Use header row as fieldnames.

        with self._connect() as connection:
            connection.isolation_level = None
            cursor = connection.cursor()
            cursor.execute('BEGIN TRANSACTION')

            # Temporarily drop triggers (too slow for bulk insert).
            for name in _expensive_constraints.keys():
                cursor.execute('DROP TRIGGER %s' % name)

            self._insert_hierarchies(cursor, fieldnames)

            # Add cells from file.
            for row in reader:
                items = zip(fieldnames, row)
                self._insert_one_cell(cursor, items)

            # Add "UNMAPPED" cell if not present.
            unmapped_items = [(x, 'UNMAPPED') for x in fieldnames]
            unmapped_dict = dict(unmapped_items)
            resultgen = self._select_cell_id(cursor, **unmapped_dict)
            if not list(resultgen):
                self._insert_one_cell(cursor, unmapped_items)

            # Check for duplicate label combinations.
            cursor.execute(_duplicate_label_sets)
            if cursor.fetchone():
                raise sqlite3.IntegrityError(
                    'CHECK constraint failed: cell_label (duplicate label set)')

            # Check for invalid unmapped levels.
            cursor.execute(_invalid_unmapped_levels)
            if cursor.fetchone():
                raise sqlite3.IntegrityError(
                    'CHECK constraint failed: cell_label (invalid unmapped level)')

            # Re-create cell constraint triggers.
            for operation in _expensive_constraints.values():
                cursor.execute(operation)

            # Insert node hash.
            node_hash = self._get_hash(cursor)
            cursor.execute('INSERT INTO node (node_hash) VALUES (?)',
                           (node_hash,))

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

    @staticmethod
    def _get_hash(cursor):
        """Return a hash to uniquely identify the nodes's cells.

        The hash value should not be affected by changes in
        hierarchy_value or hierarchy_level.

        """
        cursor.execute("""
            SELECT cell_id, hierarchy_id, label_value
            FROM cell_label
            NATURAL JOIN label
            ORDER BY cell_id, hierarchy_id, label_value
        """)
        sha256 = hashlib.sha256()
        for row in cursor:
            for cell in row:
                cell = str(cell).encode('utf-8')
                sha256.update(cell)

        hexdigest = sha256.hexdigest()

        # If hash of NULL set, set digest to None.
        nullhash = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
        if hexdigest == nullhash:
            hexdigest = None
        return hexdigest
