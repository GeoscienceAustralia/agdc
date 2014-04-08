#!/usr/bin/env python

"""
dbcompare.py - compare two databases.
"""

import sys
import re
import dbutil

#
# Constants
#

MAX_DIFFERENCES = 5

#
# Reporter Class
#

class Reporter(object):
    """Report the differences detected between two databases."""

    def __init__(self, db1_name, db2_name, verbosity, output):
        """Create a Reporter"""

        self.db = {}
        self.db[1] = db1_name
        self.db[2] = db2_name
        self.verbosity = verbosity
        self.output = output

        self.curr_table = None
        self.column_list = None
        self.diff_list = None

    def table_only_in(self, db_no, table):
        """Report a table only present in one database."""

        if self.verbosity > 0:
            msg = "Only in %s: table '%s'" % (self.db[db_no], table)
            print >> self.output, msg

    def column_only_in(self, db_no, table, column):
        """Report a column only present in one database."""

        if self.verbosity > 0:
            msg = ("Only in %s: column '%s.%s'" %
                   (self.db[db_no], table, column))
            print >> self.output, msg

    def primary_keys_differ(self, table):
        """Report a mismatch in the primary keys between the two databases."""

        if self.verbosity > 0:
            msg = "Primary keys differ: table '%s'" % table
            print >> self.output, msg

    def new_table(self, table, columns):
        """Start comparing contents for a new table."""

        self.curr_table = table
        self.column_list = columns[:]
        self.diff_list = []

    def add_difference(self, db_no, row):
        """Add a difference for the current table.

        PRE: new_table must have been called."""

        if self.verbosity > 2 or len(self.diff_list) < MAX_DIFFERENCES:
            self.diff_list.append((db_no, map(str, row)))

    def stop_adding_differences(self):
        """True if there is no need to keep checking for differences.

        PRE: new_table must have been called."""

        diff_count = len(self.diff_list)
        if diff_count > 0:
            if self.verbosity <= 1:
                return True
            elif self.verbosity == 2 and diff_count >= MAX_DIFFERENCES:
                return True
        return False

    def content_differences(self):
        """Report the content differences for a table.

        PRE: new_table must have been called."""

        if self.diff_list:
            if self.verbosity > 0:
                msg = "Contents differ: table '%s'" % self.curr_table
                print >> self.output, msg

            if self.verbosity > 1:
                db_width = max(len(self.db[1]), len(self.db[2]))

                field_width = map(len, self.column_list)
                for (db_number, row) in self.diff_list:
                    row_width = map(len, row)
                    field_width = map(max, field_width, row_width)

                col_format = ""
                for width in field_width:
                    col_format += " %-" + str(width) +  "s"

                header_format = " "*db_width + " " + col_format                
                print >> self.output, header_format % tuple(self.column_list)

                row_format = "%-" + str(db_width) + "s:" + col_format
                for (db_no, row) in self.diff_list:
                    row_values = tuple([self.db[db_no]] + row)
                    print >> self.output, row_format % row_values

                print >> self.output, ""

#
# ComparisonWrapper connection wrapper class.
#


class ComparisonWrapper(dbutil.ConnectionWrapper):
    """Wrapper for a connection intended for database comparison.

    This implements queries about the structure of the database,
    as recorded in the information schema."""

    def table_exists(self, table, schema='public'):
        """Returns True if the table exists in the database."""

        sql = ("SELECT table_name FROM information_schema.tables\n" +
               "WHERE table_schema = %(schema)s AND\n" +
               "   table_name = %(table)s AND\n" +
               "   table_type = 'BASE TABLE';")

        with self.conn.cursor() as curs:
            curs.execute(sql, {'table': table, 'schema': schema})
            tab_found = bool(curs.fetchone())

        return tab_found

    def table_list(self, schema='public'):
        """Return a list of the tables in a database."""

        sql = ("SELECT table_name FROM information_schema.tables\n" +
               "WHERE table_schema = %(schema)s AND\n" +
               "   table_type = 'BASE TABLE'\n" +
               "ORDER BY table_name;")

        with self.conn.cursor() as curs:
            curs.execute(sql, {'schema': schema})
            tab_list = [tup[0] for tup in curs.fetchall()]

        return tab_list

    def column_list(self, table, schema='public'):
        """Return a list of the columns in a database table."""

        sql = ("SELECT column_name FROM information_schema.columns\n" +
               "WHERE table_schema = %(schema)s AND table_name = %(table)s\n" +
               "ORDER BY ordinal_position;")

        with self.conn.cursor() as curs:
            curs.execute(sql, {'table': table, 'schema': schema})
            col_list = [tup[0] for tup in curs.fetchall()]

        return col_list

    def primary_key(self, table, schema='public'):
        """Returns the primary key for a table as a list of columns."""

        sql = ("SELECT column_name\n" +
               "FROM information_schema.key_column_usage\n" +
               "WHERE constraint_schema = %(schema)s AND\n" +
               "   constraint_name IN\n" +
               "      (SELECT constraint_name\n" +
               "       FROM information_schema.table_constraints\n" +
               "       WHERE table_schema = %(schema)s AND\n" +
               "          table_name = %(table)s AND\n" +
               "          constraint_type = 'PRIMARY KEY')\n" +
               "ORDER BY ordinal_position;")

        with self.conn.cursor() as curs:
            curs.execute(sql, {'table': table, 'schema': schema})
            pkey = [tup[0] for tup in curs.fetchall()]

        return pkey

#
# Local Functions
#

def _pkey_equal(row1, row2, column_list, pkey_set):
    """Return True if the pkeys of row1 and row2 are equal."""

    if row1 == None or row2 == None:
        return False
    else:
        equal_so_far = True
        for (val1, val2, col) in zip(row1, row2, column_list):
            if (col in pkey_set) and (val1 != val2):
                equal_so_far = False
        return equal_so_far


def _pkey_less(row1, row2, column_list, pkey_list):
    """Return True if the pkey of row1 is sorted earlier than that for row2."""

    if row1 == None:
        return False
    elif row2 == None:
        return True
    else:
        row1_less = False
        pkey_cmp = {}
        for (val1, val2, col) in zip(row1, row2, column_list):
            if col in pkey_list:
                pkey_cmp[col] = cmp(val1, val2)
        for col in pkey_list:
            if pkey_cmp[col] < 0:
                row1_less = True
                break
            elif pkey_cmp[col] > 0:
                break
        return row1_less

def _compare_content(db1, db2, schema1, schema2, report,
                     table, pkey_list, column_list):
    """Compare the content of a table between the two databases.

    Returns True if the content is identical for the columns in column_list."""

    pkey_set = set(pkey_list)
    column_set = set(column_list)
    extra_columns = pkey_set - column_set
    combined_columns = column_list + sorted(extra_columns)

    column_str = ', '.join(combined_columns)
    table_str1 = schema1 + '.' + table
    table_str2 = schema2 + '.' + table
    pkey_str = ', '.join(pkey_list)

    table_dump = "SELECT %s FROM %s ORDER BY %s;"

    cur1 = db1.cursor()
    cur1.execute(table_dump % (column_str, table_str1, pkey_str))

    cur2 = db2.cursor()
    cur2.execute(table_dump % (column_str, table_str2, pkey_str))

    row1 = cur1.fetchone()
    row2 = cur2.fetchone()

    report.new_table(table, combined_columns)
    differences_found = False

    while row1 or row2:
        if row1 == row2:
            row1 = cur1.fetchone()
            row2 = cur2.fetchone()

        elif _pkey_equal(row1, row2, combined_columns, pkey_set):
            differences_found = True
            report.add_difference(1, row1)
            report.add_difference(2, row2)
            row1 = cur1.fetchone()
            row2 = cur2.fetchone()

        elif _pkey_less(row1, row2, combined_columns, pkey_list):
            differences_found = True
            report.add_difference(1, row1)
            row1 = cur1.fetchone()

        else:
            differences_found = True
            report.add_difference(2, row2)
            row2 = cur2.fetchone()

        if differences_found and report.stop_adding_differences():
            break

    if differences_found:
        report.content_differences()

    return not differences_found


def _filter_list(the_list, filter_set):
    """Returns a list filtered by a set of items.

    This is used to preserve the order of a list while
    removing items."""

    return [val for val in the_list if val in filter_set]


def _dequalify_columns_for_table(table, columns):
    """Returns a list of columns with the qualifying table name removed.

    Columns qualified with a *different* table name are excluded.
    Unqualified columns are included."""

    dq_columns = []
    for col in columns:
        match = re.match(r"^(\w+)\.(\w+)$", col)
        if match:
            if match.group(1) == table:
                dq_columns.append(match.group(2))
        elif re.match(r"^\w+$", col):
            dq_columns.append(col)
        else:
            raise AssertionError("Badly formed column name '%s'." % col)
    return dq_columns


def _compare_tables(db1, db2, schema1, schema2, report,
                    table, ignore_columns):
    """Implements the compare_tables function."""

    ignore_set = set(_dequalify_columns_for_table(table, ignore_columns))

    column_list1 = db1.column_list(table, schema1)
    column_list2 = db2.column_list(table, schema2)

    column_set1 = set(column_list1) - ignore_set
    column_set2 = set(column_list2) - ignore_set

    # Use _filter_list to preserve column ordering (for output).
    only_in_db1 = _filter_list(column_list1, column_set1 - column_set2)
    only_in_db2 = _filter_list(column_list2, column_set2 - column_set1)
    column_in_both = _filter_list(column_list1, column_set1 & column_set2)

    identical_so_far = True

    if len(only_in_db1) > 0:
        identical_so_far = False
        for column in only_in_db1:
            report.column_only_in(1, table, column)

    if len(only_in_db2) > 0:
        identical_so_far = False
        for column in only_in_db2:
            report.column_only_in(2, table, column)

    pkey1 = db1.primary_key(table, schema1)
    pkey2 = db2.primary_key(table, schema1)

    if pkey1 == pkey2:
        content_matches = _compare_content(db1, db2, schema1, schema2, report,
                                           table, pkey1, column_in_both)
    else:
        content_matches = False
        report.primary_keys_differ(table)

    return identical_so_far and content_matches


#
# Interface Functions
#

def compare_databases(db1, db2, schema1='public', schema2='public',
                      ignore_tables=None, ignore_columns=None, verbosity=0,
                      output=sys.stdout):
    """Compares two databases, returns True if they are identical.

    This function looks at all of the ordinary tables in the databases,
    except for those being explicitly ignored. It does not check views,
    foreign tables, or temporary tables. For each table it checks for
    the presence of non-ignored columns, and checks that the contents match.
    It does not care about column ordering, but does use the primary key
    to order the rows for the content check. It will regard different primary
    keys as a significant difference, and will report an error if a non-ignored
    table does not have a primary key.

    Using the primary key for ordering means that the results are dependent
    on the order of insertion for records which have an auto-generated key.
    This is a limitation of the current implementation.

    Preconditions: db1 and db2 are open database connections. These are
        assumed to be psycopg2 connections to PostgreSQL databases. Tables
        that are not being explictly ignored are expected to have primary keys.

    Positional Arguments:
        db1, db2: Connections to the databases to be compared.

    Keyword Arguments:
        schema1: The schema to be used for the first database (db1), defaults
            to 'public'.
        schema2: The schema to be used for the second database (db2), defaults
            to 'public'.
        ignore_tables: A list (or other Python iterable) of tables to be
            ignored. Ignored tables will not be compared, and the comparison
            will not care if they are only in one database and not the other.
            Defaults to an empty list.
        ignore_columns: A list (or other Python iterable) of columns to be
            ignored. These need to be qualified by table e.g.
            'dataset.datetime_processed'. The contents of these columns will
            not be compared, and the comparison will not care if they are only
            in the table in one database and not the other. Defaults to an
            empty list.
        verbosity: Amount of output generated if a difference is detected.
            0 -- No output, just the return value.
            1 -- Missing tables, missing columns, mismatched primary keys,
                 one line notification of table content differences.
            2 -- As above, but prints the details of the first MAX_DIFFERENCES
                 content differences in each table.
            3 -- As above, but prints the details of all differences.
            Defaults to 0.
        output: Where the output goes. This is assumed to be a file object.
            Defaults to sys.stdout.

    Return Value: Returns True if the databases are identical, excepting
        tables and columns specified as ignored by the arguments. Returns
        False otherwise.

    Postconditions: This function should have no side effects, except for
        the output generated if verbosity is greater than 0.
    """

    if ignore_tables is None:
        ignore_tables = []
    if ignore_columns is None:
        ignore_columns = []

    # Set autocommit mode on the connections; retain the old settings.
    old_db1_autocommit = db1.autocommit
    old_db2_autocommit = db2.autocommit
    db1.autocommit = True
    db2.autocommit = True

    # Wrap the connections to gain access to database structure queries.

    db1 = ComparisonWrapper(db1)
    db2 = ComparisonWrapper(db2)

    report = Reporter(db1.database_name(), db2.database_name(),
                      verbosity, output)

    ignore_set = set(ignore_tables)

    table_set1 = set(db1.table_list(schema1)) - ignore_set
    table_set2 = set(db2.table_list(schema2)) - ignore_set

    only_in_db1 = table_set1 - table_set2
    only_in_db2 = table_set2 - table_set1
    table_in_both = table_set1 & table_set2

    identical_so_far = True

    if len(only_in_db1) > 0:
        identical_so_far = False
        for table in sorted(only_in_db1):
            report.table_only_in(1, table)

    if len(only_in_db2) > 0:
        identical_so_far = False
        for table in sorted(only_in_db2):
            report.table_only_in(2, table)

    for table in sorted(table_in_both):
        tables_match = _compare_tables(db1, db2, schema1, schema2, report,
                                       table, ignore_columns)

    # Put things back the way we found them. Use the underlying connection
    # to set the autocommit attribute.
    db1.conn.autocommit = old_db1_autocommit
    db2.conn.autocommit = old_db2_autocommit

    return identical_so_far and tables_match

def compare_tables(db1, db2, table, schema1='public', schema2='public',
                   ignore_columns=None, verbosity=0, output=sys.stdout):
    """Compares tables from two databases, returns True if identical.

    This function compares the tables named 'table' in two databases.
    It checks that non-ignored columns are present in both tables, and it
    checks that the contents match. It does not care about column ordering,
    but does use the primary key to order the rows for the content check.
    It will regard different primary keys as a significant difference,
    and will report an error if either table does not have a primary key.

    Using the primary key for ordering means that the results are dependent
    on the order of insertion for records which have an auto-generated key.
    This is a limitation of the current implementation.

    Preconditions: db1 and db2 are open database connections. These are
        assumed to be psycopg2 connections to PostgreSQL databases.
        table is the name of a table present in both databases. This
        table has a primary key in both databases.

    Positional Arguments:
        db1, db2: Connections to the databases to be compared.
        table: The name of the table to be compared.

    Keyword Arguments:
        schema1: The schema to be used for the first database (db1), defaults
            to 'public'.
        schema2: The schema to be used for the second database (db2), defaults
            to 'public'.
        ignore_columns: A list (or other Python iterable) of columns to be
            ignored. These may optionally be qualified by table e.g.
            'dataset.datetime_processed'. Qualified column names for tables
            other than table may be present in this list, but will have
            no effect. The contents of the ignored columns will not be
            compared, and the comparison will not care if they are only in the
            table in one database and not the other. Defaults to an empty list.
        verbosity: Amount of output generated if a difference is detected.
            0 -- No output, just the return value.
            1 -- Missing columns, mismatched primary keys,
                 one line notification of table content differences.
            2 -- As above, but prints the details of the first MAX_DIFFERENCES
                 content differences in each table.
            3 -- As above, but prints the details of all differences.
            Defaults to 0.
        output: Where the output goes. This is assumed to be a file object.
            Defaults to sys.stdout.

    Return Value: Returns True if the tables 'table' are identical in
        both databases, excepting columns specifed as ignored by the
        arguments. Returns False otherwise.

    Postconditions: This function should have no side effects, except for
        the output generated if verbosity is greater than 0.
    """

    if ignore_columns is None:
        ignore_columns = []

    # Set autocommit mode on the connections; retain the old settings.
    old_db1_autocommit = db1.autocommit
    old_db2_autocommit = db2.autocommit
    db1.autocommit = True
    db2.autocommit = True

    # Wrap the connections to gain access to database structure queries.

    db1 = ComparisonWrapper(db1)
    db2 = ComparisonWrapper(db2)

    report = Reporter(db1.database_name(), db2.database_name(),
                      verbosity, output)

    assert db1.table_exists(table, schema1), \
        ("Could not find table '%s' in database '%s'." %
         (table, db1.database_name()))
    assert db2.table_exists(table, schema2), \
        ("Could not find table '%s' in database '%s'." %
         (table, db2.database_name()))

    tables_match = _compare_tables(db1, db2, schema1, schema2, report,
                                   table, ignore_columns)

    # Put things back the way we found them. Use the underlying connection
    # to set the autocommit attribute.
    db1.conn.autocommit = old_db1_autocommit
    db2.conn.autocommit = old_db2_autocommit

    return tables_match
