#!/usr/bin/env python

#===============================================================================
# Copyright 2015 Geoscience Australia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

"""
dbcompare.py - compare two databases.
"""

from __future__ import absolute_import
from __future__ import print_function
import sys
import re
from . import dbutil

#
# Reporter Class
#


class Reporter(object):
    """Report the differences detected between two databases."""

    MAX_DIFFERENCES = 5
    MAX_FIELD_WIDTH = 30

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
            print(msg, file=self.output)

    def column_only_in(self, db_no, table, column):
        """Report a column only present in one database."""

        if self.verbosity > 0:
            msg = ("Only in %s: column '%s.%s'" %
                   (self.db[db_no], table, column))
            print(msg, file=self.output)

    def primary_keys_differ(self, table):
        """Report a mismatch in the primary keys between the two databases."""

        if self.verbosity > 0:
            msg = "Primary keys differ: table '%s'" % table
            print(msg, file=self.output)

    def new_table(self, table, columns):
        """Start comparing contents for a new table."""

        self.curr_table = table
        self.column_list = columns[:]
        self.diff_list = []

    def add_difference(self, db_no, row):
        """Add a difference for the current table.

        PRE: new_table must have been called."""

        if self.verbosity > 2 or len(self.diff_list) < self.MAX_DIFFERENCES:
            self.diff_list.append((db_no, [str(c) for c in row]))

    def stop_adding_differences(self):
        """True if there is no need to keep checking for differences.

        PRE: new_table must have been called."""

        diff_count = len(self.diff_list)

        if self.verbosity <= 1:
            return diff_count > 0
        elif self.verbosity == 2:
            return diff_count >= self.MAX_DIFFERENCES
        else:
            return False

    def _get_field_width(self):
        "Calculate the width of the fields for self.content_differences."

        field_width = [len(c) for c in self.column_list]
        for (dummy_db_no, row) in self.diff_list:
            row_width = [len(f) for f in row]
            field_width = list(map(max, field_width, row_width))
        field_width = [min(fw, self.MAX_FIELD_WIDTH) for fw in field_width]
        return field_width

    def _truncate_row_values(self, row):
        "Truncate the values in a row to at most MAX_FIELD_WIDTH."

        row_values = []
        for item in row:
            val = str(item)
            if len(val) > self.MAX_FIELD_WIDTH:
                val = val[0:self.MAX_FIELD_WIDTH-3] + "..."
            row_values.append(val)
        return row_values

    def content_differences(self):
        """Report the content differences for a table.

        PRE: new_table must have been called."""

        if self.diff_list:
            if self.verbosity > 0:
                msg = "Contents differ: table '%s'" % self.curr_table
                print(msg, file=self.output)

            if self.verbosity > 1:
                db_width = max(len(self.db[1]), len(self.db[2]))
                field_width = self._get_field_width()

                field_format = ""
                for width in field_width:
                    field_format += " %-" + str(width) + "s"
                header_format = " "*db_width + " " + field_format
                row_format = "%-" + str(db_width) + "s:" + field_format

                print(header_format % tuple(self.column_list), file=self.output)
                for (db_no, row) in self.diff_list:
                    row_values = self._truncate_row_values(row)
                    print_values = tuple([self.db[db_no]] + row_values)
                    print(row_format % print_values, file=self.output)

                print("", file=self.output)

#
# ComparisonWrapper connection wrapper class.
#


class ComparisonWrapper(dbutil.ConnectionWrapper):
    """Wrapper for a connection intended for database comparison.

    This implements queries about the structure of the database,
    as recorded in the information schema."""

    def __init__(self, conn, default_schema='public'):

        self.default_schema = dbutil.safe_name(default_schema)
        dbutil.ConnectionWrapper.__init__(self, conn)

    def table_exists(self, table, schema=None):
        """Returns True if the table exists in the database."""

        if schema is None:
            schema = self.default_schema

        sql = ("SELECT table_name FROM information_schema.tables\n" +
               "WHERE table_schema = %(schema)s AND\n" +
               "   table_name = %(table)s AND\n" +
               "   table_type = 'BASE TABLE';")

        with self.conn.cursor() as curs:
            curs.execute(sql, {'table': table, 'schema': schema})
            tab_found = bool(curs.fetchone())

        return tab_found

    def table_list(self, schema=None):
        """Return a list of the tables in a database."""

        if schema is None:
            schema = self.default_schema

        sql = ("SELECT table_name FROM information_schema.tables\n" +
               "WHERE table_schema = %(schema)s AND\n" +
               "   table_type = 'BASE TABLE'\n" +
               "ORDER BY table_name;")

        with self.conn.cursor() as curs:
            curs.execute(sql, {'schema': schema})
            tab_list = [tup[0] for tup in curs.fetchall()]

        return tab_list

    def column_list(self, table, schema=None):
        """Return a list of the columns in a database table."""

        if schema is None:
            schema = self.default_schema

        sql = ("SELECT column_name FROM information_schema.columns\n" +
               "WHERE table_schema = %(schema)s AND table_name = %(table)s\n" +
               "ORDER BY ordinal_position;")

        with self.conn.cursor() as curs:
            curs.execute(sql, {'table': table, 'schema': schema})
            col_list = [tup[0] for tup in curs.fetchall()]

        return col_list

    def primary_key(self, table, schema=None):
        """Returns the primary key for a table as a list of columns."""

        if schema is None:
            schema = self.default_schema

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

        if not pkey:
            # pkey empty: Try an alternative query - not all rows in
            #     the table_constraints table are accessable to an
            #     ordinary user.

            sql = ("SELECT column_name\n" +
                   "FROM information_schema.key_column_usage\n" +
                   "WHERE constraint_schema = %(schema)s AND\n"
                   "   table_name = %(table)s AND\n" +
                   "   constraint_name LIKE '%%_pkey'\n" +
                   "ORDER BY ordinal_position;")

            with self.conn.cursor() as curs:
                curs.execute(sql, {'table': table, 'schema': schema})
                pkey = [tup[0] for tup in curs.fetchall()]

        assert pkey, "Unable to find primary key for table '%s'." % table

        return pkey

    def drop_table(self, table_name):
        """Drops the named table from the database."""

        drop_sql = "DROP TABLE IF EXISTS %s;" % table_name

        with self.conn.cursor() as curs:
            curs.execute(drop_sql)

#
# Comparison class
#

class Comparison(object):
    """A comparison between two databases.

    Note that creating a comparison sets autocommit to True on
    the database connections given to the constructor. The original
    state can be restored by calling the restore_autocommit method.
    """

    def __init__(self, db1, db2, schema1='public', schema2='public',
                 verbosity=0, output=sys.stdout):
        """
        Positional Arguments:
            db1, db2: Connections to the databases to be compared.

        Keyword Arguments:
            schema1: The schema to be used for the first database (db1),
                defaults to 'public'.
            schema2: The schema to be used for the second database (db2),
                defaults to 'public'.
            verbosity: Amount of output generated if a difference is detected.
                0 -- No output, just the return value.
                1 -- Missing columns, mismatched primary keys,
                     one line notification of table content differences.
                2 -- As above, but prints the details of the first
                     MAX_DIFFERENCES content differences in each table.
                3 -- As above, but prints the details of all differences.
                     Defaults to 0.
            output: Where the output goes. This is assumed to be a file object.
                Defaults to sys.stdout.
        """

        # Set autocommit mode on the connections; retain the old settings.
        self.old_autocommit = (db1.autocommit, db2.autocommit)
        db1.autocommit = True
        db2.autocommit = True

        # Sanitise the schema names, just in case.
        self.schema1 = dbutil.safe_name(schema1)
        self.schema2 = dbutil.safe_name(schema2)

        # Wrap the connections to gain access to database structure queries.
        self.db1 = ComparisonWrapper(db1, self.schema1)
        self.db2 = ComparisonWrapper(db2, self.schema2)

        # Get the database names...
        self.db1_name = self.db1.database_name()
        self.db2_name = self.db2.database_name()

        # and qualify with the schema names if they are not 'public'
        if self.schema1 != 'public':
            self.db1_name = self.schema1 + '.' + self.db1_name
        if self.schema2 != 'public':
            self.db2_name = self.schema2 + '.' + self.db2_name

        self.report = Reporter(self.db1_name, self.db2_name,
                               verbosity, output)

    def restore_autocommit(self):
        """Restore the autocommit status of the underlying connections.

        The comparison should not be used after calling this, in
        case the connections have been reset to autocommit=False. The
        method sets the database attributes to None to enforce
        this."""

        self.db1.conn.autocommit = self.old_autocommit[0]
        self.db2.conn.autocommit = self.old_autocommit[1]

        self.db1 = None
        self.db2 = None

    @staticmethod
    def __keys_equal(row1, row2, column_list, key_set):
        """Return True if the keys of row1 and row2 are equal."""

        if row1 is None or row2 is None:
            return False
        else:
            for (val1, val2, col) in zip(row1, row2, column_list):
                if col in key_set and val1 != val2:
                    return False
            return True

    @staticmethod
    def __key_less(row1, row2, column_list, key_list):
        """Return True if the key of row1 is less than that for row2."""

        if row1 is None:
            return False
        elif row2 is None:
            return True
        else:
            key_cmp = {}
            for (val1, val2, col) in zip(row1, row2, column_list):
                if col in key_list:
                    key_cmp[col] = None if val1 == val2 else val1 < val2

            # Note that the order of columns in the key is not necessarily
            # the same as the order of columns in the table.

            for col in key_list:
                if key_cmp[col] is not None:
                    return key_cmp[col]

            return False  # Keys are equal

    @staticmethod
    def __filter_list(the_list, filter_set):
        """Returns a list filtered by a set of items.

        This is used to preserve the order of a list while
        removing items."""

        return [val for val in the_list if val in filter_set]

    @staticmethod
    def __dequalify_columns_for_table(table, columns):
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

    def __compare_content(self, table, key_list, column_list):
        """Compare the content of a table between the two databases.

        Returns True if the content is identical for the columns in
        column_list.
        """

        key_set = set(key_list)
        column_set = set(column_list)
        extra_columns = key_set - column_set
        combined_columns = column_list + sorted(extra_columns)

        column_str = ', '.join(combined_columns)
        table_str1 = self.schema1 + '.' + table
        table_str2 = self.schema2 + '.' + table
        key_str = ', '.join(key_list)

        table_dump = "SELECT %s FROM %s ORDER BY %s;"

        cur1 = self.db1.cursor()
        cur1.execute(table_dump % (column_str, table_str1, key_str))

        cur2 = self.db2.cursor()
        cur2.execute(table_dump % (column_str, table_str2, key_str))

        row1 = cur1.fetchone()
        row2 = cur2.fetchone()

        self.report.new_table(table, combined_columns)
        differences_found = False

        while row1 or row2:
            if row1 == row2:
                row1 = cur1.fetchone()
                row2 = cur2.fetchone()

            elif self.__keys_equal(row1, row2, combined_columns, key_set):
                differences_found = True
                self.report.add_difference(1, row1)
                self.report.add_difference(2, row2)
                row1 = cur1.fetchone()
                row2 = cur2.fetchone()

            elif self.__key_less(row1, row2, combined_columns, key_list):
                differences_found = True
                self.report.add_difference(1, row1)
                row1 = cur1.fetchone()

            else:
                differences_found = True
                self.report.add_difference(2, row2)
                row2 = cur2.fetchone()

            if differences_found and self.report.stop_adding_differences():
                break

        if differences_found:
            self.report.content_differences()

        return not differences_found

    def compare_tables(self, table, ignore_columns):
        """Implements the compare_tables function."""

        ignore_set = \
            set(self.__dequalify_columns_for_table(table, ignore_columns))

        column_list1 = self.db1.column_list(table)
        column_list2 = self.db2.column_list(table)

        column_set1 = set(column_list1) - ignore_set
        column_set2 = set(column_list2) - ignore_set

        # Use filter_list to preserve column ordering (for output).
        only_in_db1 = self.__filter_list(column_list1,
                                         column_set1 - column_set2)
        only_in_db2 = self.__filter_list(column_list2,
                                         column_set2 - column_set1)
        column_in_both = self.__filter_list(column_list1,
                                            column_set1 & column_set2)

        identical_so_far = True
        if len(only_in_db1) > 0:
            identical_so_far = False
            for column in only_in_db1:
                self.report.column_only_in(1, table, column)

        if len(only_in_db2) > 0:
            identical_so_far = False
            for column in only_in_db2:
                self.report.column_only_in(2, table, column)

        pkey1 = self.db1.primary_key(table)
        pkey2 = self.db2.primary_key(table)

        if pkey1 == pkey2:
            content_matches = self.__compare_content(table, pkey1,
                                                     column_in_both)
        else:
            content_matches = False
            self.report.primary_keys_differ(table)

        return identical_so_far and content_matches

#
# Interface Functions
#


# pylint: disable=too-many-arguments
#
# This is OK: two positional arguments and six keyword arguments with
# defaults.
#

# pylint: disable=too-many-locals
#
# REFACTOR: pylint is recommending simplifing this function.
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

    comparison = Comparison(db1, db2, schema1=schema1, schema2=schema2,
                            verbosity=verbosity, output=output)

    ignore_set = set(ignore_tables)

    table_set1 = set(comparison.db1.table_list()) - ignore_set
    table_set2 = set(comparison.db2.table_list()) - ignore_set

    only_in_db1 = table_set1 - table_set2
    only_in_db2 = table_set2 - table_set1
    table_in_both = table_set1 & table_set2

    identical_so_far = True

    if len(only_in_db1) > 0:
        identical_so_far = False
        for table in sorted(only_in_db1):
            comparison.report.table_only_in(1, table)

    if len(only_in_db2) > 0:
        identical_so_far = False
        for table in sorted(only_in_db2):
            comparison.report.table_only_in(2, table)

    for table in sorted(table_in_both):
        tables_match = comparison.compare_tables(table, ignore_columns)
        if not tables_match:
            identical_so_far = False

    comparison.restore_autocommit()

    return identical_so_far

# pylint: enable=too-many-locals
# pylint: enable=too-many-arguments


# pylint: disable=too-many-arguments
#
# This is OK: three positional arguments and five keyword arguments with
# defaults.
#

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

    table = dbutil.safe_name(table)

    comparison = Comparison(db1, db2, schema1=schema1, schema2=schema2,
                            verbosity=verbosity, output=output)

    assert comparison.db1.table_exists(table), \
        ("Could not find table '%s' in database '%s'." %
         (table, comparison.db1_name))
    assert comparison.db2.table_exists(table), \
        ("Could not find table '%s' in database '%s'." %
         (table, comparison.db2_name))

    tables_match = comparison.compare_tables(table, ignore_columns)

    comparison.restore_autocommit()

    return tables_match

# pylint: enable=too-many-arguments
