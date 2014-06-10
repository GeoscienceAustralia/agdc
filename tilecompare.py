#!/usr/bin/env python

"""
tilecompare.py - compare two sets of tiles.
"""

import sys
import re
import dbutil

#
# ComparisonWrapper connection wrapper class.
#


class TileComparisonWrapper(dbutil.ConnectionWrapper):
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

#
# TileComparisonPair class
#

class TileComparisonPair(object):
    """A pair of databases from which tiles are to be compared.

    Analagous to the ComparisonPair class for comparing databases, the
    TileCompare class provides for comparision of tile stores from two
    databases. The first database pertains to a benchmark tile store, and the
    second database relates to the tile store arising from the latest ingest 
    code we are seeking to verify.
    """

    def __init__(self, db1, db2, schema1, schema2):
        """
        Positional Arguments:
            db1, db2: Connections to the databases to be compared.

        Keyword Arguments:
            schema1: The schema to be used for the first database (db1)
            schema2: The schema to be used for the second database (db2)
        """

        # Set autocommit mode on the connections; retain the old settings.
        self.old_autocommit = (db1.autocommit, db2.autocommit)
        db1.autocommit = True
        db2.autocommit = True

        # Sanitise the schema names, just in case.
        self.schema1 = dbutil.safe_name(schema1)
        self.schema2 = dbutil.safe_name(schema2)

        # Wrap the connections to gain access to database structure queries.
        self.db1 = TileComparisonWrapper(db1, self.schema1)
        self.db2 = TileComparisonWrapper(db2, self.schema2)
        
        # Get the database names...
        self.db1_name = self.db1.database_name()
        self.db2_name = self.db2.database_name()

        # and qualify with the schema names if they are not 'public'
        if self.schema1 != 'public':
            self.db1_name = self.schema1 + '.' + self.db1_name
        if self.schema2 != 'public':
            self.db2_name = self.schema2 + '.' + self.db2_name

        # Set input, expected and output directores
        # Not used yet
        module = "tilecompare"
        suite = "TileCompare"
        self.input_dir = dbutil.input_directory(module, suite)
        self.output_dir = dbutil.output_directory(module, suite)
        self.expected_dir = dbutil.expected_directory(module, suite)
        # tile_root could be different to database?
        

    def restore_autocommit(self):
        """Restore the autocommit status of the underlying connections.

        The comparison pair should not be used after calling this, in
        case the connections have been reset to autocommit=False. The
        method sets the database attributes to None to enforce this."""

        self.db1.conn.autocommit = self.old_autocommit[0]
        self.db2.conn.autocommit = self.old_autocommit[1]

        self.db1 = None
        self.db2 = None





def compare_tile_stores(db1, db2, schema1='public', schema2='public',
                        verbosity=0, output=sys.stdout):
    """Compares the tile store from two databases.

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

    pair = TileComparisonPair(db1, db2, schema1, schema2)

    _create_tile_acquisition_info(pair)
    

    report = Reporter(pair.db1_name, pair.db2_name, verbosity, output)

    ignore_set = set(ignore_tables)

    table_set1 = set(pair.db1.table_list()) - ignore_set
    table_set2 = set(pair.db2.table_list()) - ignore_set

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
        tables_match = _compare_tables(pair, report, table, ignore_columns)
        if not tables_match:
            identical_so_far = False

    pair.restore_autocommit()

    return identical_so_far







# class TileCompare(object):

#     def compare_tiles(db1, db2, output=sys.stdin):
#         """Compare two sets of tiles appearing on the tile table of the database
#         connections db1 and db2."""
    
    
def _copy_ingest_tile_acquisition_info(comparison_pair, table_name):
    """Given this database connection, collate the acquisition information
    for each tile into a table. Copy this table to the production database."""
    sql = ("CREATE TABLE " + table_name + " AS" + "\n" +
           "SELECT x_index, y_index," + "\n" + 
           "end_datetime - start_datetime as aq_len," + "\n" + 
           "tile_class_id, tile_pathname, level_name," + "\n" + 
           "satellite_id, sensor_id, x_ref," + "\n" +
           "y_ref, start_datetime, end_datetime FROM tile t" + "\n" +
           "INNER JOIN dataset d on d.dataset_id=t.dataset_id" + "\n" + 
           "INNER JOIN processing_level p on p.level_id = d.level_id" + "\n" +
           "INNER JOIN acquisition a on d.acquisition_id=a.acquisition_id " +
    "\n" +
           "ORDER BY p.level_id, a.start_datetime")

    print sql
    with comparison_pair.db2.cursor() as cur:
        cur.execute(sql, {})

    dbutil.TESTSERVER.copy_table_between_databases(comparison_pair.db2_name,
                                                   comparison_pair.db1_name,
                                                   table_name)


def _create_comparison_table(comparison_pair, fresh_ingest_table, comparison_table):
    """Given Database 2's tile acquisition info in table_name, find corresponding
    records in Database 1."""
    FUZZY_MATCH_PERCENTAGE = 15.0/100.0
    sql = ("CREATE TABLE " + comparison_table  + " AS\n" + 
           "SELECT t1.start_datetime, " + "\n" +
           "t1.tile_class_id, t1.tile_pathname as tile_pathname1," + "\n" +
           "t2.tile_pathname as tile_pathname2, " + "\n" + 
           "t1.level_name as l1, p.level_name as l2 FROM " + fresh_ingest_table + " AS t1 " + "\n" +
           "LEFT JOIN acquisition a ON " + "\n" +
           "t1.satellite_id=a.satellite_id AND " + "\n" +
           "t1.sensor_id=a.sensor_id AND " + "\n" +
           "t1.start_datetime BETWEEN\n" +
           #"    a.start_datetime - t1.aq_len AND\n " +
           #"    a.start_datetime + t1.aq_len AND\n " +
           "    a.start_datetime - " + str(FUZZY_MATCH_PERCENTAGE) + "*t1.aq_len AND\n " +
           "    a.start_datetime + " + str(FUZZY_MATCH_PERCENTAGE) + "*t1.aq_len AND\n " +
           "t1.end_datetime BETWEEN\n" +
           "    a.end_datetime - " + str(FUZZY_MATCH_PERCENTAGE) + "*t1.aq_len AND\n " +
           "    a.end_datetime + " + str(FUZZY_MATCH_PERCENTAGE) + "*t1.aq_len\n " +
           #"    a.end_datetime - t1.aq_len AND\n " +
           #"    a.end_datetime + t1.aq_len\n" +
           "INNER JOIN dataset d on " + "\n" +
           "a.acquisition_id=d.acquisition_id" + "\n" +
           "INNER JOIN processing_level p on p.level_id=d.level_id " + "\n" +
           "INNER JOIN tile t2 ON " + "\n" +
           "t2.dataset_id = d.dataset_id AND " + "\n" +
           "t1.x_index=t2.x_index AND t1.y_index=t2.y_index AND " + "\n" +
           "t1.level_name=p.level_name ORDER BY t1.level_name, t1.start_datetime")
    print sql
    with comparison_pair.db1.cursor() as cur:
        cur.execute(sql, {})


#def _get_old_ingest_tile_acquisition_info(tile_comparision_pair):
#    sql = ("

# @staticmethod
# def get_tile_pathnames(expected_conn, output_conn):
#     """From two different databases, get the tile pathnames from the tile
#     table. Return each as a dictionary of
#     {basename: (tile_type_id, full path)}"""
#     sql = """-- Retrieve list of tile.tile_pathname from each database
#                      select tile_type_id, tile_pathname from tile where
#                      tile_class_id = 1
#           """
#     db_cursor = expected_conn.cursor()
#     db_cursor.execute(sql)
#     expected_dict = {}
#     for record in db_cursor:
#         expected_dict[os.path.basename(record[1])] = (record[0], record[1])
#     db_cursor = output_conn.cursor()
#     db_cursor.execute(sql)
#     output_dict = {}
#     for record in db_cursor:
#         output_dict[os.path.basename(record[1])] = (record[0], record[1])
#     return (expected_dict, output_dict)




# @staticmethod
# select tile_pathname, satellite_id, sensor_id, x_ref, y_ref,
# start_datetime, end_datetime from tile t
# inner join dataset as d on d.dataset_id=t.dataset_id
# inner join acquisition as a on d.acquisition_id=a.acquisition_id
# limit 1000;


