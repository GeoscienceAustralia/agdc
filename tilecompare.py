#!/usr/bin/env python

"""
tilecompare.py - compare two sets of tiles.
"""

import sys
import dbutil
from dbcompare import ComparisonWrapper

#
# ComparisonWrapper connection wrapper class.
#


# class TileComparisonWrapper(dbutil.ConnectionWrapper):
#     """Wrapper for a connection intended for database comparison.

#     This implements queries about the structure of the database,
#     as recorded in the information schema."""

#     def __init__(self, conn, default_schema='public'):

#         self.default_schema = dbutil.safe_name(default_schema)
#         dbutil.ConnectionWrapper.__init__(self, conn)

#     def table_exists(self, table, schema=None):
#         """Returns True if the table exists in the database."""

#         if schema is None:
#             schema = self.default_schema

#         sql = ("SELECT table_name FROM information_schema.tables\n" +
#                "WHERE table_schema = %(schema)s AND\n" +
#                "   table_name = %(table)s AND\n" +
#                "   table_type = 'BASE TABLE';")

#         with self.conn.cursor() as curs:
#             curs.execute(sql, {'table': table, 'schema': schema})
#             tab_found = bool(curs.fetchone())

#         return tab_found

#     def table_list(self, schema=None):
#         """Return a list of the tables in a database."""

#         if schema is None:
#             schema = self.default_schema

#         sql = ("SELECT table_name FROM information_schema.tables\n" +
#                "WHERE table_schema = %(schema)s AND\n" +
#                "   table_type = 'BASE TABLE'\n" +
#                "ORDER BY table_name;")

#         with self.conn.cursor() as curs:
#             curs.execute(sql, {'schema': schema})
#             tab_list = [tup[0] for tup in curs.fetchall()]

#         return tab_list

#     def column_list(self, table, schema=None):
#         """Return a list of the columns in a database table."""

#         if schema is None:
#             schema = self.default_schema

#         sql = ("SELECT column_name FROM information_schema.columns\n" +
#                "WHERE table_schema = %(schema)s AND table_name = %(table)s\n" +
#                "ORDER BY ordinal_position;")

#         with self.conn.cursor() as curs:
#             curs.execute(sql, {'table': table, 'schema': schema})
#             col_list = [tup[0] for tup in curs.fetchall()]

#         return col_list

#     def primary_key(self, table, schema=None):
#         """Returns the primary key for a table as a list of columns."""

#         if schema is None:
#             schema = self.default_schema

#         sql = ("SELECT column_name\n" +
#                "FROM information_schema.key_column_usage\n" +
#                "WHERE constraint_schema = %(schema)s AND\n" +
#                "   constraint_name IN\n" +
#                "      (SELECT constraint_name\n" +
#                "       FROM information_schema.table_constraints\n" +
#                "       WHERE table_schema = %(schema)s AND\n" +
#                "          table_name = %(table)s AND\n" +
#                "          constraint_type = 'PRIMARY KEY')\n" +
#                "ORDER BY ordinal_position;")

#         with self.conn.cursor() as curs:
#             curs.execute(sql, {'table': table, 'schema': schema})
#             pkey = [tup[0] for tup in curs.fetchall()]

#         if not pkey:
#             # pkey empty: Try an alternative query - not all rows in
#             #     the table_constraints table are accessable to an
#             #     ordinary user.

#             sql = ("SELECT column_name\n" +
#                    "FROM information_schema.key_column_usage\n" +
#                    "WHERE constraint_schema = %(schema)s AND\n"
#                    "   table_name = %(table)s AND\n" +
#                    "   constraint_name LIKE '%%_pkey'\n" +
#                    "ORDER BY ordinal_position;")

#             with self.conn.cursor() as curs:
#                 curs.execute(sql, {'table': table, 'schema': schema})
#                 pkey = [tup[0] for tup in curs.fetchall()]

#         assert pkey, "Unable to find primary key for table '%s'." % table

#         return pkey

# #
# # TileComparisonPair class
# #

class TileComparisonPair(object):
    """A pair of databases from which tiles are to be compared.

    Analagous to the ComparisonPair class for comparing databases, the
    TileCompare class provides for comparision of tile stores from two
    databases. The first database pertains to a benchmark tile store, and the
    second database relates to the tile store arising from the latest ingest
    code we are seeking to verify.
    """
    # pylint:disable=too-many-instance-attributes
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
                        output=sys.stdout):
    """Compares the tile stores from two databases.

    Database Connection db1 is assumed to represent the production tile store,
    against which we wish to verify the tile store resulting from a Fresh
    Ingest, which has taken place onto the previously-empty Database Connection
    db2.

    This function runs in three stages:
    1. Gather the Fresh Ingest information on Database Connection db2 into a
    table and copy this accross to Database Connection db1, the production
    database.

    2. On Database Connection db1, merge the table from Step 1 to find the
    corresponding production tiles.

    3. For those Fresh Ingest tiles where a production tile can be found,
    compare the two tiles and report if there is a difference. It can happen
    that the tile exists on Fresh Ingest but not on production tile store.
    This can happen for one of several reasons:
        a) The old ingest used PQA to determine the existence of lower-level
           data. By contrast, the Fresh Ingest process looks at the tile
           directly to evaluate the exitence of data.
        b) Mosaic tiles used to be created on user-request by the stacker class
           of the API. By contrast, The Fresh Ingest process does this
           automatically.
        c) The coverage method of the Fresh Ingest process will, very
        occasionally, pick up some new tiles.
         
    Such anomalies are reported in the output stream.

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
        output: Where the output goes. This is assumed to be a file object.
            Defaults to sys.stdout.

    Return Value: Returns a list (path1, path2) of those corresponding tile
        pairs where the contents differ.
    """

    pair = TileComparisonPair(db1, db2, schema1, schema2)

    # Name of table to which information from fresh ingest will be written.
    fresh_ingest_info_table = 'fresh_ingest_info'

    # Name of table comparing fresh ingest with production ingest.
    comparison_table = 'ingest_comparison'

    # Create the table pertaining to the fresh ingest and copy it to the
    # production database.

    _copy_fresh_ingest_info(pair, fresh_ingest_info_table)

    # Create the table comparing the fresh ingest with the production database.
    _create_comparison_table(pair, fresh_ingest_info_table, comparison_table)

    # Use the comparison_table to compare each fresh ingest tile with the
    # production database.
    difference_pairs = _compare_tile_contents(pair, comparison_table, output)
    return difference_pairs


def _copy_fresh_ingest_info(comparison_pair, table_name):
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

    with comparison_pair.db2.cursor() as cur:
        cur.execute(sql, {})

    dbutil.TESTSERVER.copy_table_between_databases(comparison_pair.db2_name,
                                                   comparison_pair.db1_name,
                                                   table_name)


def _create_comparison_table(comparison_pair, fresh_ingest_table,
                             comparison_table):
    """Given Database 2's tile acquisition info in table_name, find
    corresponding records in Database 1."""
    FUZZY_MATCH_PERCENTAGE = 15.0/100.0
    #TODO: Make this query cover the case where a tile exists in the benchmark
    # but not in the fresh_ingest
    #TODO Once query finalised, use FUZZY_MATCH_PERCENTAGE parameter
    sql = ("CREATE TABLE " + comparison_table +" AS SELECT " + "\n" +
           "t2.tile_class_id, t1.tile_pathname as path1," + "\n" +
           "t2.tile_pathname as path2, t2.level_name as level_name " + "\n" +
           "FROM " + fresh_ingest_table + " t2" + "\n" +
           "LEFT OUTER JOIN " + "\n" +
           "    (select x_index, y_index, tile_pathname, level_name," + "\n" +
           "     satellite_id, sensor_id, start_datetime, end_datetime\n"
           "     from acquisition a INNER JOIN dataset d on " + "\n" +
           "     a.acquisition_id=d.acquisition_id" + "\n" +
           "     INNER JOIN processing_level p on p.level_id=d.level_id\n"
           "     INNER JOIN tile t ON t.dataset_id = d.dataset_id\n" +
           "     WHERE tile_class_id <>2 ) t1" + "\n" +
           "ON  t1.satellite_id=t2.satellite_id" + "\n" +
           "AND t1.sensor_id=t2.sensor_id AND" + "\n" +
           "t1.start_datetime BETWEEN" + "\n" +
           "    t2.start_datetime - 0.15*t2.aq_len AND " + "\n" +
           "    t2.start_datetime + 0.15*t2.aq_len AND " + "\n" +
           "t1.end_datetime BETWEEN" + "\n" +
           "    t2.end_datetime - 0.15*t2.aq_len AND" + "\n" +
           "    t2.end_datetime + 0.15*t2.aq_len AND" + "\n" +
           "t1.x_index=t2.x_index AND t1.y_index=t2.y_index AND" + "\n" +
           "t1.level_name=t2.level_name")



    # sql = ("CREATE TABLE ingest_comparison AS SELECT " + "\n" +
    #        "-- t2.tile_class_id, t1.tile_pathname as path1," + "\n" +
    #        "-- t2.tile_pathname as path2, t2.level_name as level_name " + "\n" +
    #        "-- ,x_ref, y_ref\n" +
    #        "t1.tile_pathname as tile_pathname1, t2.* \n" +
    #        "FROM " + fresh_ingest_table + " t2" + "\n" +
    #        "LEFT OUTER JOIN " + "\n" +
    #        "    (select t.x_index, t.y_index, t.tile_pathname, p.level_name," + "\n" +
    #        "     a.satellite_id, a.sensor_id, a.start_datetime, a.end_datetime\n"
    #        "     from acquisition a INNER JOIN dataset d on " + "\n" +
    #        "     a.acquisition_id=d.acquisition_id" + "\n" +
    #        "     INNER JOIN processing_level p on p.level_id=d.level_id\n"
    #        "     INNER JOIN tile t ON t.dataset_id = d.dataset_id" + "\n" +
    #        "     LEFT JOIN " + fresh_ingest_table + " fi ON\n"     
    #        "      a.satellite_id=fi.satellite_id AND\n" +
    #        "      a.sensor_id=fi.sensor_id AND\n" +
    #        "      a.start_datetime BETWEEN" + "\n" +
    #        "          fi.start_datetime - 0.15*fi.aq_len AND " + "\n" +
    #        "          fi.start_datetime + 0.15*fi.aq_len AND " + "\n" +
    #        "      a.end_datetime BETWEEN" + "\n" +
    #        "          fi.end_datetime - 0.15*fi.aq_len AND" + "\n" +
    #        "          fi.end_datetime + 0.15*fi.aq_len AND" + "\n" +
    #        "      t.x_index=fi.x_index AND t.y_index=fi.y_index AND\n" +
    #        "      p.level_name=fi.level_name) t1 ON\n"
    #        "t1.x_index=t2.x_index and t1.y_index=t2.y_index AND\n" +
    #        "t1.level_name=t2.level_name AND\n"
    #        "t1.satellite_id=t2.satellite_id AND\n" +
    #        "t1.sensor_id=t2.sensor_id AND\n" +
    #        "t1.start_datetime BETWEEN" + "\n" +
    #        "    t2.start_datetime - 0.15*t2.aq_len AND " + "\n" +
    #        "    t2.start_datetime + 0.15*t2.aq_len AND " + "\n" +
    #        "t1.end_datetime BETWEEN" + "\n" +
    #        "    t2.end_datetime - 0.15*t2.aq_len AND" + "\n" +
    #        "    t2.end_datetime + 0.15*t2.aq_len")




    with comparison_pair.db1.cursor() as cur:
        cur.execute(sql, {})

def _compare_tile_contents(comparison_pair, comparison_table,
                           output=sys.stdout):
    # pylint:disable=too-many-locals
    # pylint:disable=too-many-branches
        # pylint:disable=too-many-statements
    """In the first database of the pair, use the comparison_table to
    compare differences between the tile stores of the benchmark ingest and the
    fresh ingest."""
    from osgeo import gdal
    import numpy as np
    import os
    import cube_util
    gdal.UseExceptions()
    sql = ("select * from " + comparison_table)
    rec_num = 0
    # Define a list of tuples (path1, path2) where the contents differ
    difference_pairs = []
    with comparison_pair.db1.cursor() as cur:
        cur.execute(sql, {})
        for record in cur:
            rec_num += 1
            tile_class_id2 = record[0]
            path1 = record[1]
            path2 = record[2]
            level_name = record[3]


            sys.stdout.writelines('RECORD NUMBER %d tile_class_id2=%d level=%s\n'
                                  %(rec_num, tile_class_id2, level_name))
            output.writelines('RECORD NUMBER %d tile_class_id2=%d level=%s\n'
                                  %(rec_num, tile_class_id2, level_name))
            if not path1:
                output.writelines(
                    "No benchmark tile for Fresh Ingest tile %s\n" %path2)
                continue
            if not path2:
                output.writelines(
                    "No Fresh Ingest tile for Benchmark tile %s\n" %path1)
                continue

            # For a mosaic tile, the tile entry may not be on the database, so
            # look in mosaic_cache:
            if tile_class_id2 == 4:
                path1 = os.path.join(os.path.dirname(path1), 'mosaic_cache',
                                     os.path.basename(path1))

            # Open the tile files
            try:
                dset1 = gdal.Open(path1)
            except RuntimeError:
                output.writelines("Benchmark tile %s does not exist\n"
                                  %path1)
                dset1 = None
                continue
            try:
                dset2 = gdal.Open(path2)
            except RuntimeError:
                output.writelines("Fresh Ingest tile %s does not exist\n"
                                  %path2)
                dset2 = None
                continue

            data1 = dset1.ReadAsArray()
            data2 = dset2.ReadAsArray()
            
            
            # Check the Geotransform, Projection and shape
            if dset1.GetGeoTransform() != dset2.GetGeoTransform():
                output.writelines("For following tile from From Ingest: %s\n" \
                                 "Benchmark geotransform: %s\n Fresh Ingest " \
                                 "geotransform: %s\n" \
                                  %(path2,
                                    str(dset1.GetGeoTransform()),
                                    str(dset2.GetGeoTransform())))
            if dset1.GetProjection() != dset2.GetProjection():
                output.writelines("For following tile from From Ingest: %s\n" \
                                  "Benchmark projection: %s\n Fresh Ingest " \
                                  "projection: %s\n" \
                                   %(path2,
                                     str(dset1.GetProjection()),
                                     str(dset2.GetProjection())))

            if data1.shape != data2.shape:
                output.writelines("For following tile from From Ingest: %s\n" \
                                   "Benchmark array dimensions:%s\n" \
                                   "Fresh Ingest array dimensions: %s\n" \
                                    %(path2,
                                      str(data1.shape), str(data2.shape)))

            # Do the comparison of the contents
            if tile_class_id2 in [1, 3]:
                if (data1 != data2).any():
                    output.writelines("For following tile from " \
                                      "Fresh Ingest: %s\n," \
                                      "data differs from benchmark\n")
            elif tile_class_id2 == 4:
                # mosaic tile
                if level_name == 'PQA':
                    ind = (data1 == data2)
                    # Check that differences are due to differing treatment
                    # of contiguity bit.
                    data1_diff = data1[~ind].ravel()
                    data2_diff = data2[~ind].ravel()
                    contiguity_diff =  \
                        np.logical_or(
                        np.bitwise_and(data1_diff, 1 << 8) == 0,
                        np.bitwise_and(data2_diff, 1 << 8) == 0)
                    if ~contiguity_diff.all():
                        output.writelines(
                            "On %d pixels, mosaiced tile benchmark %s differs"\
                            "from Fresh Ingest %s\n"\
                                %(np.count_nonzero(~contiguity_diff),
                                  path1, path2))
                        difference_pairs.append((path1, path2))
                else:
                    diff_cmd = ["diff",
                                "-I",
                                "[Ff]ilename",
                                "%s" %path1,
                                "%s" %path2
                                ]
                    result = cube_util.execute(diff_cmd, shell=False)
                    if result['stdout'] != '':
                        output.writelines("Differences between mosaic vrt " \
                                              "files:\n" +result['stdout'])
                        difference_pairs.append((path1, path2))
                    if result['stderr'] != '':
                        output.writelines("Error in system diff command:\n" +
                                          result['stderr'])
            dset1 = None
            dset2 = None
            data1 = None
            data2 = None
    return difference_pairs
































