#!/usr/bin/env python

"""
tilecompare.py - compare two sets of tiles.
"""

import sys
import dbutil
from dbcompare import ComparisonWrapper
# #
# # TileComparisonPair class
# #
#
# Constants
#

IGNORE_CLASS_ID = [2]
MOSAIC_CLASS_ID = [4]

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
    #Drop the fresh_ingest_info and ingest_comparison tables
    pair.db1.drop_table('fresh_ingest_info')
    pair.db1.drop_table('production_tile_info')
    pair.db1.drop_table('production_all')
    pair.db1.drop_table('fresh_ingest_all')

    #TEMPORARY delete some tiles:
    _temp_delete_some_tiles(pair)

    # Name of table to which information from fresh ingest will be written.
    fresh_ingest_info_table = 'fresh_ingest_info'

    # Name of table comparing fresh ingest with production ingest.
    comparison_table = 'ingest_comparison'

    # Create the table pertaining to the fresh ingest and copy it to the
    # production database.

    _copy_fresh_ingest_info(pair, fresh_ingest_info_table)

    # Create tuple (list_both, list_db1_not_db2, list_db2_not_db1), where each
    # list is a list of tuples (tile_class, level, path1, path2).
    (list_both, list_db1_not_db2, list_db2_not_db1) = \
        create_comparison_table(pair, fresh_ingest_info_table,
                                comparison_table)

    difference_pairs = _compare_tile_contents(pair, output, list_both,
                                              list_db1_not_db2,
                                              list_db2_not_db1)
    return difference_pairs

def _temp_delete_some_tiles(comparison_pair):
    #TEMPORARY delete some tiles from tile table to test whether
    #we can detect that they are present on DB1 but not on DB2.
    sql = ("DELETE FROM tile WHERE x_index=116")
    with comparison_pair.db2.cursor() as cur:
        cur.execute(sql, {})

def _copy_fresh_ingest_info(comparison_pair, table_name):
    """Given this database connection, collate the acquisition information
    for each tile into a table. Copy this table to the production database."""
    sql = ("CREATE TABLE " + table_name + " AS" + "\n" +
           "SELECT tile_id, x_index, y_index," + "\n" +
           "a.end_datetime - a.start_datetime as aq_len," + "\n" +
           "tile_class_id, tile_pathname, level_name," + "\n" +
           "satellite_id, sensor_id, x_ref," + "\n" +
           "y_ref, a.start_datetime, a.end_datetime FROM tile t\n"
           "INNER JOIN dataset d on d.dataset_id=t.dataset_id" + "\n" +
           "INNER JOIN processing_level p on p.level_id = d.level_id" + "\n" +
           "INNER JOIN acquisition a on d.acquisition_id=a.acquisition_id\n")

    with comparison_pair.db2.cursor() as cur:
        cur.execute(sql, {})

    dbutil.TESTSERVER.copy_table_between_databases(comparison_pair.db2_name,
                                                   comparison_pair.db1_name,
                                                   table_name)


def _create_comparison_table(comparison_pair, test_ingest_info,
                             comparison_table):
    """Given Database 2's tile acquisition info in table_name, find
    corresponding records in Database 1."""
    import re
    FUZZY_MATCH_PERCENTAGE = 15.0/100.0

    # Generate table of all tile information on Database 1
    temp_table = 'temporary_all_tile_info'
    sql = ("CREATE TEMP TABLE " + temp_table + " AS" + "\n" +
           "SELECT tile_id, x_index, y_index," + "\n" +
           "a.end_datetime - a.start_datetime as aq_len," + "\n" +
           "tile_class_id, tile_pathname, level_name," + "\n" +
           "satellite_id, sensor_id, x_ref," + "\n" +
           "y_ref, a.start_datetime, a.end_datetime FROM tile t\n"
           "INNER JOIN dataset d on d.dataset_id=t.dataset_id" + "\n" +
           "INNER JOIN processing_level p on p.level_id = d.level_id" + "\n" +
           "INNER JOIN acquisition a on d.acquisition_id=a.acquisition_id\n"+
           "WHERE tile_class_id<>2;")

    # Generate table of production_tiles in Fresh acquisitions
    production_tile_info = 'production_tile_info'
    sqltemp = ("CREATE TEMP TABLE " + production_tile_info + " AS \n" +
            "SELECT DISTINCT ON (t1.tile_id) t1.x_index, t1.y_index,\n" +
            "t1.tile_pathname, t1.satellite_id, t1.sensor_id,\n" +
            "t1.start_datetime, t1.end_datetime, t1.level_name\n FROM\n" +
            temp_table + " t1 INNER JOIN " + test_ingest_info + " fi ON\n" +
            "t1.level_name=fi.level_name AND\n"
            "t1.satellite_id=fi.satellite_id AND\n" +
            "t1.sensor_id=fi.sensor_id AND\n" +
            "t1.start_datetime BETWEEN" + "\n" +
            "    fi.start_datetime - " + str(FUZZY_MATCH_PERCENTAGE) +
                                         "*fi.aq_len AND " + "\n" +
            "    fi.start_datetime + " + str(FUZZY_MATCH_PERCENTAGE) +
                                         "*fi.aq_len AND " + "\n" +
            "t1.end_datetime BETWEEN" + "\n" +
            "    fi.end_datetime - " + str(FUZZY_MATCH_PERCENTAGE) +
                                         "*fi.aq_len AND " + "\n" +
            "    fi.end_datetime + " + str(FUZZY_MATCH_PERCENTAGE) +
                                         "*fi.aq_len;"
            )
    sql = sql + sqltemp

    # Generate table of tiles that are in Database 1 and, if the
    # corresponding tile exists on Database 2, add its information
    production_table = 'production_all'
    sqltemp = ("CREATE TEMP TABLE " + production_table + " AS SELECT\n " +
            "fi.tile_class_id,\n" +
            "pt.tile_pathname as path1, fi.tile_pathname as path2,\n" +
            "pt.level_name\n" +
            "FROM " + production_tile_info + " pt LEFT OUTER JOIN\n" +
             test_ingest_info + " fi ON\n" +
            "pt.x_index=fi.x_index and pt.y_index=fi.y_index AND\n"
            "pt.level_name=fi.level_name AND\n"
            "pt.satellite_id=fi.satellite_id AND\n" +
            "pt.sensor_id=fi.sensor_id AND\n" +
            "pt.start_datetime BETWEEN" + "\n" +
            "    fi.start_datetime - " + str(FUZZY_MATCH_PERCENTAGE) +
                                         "*fi.aq_len AND " + "\n" +
            "    fi.start_datetime + " + str(FUZZY_MATCH_PERCENTAGE) +
                                         "*fi.aq_len AND " + "\n" +
            "pt.end_datetime BETWEEN" + "\n" +
            "    fi.end_datetime - " + str(FUZZY_MATCH_PERCENTAGE) +
                                         "*fi.aq_len AND " + "\n" +
            "    fi.end_datetime + " + str(FUZZY_MATCH_PERCENTAGE) +
                                         "*fi.aq_len;"
           )
    sql = sql + sqltemp


    # Generate table of tiles that are in Database 2 and, if the
    # corresponding tile exists on Database 1, add its information.
    fresh_ingest_table = 'fresh_ingest_all'
    sqltemp = re.sub(production_table, fresh_ingest_table, sqltemp)
    sqltemp = re.sub("LEFT OUTER JOIN", "RIGHT OUTER JOIN", sqltemp)
    sql = sql + sqltemp

    # Generate table of tiles in production and test.
    sql_fetch_production_and_test = ("SELECT\n" +
               "pt.tile_class_id, pt.level_name, pt.path1, pt.path2 FROM\n" +
               production_table + " pt INNER JOIN " +
               fresh_ingest_table + " fi ON\n" +
               "pt.path1=fi.path1 AND pt.path2=fi.path2;")

    # Generate table of tiles in production, but not test.
    sql_fetch_production_not_test = ("SELECT\n" +
               "tile_class_id, level_name, path1, path2 FROM\n" +
               production_table +  " WHERE path2 is NULL;") 

    # Generate table of tiles in test, but not production.
    sql_fetch_test_not_production = ("SELECT\n" +
               "tile_class_id, level_name, path1, path2 FROM\n" +
               fresh_ingest_table +  " WHERE path1 is NULL;") 


    # Submit the queries to the database.
    with comparison_pair.db1.cursor() as cur:
        cur.execute(sql, {})
        cur.execute(sql_fetch_both, {})
        production_and_test = cur.fetchall()
        cur.execute(sql_fetch_production_not_test, {})
        production_not_test = cur.fetchall()
        cur.execute(sql_fetch_test_not_production, {})
        test_not_production = cur.fetchall()
    return (production_and_test, production_not_test, test_not_production)

def _compare_tile_contents(output=sys.stdout,
                           list_both, list_db1_not_db2, list_db2_not_db1):
    """Compare the tile pairs contained in list_both. Additionally, report
    those tiles that are only in Database 1, or only in  Database 2.
    Inputs: list_both: a list of tuples (tile_class_id, level, path1, path2)
                       where
                       tile_class_id is the class of the tile in Database 2,
                       level is the processing_level,
                       path1 is the path of the file in Database 1,
                       path2 is the path of the file in Database 2.
    Output: difference_pairs: the list of (path1, path2) where there are
                              differences between the tiles."""
    # pylint:disable=too-many-locals
    # pylint:disable=too-many-branches
    # pylint:disable=too-many-statements
    from osgeo import gdal
    import numpy as np
    import os
    import cube_util
    gdal.UseExceptions()
    rec_num = 0
    # Define a list of tuples (path1, path2) where the contents differ
    # Each 
    difference_pairs = []
    for tile_pair in list_both:
        rec_num += 1
        tile_class_id, level, path1, path2 = tile_pair
        sys.stdout.writelines('RECORD NUMBER %d tile_class_id2=%d level=%s\n'
                              %(rec_num, tile_class_id2, level_name))
        output.writelines('RECORD NUMBER %d tile_class_id2=%d level=%s\n'
                          %(rec_num, tile_class_id2, level_name))

        # For a mosaic tile, the tile entry may not be on the database, so
        # look in mosaic_cache:
        if tile_class_id2 == 4:
            path1 = os.path.join(os.path.dirname(path1), 'mosaic_cache',
                                 os.path.basename(path1))
    
        # Check the Geotransform, Projection and shape
        data1, data2, msg  = _check_metadata(dset1, dset2)
        sys.stdout.writelines(msg)
        output.writelines(msg)
        
        # Compare the tile contents
        are_different, msg = _compare_data(tile_class_id, level,
                                              data1, data2)
        if are_different:
            difference_pairs.extend((path1, path2))
        sys.stdout.writelines(msg)
        output.writelines(msg)
        #############################################################

        dset1 = None
        dset2 = None
        data1 = None
        data2 = None
    return difference_pairs




def _check_tile_metadata(path1, path2):
    """Given two tile paths, check that the projections, geotransforms and
    dimensions agree."""
    
    msg = ""
    # Open the tile files
    try:
        dset1 = gdal.Open(path1)
        data1 = dset1.ReadAsArray()
    except RuntimeError:
        msg += "ERROR:\tBenchmark tile %s does not exist\n"  %path1
        dset1 = None
        data1 = None
    try:
        dset2 = gdal.Open(path2)
        data2 = dset2.ReadAsArray()
    except RuntimeError:
        msg += "ERROR:\tBenchmark tile %s does not exist\n"  %path1
        dset2 = None
        data2 = None
    
    # Check the geotransforms
    geotransform1 = dset1.GetGeoTransform() if dset1 else None
    geotransform2 = dset2.GetGeoTransform() if dset2 else None
    if geotransform1 and geotransform2:
        if geotransform1 != geotransform2:
            msg += "\tError:\tGeotransforms disagree\n"

    if dset1 and not geotransform1:
        msg += "\tError:\tGeotransform for %s not present\n" %path1
    if dset2 and not geotransform2:
        msg += "\tError:\tGeotransform for %s not present\n" %path2

    # Check the projections
    projection1 = dset1.GetProjection() if dset1 else None
    projection2 = dset2.GetProjection() if dset2 else None
    if projection1 and projection2:
        if projection1 != projection2:
            msg += "\tError:\tGeotransforms disagree\n"

    if dset1 and not projection1:
        msg += "\tError:\tProjection for %s not present\n" %path1
    if dset2 and not projection2:
        msg += "\tError:\tProjection for %s not present\n" %path2

    # Check the dimensions of the arrays
    if dset1 and dset2:
        if data1.shape != data2.shape:
            msg += "\tError:\tDimensions of arrays disagree\n"
    if dset1 and not data1:
        msg += "\tError:\tArray data for %s not present\n" %path1
    if dset2 and not data2:
        msg += "\tError:\tArray data for %s not present\n" %path2
    
    return (data1, data2, msg)


def _compare_data(tile_class_id, level, data1, data2):
    """Given two arrays and the level name, check that the data arrays agree.
    If the level is 'PQA' and the tile is a mosaic, then only compare mosaics
    at pixels where the contiguity bit is set in both versions of the mosaic
    tile."""
    different = False
    msg = ""
    if tile_class_id not in MOSAIC_CLASS_ID :
        if (data1 != data2).any():
            msg += "For following tile from Fresh Ingest: %s\n," \
                "data differs from benchmark\n"
        else:
            # mosaic tile
            if level == 'PQA':
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
                    msg +=  "On %d pixels, mosaiced tile benchmark %s differs"\
                            "from Fresh Ingest %s\n"\
                            %(np.count_nonzero(~contiguity_diff),
                              path1, path2)
                    different = True
            else:
                #TODO: compare vrt mosaic
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
                    different = True
                if result['stderr'] != '':
                    output.writelines("Error in system diff command:\n" +
                                      result['stderr'])
    return (different, msg)























