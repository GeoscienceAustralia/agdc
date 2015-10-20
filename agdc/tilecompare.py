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
tilecompare.py - compare two sets of tiles.
"""
from __future__ import absolute_import

import sys
import os
import re
from . import dbutil
from osgeo import gdal
import numpy as np
from .dbcompare import ComparisonWrapper
from eotools.execute import execute
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

    Such anomalies are reported in the output stream with a "WARNING" prefix

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

    #TEMPORARY delete some tiles:
    _temp_delete_some_tiles(pair)

    # Create a random 9-digit string to append to tables"
    random_suffix = dbutil.random_name("_")

    # Name of table to which information from fresh ingest will be written.
    test_ingest_table = 'test_ingest%s' %random_suffix

    # Create the table pertaining to the fresh ingest and copy it to the
    # production database.
    _copy_fresh_ingest_info(pair, test_ingest_table)

    # Create tuple (list_both, list_db1_not_db2, list_db2_not_db1), where each
    # list is a list of tuples:
    # (level, tile_class_id1, tile_class_id2, path1, path2).
    (list_both, list_db1_not_db2, list_db2_not_db1) = \
        _get_comparison_pairs(pair, test_ingest_table)

    # Output information for the edge cases of tiles being in only one database
    tile_list = [p[3] for p in list_db1_not_db2]
    _log_missing_tile_info(tile_list, pair.db1_name, pair.db2_name,
                           output)
    tile_list = [p[4] for p in list_db2_not_db1]
    _log_missing_tile_info(tile_list, pair.db2_name, pair.db1_name,
                           output)

    output.writelines('There might be further mosaic tiles that are missing\n')

    # Compare the tiles if they both exist
    difference_pairs = _compare_tile_contents(list_both, output)
    return difference_pairs

def _temp_delete_some_tiles(comparison_pair):
    """Temporarily delete some files."""
    #TEMPORARY delete some tiles from tile table to test whether
    #we can detect that they are present on DB1 but not on DB2.
    sql = ("DELETE FROM tile WHERE x_index=116")
    with comparison_pair.db2.cursor() as cur:
        cur.execute(sql, {})

def _copy_fresh_ingest_info(comparison_pair, test_ingest_info_table):
    """Given this database connection, collate the acquisition information
    for each tile into a table. Copy this table to the production database."""
    sql = ("CREATE TABLE " + test_ingest_info_table + " AS" + "\n" +
           "SELECT tile_id, x_index, y_index, a.acquisition_id," + "\n" +
           "a.end_datetime - a.start_datetime as aq_len," + "\n" +
           "tile_class_id, tile_pathname, level_id, satellite_id," + "\n" +
           "sensor_id, a.start_datetime, a.end_datetime FROM tile t\n"
           "INNER JOIN dataset d on d.dataset_id=t.dataset_id" + "\n" +
           "INNER JOIN acquisition a on d.acquisition_id=a.acquisition_id\n")

    with comparison_pair.db2.cursor() as cur:
        cur.execute(sql, {})

    dbutil.TESTSERVER.copy_table_between_databases(comparison_pair.db2_name,
                                                   comparison_pair.db1_name,
                                                   test_ingest_info_table)


def _get_comparison_pairs(db_pair, test_ingest_info):
    """Given Database 2's information in test_ingest_info table, generate pairs
    of corresponding tiles from Database 1 and Database 2.

    Returns: 3 lists as follows:
    1. production_and_test: those corresponding pairs which exist on Database 1
       and Database 2.
    2. production_not_test: the tiles found only on Database 1.
    3. test_not_production: the tiles found only on Database 2.

    Each element of the above lists is a 5-tuple:
    (level_name, tile_class_id on Database 1, tile_class_id on Database 2,
     tile_pathname on Database 1, tile_pathname on Database 2)."""

    fuzzy_match_percentage = 15

    # Strip the random suffix from the test_ingest_info table and use it
    # for other tables.
    random_suffix = re.match(r'.+(_\d+)', test_ingest_info).groups(1)

    # Match the datasets from Database 2 to those in Database 1
    sql = (
        "CREATE TEMPORARY TABLE datasets_join_info AS SELECT DISTINCT\n" +
        "a.acquisition_id AS acquisition_id1, ti.acquisition_id AS\n" +
        "acquisition_id2, level_id FROM acquisition a\n" +
        "INNER JOIN " + test_ingest_info + " ti ON " +
        "    a.satellite_id=ti.satellite_id AND " +
        "    a.sensor_id=ti.sensor_id AND " +
        "    a.start_datetime BETWEEN " +
        "        ti.start_datetime - " + str(fuzzy_match_percentage/100.) +
        "                                               *ti.aq_len AND\n" +
        "        ti.start_datetime + " + str(fuzzy_match_percentage/100.) +
        "                                               *ti.aq_len AND\n" +
        "    a.end_datetime BETWEEN " +
        "        ti.end_datetime - " + str(fuzzy_match_percentage/100.) +
        "                                               *ti.aq_len AND\n" +
        "        ti.end_datetime + " + str(fuzzy_match_percentage/100.) +
        "                                               *ti.aq_len;"
        )

    # Find all tiles from Database 1 which appear in the datasets
    sqltemp = (
        "CREATE TEMPORARY TABLE tiles1 AS SELECT\n" +
        "acquisition_id1, acquisition_id2, dji.level_id,\n" +
        "tile_class_id AS tile_class_id1, tile_pathname AS path1,\n" +
        "x_index, y_index FROM datasets_join_info dji\n" +
        "INNER JOIN acquisition a ON a.acquisition_id=dji.acquisition_id1\n" +
        "INNER JOIN dataset d on d.acquisition_id=a.acquisition_id AND\n" +
        "                        d.level_id=dji.level_id\n" +
        "INNER JOIN tile t ON t.dataset_id=d.dataset_id\n" +
        "WHERE t.tile_class_id<>2;"
        )

    sql = sql + sqltemp

    # Find all tiles from test ingestion
    sqltemp = (
        "CREATE TEMPORARY TABLE tiles2 AS SELECT\n" +
        "acquisition_id1, acquisition_id2, dji.level_id,\n" +
        "tile_class_id AS tile_class_id2, tile_pathname AS path2,\n" +
        "x_index, y_index FROM datasets_join_info dji\n" +
        "INNER JOIN " + test_ingest_info + " ti ON \n" +
        "                       ti.acquisition_id=dji.acquisition_id2 AND\n" +
        "                       ti.level_id=dji.level_id;"
        )

    sql = sql + sqltemp

    # For each Database 1 tile found in the test ingest datasets, find the
    # corresponding Database 2 tile if it exists.
    production_all_tiles = 'tiles1_all%s' %random_suffix
    test_ingest_all_tiles = 'tiles2_all%s' %random_suffix
    sqltemp = (
        "CREATE TABLE " + production_all_tiles + " AS SELECT\n" +
        "level_name, tile_class_id1, tile_class_id2, path1, path2\n" +
        "FROM tiles1 t1 LEFT OUTER JOIN tiles2 t2 ON\n" +
        "t1.acquisition_id1=t2.acquisition_id1 AND\n" +
        "t1.level_id=t2.level_id AND\n" +
        "t1.x_index=t2.x_index AND t1.y_index=t2.y_index\n" +
        "INNER JOIN processing_level p on p.level_id=t1.level_id;"
        )

    sql = sql + sqltemp

    # For each Database 2 tile found in the test ingest datasets, find the
    # corresponding Database 1 tile if it exists.
    sqltemp = (
        "CREATE TABLE " + test_ingest_all_tiles + " AS SELECT\n" +
        "level_name, tile_class_id1, tile_class_id2, path1, path2\n" +
        "FROM tiles2 t2 LEFT OUTER JOIN tiles1 t1 ON\n" +
        "t1.acquisition_id1=t2.acquisition_id1 AND\n" +
        "t1.level_id=t2.level_id AND\n" +
        "t1.x_index=t2.x_index AND t1.y_index=t2.y_index\n" +
        "INNER JOIN processing_level p on p.level_id=t2.level_id; "
        )

    sql = sql+sqltemp

    # Generate list of tiles found in Database 1 and Database 2
    sql_fetch_both = ("SELECT\n" +
               "t1.level_name, t1.tile_class_id1, t2.tile_class_id2, \n" +
               "t1.path1, t2.path2 FROM\n" +
               production_all_tiles + " t1 INNER JOIN " +
               test_ingest_all_tiles + " t2 ON\n" +
               "t1.path1=t2.path1 AND t1.path2=t2.path2;")

    # Generate list of tiles found in Database 1 but not Database 2
    sql_fetch_production_not_test = ("SELECT\n" +
               "level_name, tile_class_id1, tile_class_id2, \n" +
               "path1, path2 FROM\n" +
               production_all_tiles +  " WHERE path2 is NULL;")

    # Generate list of tiles found in Database 2 but not Database 1
    sql_fetch_test_not_production = ("SELECT\n" +
               "level_name, tile_class_id1, tile_class_id2,\n" +
                                     "path1, path2 FROM\n" +
               test_ingest_all_tiles +  " WHERE path1 is NULL;")


    with db_pair.db1.cursor() as cur:
        cur.execute(sql, {})
        cur.execute(sql_fetch_both, {})
        production_and_test = cur.fetchall()
        cur.execute(sql_fetch_production_not_test, {})
        production_not_test = cur.fetchall()
        cur.execute(sql_fetch_test_not_production, {})
        test_not_production = cur.fetchall()

    db_pair.db1.drop_table(test_ingest_info)
    db_pair.db1.drop_table(production_all_tiles)
    db_pair.db1.drop_table(test_ingest_all_tiles)

    return (production_and_test, production_not_test, test_not_production)

def _log_missing_tile_info(tile_list, dbname_present, dbname_missing, output):
    """Log information from the edge case of tiles present on dbname_present,
    but missing on dbname_missing."""
    if tile_list:
        if len(tile_list) == 1:
            number_str = " is %d tile " %len(tile_list)
        else:
            number_str = " are %d tiles " %len(tile_list)
        output.writelines('Given the datasets from the Test Ingest process, ' \
                          'there are %s that are in the %s tile ' \
                          'store that are not in the %s tile store:\n'\
                          %(number_str, dbname_present, dbname_missing))
    for tile in tile_list:
        output.writelines('WARNING: Only in %s tilestore:' \
                              '%s\n'%(dbname_present, tile))

def _compare_tile_contents(list_both, output):
    """Compare the tile pairs contained in list_both. Additionally, report
    those tiles that are only in Database 1, or only in  Database 2.

    Positional arguments: 3 lists as follows:
    1. production_and_test: those corresponding pairs which exist on Database 1
       and Database 2.
    2. production_not_test: the tiles found only on Database 1.
    3. test_not_production: the tiles found only on Database 2.
    Each element of the above lists is a 5-tuple:
    (level_name, tile_class_id on Database 1, tile_class_id on Database 2,
     tile_pathname on Database 1, tile_pathname on Database 2).

    Returns:
    List of tile-path pairs (path1, path2) for which a difference has been
    detected."""
    #pylint:disable=too-many-locals

    # Define a list of tuples (path1, path2) where the contents differ
    # Each
    rec_num = 0
    difference_pairs = []
    for tile_pair in list_both:
        rec_num += 1
        is_mosaic_vrt = False
        level, tile_class_id1, tile_class_id2, path1, path2 = tile_pair
        output.writelines('RECORD NUMBER %d tile_class_id2=%d level=%s\n'
                          %(rec_num, tile_class_id2, level))
        # For a mosaic tile, the tile entry may not be on the database, so
        # look in mosaic_cache:
        if tile_class_id2 in MOSAIC_CLASS_ID:
            path1 = os.path.join(os.path.dirname(path1), 'mosaic_cache',
                                 os.path.basename(path1))
            # For non-PQA tiles, the benchmark mosaic will be .vrt extension
            if level in ['NBAR', 'ORTHO']:
                path1 = re.match(r'(.+)\.tif$', path1).groups(1)[0] + '.vrt'
                is_mosaic_vrt = True

        # Check the Geotransform, Projection and shape (unless it is a vrt)
        if is_mosaic_vrt:
            data1, data2, msg = (None, None, "")
        else:
            # Skip checking of metadata for a vrt mosaic since we will check
            # with system diff command in _compare_data
            data1, data2, msg = _check_tile_metadata(path1, path2)

        if msg:
            output.writelines(msg)

        # Compare the tile contents
        are_different, msg = _compare_data(level,
                                           tile_class_id1, tile_class_id2,
                                           path1, path2, data1, data2)
        if are_different:
            difference_pairs.extend((path1, path2))
        if msg:
            sys.stdout.writelines(msg)
            output.writelines(msg)

    return difference_pairs

def _check_tile_metadata(path1, path2):
    """Given two tile paths, check that the projections, geotransforms and
    dimensions agree. Returns a message in string msg which, if empty,
    indicates agreement on the metadata."""
    # pylint:disable=too-many-branches
    # pylint:disable=too-many-statements

    gdal.UseExceptions()
    msg = ""
    data1 = None
    data2 = None
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
        msg += "ERROR:\tTest Ingest tile %s does not exist\n"  %path2
        dset2 = None
        data2 = None

    # Check geotransforms present
    try:
        geotransform1 = dset1.GetGeoTransform()
    except RuntimeError:
        if dset1:
            # file exists but geotransform not present
            msg += "\tError:\tGeotransform for %s not present\n" %path1
        geotransform1 = None
    try:
        geotransform2 = dset2.GetGeoTransform()
    except RuntimeError:
        if dset2:
            # file exists but geotransform not present
            msg += "\tError:\tGeotransform for %s not present\n" %path2
        geotransform2 = None

    # Check geotransforms equal
    if geotransform1 and geotransform2:
        if geotransform1 != geotransform2:
            msg += "\tError:\tGeotransforms disagree for %s and %s\n"\
                %(path1, path2)

    # Check projections present
    try:
        projection1 = dset1.GetProjection()
    except RuntimeError:
        if dset1:
            # file exists but projections not present
            msg += "\tError:\tProjection for %s not present\n" %path1
        projection1 = None
    try:
        projection2 = dset2.GetProjection()
    except RuntimeError:
        if dset2:
            # file exists but projection not present
            msg += "\tError:\tProjection for %s not present\n" %path2
        projection2 = None

    # Check projections equal
    if projection1 and projection2:
        if projection1 != projection2:
            msg += "\tError:\tProjections disagree for %s and %s\n"\
                %(path1, path2)

    # Check the dimensions of the arrays
    if dset1 and dset2:
        if data1.shape != data2.shape:
            msg += "\tError:\tDimensions of arrays disagree for %s and %s\n" \
                %(path1, path2)
    if dset1 and data1 is None:
        msg += "\tError:\tArray data for %s not present\n" %path1
    if dset2 and data2 is None:
        msg += "\tError:\tArray data for %s not present\n" %path2

    return (data1, data2, msg)


def _compare_data(level, tile_class_id1, tile_class_id2, path1, path2,
                  data1, data2):
    """Given two arrays and the level name, check that the data arrays agree.
    If the level is 'PQA' and the tile is a mosaic, then only compare mosaics
    at pixels where the contiguity bit is set in both versions of the mosaic
    tile. Returns a message in string msg which, if empty indicates agreement
    on the tile data."""
    # pylint:disable=too-many-arguments
    # pylint:disable=too-many-locals
    # pylint:disable=unused-argument

    different = False
    msg = ""
    if tile_class_id2 not in MOSAIC_CLASS_ID:
        if (data1 != data2).any():
            msg += "Difference in Tile data: %s and %s\n" \
                %(path1, path2)
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
            if not contiguity_diff.all():
                msg += "On %d pixels, mosaiced tile benchmark %s differs"\
                    "from Fresh Ingest %s\n"\
                    %(np.count_nonzero(~contiguity_diff), path1, path2)
            different = True
        else:
            diff_cmd = ["diff",
                        "-I",
                        "[Ff]ilename",
                        "%s" %path1,
                        "%s" %path2
                        ]
            result = execute(diff_cmd, shell=False)
            if result['stdout'] != '':
                msg += "Difference between mosaic vrt files:\n" + \
                    result['stdout']
                different = True
            if result['stderr'] != '':
                msg += "Error in system diff command:\n" + result['stderr']

    return (different, msg)























