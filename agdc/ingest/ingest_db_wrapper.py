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
IngestDBWrapper: provides low-level database commands for the ingest process.

This class (based on ConnectionWrapper) provides low-level database
commands used by the ingest process. This is where the SQL queries go.

The methods in this class should be context free, so all context information
should be passed in as parameters and passed out as return values. To put
it another way, the database connection should be the *only* data attribute.

If you feel you need to cache the result of database queries or track context,
please do it in the calling class, not here. This is intended as a very clean
and simple interface to the database, to replace big chunks of SQL with
meaningfully named method calls.
"""
from __future__ import absolute_import
from __future__ import division

import logging
import datetime

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED
import pytz

import agdc.dbutil as dbutil
from eotools.utils import log_multiline


# Set up logger.
LOGGER = logging.getLogger(__name__)

#
# Module level constants
#

ONE_HOUR = datetime.timedelta(0, 3600)

#
# Symbolic names for tile classes
#

TC_PENDING = 0
TC_SINGLE_SCENE = 1
TC_DELETED = 2
TC_SUPERSEDED = 3
TC_MOSAIC = 4

# pylint: disable=too-many-public-methods


class IngestDBWrapper(dbutil.ConnectionWrapper):
    """IngestDBWrapper: low-level database commands for the ingest process.
    """

    #
    # Constants
    #

    # This is the +- percentage to match within for fuzzy datetime matches.
    FUZZY_MATCH_PERCENTAGE = 15

    #
    # Utility Functions
    #

    def execute_sql_single(self, sql, params):
        """Executes an sql query returning (at most) a single row.

        This creates a cursor, executes the sql query or command specified
        by the operation string 'sql' and parameters 'params', and returns
        the first row of the result, or None if there is no result."""
        with self.conn.cursor() as cur:
            self.log_sql(cur.mogrify(sql, params))
            cur.execute(sql, params)
            result = cur.fetchone()

        return result

    def execute_sql_multi(self, sql, params):
        """Executes an sql query returning multiple rows.

        This creates a cursor, executes the sql query or command specified
        by the operation string 'sql' and parameters 'params', and returns
        a list of results, or an empty list if there are no results."""
        with self.conn.cursor() as cur:
            self.log_sql(cur.mogrify(sql, params))
            cur.execute(sql, params)
            result = cur.fetchall()

        return result

    @staticmethod
    def log_sql(sql_query_string):
        """Logs an sql query to the logger at debug level.

        This uses the log_multiline utility function from eotools.utils.
        sql_query_string is as returned from cursor.mogrify."""

        log_multiline(LOGGER.debug, sql_query_string,
                                title='SQL', prefix='\t')

    #
    # Queries and Commands
    #

    def turn_off_autocommit(self):
        """Turns autocommit off for the database connection.

        Returns the old commit mode in a form suitable for passing to
        the restore_commit_mode method. Note that changeing commit mode
        must be done outside a transaction."""

        old_commit_mode = (self.conn.autocommit, self.conn.isolation_level)

        self.conn.autocommit = False
        self.conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)

        return old_commit_mode

    def turn_on_autocommit(self):
        """Turns autocommit on for the database connection.

        Returns the old commit mode in a form suitable for passing to
        the restore_commit_mode method. Note that changeing commit mode
        must be done outside a transaction."""

        old_commit_mode = (self.conn.autocommit, self.conn.isolation_level)

        self.conn.autocommit = True
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        return old_commit_mode

    def restore_commit_mode(self, commit_mode):
        """Restores the commit mode of the database connection.

        The commit mode passed in should have come from either
        the turn_off_autocommit or turn_on_autocommit method.
        This method will then restore the connection commit\
        mode to what is was before."""

        (autocommit, isolation_level) = commit_mode

        self.conn.autocommit = autocommit
        self.conn.set_isolation_level(isolation_level)

    def get_satellite_id(self, satellite_tag):
        """Finds a satellite_id in the database.

        This method returns a satellite_id found by matching the
        satellite_tag in the database, or None if it cannot be
        found."""

        sql = ("SELECT satellite_id FROM satellite\n" +
               "WHERE satellite_tag = %s;")
        params = (satellite_tag,)
        result = self.execute_sql_single(sql, params)
        satellite_id = result[0] if result else None

        return satellite_id

    def get_sensor_id(self, satellite_id, sensor_name):
        """Finds a sensor_id in the database.

        This method returns a sensor_id found by matching the
        satellite_id, sensor_name pair in the database, or None if such
        a pair cannot be found."""

        sql = ("SELECT sensor_id FROM sensor\n" +
               "WHERE satellite_id = %s AND\n" +
               "    sensor_name = %s;")
        params = (satellite_id, sensor_name)
        result = self.execute_sql_single(sql, params)
        sensor_id = result[0] if result else None

        return sensor_id

    def get_level_id(self, level_name):
        """Finds a (processing) level_id in the database.

        This method returns a level_id found by matching the level_name
        in the database, or None if it cannot be found."""

        sql = ("SELECT level_id FROM processing_level\n" +
               "WHERE level_name = %s;")
        params = (level_name,)
        result = self.execute_sql_single(sql, params)
        level_id = result[0] if result else None

        return level_id

    def get_acquisition_id_exact(self, acquisition_dict):
        """Finds the id of an acquisition record in the database.

        Returns an acquisition_id if a record matching the key fields in
        acquistion_dict is found, None otherwise. The key fields are:
            satellite_id, sensor_id, x_ref, y_ref, start_datetime,
            and end_datetime.
        The acquisition_dict must contain values for all of these.

        This query requires an exact match for the start and end datetimes.
        """

        sql = ("SELECT acquisition_id FROM acquisition\n" +
               "WHERE satellite_id = %(satellite_id)s AND\n" +
               "    sensor_id = %(sensor_id)s AND\n" +
               ("    x_ref = %(x_ref)s AND\n" if acquisition_dict['x_ref'] is not None else "    x_ref is null AND\n") +
               ("    y_ref = %(y_ref)s AND\n" if acquisition_dict['y_ref'] is not None else "    y_ref is null AND\n") +
               "    start_datetime = %(start_datetime)s AND\n" +
               "    end_datetime = %(end_datetime)s;")
        result = self.execute_sql_single(sql, acquisition_dict)
        acquisition_id = result[0] if result else None

        return acquisition_id

    def get_acquisition_id_fuzzy(self, acquisition_dict):
        """Finds the id of an acquisition record in the database.

        Returns an acquisition_id if a record matching the key fields in
        acquistion_dict is found, None otherwise. The key fields are:
            satellite_id, sensor_id, x_ref, y_ref, start_datetime,
            and end_datetime.
        The acquisition_dict must contain values for all of these.

        This query uses an approximate match for the start and end datetimes.
        """

        aq_length = (acquisition_dict['end_datetime'] -
                     acquisition_dict['start_datetime'])
        delta = (aq_length * self.FUZZY_MATCH_PERCENTAGE) // 100
        params = dict(acquisition_dict)
        params['delta'] = delta

        sql = ("SELECT acquisition_id FROM acquisition\n" +
               "WHERE satellite_id = %(satellite_id)s AND\n" +
               "    sensor_id = %(sensor_id)s AND\n" +
               ("    x_ref = %(x_ref)s AND\n" if params['x_ref'] is not None else "    x_ref is null AND\n") +
               ("    y_ref = %(y_ref)s AND\n" if params['y_ref'] is not None else "    y_ref is null AND\n") +
               "    start_datetime BETWEEN\n" +
               "        %(start_datetime)s - %(delta)s AND\n" +
               "        %(start_datetime)s + %(delta)s AND\n" +
               "    end_datetime BETWEEN\n" +
               "        %(end_datetime)s - %(delta)s AND\n" +
               "        %(end_datetime)s + %(delta)s;")
        result = self.execute_sql_single(sql, params)
        acquisition_id = result[0] if result else None

        return acquisition_id

    def insert_acquisition_record(self, acquisition_dict):
        """Creates a new acquisition record in the database.

        The values of the fields in the new record are taken from
        acquisition_dict. Returns the acquisition_id of the new record."""

        # Columns to be inserted. If gcp_count or mtl_text are empty, we
        # exclude them from the list, so they pick up the defaults instead.
        column_list = ['acquisition_id',
                       'satellite_id',
                       'sensor_id',
                       'x_ref',
                       'y_ref',
                       'start_datetime',
                       'end_datetime',
                       'll_lon',
                       'll_lat',
                       'lr_lon',
                       'lr_lat',
                       'ul_lon',
                       'ul_lat',
                       'ur_lon',
                       'ur_lat'
                       ]
        if acquisition_dict['gcp_count'] is not None:
            column_list.append('gcp_count')
        if acquisition_dict['mtl_text'] is not None:
            column_list.append('mtl_text')
        columns = "(" + ",\n".join(column_list) + ")"

        # Values are taken from the acquisition_dict, with keys the same
        # as the column name, except for acquisition_id, which is the next
        # value in the acquisition_id_seq sequence.
        value_list = []
        for column in column_list:
            if column == 'acquisition_id':
                value_list.append("nextval('acquisition_id_seq')")
            else:
                value_list.append("%(" + column + ")s")
        values = "(" + ",\n".join(value_list) + ")"

        sql = ("INSERT INTO acquisition " + columns + "\n" +
               "VALUES " + values + "\n" +
               "RETURNING acquisition_id;")

        result = self.execute_sql_single(sql, acquisition_dict)
        acquisition_id = result[0]

        return acquisition_id

    def get_dataset_id(self, dataset_dict):
        """Finds the id of a dataset record in the database.

        Returns a dataset_id if a record metching the key fields in
        dataset_dict is found, None otherwise. The key fields are:
            aquisition_id and level_id.
        The dataset_dict must contain values for both of these."""

        sql = ("SELECT dataset_id FROM dataset\n" +
               "WHERE acquisition_id = %(acquisition_id)s AND\n" +
               "    level_id = %(level_id)s;")
        result = self.execute_sql_single(sql, dataset_dict)
        dataset_id = result[0] if result else None

        return dataset_id

    def dataset_older_than_database(self, dataset_id,
                                    disk_datetime_processed,
                                    tile_class_filter=None):
        """Compares the datetime_processed of the dataset on disk with that on
        the database. The database time is the earliest of either the
        datetime_processed field from the dataset table or the earliest
        tile.ctime field for the dataset's tiles. Tiles considered are
        restricted to those with tile_class_ids listed in tile_class_filter
        if it is non-empty.
        
        Returns tuple 
        (disk_datetime_processed, database_datetime_processed, tile_ingested_datetime) 
        if no ingestion required 
        or None if ingestion is required
        """
        sql_dtp = ("SELECT datetime_processed FROM dataset\n" +
                   "WHERE dataset_id = %s;")
        result = self.execute_sql_single(sql_dtp, (dataset_id,))
        database_datetime_processed = result[0]

        if database_datetime_processed < disk_datetime_processed:
            return None

        # The database's dataset record is newer that what is on disk.
        # Consider whether the tile record's are older than dataset on disk.
        # Make the dataset's datetime_processed timezone-aware.
        utc = pytz.timezone("UTC")
        disk_datetime_processed = utc.localize(disk_datetime_processed)

        sql_ctime = ("SELECT MIN(ctime) FROM tile\n" +
                     "WHERE dataset_id = %(dataset_id)s\n" +
                     ("AND tile_class_id IN %(tile_class_filter)s\n" if
                      tile_class_filter else "") +
                     ";"
                     )
        params = {'dataset_id': dataset_id,
                  'tile_class_filter': tuple(tile_class_filter)
                  }
        result = self.execute_sql_single(sql_ctime, params)
        min_ctime = result[0]

        if min_ctime is None:
            return None

        if min_ctime < disk_datetime_processed:
            return None

        # The dataset on disk is more recent than the database records and
        # should be re-ingested. Return tuple containing relevant times
        return (disk_datetime_processed, utc.localize(database_datetime_processed), min_ctime)

    def insert_dataset_record(self, dataset_dict):
        """Creates a new dataset record in the database.

        The values of the fields in the new record are taken from
        dataset_dict. Returns the dataset_id of the new record."""

        # Columns to be inserted.
        column_list = ['dataset_id',
                       'acquisition_id',
                       'dataset_path',
                       'level_id',
                       'datetime_processed',
                       'dataset_size',
                       'crs',
                       'll_x',
                       'll_y',
                       'lr_x',
                       'lr_y',
                       'ul_x',
                       'ul_y',
                       'ur_x',
                       'ur_y',
                       'x_pixels',
                       'y_pixels',
                       'xml_text']
        columns = "(" + ",\n".join(column_list) + ")"

        # Values are taken from the dataset_dict, with keys the same
        # as the column name, except for dataset_id, which is the next
        # value in the dataset_id_seq sequence.
        value_list = []
        for column in column_list:
            if column == 'dataset_id':
                value_list.append("nextval('dataset_id_seq')")
            else:
                value_list.append("%(" + column + ")s")
        values = "(" + ",\n".join(value_list) + ")"

        sql = ("INSERT INTO dataset " + columns + "\n" +
               "VALUES " + values + "\n" +
               "RETURNING dataset_id;")

        result = self.execute_sql_single(sql, dataset_dict)
        dataset_id = result[0]

        return dataset_id

    def update_dataset_record(self, dataset_dict):
        """Updates an existing dataset record in the database.

        The record to update is identified by dataset_id, which must be
        present in dataset_dict. Its non-key fields are updated to match
        the values in dataset_dict.
        """

        # Columns to be updated
        column_list = ['dataset_path',
                       'datetime_processed',
                       'dataset_size',
                       'crs',
                       'll_x',
                       'll_y',
                       'lr_x',
                       'lr_y',
                       'ul_x',
                       'ul_y',
                       'ur_x',
                       'ur_y',
                       'x_pixels',
                       'y_pixels',
                       'xml_text']
        assign_list = [(col + " = %(" + col + ")s") for col in column_list]
        assignments = ",\n".join(assign_list)

        sql = ("UPDATE dataset\n" +
               "SET " + assignments + "\n" +
               "WHERE dataset_id = %(dataset_id)s" + "\n" +
               "RETURNING dataset_id;")
        self.execute_sql_single(sql, dataset_dict)

    def get_dataset_tile_ids(self, dataset_id, tile_class_filter=()):
        """Returns a list of tile_ids associated with a dataset.

        If tile_class_filter is not an empty tuple then the tile_ids returned are
        restricted to those with tile_class_ids that that match the
        tile_class_filter. Otherwise all tile_ids for the dataset are
        returned."""

        sql = ("SELECT tile_id FROM tile\n" +
               "WHERE dataset_id = %(dataset_id)s\n" +
               ("AND tile_class_id IN %(tile_class_filter)s\n" if
                tile_class_filter else "") +
               "ORDER By tile_id;"
               )
        params = {'dataset_id': dataset_id,
                  'tile_class_filter': tuple(tile_class_filter)
                  }
        result = self.execute_sql_multi(sql, params)
        tile_id_list = [tup[0] for tup in result]

        return tile_id_list

    def get_tile_pathname(self, tile_id):
        """Returns the pathname for a tile."""

        sql = ("SELECT tile_pathname FROM tile\n" +
               "WHERE tile_id = %s;")
        result = self.execute_sql_single(sql, (tile_id,))
        tile_pathname = result[0]

        return tile_pathname

    def remove_tile_record(self, tile_id):
        """Removes a tile record from the database."""

        sql = "DELETE FROM tile WHERE tile_id = %s RETURNING tile_id;"
        self.execute_sql_single(sql, (tile_id,))

    def get_tile_id(self, tile_dict):
        """Finds the id of a tile record in the database.

        Returns a tile_id if a record metching the key fields in
        tile_dict is found, None otherwise. The key fields are:
        dataset_id, x_index, y_index, and tile_type_id.
        The tile_dict must contain values for all of these."""

        sql = ("SELECT tile_id FROM tile\n" +
               "WHERE dataset_id = %(dataset_id)s AND\n" +
               "    x_index = %(x_index)s AND\n" +
               "    y_index = %(y_index)s AND\n" +
               "    tile_type_id = %(tile_type_id)s;")
        result = self.execute_sql_single(sql, tile_dict)
        tile_id = result[0] if result else None
        return tile_id

    def tile_footprint_exists(self, tile_dict):
        """Check the tile footprint table for an existing entry.

        The table is checked for existing entry with combination
        (x_index, y_index, tile_type_id). Returns True if such an entry
        exists and False otherwise.
        """

        sql = ("SELECT 1 FROM tile_footprint\n" +
               "WHERE x_index = %(x_index)s AND\n" +
               "      y_index = %(y_index)s AND\n" +
               "      tile_type_id = %(tile_type_id)s;")
        result = self.execute_sql_single(sql, tile_dict)
        footprint_exists = True if result else False
        return footprint_exists

    def insert_tile_footprint(self, footprint_dict):
        """Inserts an entry into the tile_footprint table of the database.

        TODO: describe how bbox generated.
        """
        # TODO Use Alex's code in email to generate bbox
        # Columns to be updated
        column_list = ['x_index',
                       'y_index',
                       'tile_type_id',
                       'x_min',
                       'y_min',
                       'x_max',
                       'y_max',
                       'bbox']

        columns = "(" + ",\n".join(column_list) + ")"

        value_list = []
        for column in column_list:
            if column == 'bbox':
                value_list.append('NULL')
            else:
                value_list.append("%(" + column + ")s")
        values = "(" + ",\n".join(value_list) + ")"

        sql = ("INSERT INTO tile_footprint " + columns + "\n" +
               "VALUES " + values + "\n" +
               "RETURNING x_index;")
        self.execute_sql_single(sql, footprint_dict)

    def insert_tile_record(self, tile_dict):
        """Creates a new tile record in the database.

        The values of the fields in the new record are taken from
        tile_dict. Returns the tile_id of the new record."""

        column_list = ['tile_id',
                       'x_index',
                       'y_index',
                       'tile_type_id',
                       'dataset_id',
                       'tile_pathname',
                       'tile_class_id',
                       'tile_size',
                       'ctime']
        columns = "(" + ",\n".join(column_list) + ")"

        # Values are taken from the tile_dict, with keys the same
        # as the column name, except for tile_id, which is the next
        # value in the dataset_id_seq sequence.
        value_list = []
        for column in column_list:
            if column == 'tile_id':
                value_list.append("nextval('tile_id_seq')")
            elif column == 'ctime':
                value_list.append('now()')
            else:
                value_list.append("%(" + column + ")s")
        values = "(" + ",\n".join(value_list) + ")"

        sql = ("INSERT INTO tile " + columns + "\n" +
               "VALUES " + values + "\n" +
               "RETURNING tile_id;")

        result = self.execute_sql_single(sql, tile_dict)
        tile_id = result[0]
        return tile_id

    def get_overlapping_dataset_ids(self,
                                    dataset_id,
                                    delta_t=ONE_HOUR,
                                    tile_class_filter=(1, 3)):
        """Return dataset ids for overlapping datasets (incuding this dataset)

        Given an original dataset specified by 'dataset_id', return the list
        of dataset_ids for datasets that overlap this one. An overlap occurs
        when a tile belonging to a target dataset overlaps in space and
        time with one from the orignal dataset. 'delta_t' sets the tolerance
        for detecting time overlaps. It should be a python datetime.timedelta
        object (obtainable by constructor or by subtracting two datetimes).

        Only tiles of a class present in the tuple 'tile_class_filter' are
        considered. Note that if the original dataset has no tiles of the
        relevent types an empty list will be returned. Otherwise the list
        will contain at least the original dataset id.
        """

        sql = ("SELECT DISTINCT od.dataset_id\n" +
               "FROM dataset d\n" +
               "INNER JOIN tile t USING (dataset_id)\n" +
               "INNER JOIN acquisition a USING (acquisition_id)\n" +
               "INNER JOIN tile o ON\n" +
               "    o.x_index = t.x_index AND\n" +
               "    o.y_index = t.y_index AND\n" +
               "    o.tile_type_id = t.tile_type_id\n" +
               "INNER JOIN dataset od ON\n" +
               "    od.dataset_id = o.dataset_id AND\n" +
               "    od.level_id = d.level_id\n" +
               "INNER JOIN acquisition oa ON\n" +
               "    oa.acquisition_id = od.acquisition_id AND\n" +
               "    oa.satellite_id = a.satellite_id\n" +
               "WHERE\n" +
               "    d.dataset_id = %(dataset_id)s\n" +
               ("    AND t.tile_class_id IN %(tile_class_filter)s\n" if
                tile_class_filter else "") +
               ("    AND o.tile_class_id IN %(tile_class_filter)s\n" if
                tile_class_filter else "") +
               "    AND (\n" +
               "        (oa.start_datetime BETWEEN\n" +
               "         a.start_datetime - %(delta_t)s AND\n" +
               "         a.end_datetime + %(delta_t)s)\n" +
               "     OR\n" +
               "        (oa.end_datetime BETWEEN\n" +
               "         a.start_datetime - %(delta_t)s AND\n" +
               "         a.end_datetime + %(delta_t)s)\n" +
               "    )\n" +
               "ORDER BY od.dataset_id;")

        params = {'dataset_id': dataset_id,
                  'delta_t': delta_t,
                  'tile_class_filter': tuple(tile_class_filter)
                  }
        result = self.execute_sql_multi(sql, params)
        dataset_id_list = [tup[0] for tup in result]
        return dataset_id_list

    def get_overlapping_tiles_for_dataset(self,
                                          dataset_id,
                                          delta_t=ONE_HOUR,
                                          input_tile_class_filter=None,
                                          output_tile_class_filter=None,
                                          dataset_filter=None):
        """Return a nested dictonary for the tiles overlapping a dataset.

        The top level dictonary is keyed by tile footprint (x_index, y_index,
        tile_type_id). Each entry is a list of tile records. Each tile record
        is a dictonary with entries for tile_id, dataset_id, tile_class,
        tile_pathname, and ctime.

        Arguments:
            dataset_id: id of the dataset to act as the base for the query.
                The input tiles are the ones associated with this dataset.
            delta_t: The tolerance used to detect overlaps in time. This
                should be a python timedelta object (from the datatime module).
            input_tile_class_filter: A tuple of tile_class_ids to restrict
                the input tiles. If non-empty, input tiles not matching these
                will be ignored.
            output_tile_class_filter: A tuple of tile_class_ids to restrict
                the output tiles. If non-empty, output tiles not matching these
                will be ignored.
            dataset_filter: A tuple of dataset_ids to restrict the datasets
                that the output tiles belong to. If non-empty, output tiles
                not from these datasets will be ignored. Used to avoid
                operating on tiles belonging to non-locked datasets.
        """

        sql = ("SELECT DISTINCT o.tile_id, o.x_index, o.y_index,\n" +
               "    o.tile_type_id, o.dataset_id, o.tile_pathname,\n" +
               "    o.tile_class_id, o.tile_size, o.ctime,\n" +
               "    oa.start_datetime\n" +
               "FROM tile t\n" +
               "INNER JOIN dataset d USING (dataset_id)\n" +
               "INNER JOIN acquisition a USING (acquisition_id)\n" +
               "INNER JOIN tile o ON\n" +
               "    o.x_index = t.x_index AND\n" +
               "    o.y_index = t.y_index AND\n" +
               "    o.tile_type_id = t.tile_type_id\n" +
               "INNER JOIN dataset od ON\n" +
               "    od.dataset_id = o.dataset_id AND\n" +
               "    od.level_id = d.level_id\n" +
               "INNER JOIN acquisition oa ON\n" +
               "    oa.acquisition_id = od.acquisition_id AND\n" +
               "    oa.satellite_id = a.satellite_id\n" +
               "WHERE\n" +
               "    d.dataset_id = %(dataset_id)s\n" +
               ("    AND od.dataset_id IN %(dataset_filter)s\n" if
                dataset_filter else "") +
               ("    AND t.tile_class_id IN %(input_tile_class_filter)s\n" if
                input_tile_class_filter else "") +
               ("    AND o.tile_class_id IN %(output_tile_class_filter)s\n" if
                output_tile_class_filter else "") +
               "    AND (\n" +
               "        (oa.start_datetime BETWEEN\n" +
               "         a.start_datetime - %(delta_t)s AND\n" +
               "         a.end_datetime + %(delta_t)s)\n" +
               "     OR\n" +
               "        (oa.end_datetime BETWEEN\n" +
               "         a.start_datetime - %(delta_t)s AND\n" +
               "         a.end_datetime + %(delta_t)s)\n" +
               "    )\n" +
               "ORDER BY oa.start_datetime;"
               )
        params = {'dataset_id': dataset_id,
                  'delta_t': delta_t,
                  'input_tile_class_filter': tuple(input_tile_class_filter),
                  'output_tile_class_filter': tuple(output_tile_class_filter),
                  'dataset_filter': tuple(dataset_filter)
                  }
        result = self.execute_sql_multi(sql, params)

        overlap_dict = {}
        for record in result:
            tile_footprint = tuple(record[1:4])
            tile_record = {'tile_id': record[0],
                           'x_index': record[1],
                           'y_index': record[2],
                           'tile_type_id': record[3],
                           'dataset_id': record[4],
                           'tile_pathname': record[5],
                           'tile_class_id': record[6],
                           'tile_size': record[7],
                           'ctime': record[8]
                           }
            if tile_footprint not in overlap_dict:
                overlap_dict[tile_footprint] = []
            overlap_dict[tile_footprint].append(tile_record)

        return overlap_dict

    def update_tile_class(self, tile_id, new_tile_class_id):
        """Update the tile_class_id of a tile to a new value."""

        sql = ("UPDATE tile\n" +
               "SET tile_class_id = %(new_tile_class_id)s\n" +
               "WHERE tile_id = %(tile_id)s\n" +
               "RETURNING tile_id;"
               )
        params = {'tile_id': tile_id,
                  'new_tile_class_id': new_tile_class_id
                  }
        self.execute_sql_single(sql, params)
