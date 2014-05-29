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

import logging
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED

import dbutil
import cube_util

# Set up logger.
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

# pylint: disable=too-many-public-methods

class IngestDBWrapper(dbutil.ConnectionWrapper):
    """IngestDBWrapper: low-level database commands for the ingest process.
    """

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

        This uses the log_multiline utility function from cube_util.
        sql_query_string is as returned from cursor.mogrify."""

        cube_util.log_multiline(LOGGER.debug, sql_query_string,
                                title='SQL', prefix='\t')

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

    def get_acquisition_id(self, acquisition_dict):
        """Finds the id of an acquisition record in the database.

        Returns an acquisition_id if a record matching the key fields in
        acquistion_dict is found, None otherwise. The key fields are:
            satellite_id, sensor_id, x_ref, y_ref, start_datetime,
            and end_datetime.
        The acquisition_dict must contain values for all of these.
        """

        sql = ("SELECT acquisition_id FROM acquisition\n" +
               "WHERE satellite_id = %(satellite_id)s AND\n" +
               "    sensor_id = %(sensor_id)s AND\n" +
               "    x_ref = %(x_ref)s AND\n" +
               "    y_ref = %(y_ref)s AND\n" +
               "    start_datetime = %(start_datetime)s AND\n" +
               "    end_datetime = %(end_datetime)s;")
        result = self.execute_sql_single(sql, acquisition_dict)
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

    def get_dataset_creation_datetime(self, dataset_id,
                                      tile_class_filter=(1,)):
        """Finds the creation date and time for a dataset.

        Returns a datetime object representing the creation time for
        the dataset, as recorded in the database. The dataset record
        is identified by dataset_id.

        The creation time is the earliest of either the datetime_processed
        field from the dataset table or the earliest tile.ctime field for
        the dataset's tiles. Tiles considered are restricted to those with
        tile_class_ids listed in tile_class_filter.
        """

        sql_dtp = ("SELECT datetime_processed FROM dataset\n" +
                   "WHERE dataset_id = %s;")
        result = self.execute_sql_single(sql_dtp, (dataset_id,))
        datetime_processed = result[0]

        sql_ctime = ("SELECT MIN(ctime) FROM tile\n" +
                     "WHERE dataset_id = %s" +
                     self.tile_class_filter_clause(tile_class_filter) + ";")
        result = self.execute_sql_single(sql_ctime, (dataset_id,))
        min_ctime = result[0]

        if min_ctime:
            creation_datetime = min(datetime_processed, min_ctime)
        else:
            creation_datetime = datetime_processed

        return creation_datetime

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
               "WHERE dataset_id = %(dataset_id)s;")
        self.execute_sql_single(sql, dataset_dict)

    def get_dataset_tile_ids(self, dataset_id, tile_class_filter=(1,)):
        """Returns a list of tile_ids associated with a dataset.

        The tile_ids returned are restricted to those with tile_class_ids
        that match the tile_class_filter."""

        sql = ("SELECT tile_id FROM tile\n" +
               "WHERE dataset_id = %s" +
               self.tile_class_filter_clause(tile_class_filter) + ";")
        result = self.execute_sql_multi(sql, (dataset_id,))
        tile_id_list = [tup[0] for tup in result]

        return tile_id_list

    @staticmethod
    def tile_class_filter_clause(tile_class_filter):
        """Returns an sql clause to filter by tile_class_ids."""

        if tile_class_filter:
            compare_list = ["    tile_class_id = %s" % tc
                            for tc in tile_class_filter]
            clause = (" AND (\n" +
                      " OR\n".join(compare_list) +
                      ")")
        else:
            clause = ""

        return clause

    def get_tile_pathname(self, tile_id):
        """Returns the pathname for a tile."""

        sql = ("SELECT pathname FROM tile\n" +
               "WHERE tile_id = %s;")
        result = self.execute_sql_single(sql, (tile_id,))
        tile_pathname = result[0]

        return tile_pathname

    def remove_tile_record(self, tile_id):
        """Removes a tile record from the database."""

        sql = "DELETE FROM tile WHERE tile_id = %s;"
        self.execute_sql_single(sql, (tile_id,))

    def get_tile_id(self, tile_dict):
        """Finds the id of a tile record in the database.

        Returns a tile_id if a record metching the key fields in
        tile_dict is found, None otherwise. The key fields are:
            dataset_id, x_index and y_index.
        The tile_dict must contain values for both of these."""

        sql = ("SELECT tile_id FROM tile\n" +
               "WHERE dataset_id = %(dataset_id)s AND\n" +
               "    x_index = %(x_index)s AND\n" +
               "    y_index = %(y_index)s;")
        result = self.execute_sql_single(sql, tile_dict)
        tile_id = result[0] if result else None
        return tile_id


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
            else:
                value_list.append("%(" + column + ")s")
        values = "(" + ",\n".join(value_list) + ")"

        sql = ("INSERT INTO tile " + columns + "\n" +
               "VALUES " + values + "\n" +
               "RETURNING tile_id;")

        result = self.execute_sql_single(sql, tile_dict)
        tile_id = result[0]
        return tile_id

    def get_overlapping_tiles(self, tile_dict):
        """Return tile records for a given footprint for the mosaicing process.

        The tile, dataset and acquisition tables are queried to determine the
        set of overlapping tiles deriving from the same satellite pass, but
        from different scenes. The returned fields should match the constant
        TILE_METADATA_FIELDS defined in tile_record.py. The returned
        records are ordered by the acquisition start_datetime."""

        sql = ("-- Find all scenes contributing data to this tile " + "\n" +
               "-- select o.*, od.*, oa.* " + "\n" +
               "select o.tile_id, o.x_index, o.y_index," + "\n" +
               "       o.tile_type_id, o.dataset_id, o.tile_pathname," + "\n" +
               "       o.tile_class_id, o.tile_size, o.ctime" + "\n" +
               "from tile t"  + "\n" +
               "inner join dataset d using(dataset_id)" + "\n" +
               "inner join acquisition a using(acquisition_id)" + "\n" +
               "inner join tile o using(x_index, y_index, tile_type_id)" +
               + "\n" +
               "inner join dataset od on"  + "\n" +
               "    od.dataset_id = o.dataset_id and" + "\n" +
               "    od.level_id = d.level_id"  + "\n" +
               "inner join acquisition oa on"  + "\n" +
               "    oa.acquisition_id = od.acquisition_id and" + "\n" +
               "    oa.satellite_id = a.satellite_id" + "\n" +
               "where" + "\n" +
               "t.tile_class_id = 1 and" + "\n" +
               "o.tile_class_id = 1 and" + "\n" +
               "t.tile_type_id = %(tile_type_id)s and" + "\n" +
               "t.x_index = %(x_index)s and" + "\n" +
               "t.y_index = %(y_index)s and" + "\n" +
               "d.dataset_id = %(dataset_id)s" + "\n" +
               "and o.tile_id <> t.tile_id" + "\n" +
               "and (oa.start_datetime between" + "\n" +
               "a.start_datetime - interval '1 hour' and" + "\n" +
               "a.end_datetime + interval '1 hour'" + "\n" +
               "or oa.end_datetime between a.start_datetime " + "\n" +
               "- interval '1 hour'" + "\n" +
               "and a.end_datetime + interval '1 hour')" + "\n" +
               "order by oa.start_datetime")
        result = self.execute_sql_multi(sql, tile_dict)
        return result


    def update_tile_records_post_mosaic(self, tile_dict_list,
                                        mosaic_final_pathname):
        """Update the database tile table to reflect the changes to the
        Tile Store resulting from the mosacing process.

        The constituent tiles in tile_dict_list have their tile_class_id
        changed from 1 to 3. Also insert a record for the mosaiced tile,
        with a tile_class_id of 4."""

        pass


#       there is already data from another existing dataset. Dictionary keys
#       are tile_id and values are corresponding rows of the tile table. One of
#       the constituent tiles will be from the current dataset_id and will thus
#       not yet be fully committed; it will be in its temporary location, to be
#        moved to its permanent location once the transaction for this dataset
#        is committed."""
