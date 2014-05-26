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

import psycopg2

import dbutil


class IngestDBWrapper(dbutil.ConnectionWrapper):
    """IngestDBWrapper: low-level database commands for the ingest process.
    """

    def turn_off_autocommit(self):
        """Turns autocommit off for the database connection.

        Returns the old commit mode in a form suitable for passing to
        the restore_commit_mode method. Note that changeing commit mode
        must be done outside a transaction."""
        
        old_commit_mode = None
        
        return old_commit_mode

    def turn_on_autocommit(self):
        """Turns autocommit on for the database connection.

        Returns the old commit mode in a form suitable for passing to
        the restore_commit_mode method. Note that changeing commit mode
        must be done outside a transaction."""

        old_commit_mode = None
        
        return old_commit_mode

    def restore_commit_mode(self, commit_mode):
        """Restores the commit mode of the database connection.

        The commit mode passed in should have come from either
        the turn_off_autocommit or turn_on_autocommit method.
        This method will then restore the connection commit\
        mode to what is was before."""

        pass

    def get_satellite_id(self, satellite_tag):
        """Finds a satellite_id in the database.

        This method returns a satellite_id found by matching the
        satellite_tag in the database, or None if it cannot be
        found."""

        satellite_id = None

        return satellite_id
    
    def get_sensor_id(self, satellite_id, sensor_name):
        """Finds a sensor_id in the database.

        This method returns a sensor_id found by matching the
        satellite_id, sensor_name pair in the database, or None if such
        a pair cannot be found."""

        sensor_id = None

        return sensor_id
    
    def get_level_id(self, level_name):
        """Finds a (processing) level_id in the database.

        This method returns a level_id found by matching the level_name
        in the database, or None if it cannot be found."""

        level_id = None

        return level_id

    def get_acquisition_id(self, acquisition_dict):
        """Finds the id of an acquisition record in the database.

        Returns an acquisition_id if a record matching the fields in
        acquistion_dict is found, None otherwise. The record matches if the
        key fields match, acquisition_dict must contain values for all of
        these."""

        acquisition_id = None

        return acquisition_id
    
    def insert_acquisition_record(self, acquisition_dict):
        """Creates a new acquisition record in the database.

        The values of the fields in the new record are taken from
        acquisition_dict. Returns the acquisition_id of the new record."""

        acquisition_id = None

        return acquisition_id

    def update_acquisition_record(self, acquisition_id, acquisition_dict):
        """Updates an existing acquisition record in the database.

        The record to update is identified by acquisiton_id. Its non-key
        fields are updated to match the values in acquisition_dict.
        """

        pass

    def get_dataset_id(self, dataset_dict):
        """Finds the id of a dataset record in the database.
        
        Returns a dataset_id if a record metching the fields in dataset_dict
        is found, None otherwise. The record matches if the key fields
        match, dataset_dict must contain values for all of these."""

        dataset_id = None

        return dataset_id

    def get_dataset_creation_datetime(self, dataset_id):
        """Finds the creation date and time for a dataset.

        Returns a datetime object representing the creation time for
        the dataset, as recorded in the database. The dataset record
        is identified by dataset_id.
        
        The creation time is the earliest of either the datetime_processed
        field from the dataset table or the earliest tile.ctime field for
        the dataset's tiles.
        """

        creation_datetime = None

        return creation_datetime

    def insert_dataset_record(self, dataset_dict):
        """Creates a new dataset record in the database.

        The values of the fields in the new record are taken from
        dataset_dict. Returns the dataset_id of the new record."""

        dataset_id = None

        return dataset_id

    def update_dataset_record(self, dataset_id, dataset_dict):
        """Updates an existing dataset record in the database.

        The record to update is identified by dataset_id. Its non-key
        fields are updated to match the values in dataset_dict."""

        pass

    def get_dataset_tile_ids(self, dataset_id):
        """Returns a list of tile_ids associated with a dataset."""

        tile_id_list = None

        return tile_id_list

    def get_tile_pathname(self, tile_id):
        """Returns the pathname for a tile."""

        tile_pathname = None

        return tile_pathname

    def remove_tile_record(self, tile_id):
        """Removes a tile record from the database."""

        pass

    def get_overlapping_tiles(self, tile_type_id, tile_footprint, dataset_id):
        """Return tile records within the given foorprint."""



        #TODO: see if can use tile_table_column_names in select clause
        #TODO: check assumption that the tiles of this (uncommited)
        #transaction are visible

        x_index, y_index = tile_footprint
        tile_table_column_names = ['tile_id', 'x_index', 'y_index',
                                   'tile_type_id', 'dataset_id', 'tile_pathname',
                                   'tile_class_id', 'tile_size', 'ctime']
        db_cursor = self.conn.cursor()
        sql = """-- Find all scenes that might contribute data to this tile
            -- TODO: specify exact items
            -- select o.*, od.*, oa.*
            select o.tile_id, o.x_index, o.y_index,
                   o.tile_type_id, o.dataset_id, o.tile_pathname,
                   o.tile_class_id, o.tile_size, o.ctime
            from tile t
            inner join dataset d using(dataset_id)
            inner join acquisition a using(acquisition_id)
            inner join tile o using(x_index, y_index, tile_type_id)
            inner join dataset od on
                od.dataset_id = o.dataset_id and
                od.level_id = d.level_id
            inner join acquisition oa on
                oa.acquisition_id = od.acquisition_id and
                oa.satellite_id = a.satellite_id
            /*
            -- Use tile_id to specify tile
            where t.tile_id = 460497
            */

            -- Use tile spatio-temporal parameters to specify tile
            where
            t.tile_class_id = 1 and
            o.tile_class_id = 1 and
            t.tile_type_id = %(tile_type_id)s and
            t.x_index = %(x_index)s and
            t.y_index = %(y_index)s and
            -- MPH comment out following two conditions:
            -- a.start_datetime = %(start_datetime)s and
            -- d.level_id = %(level_id)s
            -- and replace with: (possible since there is a one-to-one 
            -- (corrspondence between (acquisiton, level_id) and dataset
            d.dataset_id = %(dataset_id)s
            and o.tile_id <> t.tile_id -- Uncomment this to exclude original
            -- TODO check that tile created for current dataset is present
            -- i.e. is it visible if it is part of an uncommited transaction
            -- Use temporal extents to find overlaps
            and (oa.start_datetime between
            a.start_datetime - interval '1 hour' and
            a.end_datetime + interval '1 hour'
            or oa.end_datetime between a.start_datetime - interval '1 hour'  
            and a.end_datetime + interval '1 hour')
            order by oa.start_datetime
        """

        params = {'tile_type_id': tile_type_id,
                  'x_index': x_index,
                  'y_index': y_index,
                  'dataset_id': dataset_id}
        db_cursor.execute(sql, params)
        #initialise list of tiles to be mosaiced
        tile_list = []
        for record in db_cursor:
            #Form a list of dictionaries so that we can keep the datetime order
            if record[4] == self.dataset_record.dataset_id:
                #For the constituent tile deriving from the curent dataset id,
                #will need to change its location in tile_list from the value
                #stored in the database to the value stored in the
                #tile_contents object.
                tile_dir, tile_basename = os.path.split(record[5])
                tile_dir = os.path.dirname(self.tile_contents.bandstack.vrt_name)
                record[5] = os.path.join(tile_dir, tile_basename)
            tile_list.append(dict(zip(tile_table_column_names, record)))
        db_cursor.close()
        return tile_list



        there is already data from another existing dataset. Dictionary keys
        are tile_id and values are corresponding rows of the tile table. One of
        the constituent tiles will be from the current dataset_id and will thus
        not yet be fully committed; it will be in its temporary location, to be
        moved to its permanent location once the transaction for this dataset
        is committed."""
