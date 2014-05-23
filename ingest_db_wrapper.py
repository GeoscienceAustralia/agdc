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
        is identified by dataset_id."""

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
