"""
Collection: database interface class.

These classes provide an interface between the database and the top-level
ingest algorithm (AbstractIngester and its subclasses). They also provide
the implementation of the database and tile store side of the ingest
process. They are expected to be independent of the structure of any
particular dataset, but will change if the database schema or tile store
format changes.
"""

import logging
import os
import time
from cube_util import DatasetError
from tile_contents import TileContents
from acquisition_record import AcquisitionRecord
from ingest_db_wrapper import IngestDBWrapper

# Set up logger.
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class Collection(object):
    """Collection database interface class."""

    #
    # Interface methods
    #

    def __init__(self, datacube):
        """Initialise the collection object."""

        self.datacube = datacube
        self.db = IngestDBWrapper(datacube.db_connection)
        self.new_bands = self.__reindex_bands(datacube.bands)
        self.current_transaction = None

    @staticmethod
    def get_dataset_key(dataset):
        """Return the dataset key for use with the new_bands dictionary.

        This is a tuple (satellite_tag, sensor_name, processing_level) except
        that for derived datasets (currently PQA and FC) the satellite_tag is
        replaced with 'DERIVED' and the processing_level is used as the
        sensor_name. So the tuple looks like:
        ('DERIVED', processing_level, processing_level).
        """

        derived_levels = {'PQA', 'FC'}

        satellite = dataset.get_satellite_tag()
        sensor = dataset.get_sensor_name()
        level = dataset.get_processing_level()

        if level in derived_levels:
            satellite = 'DERIVED'
            sensor = level

        return (satellite, sensor, level)

    def get_temp_tile_directory(self):
        """Return a path to a directory for temporary tile related files."""

        return os.path.join(self.datacube.tile_root, 'ingest_temp')

    def check_metadata(self, dataset):
        """Check that the satellite, sensor, and bands are in the database.

        Checks that the dataset is of a kind that the database knows about
        (by checking basic metadata), and the bands that the database expects
        are present. Raises a DatasetError if the checks fail.
        """

        self.__check_satellite_and_sensor(dataset)
        self.__check_processing_level(dataset)
        self.__check_bands(dataset)

    def transaction(self):
        """Returns a Transaction context manager object.

        This is for use in a 'with' statement. It uses the Collection's
        database collection.
        """

        self.current_transaction = Transaction(self.db)
        return self.current_transaction

    def lock(self, lock_list):
        """Returns a Lock context manager object.

        lock_list is a list of the objects (strings) to be locked.

        This is for use in a 'with' statement. It uses the Collection's
        datacube object to manage the individual locks.
        """

        return Lock(self.datacube, lock_list)

    def create_acquisition_record(self, dataset):
        """Factory method to create an instance of the AcquisitonRecord class.

        This method creates a corrisponding record in the database if one
        does not already exist.
        """

        return AcquisitionRecord(self, dataset)

    def create_tile_contents(self, tile_type_id, tile_footprint,
                             band_stack):
        """Factory method to create an instance of the TileContents class.

        The tile_type_dict contains the information required for
        resampling extents and resolution.
        """

        tile_type_info = self.datacube.tile_type_dict[tile_type_id]
        tile_contents = TileContents(self.datacube.tile_root, tile_type_info,
                                     tile_footprint, band_stack)
        return tile_contents

    def mark_tile_for_removal(self, tile_pathname):
        """Mark a tile file for removal on transaction commit."""

        self.current_transaction.mark_tile_for_removal(tile_pathname)

    def mark_tile_for_creation(self, tile_contents):
        """Mark a tile file for creation on transaction commit."""

        self.current_transaction.mark_tile_for_creation(tile_contents)

    #
    # worker methods
    #

    @staticmethod
    def __reindex_bands(bands):
        """Reindex the datacube.bands nested dict structure.

        This method returns the new nested dict which is indexed by:
            new_bands[dataset_key][tile_type][file_number]
        where dataset_key is a tuple:
            (satellite_tag, sensor_name, processing_level).

        The original indexing is
            bands[tile_type][satellite_sensor][file_number]
        where satellite_sensor is a tuple:
            (satellite_tag, sensor_name)

        Note that satellite_tag and sensor_name are replaced by 'DERIVED' and
        the processing_level for PQA and FC datasets. This needs to be taken
        into account when constructing a dataset_key.
        """

        new_bands = {}

        for (tile_type, band_dict) in bands.items():
            for ((satellite, sensor), sensor_dict) in band_dict.items():
                for (file_number, band_info) in sensor_dict.items():

                    dataset_key = (satellite, sensor, band_info['level_name'])

                    new_bands.setdefault(dataset_key, {})
                    new_bands[dataset_key].setdefault(tile_type, {})
                    new_bands[dataset_key][tile_type][file_number] = band_info

        return new_bands

    def __check_satellite_and_sensor(self, dataset):
        """Check that the dataset's satellite and sensor are in the database.

        Raises a DatasetError if they are not.
        """

        satellite_id = self.db.get_satellite_id(dataset.get_satellite_tag())
        if satellite_id is None:
            raise DatasetError("Unknown satellite tag: '%s'" %
                               dataset.get_satellite_tag())

        sensor_id = self.db.get_sensor_id(satellite_id,
                                          dataset.get_sensor_name())
        if sensor_id is None:
            msg = ("Unknown satellite and sensor pair: '%s', '%s'" %
                   (dataset.get_satellite_tag(), dataset.get_sensor_name()))
            raise DatasetError(msg)

    def __check_processing_level(self, dataset):
        """Check that the dataset's processing_level is in the database.

        Raises a DatasetError if it is not.
        """

        level_id = self.db.get_level_id(dataset.get_processing_level())
        if level_id is None:
            raise DatasetError("Unknown processing level: '%s'" %
                               dataset.get_processing_level())

    def __check_bands(self, dataset):
        """Check that the dataset has the expected bands.

        Raises a DatasetError if any band expected for this dataset (according
        to the database) is missing.
        """

        try:
            dataset_bands = self.new_bands[self.get_dataset_key(dataset)]
        except KeyError:
            raise DatasetError('No tile types for this dataset.')

        for tile_type_bands in dataset_bands.values():
            for band_info in tile_type_bands.values():
                dataset.find_band_file(band_info['file_pattern'])

#
# Context manager classes
#


class Transaction(object):
    """Context manager class for a transaction involving tiles.

    This is used in a 'with' statement to wrap a transaction.
    It handles the commit or roll back of the transaction and
    the associated file operations to create and remove tile files
    in coordination with the transaction.
    """

    def __init__(self, db):
        """Initialise the transaction.

        db is the database connection to use.
        tile_remove_list is the list of tile files to remove on commit.
        tile_create_list is the list of tile contents to create on commit
            (or cleanup on roll back).
        previous_commit_mode holds the previous state of the connection
            so that it can be restored when the transaction is finished.

        Note that tile_create_list is a list of TileContent objects,
        while tile_remove_list is a list of pathnames.
        """

        self.db = db
        self.tile_remove_list = None
        self.tile_create_list = None
        self.previous_commit_mode = None

    def __enter__(self):
        """Auto called on transaction (with statement) entry.

        Clears the tile lists and sets the commit mode (saving the old one).
        Returns 'self' so that the other methods are available via an
        'as' clause.
        """

        self.tile_remove_list = []
        self.tile_create_list = []
        self.previous_commit_mode = self.db.turn_off_autocommit()

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Auto called on transaction (with statement) exit.

        Commits the transaction unless there has been an exception,
        in which case it rolls back the transaction. Restores the
        old commit mode to the database connection.

        This (implicitly) returns None which causes any exception
        to be re-raised.
        """

        if exc_type is None:
            self.__commit()
        else:
            self.__rollback()

        self.tile_remove_list = None
        self.tile_create_list = None
        self.db.restore_commit_mode(self.previous_commit_mode)

    def __commit(self):
        """Commit the transaction while handling tile files."""

        # Move tile files to their final location just before
        # the commit, to avoid commiting tile records without files.
        # Tile files without records are possible if the commit fails
        # at the last moment.

        for tile_contents in self.tile_create_list:
            tile_contents.make_permanent()

        self.db.commit()

        # Remove tile files just after the commit, to avoid removing
        # tile files when the deletion of a tile record has been rolled
        # back. Again, tile files without records are possible if there
        # is an exception or crash just after the commit.
        #
        # The tile remove list is filtered against the tile create list
        # to avoid removing a file that has just been re-created. It is
        # a bad idea to overwrite a tile file in a single transaction,
        # because it will be overwritten just before the commit (above)
        # and the wrong file will be in place if the transaction is
        # rolled back.

        tile_create_set = {t.tile_output_path
                           for t in self.tile_create_list}
        for tile_pathname in self.tile_remove_list:
            if tile_pathname not in tile_create_set:
                if os.path.isfile(tile_pathname):
                    os.remove(tile_pathname)

    def __rollback(self):
        """Roll back the transaction while handling tile files."""

        # Clean up tempoary files that are now not needed.

        for tile_contents in self.tile_create_list:
            tile_contents.remove()

        self.db.rollback()

    def mark_tile_for_removal(self, tile_pathname):
        """Mark a tile file for removal on transaction commit.

        These tiles will be deleted if the transaction is commited,
        but not if it is rolled back.
        """

        self.tile_remove_list.append(tile_pathname)

    def mark_tile_for_creation(self, tile_contents):
        """Mark a tile file for creation on transaction commit.

        These tiles will be created (moved to their permenant
        location) if the transaction is commited. If the transaction
        is rolled back the associated temprorary tile files will be
        removed.

        tile_contents should be a TileContents object (or at least
        implement the interface).
        """

        self.tile_create_list.append(tile_contents)


class Lock(object):
    """Context manager class for locking a list of objects.

    This is used in a 'with' statement to wrap code which needs the
    locks. It handles acquiring and releasing the locks as well as
    waiting and retries if the locks cannot be acquired.

    Not that this will not work for nested locks/with statements in the
    same process that attempt to lock the same object, because the
    locking mechanism  does not count the number of times an object has
    been locked.
    """

    DEFAULT_WAIT = 10
    DEFAULT_RETRIES = 6

    def __init__(self,
                 datacube,
                 lock_list,
                 wait=DEFAULT_WAIT,
                 retries=DEFAULT_RETRIES):

        """Initialise the lock object.

        Positional Arguments:
            datacube: The datacube object which manages the individual locks.
            lock_list: The list of objects to lock. This is a list of
                strings - each string should unambiguously identify the object
                that is being locked.

        Keyword Arguments:
            wait: The amount of time to wait, in seconds, before again trying
                to acquire the locks.
            retries: The maximum number of attempts before giving up and
                raising an exception.
        """

        self.datacube = datacube
        # Sort the list so that locks are always acquired in the same order.
        # This avoids mini-deadlocks and resulting retries when aquiring
        # multiple locks.
        self.lock_list = sorted(lock_list)
        self.wait = wait
        self.retries = retries

    def __enter__(self):
        """Auto called on 'with' statement entry.

        This acquires the locks or raises a LockError if it cannot
        do so (after the maximum number of tries). Note that LockError
        is a subclass of DatasetError, so it will cause a dataset skip.

        Returns 'self' so that other methods are available via an 'as'
        clause, though there are no interface methods at the moment.
        """

        for dummy_tries in range(self.retries + 1):
            try:
                self.__acquire_locks(self.lock_list)
                break
            except LockError:
                time.sleep(self.wait)
        else:
            raise LockError(("Unable to lock objects after %s tries: " %
                             self.retries) +
                            self.lock_list)

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Auto called on 'with' statement exit.

        Releases the locks whether or not there has been an
        exception. Implicitly returns None which causes any
        exception to be re-raised.
        """

        for object_to_unlock in self.lock_list:
            self.datacube.unlock_object(object_to_unlock)

    def __acquire_locks(self, lock_list):
        """Acquire all the locks on the lock_list.

        Either sucessfully acquires *all* the locks or raises a
        LockError and releases all the locks obtained so far.
        """

        if lock_list:
            if self.datacube.lock_object(lock_list[0]):
                try:
                    self.__acquire_locks(lock_list[1:])
                except:
                    self.datacube.unlock_object(lock_list[0])
                    raise
            else:
                raise LockError()

#
# Exceptions
#

class LockError(DatasetError):
    """Exception class used by the Lock context manager."""
    pass
