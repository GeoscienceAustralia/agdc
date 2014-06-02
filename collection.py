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
        """Initialise the collection object.

        Note that tile_create_list is a list of TileContent objects,
        while tile_remove_list is a list of pathnames."""

        self.datacube = datacube
        self.db = IngestDBWrapper(datacube.db_connection)
        self.new_bands = self.__reindex_bands(datacube.bands)
        self.tile_remove_list = None
        self.tile_create_list = None
        self.in_a_transaction = False
        self.previous_commit_mode = None

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

    def begin_transaction(self):
        """Begin a transaction on the collection."""

        assert not self.in_a_transaction

        self.tile_remove_list = []
        self.tile_create_list = []
        self.previous_commit_mode = self.db.turn_off_autocommit()
        self.in_a_transaction = True

    def commit_transaction(self):
        """Commit the transaction.

        This writes tile contents to the tile store and commits
        database changes.
        """

        assert self.in_a_transaction

        for tile_pathname in self.tile_remove_list:
            if os.path.isfile(tile_pathname):
                os.remove(tile_pathname)
        self.tile_remove_list = None

        for tile_contents in self.tile_create_list:
            tile_contents.make_permanent()
        self.tile_create_list = []

        self.db.commit()

        self.db.restore_commit_mode(self.previous_commit_mode)
        self.in_a_transaction = False

    def rollback_transaction(self):
        """Rollback the transaction.

        This removes tile contents and rollsback changes to the database.
        """

        assert self.in_a_transaction

        self.tile_remove_list = None

        for tile_contents in self.tile_create_list:
            tile_contents.remove()
        self.tile_create_list = None

        self.db.rollback()

        self.db.restore_autocommit(self.previous_commit_mode)
        self.in_a_transaction = False

    def create_acquisition_record(self, dataset):
        """Factory method to create an instance of the AcquisitonRecord class.

        This method creates a corrisponding record in the database if one
        does not already exist.
        """

        assert self.in_a_transaction

        return AcquisitionRecord(self, dataset)

    def create_tile_contents(self, tile_type_id, tile_footprint,
                             band_stack):
        """Factory method to create an instance of the TileContents class.

        The tile_type_dict contains the information required for
        resampling extents and resolution."""
        tile_type_info = self.datacube.tile_type_dict[tile_type_id]
        tile_contents = TileContents(self.datacube.tile_root, tile_type_info,
                                     tile_footprint, band_stack)
        self.tile_create_list.append(tile_contents)

        return tile_contents

    def mark_tile_for_removal(self, tile_pathname):
        """Mark a tile file for removal.

        These tiles will be deleted if the transaction is commited,
        but not if it is rolled back."""

        assert self.in_a_transaction

        self.tile_remove_list.append(tile_pathname)

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

        dataset_bands = self.new_bands[self.get_dataset_key(dataset)]
        for tile_type_bands in dataset_bands.values():
            for band_info in tile_type_bands.values():
                dataset.find_band_file(band_info['file_pattern'])
