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
from abstract_ingester import DatasetError
from tile_contents import TileContents
from acquisition_record import AcquisitionRecord
from ingest_db_wrapper import IngestDBWrapper

# Set up logger.
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class Collection(object):
    """Collection database interface class."""

    #
    # Constants
    #

    ACQUISITION_METADATA_FIELDS = ['satellite_tag',
                                   'sensor_name',
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
                                   'ur_lat',
                                   'gcp_count',
                                   'mtl_text',
                                   'cloud_cover'
                                   ]

    #
    # Interface methods
    #

    def __init__(self, datacube):

        self.datacube = datacube
        self.db = IngestDBWrapper(datacube.db_connection)
        self.new_bands = self.__reindex_bands(datacube.bands)
        self.tile_list = None
        self.in_a_transaction = False

    def check_metadata(self, dataset):
        """Check that the satellite, sensor, and bands are in the database.

        Checks that the dataset is of a kind that the database knows about
        (by checking basic metadata), and the bands that the database expects
        are present.
        """

        self.__check_satellite_and_sensor(dataset)
        self.__check_processing_level(dataset)
        self.__check_bands(dataset)

    def begin_transaction(self):
        """Begin a transaction on the collection."""

        assert not self.in_a_transaction

        self.tile_list = []
        self.db.turn_off_autocommit()
        self.in_a_transaction = True

    def commit_transaction(self):
        """Commit the transaction.

        This writes tile contents to the tile store and commits
        database changes.
        """

        assert self.in_a_transaction

        for tile_contents in self.tile_list:
            tile_contents.make_permanent()

        self.db.commit()
        self.db.restore_autocommit()
        self.in_a_transaction = False

    def rollback_transaction(self):
        """Rollback the transaction.

        This removes tile contents and rollsback changes to the database.
        """

        assert self.in_a_transaction

        for tile_contents in self.tile_list:
            tile_contents.remove()

        self.db.rollback()
        self.db.restore_autocommit()
        self.in_a_transaction = False

    def create_acquisition_record(self, dataset):
        """Factory method to create an instance of the AcquisitonRecord class.

        This method creates a corrisponding record in the database if one
        does not already exist.
        """

        assert self.in_a_transaction

        # Fill a dictonary with data for the acquisition.
        # Start with fields from the dataset metadata.
        acquisition_dict = {}
        for field in self.ACQUISITION_METADATA_FIELDS:
            acquisition_dict[field] = dataset.metadata_dict[field]

        # Next look up the satellite_id and sensor_id in the
        # database and fill these in.
        acquisition_dict['satellite_id'] = \
            self.db.get_satellite_id(acquisition_dict['satellite_tag'])
        acquisition_dict['sensor_id'] = \
            self.db.get_sensor_id(acquisition_dict['satellite_id'],
                                  acquisition_dict['sensor_name'])

        # Finally look up the acquisiton_id, or create a new record if it
        # does not exist, and fill it into the dictionary.
        acquisition_id = self.db.get_acquisition_id(acquisition_dict)
        if acquisition_id is None:
            acquisition_id = \
                self.db.insert_acquisition_record(acquisition_dict)
        acquisition_dict['acquisition_id'] = acquisition_id

        return AcquisitionRecord(self.datacube, self.new_bands,
                                 acquisition_dict)

    def create_tile_contents(self, tile_type_info, tile_footprint,
                             band_stack):
        """Factory method to create an instance of the TileContents class.
        
        The tile_type_dict contains the information required for
        resampling extents and resolution."""

        tile_contents = TileContents(self.datacube.tile_root, tile_type_info,
                                     tile_footprint, band_stack)
        self.tile_list.append(tile_contents)

        return tile_contents

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

    def __get_satellite_and_sensor_id(self, dataset):
        """Look up the satellite_id and sensor_id in the database.

        These are returned as a tuple (satellite_id, sensor_id). They are
        obtianed from the database by matching to the satellite_tag and
        sensor_name from the dataset metadata. They are set to None if
        they cannot be found.
        """

        satellite_id = self.db.get_satellite_id(dataset.get_satellite_tag())

        if satellite_id is not None:
            sensor_id = self.db.get_sensor_id(satellite_id,
                                              dataset.get_sensor_name())
        else:
            sensor_id = None

        return satellite_id, sensor_id

    def __check_satellite_and_sensor(self, dataset):
        """Check that the dataset's satellite and sensor are in the database.

        Raises a DatasetError if they are not.
        """

        (satellite_id, sensor_id) = self.__get_satellite_and_sensor(dataset)

        if satellite_id is None:
            raise DatasetError("Unknown satellite tag: '%s'" %
                                dataset.get_satellite_tag())

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
        
        dataset_key = (dataset.get_satellite_tag(),
                       dataset.get_sensor_name(),
                       dataset.get_processing_level())
        
        dataset_bands = self.new_bands[dataset_key]
        for (tile_type, tile_type_bands) in dataset_bands.items():
            for (file_number, band_info) in tile_type_bands.items():
                if not dataset.band_exists(file_number,
                                           band_info['file_pattern']):
                    msg = (("Unable to verify  the existance of a band: " +
                            "tile_type = %s, " +
                            "file_number = %s, " +
                            "file_pattern = '%s'") %
                            (tile_type, file_number, band_info['file_pattern']))
                    raise DatasetError(msg)

