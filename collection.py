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
from tile_contents import TileContents
from acquisition_record import AcquisitionRecord
from dbutil import ConnectionWrapper

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
        self.db = CollectionDBWrapper(datacube.db_connection)
        self.new_bands = self.__reindex_bands(datacube.bands)
        self.tile_list = None
        self.in_a_transaction = False

    def check_metadata(self, dataset):
        pass

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

class CollectionDBWrapper(ConnectionWrapper):
    pass

