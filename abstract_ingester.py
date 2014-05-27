"""
    abstract_ingester.py - top level ingestion algorithm.
"""

import os
import logging
import argparse
from abc import ABCMeta, abstractmethod

from cube_util import DatasetError
from datacube import DataCube
from collection import Collection
from cube_util import DatasetError

#
# Set up logger.
#

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
# Classes
#


class IngesterDataCube(DataCube):
    """Datacube subclass which accepts command line arguments
    as a Namespace passed into the constructer."""

    def __init__(self, args):
        self.my_args = args
        DataCube.__init__(self)

    def parse_args(self):
        return self.my_args


class AbstractIngester(object):
    """
    Partially abstract base class for ingester objects. Needs to
    be subclassed and have dataset type and format specific functions
    defined before use.
    """

    #
    # Declare this as an abstract base class, allowing the use of the
    # abstractmethod decorator.
    #
    __metaclass__ = ABCMeta

    def __init__(self, datacube=None, collection=None):
        """Set up the ingester object.

        datacube: A datacube instance (which has a database connection and
            tile_type and band dictionaries). If this is None the Ingeseter
            will create its own datacube instance using the arguments
            returned by self.parse_args().

        collection: The datacube collection which will accept the ingest.
            if this is None the Ingeseter will set up its own collection
            using self.datacube.
        """

        self.args = self.parse_args()

        if self.args.debug:
            # Set DEBUG level on the root logger
            logging.getLogger().setLevel(logging.DEBUG)

        if datacube is None:
            self.datacube = IngesterDataCube(self.args)
        else:
            self.datacube = datacube

        if collection is None:
            self.collection = Collection(self.datacube)
        else:
            self.collection = collection

    #
    # parse_args method for command line arguments. This should be
    # overridden if extra arguments (beyond --config and --debug)
    # are needed.
    #

    @staticmethod
    def parse_args():
        """Virtual function to parse command line arguments.

        Returns:
            argparse namespace object
        """
        LOGGER.debug('  Calling parse_args()')

        _arg_parser = argparse.ArgumentParser()

        default_config = os.path.join(os.path.dirname(__file__),
                                      'datacube.conf')
        _arg_parser.add_argument('-C', '--config', dest='config_file',
                                 default=default_config,
                                 help='DataCube configuration file')
        _arg_parser.add_argument('-d', '--debug', dest='debug',
                                 default=False, action='store_const',
                                 const=True,
                                 help='Debug mode flag')

        args, dummy_unknown_args = _arg_parser.parse_known_args()
        return args

    #
    # Top level algorithm
    #
    # These methods describe the top-level algorithm, which is the purpose
    # of this class.
    #

    def ingest(self, source_dir):
        """Initiate the ingestion process.

        Find datasets under 'source_dir' and ingest them into the collection.
        """

        dataset_list = self.find_datasets(source_dir)

        for dataset_path in dataset_list:
            self.ingest_individual_dataset(dataset_path)

        self.log_ingestion_process_complete(source_dir)

    def ingest_individual_dataset(self, dataset_path):
        """Ingests a single dataset at 'dataset_path' into the collection.

        If this process raises a DatasetError, the dataset is skipped,
        but the process continues.
        """

        try:
            dataset = self.open_dataset(dataset_path)

            self.collection.check_metadata(dataset)
            #dataset has metadata accessor methods

            #check_metadata method will parse file name for sat, sensor,
            #bands and check against values returned from dataset accessor
            #methods?

            self.ingest_transaction(dataset)

        except DatasetError:
            self.log_dataset_skip(dataset_path)

        else:
            self.log_dataset_ingest_complete(dataset_path)

    def ingest_transaction(self, dataset):
        """Ingests a single dataset into the collection.

        This is done in a single transaction: if anything goes wrong
        the transaction is rolled back and no changes are made, then
        the error is propagated.
        """

        self.collection.begin_transaction()
        try:

            acquisition_record = \
                self.collection.create_acquisition_record(dataset)


            dataset_record = acquisition_record.create_dataset_record(dataset)

            #dataset.metadata_dict = dataset.build_metadata_dict()
            dataset_record.mdd = dataset.metadata_dict

            self.tile_dataset(dataset_record, dataset)

            dataset_record.mark_as_tiled()

        except:
            self.collection.rollback_transaction()
            raise

        else:
            self.collection.commit_transaction()

    def tile_dataset(self, dataset_record, dataset):
        # pylint: disable=maybe-no-member
        """Tiles a dataset.
        The database entry is identified by dataset_record."""
        dataset_key = self.collection.get_dataset_key(dataset)
        tile_type_list = self.collection.list_tile_types(dataset_key)
        for tile_type_id in tile_type_list:
            dataset_bands_dict = \
                    self.collection.new_bands[dataset_key][tile_type_id]
            band_stack = dataset.stack_bands(dataset_bands_dict)
            band_stack.buildvrt(os.path.join(self.datacube.tile_root,
                                             'ingest_temp'))
            for tile_footprint in \
                    dataset_record.get_coverage(tile_type_id):
                self.make_one_tile(dataset_record, tile_type_id,
                                   tile_footprint, band_stack)
        # pylint: enable=maybe-no-member

    def make_one_tile(self, dataset_record, tile_type_id,
                      tile_footprint, band_stack):
        """Makes a single tile."""
        tile_contents = self.collection.create_tile_contents(tile_type_id,
                                                             tile_footprint,
                                                             band_stack)
        tile_contents.reproject()
        if tile_contents.has_data():
            tile_record = dataset_record.create_tile_record(tile_contents)
            tile_record.make_mosaics()
        else:
            tile_contents.remove()

    #
    # Abstract methods
    #
    # These are abstract methods, designed to be overidden. They are
    # here to document what needs to be implemented in a subclass.
    #
    # The abstract method decorator checks that the abstract method
    # has be overridden when a subclass is *instantiated* rather than
    # when the method is called.
    #

    @abstractmethod
    def find_datasets(self, source_dir):
        """Return a list of path to the datasets under 'source_dir'.

        This is an abstract method since the method of identifying a
        dataset may vary between dataset formats.
        """

        raise NotImplementedError

    @abstractmethod
    def open_dataset(self, dataset_path):
        """Create and return a dataset object.

        dataset_path: points to the dataset to be opened and have
           its metadata read.

        Dataset objects differ for different types of dataset, but
        should present the same interface to the database classes. They
        contain the dataset path.
        """

        raise NotImplementedError

    #
    # Log messages
    #

    # pylint: disable=missing-docstring, no-self-use
    #
    # These are simple and self documenting: they do not need docstrings.
    # Although these do not currently use object data, they may need to
    # be modified to add more information to the log messages.
    #

    def log_ingestion_process_complete(self, source_dir):

        LOGGER.info("Ingestion process complete for source directory " +
                    "'%s'." % source_dir)

    def log_dataset_skip(self, dataset_path):

        LOGGER.info("Ingestion skipped for dataset " +
                    "'%s':" % dataset_path)
        LOGGER.debug("Exception info:", exc_info=True)

    def log_dataset_ingest_complete(self, dataset_path):

        LOGGER.info("Ingestion complete for dataset " +
                    "'%s'." % dataset_path)

    # pylint: enable=missing-docstring, no-self-use

