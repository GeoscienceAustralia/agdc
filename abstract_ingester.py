"""
    abstract_ingester.py - top level ingestion algorithm.
"""

import os
import logging
import argparse
from abc import ABCMeta, abstractmethod
import psycopg2

from datacube import DataCube
from collection import Collection
from cube_util import DatasetError, parse_date_from_string
from ingest_db_wrapper import TC_PENDING, TC_SINGLE_SCENE, TC_SUPERSEDED
from ingest_db_wrapper import TC_MOSAIC
#from cube_util import synchronize
#import time

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

    #
    # Constants
    #

    CATALOG_MAX_TRIES = 3 # Max number of attempts for the catalog transaction.

    #
    # Constructor
    #

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
    # overridden if extra arguments, beyond those defined below,
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

        fast_filter_help = 'Filter datasets using filename patterns.'
        _arg_parser.add_argument('--fastfilter', dest='fast_filter',
                                 default=False, action='store_const',
                                 const=True, help=fast_filter_help)

        sync_time_help = 'Synchronize parallel ingestions at the given time'\
            ' in seconds after 01/01/1970'
        _arg_parser.add_argument('--synctime', dest='sync_time',
                                 default=None, help=sync_time_help)

        sync_type_help = 'Type of transaction to syncronize with synctime,'\
            + ' one of "cataloging", "tiling", or "mosaicking".'
        _arg_parser.add_argument('--synctype', dest='sync_type',
                                 default=None, help=sync_type_help)

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

            self.filter_on_metadata(dataset)

            dataset_record = self.catalog(dataset)

            self.tile(dataset_record)

            self.mosaic(dataset_record)

        except DatasetError as err:
            self.log_dataset_skip(dataset_path, err)

        else:
            self.log_dataset_ingest_complete(dataset_path)

    def filter_on_metadata(self, dataset):
        """Raises a DatasetError unless the dataset passes the filter."""

        path = dataset.get_x_ref()
        row = dataset.get_y_ref()
        dt = dataset.get_start_datetime()
        date = dt.date() if dt is not None else None

        if not self.filter(path, row, date):
            raise DatasetError('Filtered by metadata.')

    def catalog(self, dataset):
        """Catalogs a single dataset into the collection."""

        # Create or locate the acquisition and dataset_record for
        # the dataset we are ingesting. Simultanious attempts to
        # create the records may cause an IntegerityError - a retry
        # of the transaction should fix this.
        tries = 0
        while tries < self.CATALOG_MAX_TRIES:
            try:
                with self.collection.transaction():
                    acquisition_record = \
                        self.collection.create_acquisition_record(dataset)
                    dataset_record = \
                        acquisition_record.create_dataset_record(dataset)
                    break
            except psycopg2.IntegrityError:
                tries = tries + 1
        else:
            raise DatasetError('Unable to catalog: ' +
                               'persistent integrity error.')

        # Update the dataset and remove tiles if necessary.
        if dataset_record.needs_update:
            dataset_list = dataset_record.get_overlapping_datasets(
                tile_class_filter=(TC_SINGLE_SCENE, TC_SUPERSEDED))
            with self.collection.lock_datasets(dataset_list):
                with self.collection.transaction():
                    dataset_record.update()
                    dataset_record.remove_tiles()

    def tile(self, dataset_record):
        """Tiles a newly created or updated dataset."""
        pass

    def mosaic(self, dataset_record):
        """Creates mosaics for a newly tiled dataset."""
        pass

    # def ingest_transaction(self, dataset):
    #     """Ingests a single dataset into the collection.

    #     This is done in a single transaction: if anything goes wrong
    #     the transaction is rolled back and no changes are made, then
    #     the error is propagated.
    #     """

    #     self.collection.begin_transaction()
    #     try:
    #         acquisition_record = \
    #             self.collection.create_acquisition_record(dataset)

    #         dataset_record = \
    #             acquisition_record.create_dataset_record(dataset)

    #         self.collection.commit_acquisition_and_dataset()

    #         tile_record_list = self.tile_dataset(dataset_record, dataset)


    #         self.collection.commit_transaction('pre-mosaic tiles')

    #         dataset_record.mark_as_tiled()

    #         #TODO: remove the print statements
    #         print 'sync_time'
    #         print self.args.sync_time
    #         print 'Before synchronize %f' %time.time()
    #         synchronize(self.args.sync_time)
    #         print 'After synchronize %f' %time.time()
    #         print 'Returned from synchronize at %f' %time.time()
    #         self.mosaic_dataset(tile_record_list)

    #     except:
    #         self.collection.rollback_transaction()
    #         raise

    #     else:
    #         self.collection.commit_transaction('mosaic tiles')

    def tile_dataset(self, dataset_record, dataset):
        """Tiles a dataset.

        The database entry is identified by dataset_record. This method returns
        a list of tile_record objects that contain data. These will be used to
        perform any required mosaicing.
        """
        # Initialise a list of tile_record objects
        tile_record_list = []
        tile_type_list = dataset_record.list_tile_types()

        for tile_type_id in tile_type_list:
            # TODO: Is there a way of configuring .conf file to restrict to
            # certain tile_type_id?
            if tile_type_id != 1:
                continue
            tile_bands = dataset_record.get_tile_bands(tile_type_id)
            band_stack = dataset.stack_bands(tile_bands)
            band_stack.buildvrt(self.collection.get_temp_tile_directory())

            tile_footprint_list = dataset_record.get_coverage(tile_type_id)
            for tile_footprint in tile_footprint_list:
                tile_record = self.make_one_tile(dataset_record, tile_type_id,
                                                 tile_footprint, band_stack)
                if tile_record is not None:
                    tile_record_list.append(tile_record)
        return tile_record_list

    @staticmethod
    def mosaic_dataset(tile_record_list):
        """Mosaics the tiles of this dataset with any overlapping tiles that it
        finds on the database."""

        for tile_record in tile_record_list:
            tile_record.make_mosaic_db_changes()

        for tile_record in tile_record_list:
            if tile_record.mosaicing_tile_info[0] is not None:
                tile_record.make_mosaic_tiles()

    def make_one_tile(self, dataset_record, tile_type_id,
                      tile_footprint, band_stack):
        """Makes a single tile."""
        tile_contents = self.collection.create_tile_contents(tile_type_id,
                                                             tile_footprint,
                                                             band_stack)
        tile_contents.reproject()
        if tile_contents.has_data():
            tile_record = dataset_record.create_tile_record(tile_contents)
            return tile_record
            # tile_record.make_mosaics()
        else:
            tile_contents.remove()
            return None


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

        If fast filtering is turned on via the command line arguments it
        is done here, that is datasets that are filtered out should not
        be part of the list returned.
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
    # Dataset filter methods.
    #
    # The accessor methods give access to the dataset filtering values in
    # the config file via the datacube object. The filter function decides
    # wheather to filter a dataset or not.
    #

    # pylint: disable=maybe-no-member
    #
    # Stop pylint flagging the magical datacube attributes stuffed in from
    # the config file. Note that these are all checked for an attribute
    # error, so the program will not fail if they are not there.

    def get_date_range(self):
        """Return the date range for the ingest as a (start, end) tuple.

        If either value is not defined in the config file or cannot be
        converted to a date it will be returned as None.
        """

        try:
            start_date = parse_date_from_string(self.datacube.start_date)
        except AttributeError:
            start_date = None

        try:
            end_date = parse_date_from_string(self.datacube.end_date)
        except AttributeError:
            end_date = None

        return (start_date, end_date)

    def get_path_range(self):
        """Return the path range for the ingest as a (min, max) tuple.

        If either value is not defined in the config file or cannot
        be converted to an integer it will be returned as None.
        """

        try:
            min_path = int(self.datacube.min_path)
        except (AttributeError, ValueError):
            min_path = None

        try:
            max_path = int(self.datacube.max_path)
        except (AttributeError, ValueError):
            max_path = None

        return (min_path, max_path)

    def get_row_range(self):
        """Return the row range for the ingest as a (min, max) tuple.

        If either value is not defined in the config file or cannot
        be converted to an integer it will be returned as None.
        """

        try:
            min_row = int(self.datacube.min_row)
        except (AttributeError, ValueError):
            min_row = None

        try:
            max_row = int(self.datacube.max_row)
        except (AttributeError, ValueError):
            max_row = None

        return (min_row, max_row)

    # pylint: enable=maybe-no-member

    def filter(self, path, row, date):
        """Return True if the dataset should be included, False otherwise."""

        (start_date, end_date) = self.get_date_range()
        (min_path, max_path) = self.get_path_range()
        (min_row, max_row) = self.get_row_range()

        include = ((max_path is None or path is None or path <= max_path) and
                   (min_path is None or path is None or path >= min_path) and
                   (max_row is None or row is None or row <= max_row) and
                   (min_row is None or row is None or row >= min_row) and
                   (end_date is None or date is None or date <= end_date) and
                   (start_date is None or date is None or date >= start_date))

        return include

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

    def log_dataset_skip(self, dataset_path, err):

        LOGGER.info("Ingestion skipped for dataset " +
                    "'%s':" % dataset_path)
        LOGGER.info(str(err))
        LOGGER.debug("Exception info:", exc_info=True)

    def log_dataset_ingest_complete(self, dataset_path):

        LOGGER.info("Ingestion complete for dataset " +
                    "'%s'." % dataset_path)

    # pylint: enable=missing-docstring, no-self-use
