# coding=utf-8

# ===============================================================================
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
# ===============================================================================


"""
Common classes for abstract_ingester code.
"""

from __future__ import absolute_import

import os
import sys
import logging
import argparse
from datetime import datetime
import json
from abc import abstractmethod, ABCMeta

import psycopg2

from agdc.compat import with_metaclass
from ..datacube import DataCube
from ..cube_util import DatasetError, DatasetSkipError, parse_date_from_string
from .collection import Collection

LOGGER = logging.getLogger(__name__)


class IngesterDataCube(DataCube):
    """Datacube subclass which accepts command line arguments
    as a Namespace passed into the constructer."""

    def __init__(self, args):
        self.my_args = args
        DataCube.__init__(self)

    def parse_args(self):
        return self.my_args


class AbstractIngester(with_metaclass(ABCMeta)):
    """
    Partially abstract base class for ingester objects. Needs to
    be subclassed and have dataset type and format specific functions
    defined before use.
    """

    #
    # Constants
    #

    CATALOG_MAX_TRIES = 3  # Max no. of attempts for the catalog transaction.

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
        self.ingestion_start_datetime = datetime.now()

        self.args = self.arg_parser().parse_args()

        if datacube is None:
            self.datacube = IngesterDataCube(self.args)
        else:
            self.datacube = datacube

        self.agdc_root = self.datacube.agdc_root

        if collection is None:
            self.collection = Collection(self.datacube)
        else:
            self.collection = collection

    #
    # parse_args method for command line arguments. This should be
    # overridden if extra arguments, beyond those defined below,
    # are needed.
    #

    @classmethod
    def arg_parser(cls):
        """Build an argument parser.

        Additional args may be added by subclasses.

        :rtype: argparse.ArgumentParser
        """
        _arg_parser = argparse.ArgumentParser()

        default_config = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                      'agdc_default.conf')
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

        return _arg_parser

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
        start_datetime = datetime.now()

        dataset_list = self.find_datasets(source_dir)

        dataset_list = self.preprocess_dataset(dataset_list)

        for dataset_path in dataset_list:
            self.ingest_individual_dataset(dataset_path)

        self.log_ingestion_process_complete(source_dir, datetime.now() - start_datetime)

    def ingest_individual_dataset(self, dataset_path):
        """Ingests a single dataset at 'dataset_path' into the collection.

        If this process raises a DatasetError, the dataset is skipped,
        but the process continues.
        """

        start_datetime = datetime.now()
        try:
            dataset = self.open_dataset(dataset_path)

            self.collection.check_metadata(dataset)

            self.filter_on_metadata(dataset)

            dataset_record = self.catalog(dataset)

            self.tile(dataset_record, dataset)

            self.mosaic(dataset_record)

        except DatasetError as err:
            self.log_dataset_fail(dataset_path, err, datetime.now() - start_datetime)

        except DatasetSkipError as err:
            self.log_dataset_skip(dataset_path, err, datetime.now() - start_datetime)

        except:
            # Log which dataset caused the error, and re-raise it.
            LOGGER.error('Unexpected error during path %r', dataset_path)
            raise
        else:
            self.log_dataset_ingest_complete(dataset_path, datetime.now() - start_datetime)

    def filter_on_metadata(self, dataset):
        """Raises a DatasetError unless the dataset passes the filter."""

        path = dataset.get_x_ref()
        row = dataset.get_y_ref()
        dt = dataset.get_start_datetime()
        date = dt.date() if dt is not None else None

        if not self.filter_dataset(path, row, date):
            raise DatasetError('Filtered by metadata.')

    def catalog(self, dataset):
        """Catalog a single dataset into the collection."""

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
                tries += 1
                LOGGER.exception("Integrity error (attempt %d of %d)", tries, self.CATALOG_MAX_TRIES)
        else:
            raise DatasetError('Unable to catalog: ' +
                               'persistent integrity error.')

        # Update the dataset and remove tiles if necessary.
        if dataset_record.needs_update:
            overlap_list = dataset_record.get_removal_overlaps()
            with self.collection.lock_datasets(overlap_list):
                with self.collection.transaction():
                    dataset_record.remove_mosaics(overlap_list)
                    dataset_record.remove_tiles()
                    dataset_record.update()

        return dataset_record

    def tile(self, dataset_record, dataset):
        """Create tiles for a newly created or updated dataset."""

        tile_list = []
        for tile_type_id in dataset_record.list_tile_types():
            if not self.filter_tile_type(tile_type_id):
                continue

            tile_bands = dataset_record.get_tile_bands(tile_type_id)
            band_stack = dataset.stack_bands(tile_bands)
            band_stack.buildvrt(self.collection.get_temp_tile_directory())

            tile_list += dataset_record.make_tiles(tile_type_id, band_stack)

        with self.collection.lock_datasets([dataset_record.dataset_id]):
            with self.collection.transaction():
                dataset_record.store_tiles(tile_list)

    def mosaic(self, dataset_record):
        """Create mosaics for a newly tiled dataset."""

        overlap_list = dataset_record.get_creation_overlaps()

        with self.collection.lock_datasets(overlap_list):
            with self.collection.transaction():
                dataset_record.create_mosaics(overlap_list)

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

        :rtype: list[str]
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

        :type dataset_path: str
        :rtype: agdc.ingest.AbstractDataset
        """
        raise NotImplementedError

    def preprocess_dataset(self, dataset_list):
        """Performs pre-processing on the dataset_list object, potentially
        returning a different list of datasets to replace them.

        :type dataset_list: list[str]
        :rtype: list[str]
        """
        return dataset_list

    #
    # Filter methods.
    #
    # The accessor methods give access to the dataset and tile type filtering
    # values in the config file via the datacube object. The filter_dataset
    # and function decides whether to include a dataset. The filter_tile_type
    # function decides whether to include a tile_type.
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

        return start_date, end_date

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

        return min_path, max_path

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

        return min_row, max_row

    def get_tile_type_set(self):
        """Return the allowable tile types for an ingest as a set.

        If this is not defined in the config file it will be returned as None.
        """

        try:
            tile_types = self.datacube.tile_types
        except AttributeError:
            tile_types = None

        if tile_types:
            try:
                tt_set = set(json.loads(tile_types))
            except (TypeError, ValueError):
                try:
                    tt_set = {int(tile_types)}
                except ValueError:
                    raise AssertionError("Unable to parse the 'tile_types' " +
                                         "configuration file item.")
        else:
            tt_set = None

        return tt_set

    # pylint: enable=maybe-no-member

    def filter_dataset(self, path, row, date):
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

    def filter_tile_type(self, tile_type_id):
        """Return True if the dataset should be included, False otherwise."""

        tt_set = self.get_tile_type_set()
        return tt_set is None or tile_type_id in tt_set

    #
    # Log messages
    #

    # pylint: disable=missing-docstring, no-self-use
    #
    # These are simple and self documenting: they do not need docstrings.
    # Although these do not currently use object data, they may need to
    # be modified to add more information to the log messages.
    #

    def log_ingestion_process_complete(self, source_dir, elapsed_time):

        LOGGER.info("Ingestion process complete for source directory " +
                    "'%s' in %s.", os.path.abspath(source_dir), elapsed_time)

    def log_dataset_fail(self, dataset_path, err, elapsed_time):

        LOGGER.info("Ingestion failed for dataset " +
                    "'%s' in %s:", dataset_path, elapsed_time)
        LOGGER.info(str(err))
        LOGGER.debug("Exception info:", exc_info=True)

    def log_dataset_skip(self, dataset_path, err, elapsed_time):

        LOGGER.info("Ingestion skipped for dataset " +
                    "'%s' in %s:", dataset_path, elapsed_time)
        LOGGER.info(str(err))
        LOGGER.debug("Exception info:", exc_info=True)

    def log_dataset_ingest_complete(self, dataset_path, elapsed_time):

        LOGGER.info("Ingestion complete for dataset " +
                    "'%s' in %s.", dataset_path, elapsed_time)

    # pylint: enable=missing-docstring, no-self-use


def _find_files(source_path, matcher):
    """
    Find source files in the given path that return true using the given matcher.

    This may be a directory to search, or a single image.

    :type source_path: str
    :type matcher: (str) -> bool
    :return: A list of absolute paths
    :rtype: list of str
    """

    source_path = os.path.abspath(source_path)

    # Allow an individual file to be supplied as the source
    if os.path.isfile(source_path) and matcher(source_path):
        LOGGER.debug('%r is a single file', source_path)
        return [source_path]

    LOGGER.debug('Source is a directory: %s', source_path)

    assert os.path.isdir(source_path), '%s is not a directory' % source_path

    dataset_list = []
    for root, _dirs, files in os.walk(source_path):
        dataset_list += [os.path.join(root, f) for f in files if matcher(f)]

    return sorted(dataset_list)


class SourceFileIngester(AbstractIngester):
    """
    An ingester that takes a source_path argument and does a simple filename match within that path.
    """

    def __init__(self, is_valid_file, datacube=None, collection=None):
        """
        :type is_valid_file: (str) -> bool
        :param is_valid_file: Function taking a file_path, returns true if a file for ingestion.
        :type datacube: agdc.datacube.DataCube
        :type collection: agdc.abstract_ingester.collection.Collection
        """
        super(SourceFileIngester, self).__init__(datacube, collection)
        self.is_valid_file = is_valid_file

    @classmethod
    def arg_parser(cls):
        """Get a parser for required args."""

        # Extend the default parser
        _arg_parser = super(SourceFileIngester, cls).arg_parser()

        _arg_parser.add_argument('--source', dest='source_dir',
                                 required=True,
                                 help='Source root directory containing datasets')

        return _arg_parser

    def find_datasets(self, source_path):
        """Return a list of path to the datasets under 'source_dir' or a single-item list
        if source_dir is a file path
        """
        dataset_list = _find_files(source_path, self.is_valid_file)
        LOGGER.debug('%s dataset found: %r', len(dataset_list), dataset_list)
        return dataset_list


def run_ingest(ingester_class):
    """
    Do ingestion, with default environment settings.

    The ingester_class is expected to be a class that implements SourceFileIngester.
    """
    logging.basicConfig(stream=sys.stdout,
                        format='%(asctime)s %(name)s %(levelname)s %(message)s',
                        level=logging.INFO)

    ingester = ingester_class()

    if ingester.args.debug:
        logging.getLogger('agdc.ingest').setLevel(logging.DEBUG)

    ingester.ingest(ingester.args.source_dir)

    ingester.collection.cleanup()
