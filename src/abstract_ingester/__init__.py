#!/usr/bin/env python

#===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither Geoscience Australia nor the names of its contributors may be
#       used to endorse or promote products derived from this software
#       without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#===============================================================================

"""
    abstract_ingester.py - top level ingestion algorithm.
"""

import os
import logging
import argparse
from datetime import datetime
import json
from abc import ABCMeta, abstractmethod
import psycopg2

from agdc import DataCube
from agdc.cube_util import DatasetError, DatasetSkipError, parse_date_from_string
from collection import Collection
from abstract_dataset import AbstractDataset
from abstract_bandstack import AbstractBandstack
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

        if self.args.debug:
            # Set DEBUG level on the root logger
            logging.getLogger().setLevel(logging.DEBUG)

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
                tries = tries + 1
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

    def preprocess_dataset(self, dataset_list):
        """Performs pre-processing on the dataset_list object.

        dataset_list: list of datasets to be opened and have
           its metadata read.
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
                    tt_set = set([int(tile_types)])
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
                    "'%s' in %s.", source_dir, elapsed_time)

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
