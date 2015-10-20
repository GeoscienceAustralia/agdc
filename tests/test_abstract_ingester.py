#!/usr/bin/env python

#===============================================================================
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
#===============================================================================

"""
    test_abstract_ingester.py - tests for the top level ingestion algorithm
"""

import re
import os
import logging
import unittest
import subprocess

from agdc import dbutil
from agdc.cube_util import DatasetError
from agdc.ingest import AbstractIngester

#
# Set up logger.
#

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
# Constants
#

TEMP_TILE_DIR = 'temp_tile_dir'

DATASET_PATH_DICT = {
    'single_path': ['path1'],
    'multi_path': ['path1', 'path2', 'path3'],
    'skip_one': ['skip1'],
    'skip_two': ['path1', 'skip2', 'path3'],
    'skip_three': ['path1', 'path2', 'skip3'],
    'skip_four': ['skip1', 'skip2', 'path3', 'path4'],
    'rollback_one': ['rollback1'],
    'rollback_two': ['path1', 'rollback2', 'path3'],
    'rollback_three': ['path1', 'path2', 'rollback3'],
    'rollback_four': ['path1', 'path2', 'rollback3', 'rollback4'],
    'mixed_ops': ['rollback1', 'rollback2', 'path3', 'path4',
                  'skip5', 'skip6'],
    'no_paths': ['rollback1', 'skip2'],
    'empty': []
    }

DATASET_DICT = {
    'path1': 'dataset1',
    'path2': 'dataset2',
    'path3': 'dataset3',
    'path4': 'dataset4',
    }

TILE_TYPE_DICT = {
    'dataset1': [1],
    'dataset2': [1, 2],
    'dataset3': [1, 2, 3],
    'dataset4': [4]
    }

BANDS_DICT = {
    ('dataset1', 1): 'bands1.1',
    ('dataset2', 1): 'bands2.1',
    ('dataset2', 2): 'bands2.2',
    ('dataset3', 1): 'bands3.1',
    ('dataset3', 2): 'bands3.2',
    ('dataset3', 3): 'bands3.3',
    ('dataset4', 4): 'bands4.4'
    }

COVERAGE_DICT = {
    ('dataset1', 1): ['tile1', 'empty2', 'tile3'],
    ('dataset2', 1): ['tile4'],
    ('dataset2', 2): ['tile5', 'tile6'],
    ('dataset3', 1): ['tile1', 'tile2', 'tile3', 'tile4', 'empty5', 'empty6'],
    ('dataset3', 2): ['tile7', 'empty8'],
    ('dataset3', 3): ['empty9'],
    ('dataset4', 4): ['tile4']
}

#
# Database Classes
#

# pylint: disable = missing-docstring
#
# Many of the methods are simple and self documenting and so do not need
# docstrings.
#


class DummyCollection(object):
    """Dummy collection class for testing."""

    def __init__(self):
        self.tiles = []

    # pylint: disable = no-self-use
    #
    # These methods do not use object data because this is a dummy
    # class for testing, but the methods in a real implementation will,
    # so these take self as a parameter for consistancy.

    def check_metadata(self, dataset):
        """Raise a DatasetError if the dataset path starts with 'skip'."""

        LOGGER.info("Check metadata.")

        if re.match(r'^skip', dataset.dataset_path):
            raise DatasetError("Testing skip dataset.")

    def get_temp_tile_directory(self):
        LOGGER.info("Get temporary tile directory.")
        LOGGER.info("    returning: '%s'", TEMP_TILE_DIR)
        return TEMP_TILE_DIR

    def begin_transaction(self):
        LOGGER.info("Begin transaction.")

    def commit_transaction(self):
        LOGGER.info("Commit transaction.")

    def rollback_transaction(self):
        LOGGER.info("Rollback transaction.")

    def create_acquisition_record(self, dataset):
        LOGGER.info("Create acquistion record:")
        LOGGER.info("    dataset = %s", dataset)

        acquisition_record = DummyAcquisitionRecord(self, dataset)

        LOGGER.info("    returning: %s", acquisition_record)
        return acquisition_record

    def create_tile_contents(self, tile_type_id, tile_footprint, band_stack):
        LOGGER.info("Create tile contents:")
        LOGGER.info("    tile_type_id = %s", tile_type_id)
        LOGGER.info("    tile_footprint = %s", tile_footprint)
        LOGGER.info("    band_stack = %s", band_stack)

        tile_contents = DummyTileContents(tile_type_id,
                                          tile_footprint,
                                          band_stack)

        LOGGER.info("    returning: %s", tile_contents)
        return tile_contents

    def print_tiles(self):
        """Print the final tile list to the log file."""
        print_tiles("output tiles", self.tiles)

    # pylint: enable = no-self-use


class DummyAcquisitionRecord(object):
    """Dummy aquisition record class for testing."""

    def __init__(self, collection, dataset):
        self.collection = collection
        self.dataset = dataset

    def __str__(self):
        return "[AcquisitionRecord %s]" % self.dataset

    def create_dataset_record(self, dataset):
        """Raise a DatasetError if the dataset path starts with 'rollback'."""

        LOGGER.info("Create dataset record:")
        LOGGER.info("    dataset = %s", dataset)

        if re.match(r'^rollback', dataset.dataset_path):
            raise DatasetError("Testing transaction rollback.")

        assert self.dataset is dataset, \
            "Mismatched datasets in acquisition record."

        dataset_record = DummyDatasetRecord(self.collection, self.dataset)

        LOGGER.info("    returning: %s", dataset_record)
        return dataset_record


class DummyDatasetRecord(object):
    """Dummy dataset record class for testing."""

    def __init__(self, collection, dataset):
        self.collection = collection
        self.dataset_id = DATASET_DICT[dataset.dataset_path]

    def __str__(self):
        return "[DatasetRecord %s]" % self.dataset_id

    def mark_as_tiled(self):
        LOGGER.info("%s: mark as tiled.", self)

    def list_tile_types(self):
        LOGGER.info("%s: list tile types.", self)

        tile_types = TILE_TYPE_DICT[self.dataset_id]

        LOGGER.info("    returning: %s", tile_types)
        return tile_types

    def get_tile_bands(self, tile_type_id):
        LOGGER.info("%s: get tile bands:", self)
        LOGGER.info("    tile_type_id = %s", tile_type_id)

        tile_bands = BANDS_DICT[(self.dataset_id, tile_type_id)]

        LOGGER.info("    returning: %s", tile_bands)
        return tile_bands

    def get_coverage(self, tile_type_id):
        LOGGER.info("%s: get_coverage:", self)
        LOGGER.info("    tile_type_id = %s", tile_type_id)

        coverage = COVERAGE_DICT[(self.dataset_id, tile_type_id)]

        LOGGER.info("    returning: %s", coverage)
        return coverage

    def create_tile_record(self, tile_contents):
        LOGGER.info("%s: create tile record:", self)
        LOGGER.info("    tile_contents = %s", tile_contents)

        return DummyTileRecord(self.collection,
                               self.dataset_id,
                               tile_contents)


class DummyTileRecord(object):
    """Dummy tile record class for testing."""

    def __init__(self, collection, dataset_id, tile_contents):
        """Creates a dummy tile record, and adds the tile to the
        collection tile list."""

        self.collection = collection
        self.dataset_id = dataset_id
        self.tile_footprint = tile_contents.tile_footprint
        self.band_list = tile_contents.band_stack.band_list

        assert tile_contents.reprojected, \
            "Expected tile_contents to have been reprojected."

        tile_tuple = (self.dataset_id, self.tile_footprint, self.band_list)

        self.collection.tiles.append(tile_tuple)

    def __str__(self):
        return "[TileRecord %s %s %s]" % \
            (self.dataset_id, self.tile_footprint, self.band_list)

    def make_mosaics(self):
        LOGGER.info("%s: make mosaics", self)


class DummyTileContents(object):
    """Dummy tile contents class for testing."""

    def __init__(self, tile_type_id, tile_footprint, band_stack):
        self.tile_type_id = tile_type_id
        self.tile_footprint = tile_footprint
        self.band_stack = band_stack
        self.reprojected = False
        self.removed = False

        assert band_stack.vrt_built, \
            "Expected band_stack to have had a vrt built."

    def __str__(self):
        return ("[TileContents %s %s %s]" %
                (self.tile_type_id, self.tile_footprint, self.band_stack))

    def reproject(self):
        LOGGER.info("%s: reproject", self)
        self.reprojected = True

    def has_data(self):
        """Returns False if the tile footprint starts with 'empty',
        True otherwise."""

        LOGGER.info("%s: has_data", self)

        assert not self.removed, "%s: has been removed." % self

        result = bool(not re.match(r'^empty', self.tile_footprint))

        LOGGER.info("    returning: %s", result)
        return result

    def remove(self):
        LOGGER.info("%s: remove", self)
        self.removed = True

#
# Dataset Classes
#


class DummyDataset(object):
    """Dummy dataset class for testing."""

    def __init__(self, dataset_path):
        self.dataset_path = dataset_path

    def __str__(self):
        return "[Dataset %s]" % self.dataset_path

    #pylint:disable=no-self-use

    def get_x_ref(self):
        return None

    def get_y_ref(self):
        return None

    def get_start_datetime(self):
        return None

    #pylint:enable=no-self-use

    def stack_bands(self, band_list):
        LOGGER.info("%s: stack_bands:", self)
        LOGGER.info("    band_list = %s", band_list)

        band_stack = DummyBandStack(band_list)

        LOGGER.info("    returning: %s", band_stack)
        return band_stack


class DummyBandStack(object):
    """Dummy band stack class for testing."""

    def __init__(self, band_list):
        self.band_list = band_list
        self.vrt_built = False

    def __str__(self):
        return "[BandStack %s]" % self.band_list

    def buildvrt(self, temp_dir):
        LOGGER.info("%s: buildvrt:", self)
        LOGGER.info("    temp_dir = '%s'", temp_dir)
        assert temp_dir == TEMP_TILE_DIR, \
            "Unexpected temp_dir, should be '%s'." % TEMP_TILE_DIR
        self.vrt_built = True

# pylint: enable = missing-docstring

#
# DummyIngester class
#


class DummyIngester(AbstractIngester):
    """Dummy Ingester subclass for testing."""

    def __init__(self, collection):
        """Initialise the source_dir cache then call Ingester init"""

        self.source_dir = None
        AbstractIngester.__init__(self, collection=collection)

    def find_datasets(self, source_dir):
        """Cache source directory then return dummy dataset paths."""

        LOGGER.info("Ingester: find datasets")
        LOGGER.info("    source_dir = %s", source_dir)

        self.source_dir = source_dir
        dataset_list = DATASET_PATH_DICT[source_dir]

        LOGGER.info("    returning: %s", dataset_list)
        return dataset_list

    def open_dataset(self, dataset_path):
        """Check dataset_path then return dummy dataset object."""

        LOGGER.info("Ingester: open dataset")
        LOGGER.info("    dataset_path = %s", dataset_path)

        assert dataset_path in DATASET_PATH_DICT[self.source_dir], \
            "Unexpected dataset path while opening dataset."

        dataset = DummyDataset(dataset_path)

        LOGGER.info("    returning: %s", dataset)
        return dataset

#
# Utility functions
#


def print_tiles(title, tiles):
    """Print a list of tiles to the log file."""

    LOGGER.info("")
    LOGGER.info("%s:", title)
    for tile in tiles:
        LOGGER.info("    %s", tile)

#
# Test suite
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class (which has too
# many public methods according to pylint).
#


class TestIngester(unittest.TestCase):
    """Unit test for the AbstractIngester class.

    This is a partially abstract class, so the DummyIngester subclass
    (defined above) is actually under test here."""

    MODULE = 'abstract_ingester'
    SUITE = 'TestIngester'

    OUTPUT_DIR = dbutil.output_directory(MODULE, SUITE)
    EXPECTED_DIR = dbutil.expected_directory(MODULE, SUITE)

    def setUp(self):
        #
        # Parse out the name of the test case and use it to name a logfile
        #
        match = re.search(r'\.([^\.]+)$', self.id())
        if match:
            name = match.group(1)
        else:
            name = 'TestIngester'

        logfile_name = "%s.log" % name
        self.logfile_path = os.path.join(self.OUTPUT_DIR, logfile_name)
        self.expected_path = os.path.join(self.EXPECTED_DIR, logfile_name)

        #
        # Set up a handler to log to the logfile, and attach it to the
        # root logger.
        #
        self.handler = logging.FileHandler(self.logfile_path, mode='w')
        self.handler.setLevel(logging.INFO)
        self.handler.setFormatter(logging.Formatter('%(message)s'))

        root_logger = logging.getLogger()
        root_logger.addHandler(self.handler)
        root_logger.setLevel(logging.DEBUG)

        #
        # Create the collection and ingester
        #
        self.collection = DummyCollection()
        self.ingester = DummyIngester(self.collection)

    def tearDown(self):
        #
        # Flush the handler and remove it from the root logger.
        #

        self.handler.flush()

        root_logger = logging.getLogger()
        root_logger.removeHandler(self.handler)

    def check_log_file(self):
        """If an expected logfile exists, check to see if it matches."""

        self.handler.flush()

        if not os.path.isfile(self.expected_path):
            self.skipTest("Expected log file not found.")
        else:
            try:
                subprocess.check_output(['diff',
                                         self.logfile_path,
                                         self.expected_path])
            except subprocess.CalledProcessError as err:
                self.fail("Log file does not match the expected log file:\n" +
                          err.output)

    def remove_log_file(self):
        """Remove the logfile from the output directory."""

        os.remove(self.logfile_path)

    def check_tiles(self, source_dir):
        """Check the tiles recorded in the collection against expectations."""

        output_tiles = self.collection.tiles
        expected_tiles = self.generate_tiles(source_dir)

        self.assertEqual(set(output_tiles), set(expected_tiles))

    @staticmethod
    def generate_tiles(source_dir):
        """Generate the expected tiles for a given source directory.

        This replicates the ingest algorithm, only it is much simpler
        because it only has to deal with the test data."""

        tiles = []

        for dataset_path in DATASET_PATH_DICT[source_dir]:
            if not re.match(r'(skip)|(rollback)', dataset_path):
                dataset_id = DATASET_DICT[dataset_path]
                for tile_type_id in TILE_TYPE_DICT[dataset_id]:
                    tup = (dataset_id, tile_type_id)
                    bands = BANDS_DICT[tup]
                    for tile_footprint in COVERAGE_DICT[tup]:
                        if not re.match(r'empty', tile_footprint):
                            tiles.append((dataset_id, tile_footprint, bands))

        return tiles

    def test_single_path_tiles(self):
        """Test for a single dataset path: check tiles."""

        self.ingester.ingest('single_path')
        self.check_tiles('single_path')
        self.remove_log_file()

    def test_multi_path_tiles(self):
        """Test for multiple dataset paths: check tiles."""

        self.ingester.ingest('multi_path')
        self.check_tiles('multi_path')
        self.remove_log_file()

    def test_skip_one_tiles(self):
        """Test for skipped datasets, test one: check tiles."""

        self.ingester.ingest('skip_one')
        self.check_tiles('skip_one')
        self.remove_log_file()

    def test_skip_two_tiles(self):
        """Test for skipped datasets, test two: check tiles."""

        self.ingester.ingest('skip_two')
        self.check_tiles('skip_two')
        self.remove_log_file()

    def test_skip_three_tiles(self):
        """Test for skipped datasets, test three: check tiles."""

        self.ingester.ingest('skip_three')
        self.check_tiles('skip_three')
        self.remove_log_file()

    def test_skip_four_tiles(self):
        """Test for skipped datasets, test four: check tiles."""

        self.ingester.ingest('skip_four')
        self.check_tiles('skip_four')
        self.remove_log_file()

    def test_rollback_one_tiles(self):
        """Test for transaction rollback, test one: check tiles."""

        self.ingester.ingest('rollback_one')
        self.check_tiles('rollback_one')
        self.remove_log_file()

    def test_rollback_two_tiles(self):
        """Test for transaction rollback, test two: check tiles."""

        self.ingester.ingest('rollback_two')
        self.check_tiles('rollback_two')
        self.remove_log_file()

    def test_rollback_three_tiles(self):
        """Test for transaction rollback, test three: check tiles."""

        self.ingester.ingest('rollback_three')
        self.check_tiles('rollback_three')
        self.remove_log_file()

    def test_rollback_four_tiles(self):
        """Test for transaction rollback, test four: check tiles."""

        self.ingester.ingest('rollback_four')
        self.check_tiles('rollback_four')
        self.remove_log_file()

    def test_mixed_ops_tiles(self):
        """Test for mixed dataset operations: check tiles."""

        self.ingester.ingest('mixed_ops')
        self.check_tiles('mixed_ops')
        self.remove_log_file()

    def test_no_paths_tiles(self):
        """Test for source directory with no valid datasets: check tiles."""

        self.ingester.ingest('no_paths')
        self.check_tiles('no_paths')
        self.remove_log_file()

    def test_empty_tiles(self):
        """Test for source directory with no datasets: check tiles."""

        self.ingester.ingest('empty')
        self.check_tiles('empty')
        self.remove_log_file()

    def test_single_path_log(self):
        """Test for a single dataset path: check tiles."""

        self.ingester.ingest('single_path')
        self.collection.print_tiles()
        self.check_log_file()

    def test_multi_path_log(self):
        """Test for multiple dataset paths: check log file."""

        self.ingester.ingest('multi_path')
        self.collection.print_tiles()
        self.check_log_file()

    def test_skip_one_log(self):
        """Test for skipped datasets, test one: check log file."""

        self.ingester.ingest('skip_one')
        self.collection.print_tiles()
        self.check_log_file()

    def test_skip_two_log(self):
        """Test for skipped datasets, test two: check log file."""

        self.ingester.ingest('skip_two')
        self.collection.print_tiles()
        self.check_log_file()

    def test_skip_three_log(self):
        """Test for skipped datasets, test three: check log file."""

        self.ingester.ingest('skip_three')
        self.collection.print_tiles()
        self.check_log_file()

    def test_skip_four_log(self):
        """Test for skipped datasets, test four: check log file."""

        self.ingester.ingest('skip_four')
        self.collection.print_tiles()
        self.check_log_file()

    def test_rollback_one_log(self):
        """Test for transaction rollback, test one: check log file."""

        self.ingester.ingest('rollback_one')
        self.collection.print_tiles()
        self.check_log_file()

    def test_rollback_two_log(self):
        """Test for transaction rollback, test two: check log file."""

        self.ingester.ingest('rollback_two')
        self.collection.print_tiles()
        self.check_log_file()

    def test_rollback_three_log(self):
        """Test for transaction rollback, test three: check log file."""

        self.ingester.ingest('rollback_three')
        self.collection.print_tiles()
        self.check_log_file()

    def test_rollback_four_log(self):
        """Test for transaction rollback, test four: check log file."""

        self.ingester.ingest('rollback_four')
        self.collection.print_tiles()
        self.check_log_file()

    def test_mixed_ops_log(self):
        """Test for mixed dataset operations: check log file."""

        self.ingester.ingest('mixed_ops')
        self.collection.print_tiles()
        self.check_log_file()

    def test_no_paths_log(self):
        """Test for source directory with no valid datasets: check log file."""

        self.ingester.ingest('no_paths')
        self.collection.print_tiles()
        self.check_log_file()

    def test_empty_log(self):
        """Test for source directory with no datasets: check log file."""

        self.ingester.ingest('empty')
        self.collection.print_tiles()
        self.check_log_file()


#
# Define test suites
#


def the_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [TestIngester]

    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)

    suite = unittest.TestSuite(suite_list)

    return suite

#
# Run unit tests if in __main__
#

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())
