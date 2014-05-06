"""
    test_abstract_ingester.py - tests for the top level ingestion algorithm
"""

import re
import os
import logging
import unittest
import subprocess

import dbutil
from abstract_ingester import AbstractIngester, DatasetError

#
# Set up logger.
#

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
# Constants
#

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

    def begin_transaction(self):
        LOGGER.info("Begin transaction.")

    def commit_transaction(self):
        LOGGER.info("Commit transaction.")

    def rollback_transaction(self):
        LOGGER.info("Rollback transaction.")

    def create_acquisition_record(self, dataset):
        LOGGER.info("Create acquistion record:")
        LOGGER.info("    dataset = %s", dataset)

        return DummyAcquisitionRecord(self.tiles, dataset)

    def create_tile_contents(self, tile_footprint, band_stack):
        LOGGER.info("Create tile contents:")
        LOGGER.info("    tile_footprint = %s", tile_footprint)
        LOGGER.info("    band_stack = %s", band_stack)

        return DummyTileContents(tile_footprint, band_stack)

    # pylint: enable = no-self-use


class DummyAcquisitionRecord(object):
    """Dummy aquisition record class for testing."""

    def __init__(self, tiles, dataset):
        self.dataset = dataset
        self.tiles = tiles

    def create_dataset_record(self, dataset):
        """Raise a DatasetError if the dataset path starts with 'rollback'."""

        LOGGER.info("Create dataset record:")
        LOGGER.info("    dataset = %s", dataset)

        if re.match(r'^rollback', dataset.dataset_path):
            raise DatasetError("Testing transaction rollback.")

        assert self.dataset is dataset, \
            "Mismatched datasets in acquisition record."

        return DummyDatasetRecord(self.tiles, dataset)


class DummyDatasetRecord(object):
    """Dummy dataset record class for testing."""

    def __init__(self, tiles, dataset):
        self.tiles = tiles
        self.dataset_id = DATASET_DICT[dataset.dataset_path]

    def __str__(self):
        return "[DatasetRecord %s]" % self.dataset_id

    def mark_as_tiled(self):
        LOGGER.info("%s: mark as tiled.", self)

    def list_tile_types(self):
        LOGGER.info("%s: list tile types.", self)

        return TILE_TYPE_DICT[self.dataset_id]

    def list_bands(self, tile_type_id):
        LOGGER.info("%s: list bands:", self)
        LOGGER.info("    tile_type_id = %s", tile_type_id)

        return BANDS_DICT[(self.dataset_id, tile_type_id)]

    def get_coverage(self, tile_type_id):
        LOGGER.info("%s: get_coverage:", self)
        LOGGER.info("    tile_type_id = %s", tile_type_id)

        return COVERAGE_DICT[(self.dataset_id, tile_type_id)]

    def create_tile_record(self, tile_footprint, tile_contents):
        LOGGER.info("%s: create tile record:", self)
        LOGGER.info("    tile_footprint = %s", tile_footprint)
        LOGGER.info("    tile_contents = %s", tile_contents)

        return DummyTileRecord(self.tiles, self.dataset_id,
                               tile_footprint, tile_contents)


class DummyTileRecord(object):
    """Dummy tile record class for testing."""

    def __init__(self, tiles, dataset_id,
                 tile_footprint, tile_contents):
        """Creates a dummy tile record, and adds the tile to the
        collection tile list."""

        self.dataset_id = dataset_id
        self.tile_footprint = tile_footprint
        self.band_list = tile_contents.band_stack.band_list

        assert tile_contents.tile_footprint == tile_footprint, \
            "Mismatched tile footprints in tile record."

        tiles.append((self.dataset_id, self.tile_footprint, self.band_list))

    def __str__(self):
        return "[TileRecord %s %s %s]" % \
            (self.dataset_id, self.tile_footprint, self.band_list)

    def make_mosaics(self):
        LOGGER.info("%s: make mosaics", self)


class DummyTileContents(object):
    """Dummy tile contents class for testing."""

    def __init__(self, tile_footprint, band_stack):
        self.tile_footprint = tile_footprint
        self.band_stack = band_stack
        self.removed = False

    def __str__(self):
        return "[TileContents %s %s]" % (self.tile_footprint, self.band_stack)

    def has_data(self):
        """Returns False if the tile footprint starts with 'empty',
        True otherwise."""

        assert not self.removed, "%s: has been removed." % self
        return bool(not re.match(r'^empty', self.tile_footprint))

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

    def stack_bands(self, band_list):
        LOGGER.info("%s: stack_bands:", self)
        LOGGER.info("    band_list = %s", band_list)

        return DummyBandStack(band_list)


class DummyBandStack(object):
    """Dummy band stack class for testing."""

    def __init__(self, band_list):
        self.band_list = band_list

    def __str__(self):
        return "[BandStack %s]" % self.band_list


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
        return DATASET_PATH_DICT[source_dir]

    def open_dataset(self, dataset_path):
        """Check dataset_path then return dummy dataset object."""

        LOGGER.info("Ingester: open dataset")
        LOGGER.info("    dataset_path = %s", dataset_path)

        assert dataset_path in DATASET_PATH_DICT[self.source_dir], \
            "Unexpected dataset path while opening dataset."
        return DummyDataset(dataset_path)

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

    def check_tiles(self, source_dir):
        """Check the tiles recorded in the collection against expectations."""

        output_tiles = self.collection.tiles
        expected_tiles = self.generate_tiles(source_dir)

        self.print_tiles('output tiles', output_tiles)

        self.assertEqual(set(output_tiles), set(expected_tiles))

    @staticmethod
    def print_tiles(title, tiles):
        """Print a list of tiles to the log file."""

        LOGGER.info("")
        LOGGER.info("%s:", title)
        for tile in tiles:
            LOGGER.info("    %s", tile)

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

    def test_single_path(self):
        """Test for a single dataset path."""

        self.ingester.ingest('single_path')
        self.check_tiles('single_path')
        self.check_log_file()

    def test_multi_path(self):
        """Test for multiple dataset paths."""

        self.ingester.ingest('multi_path')
        self.check_tiles('multi_path')
        self.check_log_file()

    def test_skip_one(self):
        """Test for skipped datasets, test one."""

        self.ingester.ingest('skip_one')
        self.check_tiles('skip_one')
        self.check_log_file()

    def test_skip_two(self):
        """Test for skipped datasets, test two."""

        self.ingester.ingest('skip_two')
        self.check_tiles('skip_two')
        self.check_log_file()

    def test_skip_three(self):
        """Test for skipped datasets, test three."""

        self.ingester.ingest('skip_three')
        self.check_tiles('skip_three')
        self.check_log_file()

    def test_skip_four(self):
        """Test for skipped datasets, test four."""

        self.ingester.ingest('skip_four')
        self.check_tiles('skip_four')
        self.check_log_file()

    def test_rollback_one(self):
        """Test for transaction rollback, test one."""

        self.ingester.ingest('rollback_one')
        self.check_tiles('rollback_one')
        self.check_log_file()

    def test_rollback_two(self):
        """Test for transaction rollback, test two."""

        self.ingester.ingest('rollback_two')
        self.check_tiles('rollback_two')
        self.check_log_file()

    def test_rollback_three(self):
        """Test for transaction rollback, test three."""

        self.ingester.ingest('rollback_three')
        self.check_tiles('rollback_three')
        self.check_log_file()

    def test_rollback_four(self):
        """Test for transaction rollback, test four."""

        self.ingester.ingest('rollback_four')
        self.check_tiles('rollback_four')
        self.check_log_file()

    def test_mixed_ops(self):
        """Test for mixed dataset operations."""

        self.ingester.ingest('mixed_ops')
        self.check_tiles('mixed_ops')
        self.check_log_file()

    def test_no_paths(self):
        """Test for source directory with no paths to valid datasets."""

        self.ingester.ingest('no_paths')
        self.check_tiles('no_paths')
        self.check_log_file()

    def test_empty(self):
        """Test for source directory with no datasets."""

        self.ingester.ingest('empty')
        self.check_tiles('empty')
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
