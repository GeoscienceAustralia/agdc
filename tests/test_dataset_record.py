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
    test__dataset_record.py - tests for the DatasetRecord class
"""
import re
import os
import logging
import unittest
from math import floor

from agdc import dbutil
from agdc.ingest import AbstractIngester
from agdc.ingest.core import IngesterDataCube
from agdc.ingest.landsat import LandsatDataset


#
# Set up logger.
#

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
# Constants
#
# Six actual Landsat acquisitions over which we test the get_coverage methods
#
# ############### THE DATA FROM THE DATASETS: ################
# List of dataset crs from sample datasets
DATASETS_TO_INGEST = [
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition0/L1/2005-06',
                 'LS5_TM_OTH_P51_GALPGS01-002_112_084_20050626'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition1/NBAR/1999-09',
                 'LS7_ETM_NBAR_P54_GANBAR01-002_099_078_19990927'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition2/L1/2006-06',
                 'LS7_ETM_OTH_P51_GALPGS01-002_110_079_20060623'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition3/L1/2007-02',
                 'LS7_ETM_OTH_P51_GALPGS01-002_104_078_20070224'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition4/L1/1998-10'),
    os.path.join('/g/data/v10/test_resources/scenes/tiler_testing0',
                 'Condition4/L1/1999-12',
                 'LS7_ETM_OTH_P51_GALPGS01-002_094_085_19991229_1')
    ]


################ THE EXPECTED OUTPUT: ################
TOLERANCE = 1e-02 #tolerance in number of datacube pixels for bbox calculation
#List of dataset bounding box coordinates
TILE_XLL = [114.4960138888889, 136.9537, 119.59266944444444,
            129.241830556, 131.963466667, 141.899613889]
TILE_YLL = [-35.5419, -26.9021305556, -28.361997222222225,
             -26.9690027778, -34.1416166667, -37.0430361111]
TILE_XLR = [117.14373611111112, 139.307855556, 121.98962777777778,
            131.649191667, 134.522813889, 144.640577778]
TILE_YLR = [-35.567822222222226, -26.9500305556, -28.400869444444442,
             -26.9443222222, -34.1782666667, -36.9905694444]
TILE_XUL = [114.55270555555555, 137.017691667, 119.65111111111112,
            129.237872222, 132.029008333, 141.877416667]
TILE_YUL = [-33.63163888888889, -25.0193027778, -26.44214722222222,
             -25.0183444444, -32.2227555556, -35.0685777778]
TILE_XUR = [117.14047777777779, 139.334688889, 122.00699444444444,
            131.605866667, 134.533133333, 144.550966667]
TILE_YUR = [-33.65578611111111, -25.0633805556, -26.477969444444444,
             -24.9956972222, -32.2568333333, -35.0197527778]
DATASET_BBOX = zip(zip(TILE_XUL, TILE_YUL), zip(TILE_XUR, TILE_YUR),
                   zip(TILE_XLR, TILE_YLR), zip(TILE_XLL, TILE_YLL))

# Set of expected tiles inside the bounding box's maximum contained rectangle.
# Paste in list of tiles in lon_lat format from previously hand-checked run of
# test_landsat_tiler.py.
COVERAGE_TILE_STRINGS = \
    '119_-027 120_-027 121_-027 122_-027 129_-027 131_-025 136_-026 137_-027 '\
    '139_-026 141_-037 142_-037 143_-037 144_-037 119_-028 120_-028 121_-028 '\
    '122_-028 130_-026 131_-026 136_-027 138_-026 139_-027 141_-038 142_-038 '\
    '143_-038 144_-038 119_-029 120_-029 121_-029 129_-026 130_-027 131_-027 '\
    '137_-026 138_-027 141_-036 142_-036 143_-036 144_-036 114_-034 114_-036 '\
    '115_-035 116_-034 116_-036 117_-035 131_-034 132_-033 132_-035 133_-034 '\
    '134_-033 134_-035 114_-035 115_-034 115_-036 116_-035 117_-034 117_-036 '\
    '131_-035 132_-034 133_-033 133_-035 134_-034'
PATTERN = r'(\d+)_(-*\d+)'
COVERAGE = set([(int(re.match(PATTERN, tile).group(1)),
                 int(re.match(PATTERN, tile).group(2)))
                for tile in COVERAGE_TILE_STRINGS.split()])
# Set of definite tiles that are inside the maximum rectangle contained in the
# dataset bounding box:
DEFINITE_TILES = \
    set([(itile, jtile)
         for (xul, yul), (xur, yur), (xlr, ylr), (xll, yll) in \
             zip(zip(TILE_XUL, TILE_YUL), zip(TILE_XUR, TILE_YUR),
                 zip(TILE_XLR, TILE_YLR), zip(TILE_XLL, TILE_YLL)) \
             for itile in range(int(max(floor(xll), floor(xul))),
                                int(min(floor(xlr), floor(xur))) + 1) \
             for jtile in range(int(max(floor(yll), floor(ylr))),
                                int(min(floor(yul), floor(yur))) + 1)])
# Set of expected tiles outside the bounding box's maximum contained rectangle,
# but inside the minimum rectangle containing the bounding box
POSSIBLE_TILES = \
    set([(itile, jtile)
         for (xul, yul), (xur, yur), (xlr, ylr), (xll, yll) in \
             zip(zip(TILE_XUL, TILE_YUL), zip(TILE_XUR, TILE_YUR),
                 zip(TILE_XLR, TILE_YLR), zip(TILE_XLL, TILE_YLL)) \
             for itile in range(int(min(floor(xll), floor(xul))),
                                int(max(floor(xlr), floor(xur))) + 1) \
             for jtile in range(int(min(floor(yll), floor(ylr))),
                                int(max(floor(yul), floor(yur))) + 1)]) \
                                .difference(DEFINITE_TILES)

#Set of expected possible tiles that intersect the dataset bounding box
INTERSECTED_TILES = COVERAGE.difference(DEFINITE_TILES)
#Set of expected possible tiles that are wholly contained in the bounding box
CONTAINED_TILES = set()

#
# Test suite
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class (which has too
# many public methods according to pylint).
#

class TestArgs(object):
    """The sole instance of this class stores the config_path and debug
    arguments for passing to the datacube constructor."""
    pass

class TestIngester(AbstractIngester):
    """An ingester class from which to get a datacube object"""
    def __init__(self, datacube):
        AbstractIngester.__init__(self, datacube)
    def find_datasets(self, source_dir):
        pass
    def open_dataset(self, dataset_path):
        pass

class TestDatasetRecord(unittest.TestCase):
    """Unit test for the DatasetRecord class"""
    MODULE = 'dataset_record'
    SUITE = 'DatasetRecord'

    INPUT_DIR = dbutil.input_directory(MODULE, SUITE)
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

        # Create an empty database
        self.test_conn = None
        print 'Create an empty database'
        self.test_dbname = dbutil.random_name("test_dataset_record")
        print 'Creating %s' %self.test_dbname
        dbutil.TESTSERVER.create(self.test_dbname,
                                     self.INPUT_DIR, "hypercube_empty.sql")

        # Set the datacube configuration file to point to the empty database
        configuration_dict = {'dbname': self.test_dbname}
        config_file_path = dbutil.update_config_file2(configuration_dict,
                                                     self.INPUT_DIR,
                                                     self.OUTPUT_DIR,
                                                     "test_datacube.conf")

        # Set an instance of the datacube and pass it to an ingester instance
        test_args = TestArgs()
        test_args.config_file = config_file_path
        test_args.debug = False
        test_datacube = IngesterDataCube(test_args)
        self.ingester = TestIngester(datacube=test_datacube)
        self.collection = self.ingester.collection

    def tearDown(self):
        #
        # Flush the handler and remove it from the root logger.
        #
        self.handler.flush()
        root_logger = logging.getLogger()
        root_logger.removeHandler(self.handler)
        if self.test_dbname:
            print 'About to drop %s' %self.test_dbname
            dbutil.TESTSERVER.drop(self.test_dbname)

    def test_get_bbox_dataset(self, tile_type_id=1):
        """Test the DatasetRecord class get_bbox() method on six landsat
        datasets."""
        #pylint: disable=too-many-locals
        cube_tile_size = \
            (self.ingester.datacube.tile_type_dict[tile_type_id]['x_size'],
             self.ingester.datacube.tile_type_dict[tile_type_id]['y_size'])
        cube_pixels = \
            (self.ingester.datacube.tile_type_dict[tile_type_id]['x_pixels'],
             self.ingester.datacube.tile_type_dict[tile_type_id]['y_pixels'])
        tile_crs = \
            self.ingester.datacube.tile_type_dict[tile_type_id]['crs']
        for idataset in range(len(DATASETS_TO_INGEST)):
            # Get information required for calculating the bounding box.
            dset = LandsatDataset(DATASETS_TO_INGEST[idataset])
            dataset_crs = dset.get_projection()
            geotrans = dset.get_geo_transform()
            pixels = dset.get_x_pixels()
            lines = dset.get_y_pixels()
            # Create a DatasetRecord instance so that we can test its
            # get_bbox() method. In doing this we need to create a
            # collection object and entries on the acquisition and dataset
            # tables of the database.
            self.collection.begin_transaction()
            acquisition = \
                self.collection.create_acquisition_record(dset)
            dset_record = acquisition.create_dataset_record(dset)
            self.collection.commit_transaction()
            # Test the DatasetRecord get_bbox() method
            #Determine the bounding quadrilateral of the dataset extent
            transformation = \
                dset_record.define_transformation(dataset_crs, tile_crs)
            bbox = dset_record.get_bbox(transformation, geotrans,
                                        pixels, lines)
            reference_dataset_bbox = DATASET_BBOX[idataset]
            #Check bounding box is as expected
            print 'Checking bbox for Dataset %d' %idataset
            residual_in_pixels = \
                [((x2 - x1) * cube_pixels[0] / cube_tile_size[0],
                  (y2 - y1) * cube_pixels[1] / cube_tile_size[1])
                 for ((x1, y1), (x2, y2)) in zip(reference_dataset_bbox, bbox)]
            assert all(abs(dx) < TOLERANCE and  abs(dy) < TOLERANCE
                       for (dx, dy) in residual_in_pixels), \
                       "bounding box calculation incorrect"

    def test_get_coverage(self, tile_type_id=1):
        # pylint: disable=too-many-locals
        """Test the methods called by the dataset_record.get_coverage() method.

        The constants at the top of this file provide test data expected to be
        returned by the tested get_coverage methods:
        1. TILE_XLL, TILE_YLL,... : dataset bounding box in tile projection
                                    coordinates TILE_CRS
        2. DEFINITE_TILES: tiles in inner rectangle
        3. POSSIBLE_TILES: tiles in outer rectangle
        4. INTERSECTED_TILES: those tiles from the outer rectangle that
        intersect the dataset bounding box
        5. CONTAINED_TILES: those tiles from outer rectangle wholly contained
        in the dataset bounding box
        6. COVERAGE: the tiles to be returned from DatasetRecord.get_coverage()
        """
        total_definite_tiles = set()
        total_possible_tiles = set()
        total_intersected_tiles = set()
        total_contained_tiles = set()
        total_touched_tiles = set()
        total_coverage = set()
        cube_origin = \
            (self.ingester.datacube.tile_type_dict[tile_type_id]['x_origin'],
             self.ingester.datacube.tile_type_dict[tile_type_id]['y_origin'])
        cube_tile_size = \
            (self.ingester.datacube.tile_type_dict[tile_type_id]['x_size'],
             self.ingester.datacube.tile_type_dict[tile_type_id]['y_size'])
        tile_crs = \
            self.ingester.datacube.tile_type_dict[tile_type_id]['crs']
        for idataset in range(len(DATASETS_TO_INGEST)):
            print 'Getting the coverage from Dataset %d' %idataset
            dset = LandsatDataset(DATASETS_TO_INGEST[idataset])
            dataset_crs = dset.get_projection()
            geotrans = dset.get_geo_transform()
            pixels = dset.get_x_pixels()
            lines = dset.get_y_pixels()
            # Create a DatasetRecord instance so that we can test its
            # get_coverage() method. In doing this we need to create a
            # collection object and entries on the acquisition and dataset
            # tables of the database.
            self.collection.begin_transaction()
            acquisition = \
                self.collection.create_acquisition_record(dset)
            dset_record = acquisition.create_dataset_record(dset)
            self.collection.commit_transaction()
            # Test the DatasetRecord get_bbox() method
            #Determine the bounding quadrilateral of the dataset extent
            transformation = \
                dset_record.define_transformation(dataset_crs, tile_crs)
            #Determine the bounding quadrilateral of the dataset extent
            bbox = dset_record.get_bbox(transformation, geotrans,
                                        pixels, lines)
            #Get the definite and possible tiles from this dataset and
            #accumulate in running total
            definite_tiles, possible_tiles = \
                dset_record.get_definite_and_possible_tiles(bbox, cube_origin,
                                                            cube_tile_size)
            total_definite_tiles = \
                total_definite_tiles.union(definite_tiles)
            total_possible_tiles = \
                total_possible_tiles.union(possible_tiles)
            #Get intersected tiles and accumulate in running total
            intersected_tiles = \
                dset_record.get_intersected_tiles(possible_tiles,
                                                  bbox,
                                                  cube_origin,
                                                  cube_tile_size)
            total_intersected_tiles = \
                total_intersected_tiles.union(intersected_tiles)
            #Take out intersected tiles from possibole tiles and get contained
            possible_tiles = possible_tiles.difference(intersected_tiles)
            contained_tiles = \
                dset_record.get_contained_tiles(possible_tiles,
                                                     bbox,
                                                     cube_origin,
                                                     cube_tile_size)
            total_contained_tiles = \
                total_contained_tiles.union(contained_tiles)
            #Use parent method to get touched tiles
            touched_tiles = \
                dset_record.get_touched_tiles(bbox,
                                              cube_origin,
                                              cube_tile_size)
            total_touched_tiles = total_touched_tiles.union(touched_tiles)
            #use parent method get_coverage to get coverage
            coverage = dset_record.get_coverage(tile_type_id)
            total_coverage = total_coverage.union(coverage)

        #Check definite and possible tiles are as expected
        assert total_definite_tiles == DEFINITE_TILES, \
            "Set of definite tiles disagrees with test data"
        assert total_possible_tiles == POSSIBLE_TILES, \
            "Set of possible tiles disagrees with test data"
        #Check intersected tiles are as expected
        assert total_intersected_tiles == INTERSECTED_TILES, \
            "Set of intersected tiles disagrees with test data"
         #Check contained tiles are as expected
        assert total_contained_tiles == CONTAINED_TILES, \
            "Set of tiles not in the definite set but wholly contained " \
            "within the dataset bbox does not agree with test data"
         #Check results of get_touced_tiles against expectations
        assert total_touched_tiles == COVERAGE, \
            "Set of tiles returned by get_touched_tiles does not agree " \
            "with test data"
        assert total_coverage == COVERAGE, \
            "Set of tiles returned by get_coverage does not agree " \
            "with test data"

def the_suite():
    "Runs the tests"""
    test_classes = [TestDatasetRecord]
    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)
    suite = unittest.TestSuite(suite_list)
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())
