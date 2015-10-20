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
    test_tile_record.py - tests for the TileRecord class
"""
# pylint: disable=too-many-public-methods
import re
import os
import logging
import sys
import unittest

import numpy as np
from eotools.execute import execute

from agdc import dbutil
from agdc.ingest import AbstractIngester
from agdc.ingest.core import IngesterDataCube
from agdc.ingest.landsat import LandsatDataset
from test_landsat_tiler import TestLandsatTiler
import ingest_test_data as TestIngest
from test_tile_contents import TestTileContents

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

# Add a streamhandler to write output to console
streamhandler = logging.StreamHandler(stream=sys.stdout)
streamhandler.setLevel(logging.INFO)
streamhandler.setFormatter(logging.Formatter('%(message)s'))
LOGGER.addHandler(streamhandler)

#
# Constants
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

class TestTileRecord(unittest.TestCase):
    """Unit tests for the TileRecord class"""
    # pylint: disable=too-many-instance-attributes
    MODULE = 'tile_record'
    SUITE = 'TileRecord2'

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
        LOGGER.addHandler(self.handler)

        # Create an empty database
        self.test_conn = None
        self.test_dbname = dbutil.random_name("test_tile_record")
        LOGGER.info('Creating %s', self.test_dbname)
        dbutil.TESTSERVER.create(self.test_dbname,
                                     self.INPUT_DIR, "hypercube_empty.sql")

        # Set the datacube configuration file to point to the empty database
        configuration_dict = {'dbname': self.test_dbname,
                              'tile_root': os.path.join(self.OUTPUT_DIR,
                                                        'tiles')}
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

        # Delete all rows of tile_footprint table:
        #sql = "Delete from tile_footprint;"
        #with self.collection.db.conn.cursor() as cur:
        #    cur.execute(sql)

    def tearDown(self):
        #
        # Flush the handler and remove it from the root logger.
        #
        self.handler.flush()
        streamhandler.flush()
        root_logger = logging.getLogger()
        root_logger.removeHandler(self.handler)
        #if self.test_dbname:
        #    LOGGER.info('About to drop %s', self.test_dbname)
        #    dbutil.TESTSERVER.drop(self.test_dbname)

    def test_insert_tile_record(self):
        """Test the Landsat tiling process method by comparing output to a
        file on disk."""
        # pylint: disable=too-many-locals
        # Test a single dataset for tile_record creation
        processing_level = 'PQA'
        dataset_path = TestIngest.DATASETS_TO_INGEST[processing_level][0]
        LOGGER.info('Testing Dataset %s', dataset_path)
        dset = LandsatDataset(dataset_path)
        # Create a DatasetRecord instance so that we can access its
        # list_tile_types() method. In doing this we need to create a
        # collection object and entries on the acquisition and dataset
        # tables of the database.
        self.collection.begin_transaction()
        acquisition = \
            self.collection.create_acquisition_record(dset)
        dset_record = acquisition.create_dataset_record(dset)

        # List the benchmark footprints associated with this datset
        ftprints = \
            TestTileContents.get_benchmark_footprints(dset_record.mdd,
                                                      TestIngest.BENCHMARK_DIR)
        LOGGER.info('bench_footprints=%s', str(ftprints))
        # Get tile types
        dummy_tile_type_list = dset_record.list_tile_types()
        # Assume dataset has tile_type = 1 only:
        tile_type_id = 1
        dataset_bands_dict = dset_record.get_tile_bands(tile_type_id)
        ls_bandstack = dset.stack_bands(dataset_bands_dict)
        temp_dir = os.path.join(self.ingester.datacube.tile_root,
                                'ingest_temp')
        # Form scene vrt
        ls_bandstack.buildvrt(temp_dir)
        # Reproject scene data onto selected tile coverage
        tile_footprint_list = dset_record.get_coverage(tile_type_id)
        LOGGER.info('coverage=%s', str(tile_footprint_list))
        for tile_footprint in tile_footprint_list:
            tile_contents = \
                self.collection.create_tile_contents(tile_type_id,
                                                     tile_footprint,
                                                     ls_bandstack)
            LOGGER.info('reprojecting for %s tile %s',
                        processing_level, str(tile_footprint))
            #Need to call reproject to set tile_contents.tile_extents
            tile_contents.reproject()
            if tile_contents.has_data():
                dummy_tile_record = \
                    dset_record.create_tile_record(tile_contents)
        self.collection.commit_transaction()

    def test_make_mosaics(self):
        """Make mosaic tiles from two adjoining scenes."""
        # pylint: disable=too-many-locals
        nbar1, nbar2 = TestIngest.MOSAIC_SOURCE_NBAR
        ortho1, ortho2 = TestIngest.MOSAIC_SOURCE_ORTHO
        pqa1, pqa2 = TestIngest.MOSAIC_SOURCE_PQA
        # Set the list of datset paths which should result in mosaic tiles
        dataset_list = [nbar1, nbar2, ortho1, ortho2, pqa1, pqa2]
        dataset_list = [pqa1, pqa2]
        for dataset_path in dataset_list:
            dset = LandsatDataset(dataset_path)
            self.collection.begin_transaction()
            acquisition = \
                self.collection.create_acquisition_record(dset)
            dset_record = acquisition.create_dataset_record(dset)
            # Get tile types
            dummy_tile_type_list = dset_record.list_tile_types()
            # Assume dataset has tile_type = 1 only:
            tile_type_id = 1
            dataset_bands_dict = dset_record.get_tile_bands(tile_type_id)
            ls_bandstack = dset.stack_bands(dataset_bands_dict)
            temp_dir = os.path.join(self.ingester.datacube.tile_root,
                                    'ingest_temp')
            # Form scene vrt
            ls_bandstack.buildvrt(temp_dir)
            # Reproject scene data onto selected tile coverage
            tile_footprint_list = dset_record.get_coverage(tile_type_id)
            LOGGER.info('coverage=%s', str(tile_footprint_list))
            for tile_ftprint in tile_footprint_list:
                #Only do that footprint for which we have benchmark mosaics
                if tile_ftprint not in [(150, -26)]:
                    continue
                tile_contents = \
                    self.collection.create_tile_contents(tile_type_id,
                                                         tile_ftprint,
                                                         ls_bandstack)
                LOGGER.info('Calling reproject for %s tile %s...',
                            dset_record.mdd['processing_level'], tile_ftprint)
                tile_contents.reproject()
                LOGGER.info('...finished')
                if tile_contents.has_data():
                    LOGGER.info('tile %s has data',
                                tile_contents.temp_tile_output_path)
                    tile_record = dset_record.create_tile_record(tile_contents)
                    mosaic_required = tile_record.make_mosaics()

                    if not mosaic_required:
                        continue
                    #Test mosaic tiles against benchmark
                    mosaic_benchmark = TestTileContents.get_benchmark_tile(
                        dset_record.mdd,
                        os.path.join(TestIngest.BENCHMARK_DIR,
                                     'mosaic_cache'),
                        tile_ftprint)
                    mosaic_new = TestTileContents.get_benchmark_tile(
                        dset_record.mdd,
                        os.path.join(os.path.dirname(
                                tile_contents.temp_tile_output_path),
                                     'mosaic_cache'),
                        tile_ftprint)
                    LOGGER.info("Calling load_and_check...")
                    ([data1, data2], dummy_nlayers) = \
                        TestLandsatTiler.load_and_check(
                        mosaic_benchmark,
                        mosaic_new,
                        tile_contents.band_stack.band_dict,
                        tile_contents.band_stack.band_dict)
                    LOGGER.info('Checking arrays ...')
                    if dset_record.mdd['processing_level'] == 'PQA':
                        ind = (data1 == data2)
                        # Check that differences are due to differing treatment
                        # of contiguity bit.
                        data1_diff = data1[~ind]
                        data2_diff = data2[~ind]
                        contiguity_diff =  \
                            np.logical_or(
                            np.bitwise_and(data1_diff, 1 << 8) == 0,
                            np.bitwise_and(data2_diff, 1 << 8) == 0)
                        assert contiguity_diff.all(), \
                            "mosaiced tile %s differs from benchmark %s" \
                            %(mosaic_new, mosaic_benchmark)
                    else:
                        diff_cmd = ["diff",
                                    "-I",
                                    "[Ff]ilename",
                                    "%s" %mosaic_benchmark,
                                    "%s" %mosaic_new
                                    ]
                        result = execute(diff_cmd, shell=False)
                        assert result['stdout'] == '', \
                            "Differences between vrt files"
                        assert result['stderr'] == '', \
                            "Error in system diff command"
                else:
                    LOGGER.info('... tile has no data')
                    tile_contents.remove()
            self.collection.commit_transaction()



def the_suite():
    "Runs the tests"""
    test_classes = [TestTileRecord]
    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)
    suite = unittest.TestSuite(suite_list)
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())








