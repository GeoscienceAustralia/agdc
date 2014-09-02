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
#     * Neither [copyright holder] nor the names of its contributors may be
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
    test_tile_contents.py - tests for the TileContents class
"""
# pylint: disable=too-many-public-methods
import re
import os
import logging
import sys
import unittest
import dbutil
#import landsat_bandstack
from abstract_ingester import AbstractIngester
from abstract_ingester import IngesterDataCube
from landsat_dataset import LandsatDataset
#import cube_util
from test_landsat_tiler import TestLandsatTiler
import ingest_test_data as TestIngest

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

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

class TestTileContents(unittest.TestCase):
    """Unit tests for the TileContents class"""
    #pylint: disable=too-many-instance-attributes
    ############################### User area #################################
    MODULE = 'tile_contents'
    SUITE = 'TileContents'
    # Set to true if we want to populate expected directory with results,
    # without doing comparision. Set to False if we want to put (often
    # a subset of) results in output directory and compare against the
    # previously populated expected directory.
    POPULATE_EXPECTED = False
    ###########################################################################


    INPUT_DIR = dbutil.input_directory(MODULE, SUITE)
    OUTPUT_DIR = dbutil.output_directory(MODULE, SUITE)
    EXPECTED_DIR = dbutil.expected_directory(MODULE, SUITE)

    if POPULATE_EXPECTED:
        destination_dir = 'expected'
    else:
        destination_dir = 'output'
    TEMP_DIR = dbutil.temp_directory(MODULE, SUITE, destination_dir)
    TILE_ROOT_DIR = dbutil.tile_root_directory(MODULE, SUITE, destination_dir)

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

        # Add a streamhandler to write output to console
        # self.stream_handler = logging.StreamHandler(stream=sys.stdout)
        # self.stream_handler.setLevel(logging.INFO)
        # self.stream_handler.setFormatter(logging.Formatter('%(message)s'))
        # LOGGER.addHandler(self.stream_handler)


        # Create an empty database
        self.test_conn = None
        self.test_dbname = dbutil.random_name("test_tile_contents")
        LOGGER.info('Creating %s', self.test_dbname)
        dbutil.TESTSERVER.create(self.test_dbname,
                                     self.INPUT_DIR, "hypercube_empty.sql")

        # Set the datacube configuration file to point to the empty database
        configuration_dict = {'dbname': self.test_dbname,
                              'temp_dir': self.TEMP_DIR,
                              'tile_root': self.TILE_ROOT_DIR}
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
        # self.stream_handler.flush()
        LOGGER.removeHandler(self.handler)
        # LOGGER.removeHandler(self.stream_handler)
        if self.test_dbname:
            if self.POPULATE_EXPECTED:
                dbutil.TESTSERVER.save(self.test_dbname, self.EXPECTED_DIR,
                            'hypercube_tile_contents.sql')
            else:
                #TODO: make dbase comaprision
                kkk=-1
            LOGGER.info('About to drop %s', self.test_dbname)
            dbutil.TESTSERVER.drop(self.test_dbname)

    @staticmethod
    def swap_dir_in_path(fullpath, dir1, dir2):
        """Given a pathname fullpath, replace right-most occurrence of dir1
        with dir2 and return the result."""
        dirname = fullpath
        leaf = None
        newpath_list = []
        while leaf != '':
            dirname, leaf = os.path.split(dirname)
            if leaf == dir1:
                newpath_list.append(dir2)
                break
            newpath_list.append(leaf)
        newpath = dirname
        newpath_list.reverse()
        for subdirectory in newpath_list:
            newpath = os.path.join(newpath, subdirectory)
        return newpath

    @staticmethod
    def get_benchmark_footprints(dset_dict, benchmark_dir):
        """Given a dataset_dict, parse the list of files in benchmark directory
        and generate a list of tile footprints."""
        # Get information from dataset that will be used to match
        # tile pathnames in the benchmark directory
        sat = dset_dict['satellite_tag'].upper()
        sensor = dset_dict['sensor_name'].upper()
        level = dset_dict['processing_level']
        ymd_str = re.match(r'(.*)T',
                           dset_dict['start_datetime'].isoformat()).group(1)
        #return
        # From the list of benchmark files get the list of footprints
        # for this dataset.
        pattern = re.compile(r'%s_%s_%s_(?P<xindex>[-]*\d+)_'
                             r'(?P<yindex>[-]*\d+)_'
                             r'.*%s.*ti[f]*$'
                             %(sat, sensor, level, ymd_str))
        matchobject_list = [re.match(pattern, filename).groupdict()
                            for filename in  os.listdir(benchmark_dir)
                            if re.match(pattern, filename)]
        footprints = [(int(m['xindex']), int(m['yindex']))
                      for m in matchobject_list]
        return footprints

    @staticmethod
    def get_benchmark_tile(dset_dict, benchmark_dir, footprint):
        """Given the dataset metadata dictionary and the benchmark directory,
        get the tile which matches the current footprint."""
        xindex, yindex = footprint
        sat = dset_dict['satellite_tag'].upper()
        sensor = dset_dict['sensor_name'].upper()
        if sensor == 'ETM+':
            sensor = 'ETM'
        level = dset_dict['processing_level']
        ymd_str = re.match(r'(.*)T',
                           dset_dict['start_datetime'].isoformat()).group(1)

        # Match .tif or .vrt
        file_pattern = r'%s_%s_%s_%03d_%04d_%s.*\.(tif{1,2}|vrt)$' \
            %(sat, sensor, level, xindex, yindex, ymd_str)
        filelist = [filename for filename in os.listdir(benchmark_dir)
                    if re.match(file_pattern, filename)]
        assert len(filelist) <= 1, "Unexpected multiple benchmark tiles"
        if len(filelist) == 1:
            return os.path.join(benchmark_dir, filelist[0])
        else:
            return None

    def test_reproject(self):
        """Test the Landsat tiling process method by comparing output to a
        file on disk."""
        # pylint: disable=too-many-locals
        #For each processing_level, and dataset keep record of those
        #tile footprints in the benchmark set.
        for iacquisition in range(len(TestIngest.DATASETS_TO_INGEST['PQA'])):
            for processing_level in ['PQA', 'NBAR', 'ORTHO']:
                #Skip all but PQA and ORTHO for first dataset.
                #TODO program this in as a paramter of the suite
                #if iacquisition > 0:
                #    continue
                #if processing_level in ['NBAR']:
                #    continue
                dataset_path =  \
                    TestIngest.DATASETS_TO_INGEST[processing_level]\
                    [iacquisition]
                LOGGER.info('Testing Dataset %s', dataset_path)
                dset = LandsatDataset(dataset_path)
                #return
                # Create a DatasetRecord instance so that we can access its
                # list_tile_types() method. In doing this we need to create a
                # collection object and entries on the acquisition and dataset
                # tables of the database.
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
                # Form scene vrt
                ls_bandstack.buildvrt(self.collection.
                                      get_temp_tile_directory())
                # Reproject scene data onto selected tile coverage
                tile_footprint_list = dset_record.get_coverage(tile_type_id)
                LOGGER.info('coverage=%s', str(tile_footprint_list))
                for tile_footprint in tile_footprint_list:
                    #Skip all but PQA and ORTHO for first dataset.
                    #TODO program this in as a paramter of the suite
                    #if tile_footprint not in [(117, -35), (115, -34)]:
                    #    continue
                    tile_contents = \
                        self.collection.create_tile_contents(tile_type_id,
                                                             tile_footprint,
                                                             ls_bandstack)
                    LOGGER.info('reprojecting for %s tile %s...',
                                processing_level, str(tile_footprint))
                    tile_contents.reproject()
                    LOGGER.info('...done')

                    if self.POPULATE_EXPECTED:
                        continue
                    #Do comparision with expected results
                    tile_benchmark = self.swap_dir_in_path(tile_contents.
                                                           tile_output_path,
                                                           'output',
                                                           'expected')
                    if tile_contents.has_data():
                        LOGGER.info('Tile %s has data', str(tile_footprint))
                        LOGGER.info("Comparing test output with benchmark:\n"\
                                        "benchmark: %s\ntest output: %s",
                                    tile_benchmark,
                                    tile_contents.temp_tile_output_path)
                        # Do comparision with expected directory
                        LOGGER.info('Calling load and check ...')
                        ([data1, data2], dummy_nlayers) = \
                            TestLandsatTiler.load_and_check(
                            tile_benchmark,
                            tile_contents.temp_tile_output_path,
                            tile_contents.band_stack.band_dict,
                            tile_contents.band_stack.band_dict)
                        LOGGER.info('Checking arrays ...')
                        if not (data1 == data2).all():
                            self.fail("Reprojected tile differs " \
                                          "from %s" %tile_benchmark)
                        LOGGER.info('...OK')
                    else:
                        LOGGER.info('No data in %s', str(tile_footprint))
                    LOGGER.info('-' * 80)
                self.collection.commit_transaction()

def the_suite():
    "Runs the tests"""
    test_classes = [TestTileContents]
    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)
    suite = unittest.TestSuite(suite_list)
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())






