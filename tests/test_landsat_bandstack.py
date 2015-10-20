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
    test_landsat_bandstack.py - tests for the LandsatBandstack class
"""
# pylint: disable=too-many-public-methods
import re
import os
import logging
import unittest

from eotools.execute import execute

import agdc.dbutil as dbutil
from agdc.ingest.core import IngesterDataCube
from agdc.ingest.landsat import LandsatDataset
from agdc.ingest import AbstractIngester

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
# Constants
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

SCENE_VRT_DIR = '/g/data/v10/test_resources/benchmark/scene_vrt_files/'

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

class TestLandsatBandstack(unittest.TestCase):
    """Unit tests for the LandsatBandstack class"""
    MODULE = 'landsat_bandstack'
    SUITE = 'LandsatBandstack'

    INPUT_DIR = dbutil.input_directory(MODULE, SUITE)
    OUTPUT_DIR = dbutil.output_directory(MODULE, SUITE)
    EXPECTED_DIR = dbutil.expected_directory(MODULE, SUITE)

    TEMP_DIR = dbutil.temp_directory(MODULE, SUITE, 'output')
    TILE_ROOT_DIR = dbutil.tile_root_directory(MODULE, SUITE, 'output')
    BENCHMARK_DIR = os.path.join(EXPECTED_DIR, 'scene_vrt_files')

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
        logging.getLogger().addHandler(self.handler)

        # Create an empty database
        self.test_conn = None
        print 'Create an empty database'
        self.test_dbname = dbutil.random_name("test_landsat_bandstack")
        print 'Creating %s' %self.test_dbname
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

    def test_buildvrt_01(self):
        """Test of LandsatBandstack.buildvrt() method, test one."""
        self.check_buildvrt(0)

    def test_buildvrt_02(self):
        """Test of LandsatBandstack.buildvrt() method, test two."""
        self.check_buildvrt(1)

    def test_buildvrt_03(self):
        """Test of LandsatBandstack.buildvrt() method, test three."""
        self.check_buildvrt(2)

    def test_buildvrt_04(self):
        """Test of LandsatBandstack.buildvrt() method, test four."""
        self.check_buildvrt(3)

    def test_buildvrt_05(self):
        """Test of LandsatBandstack.buildvrt() method, test five."""
        self.check_buildvrt(4)

    def test_buildvrt_06(self):
        """Test of LandsatBandstack.buildvrt() method, test six."""
        self.check_buildvrt(5)

    def check_buildvrt(self, idataset):
        """Test the LandsatBandstack.buildvrt() method by comparing output to a
        file on disk"""

        assert idataset in range(len(DATASETS_TO_INGEST))

        print 'Testing Dataset %s' %DATASETS_TO_INGEST[idataset]
        dset = LandsatDataset(DATASETS_TO_INGEST[idataset])
        # Create a DatasetRecord instance so that we can access its
        # list_tile_types() method. In doing this we need to create a
        # collection object and entries on the acquisition and dataset
        # tables of the database.
        self.collection.begin_transaction()
        acquisition = \
            self.collection.create_acquisition_record(dset)
        dset_record = acquisition.create_dataset_record(dset)
        self.collection.commit_transaction()
        tile_type_list = dset_record.list_tile_types()
        #Assume dataset has tile_type = 1 only:
        tile_type_id = 1
        dataset_bands_dict = dset_record.get_tile_bands(tile_type_id)
        ls_bandstack = dset.stack_bands(dataset_bands_dict)
        temp_dir = self.collection.get_temp_tile_directory()
        ls_bandstack.buildvrt(temp_dir)
        # Get benchmark vrt for comparision
        vrt_benchmark = os.path.join(self.BENCHMARK_DIR,
                                     os.path.basename(ls_bandstack.vrt_name))
        diff_cmd = ["diff",
                    "-I",
                    "[Ff]ilename",
                    "%s" %vrt_benchmark,
                    "%s" %ls_bandstack.vrt_name
                    ]
        result = execute(diff_cmd, shell=False)
        if result['stdout'] != '':
            self.fail("Differences between vrt files:\n" + result['stdout'])
        if result['stderr'] != '':
            self.fail("Error in system diff command:\n" + result['stderr'])

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

def the_suite():
    "Runs the tests"""
    test_classes = [TestLandsatBandstack]
    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)
    suite = unittest.TestSuite(suite_list)
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())
















