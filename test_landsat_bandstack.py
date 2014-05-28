import re
import os
import logging
import unittest
import dbutil
import landsat_bandstack
from abstract_ingester import AbstractIngester
from abstract_ingester import IngesterDataCube
from math import floor
import cube_util

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
# Constants
#
EXAMPLE_VRT = ''


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
        self.test_dbname = dbutil.random_name("test_landsat_bandstack")
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

    def xxxxsetUp(self):
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
        # Set an instance of the ingester to get a datacube object
        self.ingester = TestIngester()
        # Set a DatasetRecord instance
        self.dset_record = \
            dataset_record.DatasetRecord(self.ingester.collection, None, None)   
        # Set the LandsatBandstack instance and pass it a metadata_dict
        # pertaining to the single acquistion in /g/data/v10/test_resources
        self.dset_record.mdd = {}
        #metadata_dict = {'dataset_path':
        #                'satellite_tag':
        #                'sensor_name':
        #                'start_datetime':
        #                'end_datetime':
        #                'x_ref':
        #                'y_ref':
        #                }
        self.dset_record.mdd.update(metadata_dict)
        tile_type_set, dataset_bands = \
            self.dset_record.list_tile_types_and_bands()
        tile_type_id = 1
        tile_type_info = self.ingester.datacube.tile_type_dict[tile_type_id]
        band_dict = dataset_bands[tile_type_id]
        self.landsat_bandstack = LandsatBandstack(band_dict, metadata_dict)
        #Set up a vrt benchmark for comparision
        
    def test_test(self):
        pass

    def xxxtest_buildvrt(self, vrt_benchmark):
        """Test the LandsatBandstack.buildvrt() method by comparing output to a
        file on disk"""
        self.landsat_bandstack.buildvrt(self.ingester.datacube.temp_dir)
        diff_cmd = ["diff %s %s" %(self.landsat_bandstack.vrt_name,
                                   EXAMPLE_VRT)] 
        result = cube_util.execute(diff_cmd)
        #TODO check whether there are any valid differences such as creation
        #time 
        assert result['stdout'] == ''

    def xxxtest_buildvrt(self, vrt_benchmark):
        """Test the LandsatBandstack.buildvrt() method by comparing output to a
        file on disk"""
        self.landsat_bandstack.buildvrt(self.ingester.datacube.temp_dir)
        diff_cmd = ["diff %s %s" %(self.landsat_bandstack.vrt_name,
                                   EXAMPLE_VRT)] 
        result = cube_util.execute(diff_cmd)
        #TODO check whether there are any valid differences such as creation
        #time 
        assert result['stdout'] == ''

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


    
        
            
