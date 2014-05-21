import re
import os
import logging
import unittest
import dbutil
import landsat_bandstack
from abstract_ingester import AbstractIngester
from math import floor
import cube_util

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
# Constants
#
EXAMPLE_VRT = ''

class TestIngester(AbstractIngester):
    """An ingester class from which to get a datacube object"""
    def __init__(self):
        AbstractIngester.__init__(self)
    def find_datasets(self, source_dir):
        pass
    def open_dataset(self, dataset_path):
        pass

class TestLandsatBandstack(unittest.TestCase):
    """Unit tests for the LandsatBandstack class"""
    MODULE = 'landsat_bandstack'
    SUITE = 'LandsatBandstack'

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
        # Set an instance of the ingester to get a datacube object
        self.ingester = TestIngester()
        # Set a DatasetRecord instance
        self.dset_record = \
            dataset_record.DatasetRecord(self.ingester.collection, None, None)   
        # Set the LandsatBandstack instance and pass it a metadata_dict
        # pertaining to the single acquistion in /g/data/v10/test_resources
        self.dset_record.mdd = {}
        metadat_dict = {'dataset_path':
                        'satellite_tag':
                        'sensor_name':
                        'start_datetime':
                        'end_datetime':
                        'x_ref':
                        'y_ref':
                        }
        self.dset_record.mdd.update(metadata_dict)
        tile_type_set, dataset_bands = \
            self.dset_record.list_tile_types_and_bands()
        tile_type_id = 1
        tile_type_info = self.ingester.datacube.tile_type_dict[tile_type_id]
        band_dict = dataset_bands[tile_type_id]
        self.landsat_bandstack = LandsatBandstack(band_dict, metadata_dict)
        #Set up a vrt benchmark for comparision
        

    def test_buildvrt(self, vrt_benchmark):
        """Test the LandsatBandstack.buildvrt() method by comparing output to a
        file on disk"""
        self.landsat_bandstack.buildvrt(self.ingester.datacube.temp_dir)
        diff_cmd = ["diff %s %s" %(self.landsat_bandstack.vrt_name,
                                   EXAMPLE_VRT)] 
        result = cube_util.execute(diff_cmd)
        #TODO check whether there are any valid differences such as creation
        #time 
        assert result['stdout'] == ''

def the_suite():
    "Runs the tests"""
    test_classes = [TestTileContents]
    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)
    suite = unittest.TestSuite(suite_list)
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())


    
        
            
