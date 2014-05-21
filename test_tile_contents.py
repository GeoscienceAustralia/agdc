import re
import os
import logging
import unittest
import dbutil
import landsat_bandstack
from abstract_ingester import AbstractIngester
import cube_util
from test_landsat_tiler import load_and_check

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
# Constants
#
TILE_FOOTPRINT = (123, -023)
BENCHMARK_NBAR_VRT = ''
BENCHMARK_NBAR_TILE = ''
BENCHMARK_ORTHO_VRT = ''
BENCHMARK_ORTHO_TILE = ''
BENCHMARK_PQA_VRT = ''
BENCHMARK_PQA_TILE = ''

class TestIngester(AbstractIngester):
    """An ingester class from which to get a datacube object"""
    def __init__(self):
        AbstractIngester.__init__(self)
    def find_datasets(self, source_dir):
        pass
    def open_dataset(self, dataset_path):
        pass

class TestTileContents(unittest.TestCase):
    """Unit tests for the LandsatBandstack class"""
    MODULE = 'tile_contents'
    SUITE = 'TileContents'

    OUTPUT_DIR = dbutil.output_directory(MODULE, SUITE)
    EXPECTED_DIR = dbutil.expected_directory(MODULE, SUITE)

    EXAMPLE_TILE = '/g/data/v10/test_resources/benchmark_results/gdalwarp/...'
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
        metadata_dict = {'dataset_path': '/g/data/v10/test_resources/scenes/...' #TODO fill in
                        'satellite_tag':
                        'sensor_name':
                        'processing_level':
                        'start_datetime':
                        'end_datetime':
                        'x_ref':
                        'y_ref':
                        }
        self.dset_record.mdd.update(metadata_dict)
        tile_type_set, dataset_bands = \
            self.dset_record.list_tile_types_and_bands()
        tile_type_id = 1
        self.tile_type_info = self.ingester.datacube.tile_type_dict[tile_type_id]
        band_dict = dataset_bands[tile_type_id]
        self.landsat_bandstack = LandsatBandstack(band_dict, metadata_dict)        
        #Set up a benchmark reprojected tile for comparision

    def test_NBAR_tiling(self):
        """Test the LandsatBandstack.buildvrt() and
        TileContentsmethod.reproject() methods 
        by comparing output to benchmarked files on disk"""
        new_processing_level = 'NBAR'
        number_bands = 6
        benchmark_vrt = BENCHMARK_NBAR_VRT
        benchmark_tile = BENCHMARK_NBAR_TILE

        self.switch_metadata_dict(self.dset_record.mdd, new_processing_level)
        tile_type_set, dataset_bands = \
            self.dset_record.list_tile_types_and_bands()
        tile_type_id = 1
        tile_type_info = self.ingester.datacube.tile_type_dict[tile_type_id]
        band_dict = dataset_bands[tile_type_id]
        assert len(band_dict) == 6, "Expected %d bands for %s got %d" \
            %(number_bands, new_processing_level, len(band_dict))
        landsat_bandstack = LandsatBandstack(band_dict, self.dset_record.mdd)
        self.landsat_bandstack.buildvrt(self.ingester.datacube.temp_dir)
        diff_cmd = ["diff %s %s" %(self.landsat_bandstack.vrt_name,
                                   benchmark_vrt)]
        result = cube_util.execute(diff_cmd)
        #TODO check whether there are any valid differences such as creation
        #time 
        assert result['stdout'] == ''
        #TODO set TILE_FOOTPRINT
        tile_footprint = TILE_FOOTPRINT
        tile_contents = TileContents(self.ingester.datacube.temp_dir,
                                     tile_type_info, tile_footprint,
                                     landsat_bandstack)
        tile_contents.reproject()
        ([arr1, arr2], ndims) =
        test_landsat_tiler.load_and_check(benchmark_tile,
                                          tile_contents.temp_tile_output_path,
                                          band_dict, band_dict)
        assert ndims == 6, "Expected load_and_check to return %d for %s " \
            "got %d" %(number_bands, new_processing_level, ndims)
        max_diff = abs(arr1 - arr2).max()
        assert max_diff == 0.0, "Maximum difference of %f between benchmark" \
            "tile:\n%s and test result:\n%s" \
            %(max_diff, benchmark_tile, tile_contents.temp_tile_output_path)
        

    def test_ORTHO_tiling(self):
        """Test the LandsatBandstack.buildvrt() and
        TileContentsmethod.reproject() methods 
        by comparing output to benchmarked files on disk"""
        new_processing_level = 'ORTHO'
        number_bands = 2
        benchmark_vrt = BENCHMARK_ORTHO_VRT
        benchmark_tile = BENCHMARK_ORTHO_TILE

        self.switch_metadata_dict(self.dset_record.mdd, new_processing_level)
        tile_type_set, dataset_bands = \
            self.dset_record.list_tile_types_and_bands()
        tile_type_id = 1
        tile_type_info = self.ingester.datacube.tile_type_dict[tile_type_id]
        band_dict = dataset_bands[tile_type_id]
        assert len(band_dict) == 6, "Expected %d bands for %s got %d" \
            %(number_bands, new_processing_level, len(band_dict))
        landsat_bandstack = LandsatBandstack(band_dict, self.dset_record.mdd)
        self.landsat_bandstack.buildvrt(self.ingester.datacube.temp_dir)
        diff_cmd = ["diff %s %s" %(self.landsat_bandstack.vrt_name,
                                   benchmark_vrt)]
        result = cube_util.execute(diff_cmd)
        #TODO check whether there are any valid differences such as creation
        #time 
        assert result['stdout'] == ''
        #TODO set TILE_FOOTPRINT
        tile_footprint = TILE_FOOTPRINT
        tile_contents = TileContents(self.ingester.datacube.temp_dir,
                                     tile_type_info, tile_footprint,
                                     landsat_bandstack)
        tile_contents.reproject()
        ([arr1, arr2], ndims) =
        test_landsat_tiler.load_and_check(benchmark_tile,
                                          tile_contents.temp_tile_output_path,
                                          band_dict, band_dict)
        assert ndims == 6, "Expected load_and_check to return %d for %s " \
            "got %d" %(number_bands, new_processing_level, ndims)
        max_diff = abs(arr1 - arr2).max()
        assert max_diff == 0.0, "Maximum difference of %f between benchmark" \
            "tile:\n%s and test result:\n%s" \
            %(max_diff, benchmark_tile, tile_contents.temp_tile_output_path)
        


    def test_PQA_tiling(self):
        """Test the LandsatBandstack.buildvrt() and
        TileContentsmethod.reproject() methods 
        by comparing output to benchmarked files on disk"""
        new_processing_level = 'PQA'
        number_bands = 1
        benchmark_vrt = BENCHMARK_PQA_VRT
        benchmark_tile = BENCHMARK_PQA_TILE

        self.switch_metadata_dict(self.dset_record.mdd, new_processing_level)
        tile_type_set, dataset_bands = \
            self.dset_record.list_tile_types_and_bands()
        tile_type_id = 1
        tile_type_info = self.ingester.datacube.tile_type_dict[tile_type_id]
        band_dict = dataset_bands[tile_type_id]
        assert len(band_dict) == 6, "Expected %d bands for %s got %d" \
            %(number_bands, new_processing_level, len(band_dict))
        landsat_bandstack = LandsatBandstack(band_dict, self.dset_record.mdd)
        self.landsat_bandstack.buildvrt(self.ingester.datacube.temp_dir)
        diff_cmd = ["diff %s %s" %(self.landsat_bandstack.vrt_name,
                                   benchmark_vrt)]
        result = cube_util.execute(diff_cmd)
        #TODO check whether there are any valid differences such as creation
        #time 
        assert result['stdout'] == ''
        #TODO set TILE_FOOTPRINT
        tile_footprint = TILE_FOOTPRINT
        tile_contents = TileContents(self.ingester.datacube.temp_dir,
                                     tile_type_info, tile_footprint,
                                     landsat_bandstack)
        tile_contents.reproject()
        ([arr1, arr2], ndims) =
        test_landsat_tiler.load_and_check(benchmark_tile,
                                          tile_contents.temp_tile_output_path,
                                          band_dict, band_dict)
        assert ndims == 6, "Expected load_and_check to return %d for %s " \
            "got %d" %(number_bands, new_processing_level, ndims)
        max_diff = abs(arr1 - arr2).max()
        assert max_diff == 0.0, "Maximum difference of %f between benchmark" \
            "tile:\n%s and test result:\n%s" \
            %(max_diff, benchmark_tile, tile_contents.temp_tile_output_path)
        
    @staticmethod
    def switch_metadata_dict(mdd, new_level):
        """Switch the metadata dict values to reflect the new processing level"""
        old_level = mdd['processing_level']
        old_path = mdd['dataset_path']
        mdd['dataset_path'] = self.switch_dataset_path(old_path, old_level, new_level)
        mdd['processing_level'] = new_level

    @staticmethod
    def switch_dataset_path(old_path, old_level, new_level):
        """Switch full path to dataset based on the processing level change"""
        olddir, oldfile = os.path.split(old_path)
        if old_level == 'ORTHO':
            old_str = 'L1'
        if new_level == 'ORTHO':
            new_str = 'L1'
        newdir = re.sub(old_str, new_str, olddir)
        newfile = re.sub(old_level, new_level, oldfile)
        return os.path.join(newdir, newfile)

def the_suite():
    "Runs the tests"""
    test_classes = [TestTileContents]
    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)
    suite = unittest.TestSuite(suite_list)
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())
    
            
