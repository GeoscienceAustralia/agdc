"""
    test__dataset_record.py - tests for the DatasetRecord class
"""
import re
import os
import logging
import unittest
import dbutil
from landsat_dataset import LandsatDataset
from dataset_record import DatasetRecord
from abstract_ingester import AbstractIngester
from abstract_dataset import AbstractDataset
from math import floor
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
DATASET_CRS = [
    r'PROJCS["GDA94 / MGA zone 50",GEOGCS["GDA94",DATUM["Geocentric_Datum_' \
    r'of_Australia_1994",SPHEROID["GRS 1980",6378137,298.2572221010002,' \
    r'AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6283"]],PRIMEM["Greenwich"' \
    r',0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4283"]],' \
    r'PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],' \
    r'PARAMETER["central_meridian",117],PARAMETER["scale_factor",0.9996], ' \
    r'PARAMETER["false_easting",500000],PARAMETER["false_northing",10000000]' \
    r',UNIT["metre",1,AUTHORITY["EPSG","9001"]],AUTHORITY["EPSG","28350"]]'
    ,
    r'PROJCS["GDA94 / MGA zone 54",GEOGCS["GDA94",DATUM["Geocentric_Datum_' \
    r'of_Australia_1994",SPHEROID["GRS 1980",6378137,298.2572221010002,' \
    r'AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6283"]],PRIMEM["Greenwich"' \
    r',0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4283"]],' \
    r'PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],' \
    r'PARAMETER["central_meridian",141],PARAMETER["scale_factor",0.9996],' \
    r'PARAMETER["false_easting",500000],PARAMETER["false_northing",10000000]' \
    r',UNIT["metre",1,AUTHORITY["EPSG","9001"]],AUTHORITY["EPSG","28354"]]'
    ,
    r'PROJCS["GDA94 / MGA zone 51",GEOGCS["GDA94",DATUM["Geocentric_Datum_' \
    r'of_Australia_1994",SPHEROID["GRS 1980",6378137,298.2572221010002,' \
    r'AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6283"]],PRIMEM["Greenwich"' \
    r',0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4283"]],' \
    r'PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],' \
    r'PARAMETER["central_meridian",123],PARAMETER["scale_factor",0.9996],' \
    r'PARAMETER["false_easting",500000],PARAMETER["false_northing",10000000]' \
    r',UNIT["metre",1,AUTHORITY["EPSG","9001"]],AUTHORITY["EPSG","28351"]]'
    ,
    r'PROJCS["GDA94 / MGA zone 52",GEOGCS["GDA94",DATUM["Geocentric_Datum_' \
    r'of_Australia_1994",SPHEROID["GRS 1980",6378137,298.2572221010002,' \
    r'AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6283"]],PRIMEM["Greenwich"' \
    r',0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4283"]],' \
    r'PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],' \
    r'PARAMETER["central_meridian",129],PARAMETER["scale_factor",0.9996],' \
    r'PARAMETER["false_easting",500000],PARAMETER["false_northing",10000000]' \
    r',UNIT["metre",1,AUTHORITY["EPSG","9001"]],AUTHORITY["EPSG","28352"]]'
    ,
    r'PROJCS["GDA94 / MGA zone 53",GEOGCS["GDA94",DATUM["Geocentric_Datum_' \
    r'of_Australia_1994",SPHEROID["GRS 1980",6378137,298.2572221010002,' \
    r'AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6283"]],PRIMEM["Greenwich"' \
    r',0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4283"]],' \
    r'PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],' \
    r'PARAMETER["central_meridian",135],PARAMETER["scale_factor",0.9996],' \
    r'PARAMETER["false_easting",500000],PARAMETER["false_northing",10000000]' \
    r',UNIT["metre",1,AUTHORITY["EPSG","9001"]],AUTHORITY["EPSG","28353"]]'
    ,
    r'PROJCS["GDA94 / MGA zone 54",GEOGCS["GDA94",DATUM["Geocentric_Datum_' \
    r'of_Australia_1994",SPHEROID["GRS 1980",6378137,298.2572221010002,' \
    r'AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6283"]],PRIMEM["Greenwich"' \
    r',0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4283"]]' \
    r',PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],'
    r'PARAMETER["central_meridian",141],PARAMETER["scale_factor",0.9996],' \
    r'PARAMETER["false_easting",500000],PARAMETER["false_northing",10000000]' \
    r',UNIT["metre",1,AUTHORITY["EPSG","9001"]],AUTHORITY["EPSG","28354"]]']

# List of geotransforms, pixels and lines
DATASET_GEOTRANSFORM = [(273000.0, 25.0, 0.0, 6276000.0, 0.0, -25.0),
                        (98000.0, 25.0, 0.0, 7227000.0, 0.0, -25.0),
                        (166000.0, 25.0, 0.0, 7071000.0, 0.0, -25.0),
                        (524000.0, 25.0, 0.0, 7233000.0, 0.0, -25.0),
                        (220000.0, 25.0, 0.0, 6431000.0, 0.0, -25.0),
                        (580000.0, 25.0, 0.0, 6119000.0, 0.0, -25.0)]
DATASET_XPIXELS = [9601, 9361, 9401, 9561, 9441, 9761]
DATASET_YPIXELS = [8481, 8361, 8521, 8641, 8521, 8761]

# List of tile crs for datacube
TILE_CRS = ['EPSG:4326', 'EPSG:4326', 'EPSG:4326',
            'EPSG:4326', 'EPSG:4326', 'EPSG:4326']

#Values to be given to metadata_dict
SATELLITE_SENSOR = ['LS5-TM', 'LS7-ETM+', 'LS7-ETM+', 'LS7-ETM+', 'LS5-TM',
                   'LS7-ETM+']

DATASET_SATELLITE_TAG = \
    [re.match(r'([\w+]+)-([\w+]+)', sat_sen_string).group(1)
     for sat_sen_string in SATELLITE_SENSOR]
DATASET_SENSOR_NAME = \
    [re.match(r'([\w+]+)-([\w+]+)', sat_sen_string).group(2)
     for sat_sen_string in SATELLITE_SENSOR]


################ THE EXPECTED OUTPUT: ################
TOLERANCE = 1e-02 #tolerance in number of pixels for bbox calculation
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
class TestIngester(AbstractIngester):
    """An ingester class from which to get a datacube object"""
    def __init__(self):
        AbstractIngester.__init__(self)
    def find_datasets(self, source_dir):
        pass
    def open_dataset(self, dataset_path):
        pass

class TestDatasetRecord(unittest.TestCase):
    """Unit test for the DatasetRecord class"""
    MODULE = 'dataset_record'
    SUITE = 'DatasetRecord'

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

        # Set the DatasetRecord instance
        # Set an instance of the ingester to get a datacube object
        self.ingester = TestIngester()
        self.dataset = \
            LandsatDataset('/g/data/v10/test_resources/mph547/input/' \
                            'landsat_tiler/six_acquisitions/tiler_testing/' \
                            'Condition0/L1/2005-06/LS5_TM_OTH_P51_GALPGS01' \
                            '-002_112_084_20050626/scene01')
        self.ingester.collection.in_a_transaction = True
        self.ingester.collection.db = dbutil.TESTSERVER #Try this
        self.acquisition = \
            self.ingester.collection.create_acquisition_record(self.dataset)
        self.dset_record = DatasetRecord(self.ingester.collection,
                                         self.acquisition,
                                         self.dataset)
        #Create a metedata_dict, as if constructed by AbstractDataset.__init__
        #To be updated before we call dataset_record.get_coverage() method on
        #each dataset.
        self.dset_record.mdd = {}
        mdd = {'satellite_tag': 'LS7', 'sensor_name': 'ETM+',
               'processing_level': 'ORTHO'}
        self.dset_record.mdd.update(mdd)
    def tearDown(self):
        #
        # Flush the handler and remove it from the root logger.
        #

        self.handler.flush()

        root_logger = logging.getLogger()
        root_logger.removeHandler(self.handler)

    def test_get_bbox(self, tile_type_id=1):
        #pylint: disable=too-many-locals
        """Test the dataset_record.get_bbox() method.
        At the top of this file, constants define data pertaining to six actual
        landsat scenes. This includes assumed return values for the
        dataset.get_xxx() methods, Specifically:
        1. DATASET_CRS:          dataset.get_projection()
        2. DATASET_GEOTRANSFORM: dataset.get_geotransform()
        3. DATASET_XPIXELS:      dataset.x_pixels()
        4. DATASET_YPIXELS:      dataset.y_pixels()
        The constants also provide test data expected to be returned by the
        tested get_coverage methods:
        1. TILE_XLL, TILE_YLL,... : dataset bounding box in tile projection
                                    coordinates TILE_CRS
        """
        example_set = \
            zip(DATASET_CRS, TILE_CRS, DATASET_GEOTRANSFORM,
                DATASET_XPIXELS, DATASET_YPIXELS,
                zip(zip(TILE_XUL, TILE_YUL), zip(TILE_XUR, TILE_YUR),
                    zip(TILE_XLR, TILE_YLR), zip(TILE_XLL, TILE_YLL)))
        cube_tile_size = \
            (self.ingester.datacube.tile_type_dict[tile_type_id]['x_size'],
             self.ingester.datacube.tile_type_dict[tile_type_id]['y_size'])
        cube_pixels = \
            (self.ingester.datacube.tile_type_dict[tile_type_id]['x_pixels'],
             self.ingester.datacube.tile_type_dict[tile_type_id]['y_pixels'])
        #Check that bounding box in tile space is within 1% of a pixel size
        for example in example_set:
            dataset_crs, tile_crs, geotrans, \
                pixels, lines, dataset_bbox = example
            transformation = \
                self.dset_record.define_transformation(dataset_crs, tile_crs)
            #Determine the bounding quadrilateral of the dataset extent
            bbox = self.dset_record.get_bbox(transformation, geotrans,
                                             pixels, lines)
            #Check bounding box is as expected
            residual_in_pixels = \
                [((x2 - x1) * cube_pixels[0] / cube_tile_size[0],
                  (y2 - y1) * cube_pixels[1] / cube_tile_size[1])
                 for ((x1, y1), (x2, y2)) in zip(dataset_bbox, bbox)]
            assert all(abs(dx) < TOLERANCE and  abs(dy) < TOLERANCE
                       for (dx, dy) in residual_in_pixels), \
                       "bounding box calculation incorrect"

    def test_get_coverage(self, tile_type_id=1):
        #pylint: disable=too-many-locals
        """Test the methods called by the dataset_record.get_coverage() method.
        At the top of this file, constants define data pertaining to six actual
        landsat scenes. This includes assumed return values for the
        dataset.get_xxx() methods, Specifically:
        1. DATASET_CRS:          dataset.get_projection()
        2. DATASET_GEOTRANSFORM: dataset.get_geotransform()
        3. DATASET_XPIXELS:      dataset.x_pixels()
        4. DATASET_YPIXELS:      dataset.y_pixels()
        The constants also provide test data expected to be returned by the
        tested get_coverage methods:
        1. TILE_XLL, TILE_YLL,... : dataset bounding box in tile projection
                                    coordinates TILE_CRS
        2. DEFINITE_TILES: tiles in inner rectangle
        3. POSSIBLE_TILES: tiles in outer rectangle
        4. INTERSECTED_TILES: those tiles from the outer rectangle that
        intersect the dataset bounding box
        5. CONTAINED_TILES: those tiles from outer rectangle wholly contained
        in the dataset bounding box
        6. COVERAGE: the tiles to be returned from dataset_record.get_coverage
        """
        example_set = \
            zip(DATASET_CRS, TILE_CRS, DATASET_GEOTRANSFORM,
                DATASET_XPIXELS, DATASET_YPIXELS,
                DATASET_SATELLITE_TAG, DATASET_SENSOR_NAME,
                zip(zip(TILE_XUL, TILE_YUL), zip(TILE_XUR, TILE_YUR),
                    zip(TILE_XLR, TILE_YLR), zip(TILE_XLL, TILE_YLL)))
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
        for example in example_set:
            dataset_crs, tile_crs, geotrans, \
                pixels, lines, satellite_tag, sensor_name, \
            dummy_dataset_bbox = example
            #update mdd, for use by get_coverage() method
            self.dset_record.mdd['projection'] = dataset_crs
            self.dset_record.mdd['geotransform'] = geotrans
            self.dset_record.mdd['x_pixels'] = pixels
            self.dset_record.mdd['y_pixels'] = lines
            self.dset_record.mdd['satellite_tag'] = satellite_tag
            self.dset_record.mdd['sensor_name'] = sensor_name
            transformation = \
                self.dset_record.define_transformation(dataset_crs, tile_crs)
            #Determine the bounding quadrilateral of the dataset extent
            bbox = self.dset_record.get_bbox(transformation, geotrans,
                                             pixels, lines)
            #Get the definite and possible tiles from this dataset and
            #accumulate in running total
            definite_tiles, possible_tiles = \
                self.dset_record.get_definite_and_possible_tiles(
                bbox, cube_origin, cube_tile_size)
            total_definite_tiles = \
                total_definite_tiles.union(definite_tiles)
            total_possible_tiles = \
                total_possible_tiles.union(possible_tiles)
            #Get intersected tiles and accumulate in running total
            intersected_tiles = \
                self.dset_record.get_intersected_tiles(possible_tiles,
                                                       bbox,
                                                       cube_origin,
                                                       cube_tile_size)
            total_intersected_tiles = \
                total_intersected_tiles.union(intersected_tiles)
            #Take out intersected tiles from possibole tiles and get contained
            possible_tiles = possible_tiles.difference(intersected_tiles)
            contained_tiles = \
                self.dset_record.get_contained_tiles(possible_tiles,
                                                     bbox,
                                                     cube_origin,
                                                     cube_tile_size)
            total_contained_tiles = \
                total_contained_tiles.union(contained_tiles)
            #Use parent method to get touched tiles
            touched_tiles = \
                self.dset_record.get_touched_tiles(bbox,
                                                   cube_origin,
                                                   cube_tile_size)
            total_touched_tiles = total_touched_tiles.union(touched_tiles)
            #use parent method get_coverage to get coverage
            coverage = self.dset_record.get_coverage(tile_type_id)
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
