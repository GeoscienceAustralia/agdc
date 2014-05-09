"""
    test__dataset_record.py - tests for the DatasetRecord class
"""
import re
import os
import logging
import unittest
import dbutil
import dataset_record
from abstract_ingester import DatasetError
#
# Set up logger.
#

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
# Constants
#

#List of dataset crs from sample datasets
DATASET_CRS = [
    r'PROJCS["GDA94 / MGA zone 50",GEOGCS["GDA94",DATUM["Geocentric_Datum_' \
    r'of_Australia_1994",SPHEROID["GRS 1980",6378137,298.2572221010002,' \
    r'AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6283"]],PRIMEM["Greenwich"' \
    r',0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4283"]],' \
    r'PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],' \
    r'PARAMETER["central_meridian",117],PARAMETER["scale_factor",0.9996], ' \
    r'PARAMETER["false_easting",500000],PARAMETER["false_northing",10000000' \
    r'],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AUTHORITY["EPSG","28350"]]'
    ,
    r'PROJCS["GDA94 / MGA zone 51",GEOGCS["GDA94",DATUM["Geocentric_Datum_' \
    r'of_Australia_1994",SPHEROID["GRS 1980",6378137,298.2572221010002,' \
    r'AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6283"]],PRIMEM["Greenwich"' \
    r',0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4283"]],' \
    r'PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],' \
    r'PARAMETER["central_meridian",123],PARAMETER["scale_factor",0.9996],' \
    r'PARAMETER["false_easting",500000],PARAMETER["false_northing",10000000' \
    r'],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AUTHORITY["EPSG","28351"]]'
    ]
#List of tile crs for
TILE_CRS = ['EPSG:4326', 'EPSG:4326']

#List of dataset bounding box coordinates
XLL = [114.4960138888889, 119.59266944444444]
YLL = [-35.5419, -28.361997222222225]
XLR = [117.14373611111112, 121.98962777777778]
YLR = [-35.567822222222226, -28.400869444444442]
XUL = [114.55270555555555, 119.65111111111112]
YUL = [-33.63163888888889, -26.44214722222222]
XUR = [117.14047777777779, 122.00699444444444]
YUR = [-33.65578611111111, -26.477969444444444]

GEOTRANSFORM = [(273000.0, 25.0, 0.0, 6276000.0, 0.0, -25.0),
                (166000.0, 25.0, 0.0, 7071000.0, 0.0, -25.0)]
XPIXELS = [9601, 9401]
YPIXELS = [8481, 8521]

#
# Test suite
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class (which has too
# many public methods according to pylint).
#

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
        print 'AAA%sBBB' %name

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
        self.dset_record = dataset_record.DatasetRecord(None, None, None)
    def tearDown(self):
        #
        # Flush the handler and remove it from the root logger.
        #

        self.handler.flush()

        root_logger = logging.getLogger()
        root_logger.removeHandler(self.handler)
    def test_get_bbox(self):
        """Test the spatial creation of a spatial reference given a coordinate
        reference system as well-known-text (WKT) or EPSG format"""
        TOLERANCE = 1e-05
        for example in \
                zip(DATASET_CRS, TILE_CRS, GEOTRANSFORM,
                    XPIXELS, YPIXELS, zip(zip(XUL, YUL), zip(XUR, YUR),
                                          zip(XLR, YLR), zip(XLL, YLL))):
            dataset_crs, tile_crs, geotrans, pixels, lines, bbox = example
            transformation = \
                self.dset_record.define_transformation(dataset_crs, tile_crs)
            #Determine the bounding quadrilateral of the dataset extent
            dataset_bbox = self.dset_record.get_bbox(transformation, geotrans,
                                         pixels, lines)
            res = [(x2 - x1, y2 - y1) for ((x1, y1), (x2, y2)) in
                       zip(bbox, dataset_bbox)]
            assert all(abs(dx) < TOLERANCE and  abs(dy) < TOLERANCE
                       for (dx, dy) in res)

def the_suite():
    "Runs the tests"""
    test_classes = [TestDatasetRecord]
    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)
    suite = unittest.TestSuite(suite_list)
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())
