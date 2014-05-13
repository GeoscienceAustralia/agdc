"""
    test_landsat_dataset.py - unit tests for the landsat_dataset module.
"""

import unittest
import os
import subprocess

import dbutil
from landsat_dataset import LandsatDataset

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class (which has too
# many public methods according to pylint).
#


class TestLandsatDataset(unittest.TestCase):
    """Unit tests for the LandsatDataset class."""

    MODULE = 'landsat_dataset'
    SUITE = 'TestLandsatDataset'

    INPUT_DIR = dbutil.input_directory(MODULE, SUITE)
    OUTPUT_DIR = dbutil.output_directory(MODULE, SUITE)
    EXPECTED_DIR = dbutil.expected_directory(MODULE, SUITE)

    ORTHO_DIR = 'tiler_testing/Condition1/L1/2012-05'
    ORTHO_SCENE = 'LS7_ETM_OTH_P51_GALPGS01-002_092_089_20120507'

    NBAR_DIR = 'tiler_testing/Condition1/NBAR/2012-05'
    NBAR_SCENE = 'LS7_ETM_NBAR_P54_GANBAR01-002_092_089_20120507'

    PQ_DIR = 'tiler_testing/Condition1/PQ/2012-05'
    PQ_SCENE = 'LS7_ETM_PQ_P55_GAPQ01-002_092_089_20120507'

    METADATA_KEYS = ['dataset_path',
                     'satellite_tag',
                     'sensor_name',
                     'processing_level',
                     'x_ref',
                     'y_ref',
                     'start_datetime',
                     'end_datetime',
                     'datetime_processed',
                     'dataset_size',
                     'll_lon',
                     'll_lat',
                     'lr_lon',
                     'lr_lat',
                     'ul_lon',
                     'ul_lat',
                     'ur_lon',
                     'ur_lat',
                     'projection',
                     'll_x',
                     'll_y',
                     'lr_x',
                     'lr_y',
                     'ul_x',
                     'ul_y',
                     'ur_x',
                     'ur_y',
                     'x_pixels',
                     'y_pixels',
                     'gcp_count',
                     'mtl_text',
                     'cloud_cover',
                     'xml_text',
                     'geo_transform'
                     ]

    LARGE_METADATA_KEYS = ['mtl_text', 'xml_text']

    SMALL_METADATA_KEYS = [k for k in METADATA_KEYS
                           if k not in LARGE_METADATA_KEYS]

    def test_build_metadata_dict(self):
        """Test for the build_metadata_dict method.

        This method is actually defined in AbstractDataset, but
        an AbstractDataset cannot be instantiated, so it is tested here.
        """

        ortho_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                               self.ORTHO_DIR,
                                               self.ORTHO_SCENE))

        mdd = ortho_ds.build_metadata_dict()

        self.assertEqual(set(self.METADATA_KEYS), set(mdd.keys()))

        for k in mdd.keys():
            mdd_value = mdd[k]
            accessor_name = 'get_' + k
            accessor_value = getattr(ortho_ds, accessor_name)()
            self.assertEqual(mdd_value, accessor_value)

    def test_ortho_scene(self):
        """Test for an ORTHO (level 1) scene."""

        ortho_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                               self.ORTHO_DIR,
                                               self.ORTHO_SCENE))
        mdd = ortho_ds.build_metadata_dict()

        self.dump_metadata('ortho_metadata.txt', mdd, self.SMALL_METADATA_KEYS)
        self.dump_string('ortho_xml.xml', mdd['xml_text'])
        self.dump_string('ortho_mtl.txt', mdd['mtl_text'])

        self.check_file('ortho_metadata.txt')
        self.check_file('ortho_xml.xml')
        self.check_file('ortho_mtl.txt')

    def test_nbar_scene(self):
        """Test for an NBAR scene."""

        nbar_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                              self.NBAR_DIR,
                                              self.NBAR_SCENE))
        mdd = nbar_ds.build_metadata_dict()

        self.dump_metadata('nbar_metadata.txt', mdd, self.SMALL_METADATA_KEYS)
        self.dump_string('nbar_xml.xml', mdd['xml_text'])
        self.assertIsNone(mdd['mtl_text'])

        self.check_file('nbar_metadata.txt')
        self.check_file('nbar_xml.xml')

#    def test_pq_scene(self):
#        """Test for a PQ scene."""
#
#        pq_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
#                                            self.PQ_DIR,
#                                            self.PQ_SCENE))
#        mdd = pq_ds.build_metadata_dict()

    def dump_metadata(self, file_name, mdd, md_keys):
        """Dump a metadata dictionary to a file.

        file_name - The name of the file. This will be created in the
            output directory.
        mdd - The dictionary containing the metadata.
        md_keys - A list of keys to use for the dump. These must be in mdd.
        """

        out_file_path = os.path.join(self.OUTPUT_DIR, file_name)
        out_file = open(out_file_path, 'w')

        for k in md_keys:
            print >> out_file, "%s: %s" % (k, mdd[k])

        out_file.close()

    def dump_string(self, file_name, string):
        """Dump a string to a file.

        file_name - The name of the file. This will be created in the
            output directory.
        string - The string to be dumped.
        """

        out_file_path = os.path.join(self.OUTPUT_DIR, file_name)
        out_file = open(out_file_path, 'w')
        out_file.write(string)
        out_file.close()

    def check_file(self, file_name):
        """Checks to see if a file is as expected.

        This checks the file in the output directory against the file
        of the same name in the expected directory. It uses the diff
        program to generate useful output in case of a difference. It
        skips the test if the expected file does not exist.
        """

        output_path = os.path.join(self.OUTPUT_DIR, file_name)
        expected_path = os.path.join(self.EXPECTED_DIR, file_name)

        if not os.path.isfile(expected_path):
            self.skipTest("Expected file '%s' not found." % file_name)
        else:
            try:
                subprocess.check_output(['diff',
                                         output_path,
                                         expected_path])
            except subprocess.CalledProcessError as err:
                self.fail("File '%s' not as expected:\n" % file_name +
                          err.output)

#
# Define test suites
#


def the_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [TestLandsatDataset]

    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)

    suite = unittest.TestSuite(suite_list)

    return suite

#
# Run unit tests if in __main__
#

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())
