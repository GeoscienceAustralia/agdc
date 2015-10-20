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
    test_landsat_dataset.py - unit tests for the landsat_dataset module.
"""

import unittest
import os
import subprocess
import logging

from agdc import dbutil
from agdc.ingest.landsat import LandsatDataset

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

    ORTHO_DIR = 'dataset_testing/L1/2012-05'
    ORTHO_SCENE = 'LS7_ETM_OTH_P51_GALPGS01-002_092_089_20120507'

    NBAR_DIR = 'dataset_testing/NBAR/2012-05'
    NBAR_SCENE = 'LS7_ETM_NBAR_P54_GANBAR01-002_092_089_20120507'

    PQ_DIR = 'dataset_testing/PQ/2012-05'
    PQ_SCENE = 'LS7_ETM_PQ_P55_GAPQ01-002_092_089_20120507'

    FC_DIR = 'dataset_testing/FC/2012-05'
    FC_SCENE = 'LS7_ETM_FC_P54_GAFC01-002_092_089_20120507'

    ORTHO8_DIR = 'dataset_testing/L1/2014-03'
    ORTHO8_SCENE = 'LS8_OLITIRS_OTH_P51_GALPGS01-002_089_082_20140313'

    NBAR8_DIR = 'dataset_testing/NBAR/2014-03'
    NBAR8_SCENE = 'LS8_OLI_TIRS_NBAR_P54_GANBAR01-002_089_082_20140313'

    PQ8_DIR = 'dataset_testing/PQ/2014-03'
    PQ8_SCENE = 'LS8_OLI_TIRS_PQ_P55_GAPQ01-002_089_082_20140313'

    FC8_DIR = 'dataset_testing/FC/2014-03'
    FC8_SCENE = 'LS8_OLI_TIRS_FC_P54_GAFC01-002_089_082_20140313'

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
                     'geo_transform',
                     'pq_tests_run'
                     ]

    LARGE_METADATA_KEYS = ['mtl_text', 'xml_text']

    SMALL_METADATA_KEYS = [k for k in METADATA_KEYS
                           if k not in LARGE_METADATA_KEYS]

    CROSSCHECK_KEYS_ONE = ['satellite_tag',
                           'sensor_name',
                           'x_ref',
                           'y_ref',
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
                           'cloud_cover',
                           'geo_transform'
                           ]

    CROSSCHECK_KEYS_TWO = CROSSCHECK_KEYS_ONE + ['start_datetime',
                                                 'end_datetime']

    @classmethod
    def setUpClass(cls):
        """Set up logging for eotools.drivers._scene_dataset.SceneDataset debug output."""

        cls.SD_LOGGER = logging.getLogger('eotools.drivers._scene_dataset')
        cls.SD_HANDLER = logging.FileHandler(os.path.join(cls.OUTPUT_DIR,
                                                          'scene_dataset.log'),
                                             mode='w')
        cls.SD_LOGGER.addHandler(cls.SD_HANDLER)
        cls.SD_OLD_LEVEL = cls.SD_LOGGER.level
        cls.SD_LOGGER.setLevel(logging.DEBUG)

    @classmethod
    def tearDownClass(cls):
        """Clean up _scene_dataset logging."""

        cls.SD_LOGGER.setLevel(cls.SD_OLD_LEVEL)
        cls.SD_LOGGER.removeHandler(cls.SD_HANDLER)
        cls.SD_HANDLER.close()

    def setUp(self):
        self.SD_LOGGER.debug("")
        self.SD_LOGGER.debug("---%s" + "-"*(72-(len(self._testMethodName)+3)),
                             self._testMethodName)
        self.SD_LOGGER.debug("")

    def tearDown(self):
        self.SD_LOGGER.debug("")
        self.SD_LOGGER.debug("-"*72)
        self.SD_LOGGER.debug("")
        self.SD_HANDLER.flush()

    def test_build_metadata_dict(self):
        """Test for the build_metadata_dict method.

        This method is actually defined in AbstractDataset, but
        an AbstractDataset cannot be instantiated, so it is tested here.
        """

        ortho_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                               self.ORTHO_DIR,
                                               self.ORTHO_SCENE))

        mdd = ortho_ds.metadata_dict

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
        mdd = ortho_ds.metadata_dict

        self.dump_metadata('ortho_metadata.txt', mdd, self.SMALL_METADATA_KEYS)
        self.dump_string('ortho_xml.xml', mdd['xml_text'])
        self.dump_string('ortho_mtl.txt', mdd['mtl_text'])

        self.check_file('ortho_metadata.txt')
        self.check_file('ortho_xml.xml')
        self.check_file('ortho_mtl.txt')

    def test_ortho8_scene(self):
        """Test for a Landsat 8 ORTHO (level 1) scene."""

        ortho_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                               self.ORTHO8_DIR,
                                               self.ORTHO8_SCENE))
        mdd = ortho_ds.metadata_dict

        self.dump_metadata('ortho8_metadata.txt', mdd,
                           self.SMALL_METADATA_KEYS)
        self.dump_string('ortho8_xml.xml', mdd['xml_text'])
        self.dump_string('ortho8_mtl.txt', mdd['mtl_text'])

        self.check_file('ortho8_metadata.txt')
        self.check_file('ortho8_xml.xml')
        self.check_file('ortho8_mtl.txt')

    def test_nbar_scene(self):
        """Test for an NBAR scene."""

        nbar_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                              self.NBAR_DIR,
                                              self.NBAR_SCENE))
        mdd = nbar_ds.metadata_dict

        self.dump_metadata('nbar_metadata.txt', mdd, self.SMALL_METADATA_KEYS)
        self.dump_string('nbar_xml.xml', mdd['xml_text'])
        self.assertIsNone(mdd['mtl_text'])

        self.check_file('nbar_metadata.txt')
        self.check_file('nbar_xml.xml')

    def test_nbar8_scene(self):
        """Test for a Landsat 8 NBAR scene."""

        nbar_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                              self.NBAR8_DIR,
                                              self.NBAR8_SCENE))
        mdd = nbar_ds.metadata_dict

        self.dump_metadata('nbar8_metadata.txt', mdd, self.SMALL_METADATA_KEYS)
        self.dump_string('nbar8_xml.xml', mdd['xml_text'])
        self.assertIsNone(mdd['mtl_text'])

        self.check_file('nbar8_metadata.txt')
        self.check_file('nbar8_xml.xml')

    def test_pq_scene(self):
        """Test for a Pixel Quality scene."""

        pq_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                            self.PQ_DIR,
                                            self.PQ_SCENE))
        mdd = pq_ds.metadata_dict

        self.dump_metadata('pq_metadata.txt', mdd, self.SMALL_METADATA_KEYS)
        self.dump_string('pq_xml.xml', mdd['xml_text'])
        self.assertIsNone(mdd['mtl_text'])

        self.check_file('pq_metadata.txt')
        self.check_file('pq_xml.xml')

    def test_pq8_scene(self):
        """Test for a Landsat 8 Pixel Quality scene."""

        pq_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                            self.PQ8_DIR,
                                            self.PQ8_SCENE))
        mdd = pq_ds.metadata_dict

        self.dump_metadata('pq8_metadata.txt', mdd, self.SMALL_METADATA_KEYS)
        self.dump_string('pq8_xml.xml', mdd['xml_text'])
        self.assertIsNone(mdd['mtl_text'])

        self.check_file('pq8_metadata.txt')
        self.check_file('pq8_xml.xml')

    def test_fc_scene(self):
        """Test for a Fractional Cover scene."""

        fc_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                            self.FC_DIR,
                                            self.FC_SCENE))
        mdd = fc_ds.metadata_dict

        self.dump_metadata('fc_metadata.txt', mdd, self.SMALL_METADATA_KEYS)
        self.dump_string('fc_xml.xml', mdd['xml_text'])
        self.assertIsNone(mdd['mtl_text'])

        self.check_file('fc_metadata.txt')
        self.check_file('fc_xml.xml')

    def test_fc8_scene(self):
        """Test for a Landsat 8 Fractional Cover scene."""

        fc_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                            self.FC8_DIR,
                                            self.FC8_SCENE))
        mdd = fc_ds.metadata_dict

        self.dump_metadata('fc8_metadata.txt', mdd, self.SMALL_METADATA_KEYS)
        self.dump_string('fc8_xml.xml', mdd['xml_text'])
        self.assertIsNone(mdd['mtl_text'])

        self.check_file('fc8_metadata.txt')
        self.check_file('fc8_xml.xml')

    def test_crosscheck_ortho_nbar(self):
        """Cross-check metadata between ortho and nbar datasets."""

        ortho_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                               self.ORTHO_DIR,
                                               self.ORTHO_SCENE))

        nbar_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                              self.NBAR_DIR,
                                              self.NBAR_SCENE))

        self.cross_check(ortho_ds, nbar_ds, self.CROSSCHECK_KEYS_TWO)

    def test_crosscheck_ortho8_nbar8(self):
        """Cross-check metadata between Landsat 8 ortho and nbar datasets."""

        ortho_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                               self.ORTHO8_DIR,
                                               self.ORTHO8_SCENE))

        nbar_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                              self.NBAR8_DIR,
                                              self.NBAR8_SCENE))

        self.cross_check(ortho_ds, nbar_ds, self.CROSSCHECK_KEYS_TWO)

    def test_crosscheck_ortho_pq(self):
        """Cross-check metadata between ortho and pq datasets."""

        ortho_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                               self.ORTHO_DIR,
                                               self.ORTHO_SCENE))

        pq_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                            self.PQ_DIR,
                                            self.PQ_SCENE))

        self.cross_check(ortho_ds, pq_ds, self.CROSSCHECK_KEYS_TWO)

    def test_crosscheck_ortho8_pq8(self):
        """Cross-check metadata between Landsat 8 ortho and pq datasets."""

        ortho_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                               self.ORTHO8_DIR,
                                               self.ORTHO8_SCENE))

        pq_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                            self.PQ8_DIR,
                                            self.PQ8_SCENE))

        self.cross_check(ortho_ds, pq_ds, self.CROSSCHECK_KEYS_TWO)

    def test_crosscheck_ortho_fc(self):
        """Cross-check metadata between ortho and fc datasets."""

        ortho_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                               self.ORTHO_DIR,
                                               self.ORTHO_SCENE))

        fc_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                            self.FC_DIR,
                                            self.FC_SCENE))

        self.cross_check(ortho_ds, fc_ds, self.CROSSCHECK_KEYS_TWO)

    def test_crosscheck_ortho8_fc8(self):
        """Cross-check metadata between Landsat 8 ortho and fc datasets."""

        ortho_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                               self.ORTHO8_DIR,
                                               self.ORTHO8_SCENE))

        fc_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                            self.FC8_DIR,
                                            self.FC8_SCENE))

        self.cross_check(ortho_ds, fc_ds, self.CROSSCHECK_KEYS_TWO)

    def test_crosscheck_pq_fc(self):
        """Cross-check metadata between pc and fc datasets."""

        pq_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                            self.PQ_DIR,
                                            self.PQ_SCENE))

        fc_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                            self.FC_DIR,
                                            self.FC_SCENE))

        self.cross_check(pq_ds, fc_ds, self.CROSSCHECK_KEYS_TWO)

    def test_crosscheck_pq8_fc8(self):
        """Cross-check metadata between Landsat 8 pc and fc datasets."""

        pq_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                            self.PQ8_DIR,
                                            self.PQ8_SCENE))

        fc_ds = LandsatDataset(os.path.join(self.INPUT_DIR,
                                            self.FC8_DIR,
                                            self.FC8_SCENE))

        self.cross_check(pq_ds, fc_ds, self.CROSSCHECK_KEYS_TWO)

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
            val = mdd[k]
            if k == 'pq_tests_run' and val is not None:
                val = '{:016b}'.format(val)
            print >> out_file, "%s: %s" % (k, val)

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

    def cross_check(self, ds1, ds2, md_keys):
        """Checks that the metadata from two datasets matches.

        ds1 and ds2 are two datasets, md_keys is a list of keys
        to be checked. The routine checks that the metadata matches
        for each key in md_keys."""

        mdd1 = ds1.metadata_dict
        mdd2 = ds2.metadata_dict

        for k in md_keys:
            self.assertEqual(mdd1[k], mdd2[k])

    def check_fuzzy_datetime_match(self, ds1, ds2):
        """Checks for an approximate match between start and end datetimes."""

        start1 = ds1.metadata_dict['start_datetime']
        end1 = ds1.metadata_dict['end_datetime']

        start2 = ds2.metadata_dict['start_datetime']
        end2 = ds2.metadata_dict['end_datetime']

        overlap = self.calculate_overlap(start1, end1, start2, end2)

        self.assertGreaterEqual(overlap, 0.9)

    @staticmethod
    def calculate_overlap(start1, end1, start2, end2):
        """Calculate the fractional overlap between time intervals."""

        interval_length = max((end1 - start1), (end2 - start2))
        interval_seconds = interval_length.total_seconds()

        overlap_start = max(start1, start2)
        overlap_end = min(end1, end2)
        if overlap_end > overlap_start:
            overlap_length = overlap_end - overlap_start
            overlap_seconds = overlap_length.total_seconds()
        else:
            overlap_seconds = 0.0

        return overlap_seconds / interval_seconds

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
