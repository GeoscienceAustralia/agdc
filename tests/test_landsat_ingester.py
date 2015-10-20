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
    test_landsat_ingester.py - unit tests for the landsat_ingester module.
"""

import unittest
import os
import sys
import subprocess

from agdc import dbutil
from agdc.cube_util import DatasetError, Stopwatch
from agdc.ingest.landsat import LandsatIngester

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class (which has too
# many public methods according to pylint).
#

class TestDatasetFiltering(unittest.TestCase):
    """Unit and performance tests for dataset filtering."""

    MODULE = 'landsat_ingester'
    SUITE = 'TestDatasetFiltering'

    INPUT_DIR = dbutil.input_directory(MODULE, SUITE)
    OUTPUT_DIR = dbutil.output_directory(MODULE, SUITE)
    EXPECTED_DIR = dbutil.expected_directory(MODULE, SUITE)

    SOURCE_DIR = '/g/data1/rs0/scenes/ARG25_V0.0/2014-03'

    def setUp(self):
        """Set up ingester."""

        self.stopwatch = Stopwatch()

        updates = {'start_date': '01/03/2014',
                   'end_date': '15/03/2014',
                   'min_path': '090',
                   'max_path': '093',
                   'min_row': '087',
                   'max_row': '090'
                   }

        config_file = dbutil.update_config_file2(updates,
                                                 self.INPUT_DIR,
                                                 self.OUTPUT_DIR,
                                                 'test_datacube.conf')

        sys.argv = [sys.argv[0],
                    "--config=%s" % config_file,
                    "--source=%s" % self.SOURCE_DIR
                    ]

        self.ingester = LandsatIngester()

    @staticmethod
    def dump_dataset_names(output_path, dataset_list):
        """Dump the names of the datasets to a file.

        This writes a list of basenames from the paths in dataset_list to
        a file at output_path."""

        out = open(output_path, 'w')

        for dataset_path in dataset_list:
            out.write(os.path.basename(dataset_path) + '\n')

        out.close()

    def check_datasets_list(self, output_path, expected_path):
        """If an expected datasets file exists, check to see if it matches."""

        if not os.path.isfile(expected_path):
            self.skipTest("Expected dataset list file not found.")
        else:
            try:
                subprocess.check_output(['diff', output_path, expected_path])
            except subprocess.CalledProcessError as err:
                self.fail("Filtered datasets do not match those expected:\n" +
                          err.output)

    def test_fast_filter(self):
        """Test the results of a fast (filename based) filter."""

        print ""
        print "Finding datasets ..."

        self.stopwatch.start()
        dataset_list = self.ingester.find_datasets(self.SOURCE_DIR)
        self.stopwatch.stop()
        (elapsed_time, cpu_time) = self.stopwatch.read()

        print ""
        print "%s datasets found." % len(dataset_list)
        print "elapsed time: %s" % elapsed_time
        print "cpu time: %s" % cpu_time

        print ""
        print "Doing fast filter ..."

        self.stopwatch.reset()
        self.stopwatch.start()
        filtered_list = self.ingester.fast_filter_datasets(dataset_list)
        self.stopwatch.stop()
        (elapsed_time, cpu_time) = self.stopwatch.read()

        print ""
        print "%s out of %s datasets remain." % \
            (len(filtered_list), len(dataset_list))
        print "elapsed time: %s" % elapsed_time
        print "cpu time: %s" % cpu_time
        print ""

        output_path = os.path.join(self.OUTPUT_DIR, 'fast_filter_datasets.txt')
        self.dump_dataset_names(output_path, filtered_list)

        expected_path = os.path.join(self.EXPECTED_DIR, 'filter_datasets.txt')
        self.check_datasets_list(output_path, expected_path)

    def test_metadata_filter(self):
        """Test the results of a metadata based filter."""

        print ""
        print "Finding datasets ..."

        self.stopwatch.start()
        dataset_list = self.ingester.find_datasets(self.SOURCE_DIR)
        self.stopwatch.stop()
        (elapsed_time, cpu_time) = self.stopwatch.read()

        print ""
        print "%s datasets found." % len(dataset_list)
        print "elapsed time: %s" % elapsed_time
        print "cpu time: %s" % cpu_time

        print ""
        print "Doing metadata filter ..."

        self.stopwatch.reset()
        self.stopwatch.start()

        filtered_list = []
        for dataset_path in dataset_list:
            dataset = self.ingester.open_dataset(dataset_path)
            try:
                self.ingester.filter_on_metadata(dataset)
            except DatasetError:
                pass
            else:
                filtered_list.append(dataset_path)

        self.stopwatch.stop()
        (elapsed_time, cpu_time) = self.stopwatch.read()

        print ""
        print "%s out of %s datasets remain." % \
            (len(filtered_list), len(dataset_list))
        print "elapsed time: %s" % elapsed_time
        print "cpu time: %s" % cpu_time
        print ""

        output_path = os.path.join(self.OUTPUT_DIR,
                                   'metadata_filter_datasets.txt')
        self.dump_dataset_names(output_path, filtered_list)

        expected_path = os.path.join(self.EXPECTED_DIR, 'filter_datasets.txt')
        self.check_datasets_list(output_path, expected_path)

#
# Define test suites
#


def the_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [TestDatasetFiltering]

    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)

    suite = unittest.TestSuite(suite_list)

    return suite

#
# Run unit tests if in __main__
#

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())
