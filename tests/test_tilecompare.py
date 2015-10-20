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

"""Tests for the tilecompare.py module."""

import os
import sys
import logging
import re
import unittest
from agdc import dbutil
from agdc import tilecompare

#
# Test cases
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class.
#

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

class TestTileCompare(unittest.TestCase):
    """Unit tests foe tilecompare functions."""
    # pylint:disable=too-many-instance-attributes
    MODULE = 'tilecompare'
    SUITE = 'TileCompare'

    INPUT_DIR = dbutil.input_directory(MODULE, SUITE)
    OUTPUT_DIR = dbutil.output_directory(MODULE, SUITE)
    EXPECTED_DIR = dbutil.expected_directory(MODULE, SUITE)

    def setUp(self):

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
        self.stream_handler = logging.StreamHandler(stream=sys.stdout)
        self.stream_handler.setLevel(logging.INFO)
        self.stream_handler.setFormatter(logging.Formatter('%(message)s'))
        LOGGER.addHandler(self.stream_handler)
        self.dbname1 = None #production database
        self.dbname2 = None #result of test_ingest
        self.conn1 = None
        self.conn2 = None

    def tearDown(self):
        #
        # Flush the handler and remove it from the root logger.
        #
        self.handler.flush()
        self.stream_handler.flush()
        if self.dbname2:
            LOGGER.info('About to drop %s', self.dbname2)
            self.conn2.close()
            dbutil.TESTSERVER.drop(self.dbname2)
        LOGGER.removeHandler(self.handler)
        LOGGER.removeHandler(self.stream_handler)

    def xxxtest_create_database(self):
        "Test random_name random database name generator."
        self.dbname1 = 'hypercube_v0'
        self.dbname2 = dbutil.random_name('test_create_database')
        LOGGER.info('Creating database %s', self.dbname2)
        dbutil.TESTSERVER.create(self.dbname2, self.INPUT_DIR,
                                 'hypercube_test_ingest.sql')
        self.conn1 = dbutil.TESTSERVER.connect(self.dbname1)
        self.conn2 = dbutil.TESTSERVER.connect(self.dbname2)

        LOGGER.info('About to create database from file')
        dbutil.TESTSERVER.create(self.dbname, self.INPUT_DIR,
                                'hypercube_test_ingest.sql')
        LOGGER.info('.done')

    def xxxtest_create_tile_acqusition_info(self):
        "Test creation of tile_acquisition_info table."""
        self.dbname1 = 'hypercube_test'
        self.dbname2 = dbutil.random_name('test_tilecompare')
        LOGGER.info('Creating database %s', self.dbname2)
        dbutil.TESTSERVER.create(self.dbname2, self.INPUT_DIR,
                                 'hypercube_test_ingest.sql')
        self.conn1 = dbutil.TESTSERVER.connect(self.dbname1, autocommit=False)
        self.conn2 = dbutil.TESTSERVER.connect(self.dbname2, autocommit=False)
        LOGGER.info('About to create comparision pair')
        pair = tilecompare.TileComparisonPair(self.conn1, self.conn2,
                                              'public', 'public')
        LOGGER.info('About to create table from fresh ingest')
        fresh_ingest_info_table = 'fresh_ingest_info'
        comparison_table = 'ingest_comparison'
        tilecompare._copy_ingest_tile_acquisition_info(pair,
                                                       fresh_ingest_info_table)
        LOGGER.info('About to create comparison table')
        tilecompare._create_comparison_table(pair, fresh_ingest_info_table,
                                             comparison_table)
        LOGGER.info('About to compare the tile contents')
        tilecompare._compare_tile_contents(pair, comparison_table)

    def test_compare_tile_stores(self):
        "Test creation of tile_acquisition_info table."""
        self.dbname1 = 'hypercube_test2_v0'
        self.dbname2 = dbutil.random_name('test_tilecompare')
        LOGGER.info('Creating database %s', self.dbname2)
        dbutil.TESTSERVER.create(self.dbname2, self.INPUT_DIR,
                                 'hypercube_test_ingest.sql')

        #Temp
        #print 'Loading production database %s' %self.dbname1
        #dbutil.TESTSERVER.create(self.dbname1, '/g/data/v10/test_resources/databases',
        #                         'hypercube_v0.sql')
        #self.dbname1 = 'hypercube_test2_v0'
        #print 'Loading production database %s' %self.dbname1
        #dbutil.TESTSERVER.create(self.dbname1, '/g/data/v10/test_resources/databases',
        #                         'hypercube_v0.sql')
        #return
        #Temp

        self.conn1 = dbutil.TESTSERVER.connect(self.dbname1, autocommit=False)
        self.conn2 = dbutil.TESTSERVER.connect(self.dbname2, autocommit=False)

        LOGGER.info('About to call compare_tile_stores')
        fout = open(os.path.join(self.OUTPUT_DIR,
                                 'tile_comparison_output.txt'), 'w')
        fout = sys.stdout #temp
        difference_pairs = tilecompare.compare_tile_stores(self.conn1,
                                                           self.conn2,
                                                           output=fout)
        LOGGER.info('Finished calling compare_tile_stores')
        if difference_pairs != []:
            report_string = "Fresh ingest tile content differs from the " \
                "benchmark:\n"
            for pair in difference_pairs:
                report_string = report_string + "Benchmark tile:\n%s\nFresh" \
                    "Ingest tile:\n%s\n" %(pair[0], pair[1])
            self.fail(report_string)
        else:
            print 'Everything passed'


def the_suite():
    "Runs the tests"""
    test_classes = [TestTileCompare]
    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)
    suite = unittest.TestSuite(suite_list)
    return suite

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())

















