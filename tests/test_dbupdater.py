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

"""Tests for the dbupdater.py module."""

import os
import subprocess
import unittest
from agdc import dbutil
from agdc import dbcompare

#
# Test cases
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class (which has too
# many public methods according to pylint).
#


class TestDBUpdater(unittest.TestCase):
    """Unit tests for the dbupdater.py script"""

    MODULE = 'dbupdater'
    SUITE = 'TestDBUpdater'

    INPUT_DIR = dbutil.input_directory(MODULE, SUITE)
    OUTPUT_DIR = dbutil.output_directory(MODULE, SUITE)
    EXPECTED_DIR = dbutil.expected_directory(MODULE, SUITE)

    def setUp(self):
        self.test_dbname = None
        self.expected_dbname = None

        self.test_conn = None
        self.expected_conn = None

        self.logfile = None

    def test_onescene(self):
        """Test database update for a single scene."""

        # This test is intended as an example, and so is extensively
        # commented.

        # Open a log file
        logfile_path = os.path.join(self.OUTPUT_DIR, "test_onescene.log")
        self.logfile = open(logfile_path, "w")

        #
        # Create the initial database
        #

        # Randomise the name to avoid collisons with other users.
        self.test_dbname = dbutil.random_name("test_onescene")

        # Create the database.
        dbutil.TESTSERVER.create(self.test_dbname,
                                 self.INPUT_DIR, "hypercube_empty.sql")

        #
        # Run dbupdater on the test database and save the result
        #

        # Create an updated datacube_conf file with the new dbname
        config_file_path = dbutil.update_config_file(self.test_dbname,
                                                     self.INPUT_DIR,
                                                     self.OUTPUT_DIR,
                                                     "test_datacube.conf")

        # Run dbupdater

        ingest_dir = os.path.join(self.INPUT_DIR, 'onescene')
        dbupdater_cmd = ["python",
                         "dbupdater.py",
                         "--debug",
                         "--config=%s" % config_file_path,
                         "--source=%s" % ingest_dir,
                         "--removedblist",
                         "--followsymlinks"]
        subprocess.check_call(dbupdater_cmd, stdout=self.logfile,
                              stderr=subprocess.STDOUT)

        # Save the updated database
        dbutil.TESTSERVER.save(self.test_dbname, self.OUTPUT_DIR,
                               "onescene.sql")

        #
        # If an expected result exists then load it and compare
        #

        # Check for expected result
        if os.path.isfile(os.path.join(self.EXPECTED_DIR, "onescene.sql")):
            # Create a randomised name...
            self.expected_dbname = dbutil.random_name("expected_onescene")

            # load the database...
            dbutil.TESTSERVER.create(self.expected_dbname,
                                     self.EXPECTED_DIR, "onescene.sql")

            # create database connections...
            self.test_conn = dbutil.TESTSERVER.connect(self.test_dbname)
            self.expected_conn = dbutil.TESTSERVER.connect(
                self.expected_dbname)

            # and compare.

            self.assertTrue(dbcompare.compare_databases(self.test_conn,
                                                        self.expected_conn,
                                                        output=self.logfile,
                                                        verbosity=3),
                            "Databases do not match.")
        else:
            self.skipTest("Expected database save file not found.")

    def tearDown(self):
        # Remove any tempoary databases that have been created.

        if self.test_conn:
            self.test_conn.close()
        if self.expected_conn:
            self.expected_conn.close()

        if self.test_dbname:
            dbutil.TESTSERVER.drop(self.test_dbname)
        if self.expected_dbname:
            dbutil.TESTSERVER.drop(self.expected_dbname)

        # Close the logfile
        if self.logfile:
            self.logfile.close()

#
# Define test suites
#


def the_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [TestDBUpdater]

    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)

    suite = unittest.TestSuite(suite_list)

    return suite

#
# Run unit tests if in __main__
#

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())
