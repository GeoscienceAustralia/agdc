#!/usr/bin/env python

#===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither [copyright holder] nor the names of its contributors may be
#       used to endorse or promote products derived from this software
#       without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#===============================================================================

"""Tests for the dbupdater.py module."""

import os
import subprocess
import unittest
import dbutil
import dbcompare

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
