#!/usr/bin/env python
"""Tests for the dbcompare.py module."""

import os
import unittest
import psycopg2
import dbutil
import dbcompare

#
# Test cases
#


class TestComparisonWrapper(unittest.TestCase):

    TEST_DB_FILE = "test_hypercube_empty.sql"

    EXPECTED_TABLE_LIST = [
        'acquisition',
        'acquisition_footprint',
        'band',
        'band_equivalent',
        'band_source',
        'dataset',
        'lock',
        'lock_type',
        'processing_level',
        'satellite',
        'sensor',
        'spatial_ref_sys',
        'tile',
        'tile_class',
        'tile_footprint',
        'tile_type'
        ]
        
    def setUp(self):
        self.conn = None
        self.dbname = dbutil.random_name('test_wrapper_db')

        dbutil.TESTSERVER.create(self.dbname, self.TEST_DB_FILE)

        self.conn = dbutil.TESTSERVER.connect(self.dbname)
        self.conn = dbcompare.ComparisonWrapper(self.conn)

    def test_table_list(self):
        "Test get table list."
        tab_list = self.conn.table_list()
        self.assertEqual(tab_list, self.EXPECTED_TABLE_LIST)

    def tearDown(self):

        if self.conn:
            self.conn.close()

        dbutil.TESTSERVER.drop(self.dbname)

#
# Define test suites
#


def the_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [TestComparisonWrapper]

    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)

    suite = unittest.TestSuite(suite_list)

    return suite

#
# Run unit tests if in __main__
#

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())
