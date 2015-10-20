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

"""Tests for the dbcompare.py module."""

import os
import unittest
import StringIO
from agdc import dbutil
from agdc import dbcompare

#
# Constants
#

MODULE = 'dbcompare'

#
# Test cases
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class.
#


class TestReporter(unittest.TestCase):
    """Unit tests for the Reporter class."""

    TEST_COLUMNS = ['number_col', 'text_col1', 'text_col2']

    TEST_ROWS = [
        (1, 't1', 't2'),
        (2, 'long text one', 'short t2'),
        (4, 't1.3', 'a loooooooooooooooooooooooooooooooong text field'),
        (4, 't1.4', 't2.4')
        ]

    SUITE = 'TestReporter'

    TEST_TABLE = 'test_table'
    TEST_COLUMN = 'test_column'

    def setUp(self):
        # Create strings masquerading as output files.
        self.output = [None]*4
        for i in range(4):
            self.output[i] = StringIO.StringIO()

        # Create test reporter objects.
        self.report = [None]*4
        for i in range(4):
            self.report[i] = dbcompare.Reporter('test_db1', 'test_db2',
                                                i, self.output[i])

    def check_output(self, file_name, output_str):
        """Check the output against an expected output file.

        This method also writes the output to a temporary
        directory, and skips the test if the expected output
        file is not present. The temporary output can be used as
        the expected output if it passes a manual check."""

        output_dir_path = dbutil.output_directory(MODULE, self.SUITE)
        output_file_path = os.path.join(output_dir_path, file_name)
        with open(output_file_path, 'w') as output_file:
            output_file.write(output_str)

        expected_dir_path = dbutil.expected_directory(MODULE, self.SUITE)
        expected_file_path = os.path.join(expected_dir_path, file_name)
        if os.path.isfile(expected_file_path):
            with open(expected_file_path) as expected_file:
                expected_str = expected_file.read()
            self.assertEqual(output_str, expected_str)
        else:
            self.skipTest(("expected output file '%s' not found for " +
                           "module '%s', suite '%s'.") %
                          (file_name, MODULE, self.SUITE))

    def test_table_only_in_v0(self):
        "Test reporting of extra tables, verbosity 0:"

        self.report[0].table_only_in(1, self.TEST_TABLE)
        self.assertEqual(self.output[0].getvalue(), "")

    def test_table_only_in_v1(self):
        "Test reporting of extra tables, verbosity 1:"

        self.report[1].table_only_in(1, self.TEST_TABLE)
        self.check_output('test_table_only_in_v1.txt',
                          self.output[1].getvalue())

    def test_table_only_in_v2(self):
        "Test reporting of extra tables, verbosity 2:"

        self.report[2].table_only_in(2, self.TEST_TABLE)
        self.check_output('test_table_only_in_v2.txt',
                          self.output[2].getvalue())

    def test_column_only_in_v0(self):
        "Test reporting of extra columns, verbosity 0:"

        self.report[0].column_only_in(1, self.TEST_TABLE, self.TEST_COLUMN)
        self.assertEqual(self.output[0].getvalue(), "")

    def test_column_only_in_v1(self):
        "Test reporting of extra columns, verbosity 1:"

        self.report[1].column_only_in(2, self.TEST_TABLE, self.TEST_COLUMN)
        self.check_output('test_column_only_in_v1.txt',
                          self.output[1].getvalue())

    def test_column_only_in_v3(self):
        "Test reporting of extra columns, verbosity 3:"

        self.report[3].column_only_in(1, self.TEST_TABLE, self.TEST_COLUMN)
        self.check_output('test_column_only_in_v3.txt',
                          self.output[3].getvalue())

    def test_primary_keys_differ_v0(self):
        "Test reporting of primary key mismatch, verbosity 0:"

        self.report[0].primary_keys_differ(self.TEST_TABLE)
        self.assertEqual(self.output[0].getvalue(), "")

    def test_primary_keys_differ_v1(self):
        "Test reporting of primary key mismatch, verbosity 1:"

        self.report[1].primary_keys_differ(self.TEST_TABLE)
        self.check_output('test_primary_keys_differ_v1.txt',
                          self.output[1].getvalue())

    def test_content_differences_v3(self):
        "Test reporting of table content differences, verbosity 3:"

        self.report[3].new_table(self.TEST_TABLE, self.TEST_COLUMNS)

        self.report[3].add_difference(1, self.TEST_ROWS[0])
        self.report[3].add_difference(2, self.TEST_ROWS[1])
        self.report[3].add_difference(1, self.TEST_ROWS[2])
        self.report[3].add_difference(2, self.TEST_ROWS[3])

        self.report[3].content_differences()

        self.check_output('test_content_differences_v3.txt',
                          self.output[3].getvalue())


class TestComparisonWrapper(unittest.TestCase):
    """Unit tests for ComparisonWrapper class."""

    SAVE_DIR = dbutil.input_directory('dbcompare', 'TestComparisonWrapper')
    TEST_DB_FILE = "test_hypercube_empty.sql"

    NOT_A_TABLE = "not_a_table_name_at_all_at_all"

    EXPECTED_TABLE_LIST = [
        'acquisition',
        'acquisition_footprint',
        'band',
        'band_equivalent',
        'band_source',
        'band_type',
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

    COLUMN_LIST_TABLE = "tile"

    EXPECTED_COLUMN_LIST = [
        'tile_id',
        'x_index',
        'y_index',
        'tile_type_id',
        'tile_pathname',
        'dataset_id',
        'tile_class_id',
        'tile_size',
        'ctime',
        'tile_status'
        ]

    SIMPLE_PKEY_TABLE = "tile"
    EXPECTED_SIMPLE_PKEY = ['tile_id']

    COMPOUND_PKEY_TABLE = "acquisition_footprint"
    EXPECTED_COMPOUND_PKEY = ['tile_type_id', 'acquisition_id']

    SREF_PKEY_TABLE = "spatial_ref_sys"
    EXPECTED_SREF_PKEY = ['srid']

    def setUp(self):
        self.conn = None
        self.dbname = dbutil.random_name('test_wrapper_db')

        dbutil.TESTSERVER.create(self.dbname, self.SAVE_DIR, self.TEST_DB_FILE)

        self.conn = dbutil.TESTSERVER.connect(self.dbname)
        self.conn = dbcompare.ComparisonWrapper(self.conn)

    def test_table_exists(self):
        "Test check for table existance."

        self.assertTrue(self.conn.table_exists(self.EXPECTED_TABLE_LIST[0]),
                        "table_exists cannot find expected table '%s'." %
                        self.EXPECTED_TABLE_LIST[0])
        self.assertTrue(self.conn.table_exists(self.EXPECTED_TABLE_LIST[-1]),
                        "table_exists cannnot find expected table '%s'." %
                        self.EXPECTED_TABLE_LIST[-1])
        self.assertTrue(self.conn.table_exists(self.EXPECTED_TABLE_LIST[5]),
                        "table_exists cannnot find expected table '%s'." %
                        self.EXPECTED_TABLE_LIST[5])

        self.assertFalse(self.conn.table_exists(self.NOT_A_TABLE),
                         "table_exists found nonexistant table '%s'." %
                         self.NOT_A_TABLE)

    def test_table_list(self):
        "Test get table list."

        tab_list = self.conn.table_list()
        self.assertEqual(tab_list, self.EXPECTED_TABLE_LIST)

    def test_column_list(self):
        "Test get column list."

        col_list = self.conn.column_list(self.COLUMN_LIST_TABLE)
        self.assertEqual(col_list, self.EXPECTED_COLUMN_LIST)

    def test_primary_key_simple(self):
        "Test get primary key, simple key."

        pkey = self.conn.primary_key(self.SIMPLE_PKEY_TABLE)
        self.assertEqual(pkey, self.EXPECTED_SIMPLE_PKEY)

    def test_primary_key_compound(self):
        "Test get primary key, compound key."

        pkey = self.conn.primary_key(self.COMPOUND_PKEY_TABLE)
        self.assertEqual(pkey, self.EXPECTED_COMPOUND_PKEY)

    def test_primary_key_sref(self):
        "Test get primary key, spatial_ref_sys table."

        pkey = self.conn.primary_key(self.SREF_PKEY_TABLE)
        self.assertEqual(pkey, self.EXPECTED_SREF_PKEY)

    def tearDown(self):

        if self.conn:
            self.conn.close()

        dbutil.TESTSERVER.drop(self.dbname)


class TestCompareFunctions(unittest.TestCase):
    """Unit tests for dbcompare interface functions."""

    SUITE = 'TestCompareFunctions'
    INPUT_DIR = dbutil.input_directory(MODULE, SUITE)
    OUTPUT_DIR = dbutil.output_directory(MODULE, SUITE)
    EXPECTED_DIR = dbutil.expected_directory(MODULE, SUITE)
    VERSION = dbutil.version_or_user()

    DB_LIST = [
        'hypercube_empty.sql',
        'hypercube_empty.sql',
        'hypercube_onescene.sql'
        ]

    def setUp(self):
        self.db_count = len(self.DB_LIST)
        self.conn = [None] * self.db_count
        self.dbname = [None] * self.db_count

        for i in range(self.db_count):
            self.dbname[i] = self.VERSION + "_test_compare_db" + str(i)

            if not dbutil.TESTSERVER.exists(self.dbname[i]):
                dbutil.TESTSERVER.create(self.dbname[i],
                                         self.INPUT_DIR,
                                         self.DB_LIST[i])

            self.conn[i] = dbutil.TESTSERVER.connect(self.dbname[i])

    def test_compare_empty(self):
        "Compare two empty databases."

        result = dbcompare.compare_databases(self.conn[0], self.conn[1],
                                             verbosity=2)
        self.assertTrue(result, "Identical empty databases are " +
                        "not comparing as equal.")

    def test_compare_equal_tables(self):
        "Compare two equal tables."

        result = dbcompare.compare_tables(self.conn[0], self.conn[2],
                                          'band_source', verbosity=2)
        self.assertTrue(result, "Identical tables are not comparing " +
                        "as equal.")

    def test_compare_different(self):
        "Compare two databases with differences."

        file_name = 'test_compare_different_v3.txt'

        output = StringIO.StringIO()
        result = dbcompare.compare_databases(self.conn[0], self.conn[2],
                                             verbosity=3, output=output)

        output_file_path = os.path.join(self.OUTPUT_DIR, file_name)
        with open(output_file_path, 'w') as output_file:
            output_file.write(output.getvalue())

        self.assertFalse(result, "Databases with differences are " +
                         "comparing as equal.")

        expected_file_path = os.path.join(self.EXPECTED_DIR, file_name)
        if os.path.isfile(expected_file_path):
            with open(expected_file_path) as expected_file:
                expected_str = expected_file.read()
            self.assertEqual(output.getvalue(), expected_str)
        else:
            self.skipTest("expected output file not found.")

    def test_compare_unequal_tables(self):
        "Compare two tables with differences."

        file_name = 'test_compare_unequal_tables_v3.txt'

        output = StringIO.StringIO()
        result = dbcompare.compare_tables(self.conn[0], self.conn[2],
                                          'dataset', verbosity=3,
                                          output=output)

        output_file_path = os.path.join(self.OUTPUT_DIR, file_name)
        with open(output_file_path, 'w') as output_file:
            output_file.write(output.getvalue())

        self.assertFalse(result, "Tables with differences are comparing " +
                         "as equal.")

        expected_file_path = os.path.join(self.EXPECTED_DIR, file_name)
        if os.path.isfile(expected_file_path):
            with open(expected_file_path) as expected_file:
                expected_str = expected_file.read()
            self.assertEqual(output.getvalue(), expected_str)
        else:
            self.skipTest("expected output file not found.")

    def tearDown(self):
        for i in range(self.db_count):
            if self.conn[i]:
                self.conn[i].close()

#
# Define test suites
#


def the_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [
        TestReporter,
        TestComparisonWrapper,
        TestCompareFunctions
        ]

    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)

    suite = unittest.TestSuite(suite_list)

    return suite

#
# Run unit tests if in __main__
#

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())
