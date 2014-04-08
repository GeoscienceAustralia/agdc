#!/usr/bin/env python
"""Tests for the dbcompare.py module."""

import os
import unittest
import StringIO
import dbutil
import dbcompare

#
# Test cases
#

# pylint: disable=too-many-public-methods


class TestReporter(unittest.TestCase):
    """Unit tests for the Reporter class."""

    TEST_COLUMNS = ['number_col', 'text_col1', 'text_col2']

    TEST_ROWS = [
        (1, 't1', 't2'),
        (2, 'long text one', 'short t2'),
        (4, 't1.3', 'a loooooooooooooooooooooooooooooooong text field'),
        (4, 't1.4', 't2.4')
        ]

    MODULE = 'dbcompare'
    SUITE = 'TestReporter'

    TEST_TABLE = 'test_table'
    TEST_COLUMN = 'test_column'

    def setUp(self):
        # Create strings masquerading as output files.
        self.output0 = StringIO.StringIO()
        self.output1 = StringIO.StringIO()
        self.output2 = StringIO.StringIO()
        self.output3 = StringIO.StringIO()

        # Create test reporter objects.
        self.report0 = dbcompare.Reporter('test_db1', 'test_db2',
                                          0, self.output0)
        self.report1 = dbcompare.Reporter('test_db1', 'test_db2',
                                          1, self.output1)
        self.report2 = dbcompare.Reporter('test_db1', 'test_db2',
                                          2, self.output2)
        self.report3 = dbcompare.Reporter('test_db1', 'test_db2',
                                          3, self.output3)

    def check_output(self, file_name, output_str):
        """Check the output against an expected output file.

        This method also writes the output to a temporary
        directory, and skips the test if the expected output
        file is not present. The temporay output can be used as
        the expected output if it passes a manual check."""

        output_dir_path = dbutil.output_directory(self.MODULE, self.SUITE)
        output_file_path = os.path.join(output_dir_path, file_name)
        with open(output_file_path, 'w') as output_file:
            output_file.write(output_str)

        expected_dir_path = dbutil.expected_directory(self.MODULE, self.SUITE)
        expected_file_path = os.path.join(expected_dir_path, file_name)
        if os.path.isfile(expected_file_path):
            with open(expected_file_path) as expected_file:
                expected_str = expected_file.read()
            self.assertEqual(output_str, expected_str)
        else:
            self.skipTest(("expected output file '%s' not found for " +
                           "module '%s', suite '%s'.") %
                          (file_name, self.MODULE, self.SUITE))

    def test_table_only_in_v0(self):
        "Test reporting of extra tables, verbosity 0:"

        self.report0.table_only_in(1, self.TEST_TABLE)
        self.assertEqual(self.output0.getvalue(), "")

    def test_table_only_in_v1(self):
        "Test reporting of extra tables, verbosity 1:"

        self.report1.table_only_in(1, self.TEST_TABLE)
        self.check_output('test_table_only_in_v1.txt', self.output1.getvalue())

    def test_table_only_in_v2(self):
        "Test reporting of extra tables, verbosity 2:"

        self.report2.table_only_in(2, self.TEST_TABLE)
        self.check_output('test_table_only_in_v2.txt', self.output2.getvalue())


    def test_column_only_in_v0(self):
        "Test reporting of extra columns, verbosity 0:"

        self.report0.column_only_in(1, self.TEST_TABLE, self.TEST_COLUMN)
        self.assertEqual(self.output0.getvalue(), "")

    def test_column_only_in_v1(self):
        "Test reporting of extra columns, verbosity 1:"

        self.report1.column_only_in(2, self.TEST_TABLE, self.TEST_COLUMN)
        self.check_output('test_column_only_in_v1.txt',
                          self.output1.getvalue())

    def test_column_only_in_v3(self):
        "Test reporting of extra columns, verbosity 3:"

        self.report3.column_only_in(1, self.TEST_TABLE, self.TEST_COLUMN)
        self.check_output('test_column_only_in_v3.txt',
                          self.output3.getvalue())

    def test_primary_keys_differ_v0(self):
        "Test reporting of primary key mismatch, verbosity 0:"

        self.report0.primary_keys_differ(self.TEST_TABLE)
        self.assertEqual(self.output0.getvalue(), "")

    def test_primary_keys_differ_v1(self):
        "Test reporting of primary key mismatch, verbosity 1:"

        self.report1.primary_keys_differ(self.TEST_TABLE)
        self.check_output('test_primary_keys_differ_v1.txt',
                          self.output1.getvalue())

    def test_content_differences_v3(self):
        "Test reporting of table content differences, verbosity 3:"

        self.report3.new_table(self.TEST_TABLE, self.TEST_COLUMNS)

        self.report3.add_difference(1, self.TEST_ROWS[0])
        self.report3.add_difference(2, self.TEST_ROWS[1])
        self.report3.add_difference(1, self.TEST_ROWS[2])
        self.report3.add_difference(2, self.TEST_ROWS[3])

        self.report3.content_differences()

        self.check_output('test_content_differences_v3.txt',
                          self.output3.getvalue())


class TestComparisonWrapper(unittest.TestCase):
    """Unit tests for ComparisonWrapper class."""

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

    def setUp(self):
        self.conn = None
        self.dbname = dbutil.random_name('test_wrapper_db')

        dbutil.TESTSERVER.create(self.dbname, self.TEST_DB_FILE)

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

    def test_primary_key(self):
        "Test get primary key."

        pkey = self.conn.primary_key(self.SIMPLE_PKEY_TABLE)
        self.assertEqual(pkey, self.EXPECTED_SIMPLE_PKEY)

        pkey = self.conn.primary_key(self.COMPOUND_PKEY_TABLE)
        self.assertEqual(pkey, self.EXPECTED_COMPOUND_PKEY)

    def tearDown(self):

        if self.conn:
            self.conn.close()

        dbutil.TESTSERVER.drop(self.dbname)

#
# Define test suites
#


def the_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [TestReporter]

    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)

    suite = unittest.TestSuite(suite_list)

    return suite

#
# Run unit tests if in __main__
#

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())
