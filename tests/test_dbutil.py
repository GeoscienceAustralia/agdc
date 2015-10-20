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

"""Tests for the dbutil.py module."""

import os
import unittest
import subprocess
import psycopg2
from agdc import dbutil
from agdc import dbcompare

#
# Test cases
#

# pylint: disable=too-many-public-methods
#
# Disabled to avoid complaints about the unittest.TestCase class.
#


class TestUtilityFunctions(unittest.TestCase):
    """Unit tests for utility functions."""

    MODULE = 'dbutil'
    SUITE = 'TestUtilityFunctions'

    def test_random_name(self):
        "Test random_name random database name generator."

        basename = 'Fred'
        rnd_name1 = dbutil.random_name(basename)
        rnd_name2 = dbutil.random_name(basename)

        self.assertRegexpMatches(rnd_name1, r'^Fred_[\d]{9}',
                                 "Random name has unexpected format '%s'" %
                                 rnd_name1)
        self.assertRegexpMatches(rnd_name2, r'^Fred_[\d]{9}',
                                 "Random name has unexpected format '%s'" %
                                 rnd_name2)
        self.assertNotEqual(rnd_name1, rnd_name2,
                            "Random names are equal: '%s'" % rnd_name1)

    def test_safe_name(self):
        "Test safe_name database name sanitiser."

        self.assertEqual(dbutil.safe_name('Fred'), 'Fred')
        self.assertEqual(dbutil.safe_name('Fred_123456789'), 'Fred_123456789')
        self.assertEqual(dbutil.safe_name('Fred!@%&***'), 'Fred')
        self.assertEqual(dbutil.safe_name('Fred;drop postgres;'),
                         'Freddroppostgres')

    def check_directory(self, path, expected_path):
        "Check that a directory exists and has the right path."

        self.assertEqual(path, expected_path)
        self.assertTrue(os.path.isdir(path),
                        "Test directory does not seem to have been " +
                        "created:\n" + path)

        # Check directory permissons. We are interested in user, group, and
        # world permisions (not SUID, SGID, or sticky bit). This is octal
        # 0777 as a mask.
        mode = os.stat(path).st_mode & 0o0777
        # We want all user and group permissions, but not world permisions.
        # This is octal 770.
        self.assertEqual(mode, 0o770, "Test directory has the wrong " +
                         "permissions:\n" + path)

    def test_resources_directory(self):
        "Test test resources directory finder/creator."

        dummy_user = dbutil.random_name('user')
        expected_path = os.path.join(dbutil.TEST_RESOURCES_ROOT,
                                     dummy_user, 'test', 'module', 'suite')
        try:
            path = dbutil.resources_directory(dummy_user,
                                              'test', 'module', 'suite')
            self.check_directory(path, expected_path)
        finally:
            os.removedirs(path)

    def test_output_directory_1(self):
        "Test test output directory finder/creator, test 1."

        dummy_user = dbutil.random_name('user')
        expected_path = os.path.join(dbutil.TEST_RESOURCES_ROOT,
                                     dummy_user, 'output', 'module', 'suite')
        try:
            path = dbutil.output_directory('module', 'suite', user=dummy_user)
            self.check_directory(path, expected_path)
        finally:
            os.removedirs(path)

    def test_output_directory_2(self):
        "Test test output directory finder/creator, test 2."

        dummy_user = dbutil.random_name('user')
        expected_path = os.path.join(dbutil.TEST_RESOURCES_ROOT,
                                     dummy_user, 'output', 'module', 'suite')
        old_user = os.environ['USER']

        try:
            os.environ['USER'] = dummy_user
            path = dbutil.output_directory('module', 'suite')
            self.check_directory(path, expected_path)
        finally:
            os.environ['USER'] = old_user
            os.removedirs(path)

    def test_expected_directory_1(self):
        "Test test expected directory finder/creator, test 1."

        dummy_user = dbutil.random_name('user')
        expected_path = os.path.join(dbutil.TEST_RESOURCES_ROOT,
                                     dummy_user, 'expected', 'module', 'suite')
        old_user = os.environ['USER']

        try:
            os.environ['USER'] = dummy_user
            path = dbutil.expected_directory('module', 'suite')
            self.check_directory(path, expected_path)
        finally:
            os.environ['USER'] = old_user
            os.removedirs(path)

    def test_expected_directory_2(self):
        "Test test expected directory finder/creator, test 2."

        dummy_user = dbutil.random_name('user')
        expected_path = os.path.join(dbutil.TEST_RESOURCES_ROOT,
                                     dummy_user, 'expected', 'module', 'suite')
        old_user = os.environ['USER']

        try:
            os.environ['USER'] = dummy_user
            path = dbutil.expected_directory('module', 'suite', version='user')
            self.check_directory(path, expected_path)
        finally:
            os.environ['USER'] = old_user
            os.removedirs(path)

    def test_expected_directory_3(self):
        "Test test expected directory finder/creator, test 3."

        dummy_user = dbutil.random_name('user')
        expected_path = os.path.join(dbutil.TEST_RESOURCES_ROOT,
                                     dummy_user, 'expected', 'module', 'suite')
        try:
            path = dbutil.expected_directory('module', 'suite',
                                             version='user', user=dummy_user)
            self.check_directory(path, expected_path)
        finally:
            os.removedirs(path)

    def test_expected_directory_4(self):
        "Test test expected directory finder/creator, test 4."

        dummy_version = dbutil.random_name('version')
        expected_path = os.path.join(dbutil.TEST_RESOURCES_ROOT,
                                     dummy_version, 'expected',
                                     'module', 'suite')
        try:
            path = dbutil.expected_directory('module', 'suite',
                                             version=dummy_version)
            self.check_directory(path, expected_path)
        finally:
            os.removedirs(path)

    def test_expected_directory_5(self):
        "Test test expected directory finder/creator, test 5."

        dummy_version = dbutil.random_name('version')
        old_version = os.environ.get('DATACUBE_VERSION', None)
        expected_path = os.path.join(dbutil.TEST_RESOURCES_ROOT,
                                     dummy_version, 'expected',
                                     'module', 'suite')
        try:
            os.environ['DATACUBE_VERSION'] = dummy_version
            path = dbutil.expected_directory('module', 'suite')
            self.check_directory(path, expected_path)
        finally:
            if old_version is None:
                del os.environ['DATACUBE_VERSION']
            else:
                os.environ['DATACUBE_VERSION'] = old_version
            os.removedirs(path)

    def test_input_directory(self):
        "Test test input directory finder/creator."

        dummy_version = dbutil.random_name('version')
        input_path = os.path.join(dbutil.TEST_RESOURCES_ROOT,
                                  dummy_version, 'input',
                                  'module', 'suite')
        try:
            path = dbutil.input_directory('module', 'suite',
                                          version=dummy_version)
            self.check_directory(path, input_path)
        finally:
            os.removedirs(path)

    def test_update_config_file(self):
        "Test config file update utility."

        input_dir = dbutil.input_directory(self.MODULE, self.SUITE)
        output_dir = dbutil.output_directory(self.MODULE, self.SUITE)
        expected_dir = dbutil.expected_directory(self.MODULE, self.SUITE)

        dbname = 'TEST_TEST_TEST'
        config_file_name = 'test_datacube.conf'

        output_path = dbutil.update_config_file(dbname, input_dir,
                                                output_dir, config_file_name)

        expected_path = os.path.join(expected_dir, config_file_name)
        if not os.path.isfile(expected_path):
            self.skipTest("Expected config file not found.")
        else:
            try:
                subprocess.check_output(['diff', output_path, expected_path])
            except subprocess.CalledProcessError as err:
                self.fail("Config file does not match expected result:\n" +
                          err.output)

    def test_update_config_file2(self):
        "Test config file update utility, version 2."

        input_dir = dbutil.input_directory(self.MODULE, self.SUITE)
        output_dir = dbutil.output_directory(self.MODULE, self.SUITE)
        expected_dir = dbutil.expected_directory(self.MODULE, self.SUITE)

        updates = {'dbname': 'TEST_DBNAME',
                   'temp_dir': 'TEST_TEMP_DIR',
                   'tile_root': 'TEST_TILE_ROOT'
                   }
        config_file_name = 'test_datacube.conf'
        output_file_name = 'test2_datacube.conf'

        output_path = dbutil.update_config_file2(updates,
                                                 input_dir,
                                                 output_dir,
                                                 config_file_name,
                                                 output_file_name)

        expected_path = os.path.join(expected_dir, output_file_name)
        if not os.path.isfile(expected_path):
            self.skipTest("Expected config file not found.")
        else:
            try:
                subprocess.check_output(['diff', output_path, expected_path])
            except subprocess.CalledProcessError as err:
                self.fail("Config file does not match expected result:\n" +
                          err.output)


class TestServer(unittest.TestCase):
    """Unit tests for Server class."""

    SAVE_DIR = dbutil.input_directory('dbutil', 'TestServer')
    SAVE_FILE = "test_create_db.sql"
    TEST_TEMPLATE_DB = "hypercube_empty_template"

    MAINTENANCE_DB = "postgres"
    TEST_CONNECT_DB = "postgres"

    def setUp(self):
        self.dbname1 = None
        self.dbname2 = None

        self.filename1 = None

    def test_connect(self):
        "Test pscopg2 connection to the server"

        # Attempt to connect as ordinary user.
        try:
            conn = dbutil.TESTSERVER.connect(self.TEST_CONNECT_DB)
        except psycopg2.Error as err:
            self.fail("Unable to connect as user '%s'" %
                      dbutil.TESTSERVER.user +
                      ((":\n" + err.pgerr) if err.pgerr else ""))
        else:
            conn.close()

        # Attempt to connect as superuser.
        try:
            conn = dbutil.TESTSERVER.connect(self.TEST_CONNECT_DB,
                                             superuser=True)
        except psycopg2.Error as err:
            self.fail("Unable to connect as superuser '%s'" %
                      dbutil.TESTSERVER.superuser +
                      ((":\n" + err.pgerr) if err.pgerr else ""))
        else:
            conn.close()

    def test_exists(self):
        "Test database existance check."

        self.assertTrue(dbutil.TESTSERVER.exists(self.MAINTENANCE_DB),
                        "Unable to verify existance of the " +
                        "maintenance database '%s'." % self.MAINTENANCE_DB)

        dummy_dbname = dbutil.random_name('dummy')
        self.assertFalse(dbutil.TESTSERVER.exists(dummy_dbname),
                         "Dummy database '%s' reported as existing." %
                         dummy_dbname)

    def test_dblist(self):
        "Test database list."

        db_list = dbutil.TESTSERVER.dblist()

        self.assertIn(self.MAINTENANCE_DB, db_list,
                      "Unable to find the maintenance database " +
                      "in the list of databases.")

    def test_create(self):
        "Test database creation and loading"

        self.dbname1 = dbutil.random_name('test_create_db')

        # Create a new database.
        dbutil.TESTSERVER.create(self.dbname1, self.SAVE_DIR, self.SAVE_FILE)

        # Check if the newly created database exists.
        maint_conn = dbutil.TESTSERVER.connect(self.MAINTENANCE_DB,
                                               superuser=True)
        try:
            maint_conn = dbutil.MaintenanceWrapper(maint_conn)
            self.assertTrue(maint_conn.exists(self.dbname1),
                            "New database does not seem to be there.")
        finally:
            maint_conn.close()

    def test_create_from_template(self):
        "Test database creation from template"

        self.dbname1 = dbutil.random_name('test_create_from_template_db')

        # Create a new database.
        dbutil.TESTSERVER.create(self.dbname1,
                                 template_db=self.TEST_TEMPLATE_DB)

        # Check if the newly created database exists.
        maint_conn = dbutil.TESTSERVER.connect(self.MAINTENANCE_DB,
                                               superuser=True)
        try:
            maint_conn = dbutil.MaintenanceWrapper(maint_conn)
            self.assertTrue(maint_conn.exists(self.dbname1),
                            "New database does not seem to be there.")
        finally:
            maint_conn.close()

    def test_drop(self):
        "Test ability to drop a database"

        self.dbname1 = dbutil.random_name('test_drop_db')

        # Create a new database.
        dbutil.TESTSERVER.create(self.dbname1, self.SAVE_DIR, self.SAVE_FILE)

        # Connect to the newly created database, to make sure it is
        # there, and to create a pgbouncer pool.
        conn = dbutil.TESTSERVER.connect(self.dbname1)
        try:
            conn = dbutil.ConnectionWrapper(conn)
            dbname = conn.database_name()
            self.assertEqual(dbname, self.dbname1)
        finally:
            conn.close()

        # Now drop the database...
        dbutil.TESTSERVER.drop(self.dbname1)

        # and make sure it is gone.
        maint_conn = dbutil.TESTSERVER.connect(self.MAINTENANCE_DB,
                                               superuser=True)
        try:
            maint_conn = dbutil.MaintenanceWrapper(maint_conn)
            self.assertFalse(maint_conn.exists(self.dbname1),
                             "Dropped database still seems to be there.")
        finally:
            maint_conn.close()

    def test_recreate(self):
        "Test ablility to recreate a database on top of itself"

        self.dbname1 = dbutil.random_name('test_recreate_db')

        # Create a new database.
        dbutil.TESTSERVER.create(self.dbname1, self.SAVE_DIR, self.SAVE_FILE)

        # Connect to the newly created database, to make sure it is
        # there, and to create a pgbouncer pool.
        conn = dbutil.TESTSERVER.connect(self.dbname1)
        try:
            conn = dbutil.ConnectionWrapper(conn)
            dbname = conn.database_name()
            self.assertEqual(dbname, self.dbname1)
        finally:
            conn.close()

        # Now recreate on top of the existing database...
        dbutil.TESTSERVER.create(self.dbname1, self.SAVE_DIR, self.SAVE_FILE)

        # and check that it exists.
        maint_conn = dbutil.TESTSERVER.connect(self.MAINTENANCE_DB,
                                               superuser=True)
        try:
            maint_conn = dbutil.MaintenanceWrapper(maint_conn)
            self.assertTrue(maint_conn.exists(self.dbname1),
                            "Recreated database does not seem to be there.")
        finally:
            maint_conn.close()

    def test_save(self):
        "Test database saving (and reload)."

        self.dbname1 = dbutil.random_name('test_save_db')
        self.filename1 = self.dbname1 + ".sql"

        # Create a new database.
        dbutil.TESTSERVER.create(self.dbname1,
                                 self.SAVE_DIR,
                                 self.SAVE_FILE)

        # Save the database to disk.
        dbutil.TESTSERVER.save(self.dbname1, self.SAVE_DIR, self.filename1,
                               'processing_level')

        # Now reload the file as a new database...
        self.dbname2 = dbutil.random_name('test_save_db_copy')
        dbutil.TESTSERVER.create(self.dbname2, self.SAVE_DIR, self.filename1)

        # and check that it exists.
        maint_conn = dbutil.TESTSERVER.connect(self.MAINTENANCE_DB,
                                               superuser=True)
        try:
            maint_conn = dbutil.MaintenanceWrapper(maint_conn)
            self.assertTrue(maint_conn.exists(self.dbname2),
                            "Saved and reloaded database " +
                            "does not seem to be there.")
        finally:
            maint_conn.close()

    def test_copy_table_between_databases(self):
        "Test copy of a table from one database to another database."

        self.dbname1 = dbutil.random_name('test_copy_db')
        self.dbname2 = dbutil.random_name('test_copy_db')
        self.filename1 = self.dbname1 + ".sql"

        # Create the databases.
        dbutil.TESTSERVER.create(self.dbname1,
                                 self.SAVE_DIR,
                                 self.SAVE_FILE)

        dbutil.TESTSERVER.create(self.dbname2,
                                 self.SAVE_DIR,
                                 self.SAVE_FILE)

        # Connect to each database
        conn1 = dbutil.TESTSERVER.connect(self.dbname1, superuser=True)
        conn2 = dbutil.TESTSERVER.connect(self.dbname2, superuser=True)
        conn1 = dbcompare.ComparisonWrapper(conn1)
        conn2 = dbcompare.ComparisonWrapper(conn2)

        # Create a dummy table in Database 1
        table_name = 'some_dummy_table_name'
        sql = ("CREATE TABLE " + table_name + " AS " + "\n" +
               "SELECT * FROM tile_type;")
        with conn1.cursor() as cur:
            cur.execute(sql)

        # Verify that the table exists in Database 1.
        exists = conn1.table_exists(table_name)
        if not exists:
            self.fail('Table ' + table_name + ' should exist on Database 1')

        # Verify that the table does not exist in Database 2.
        exists = conn2.table_exists(table_name)
        if exists:
            self.fail('Table ' + table_name +
                      ' should not exist in Database 2')

        # Copy the table from Database 1 to Database 2
        dbutil.TESTSERVER.copy_table_between_databases(self.dbname1,
                                                       self.dbname2,
                                                       table_name)

        #Verify that the table does exist in Database 2.
        exists = conn2.table_exists(table_name)
        if not exists:
            self.fail('Table ' + table_name + ' should exist')


    def tearDown(self):
        # Attempt to drop any test databases that may have been created.
        if self.dbname1:
            dbutil.TESTSERVER.drop(self.dbname1)
        if self.dbname2:
            dbutil.TESTSERVER.drop(self.dbname2)

        # Attempt to remove any test save files that may have been created.
        if self.filename1:
            filepath1 = os.path.join(self.SAVE_DIR, self.filename1)
            try:
                os.remove(filepath1)
            except OSError:
                # Don't worry if it is not there: test may have bombed
                # before creating the file.
                pass


class TestConnectionWrapper(unittest.TestCase):
    """Unit tests for ConnectionWrapper classs."""

    SAVE_DIR = dbutil.input_directory('dbutil', 'TestConnectionWrapper')
    TEST_DB_FILE = "test_hypercube_empty.sql"

    def setUp(self):
        self.conn = None
        self.dbname = dbutil.random_name('test_connection_wrapper_db')

        dbutil.TESTSERVER.create(self.dbname, self.SAVE_DIR, self.TEST_DB_FILE)

        self.conn = dbutil.TESTSERVER.connect(self.dbname)
        self.conn = dbutil.ConnectionWrapper(self.conn)

    def test_database_name(self):
        "Test get database name."

        dbname = self.conn.database_name()
        self.assertEqual(dbname, self.dbname)

    def tearDown(self):

        if self.conn:
            self.conn.close()

        dbutil.TESTSERVER.drop(self.dbname)

#
# Define test suites
#


def the_suite():
    """Returns a test suite of all the tests in this module."""

    test_classes = [TestUtilityFunctions,
                    TestServer,
                    TestConnectionWrapper]

    suite_list = map(unittest.defaultTestLoader.loadTestsFromTestCase,
                     test_classes)

    suite = unittest.TestSuite(suite_list)

    return suite

#
# Run unit tests if in __main__
#

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())
