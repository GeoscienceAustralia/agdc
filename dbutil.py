#!/usr/bin/env python

"""dbutil.py - PostgreSQL database utilities for testing.

This module provides easy access to the test database server, and
provides an easy way to save, create, and drop databases from this server.
"""

import psycopg2
import unittest

#
# Setup information for the test server. This might be better off loaded
# from a config file, but this will do for now. The password is kept
# in a .pgpass file to avoid saving it in versioned files. This is likely
# a better solution than recording the password either here or in a config
# file.
#

TESTSERVER_NAME = 'test_server'
TESTSERVER_HOST = '130.56.244.226'
TESTSERVER_PORT = '6432'
TESTSERVER_USER = 'cube_tester'
TESTSERVER_SUPERUSER = 'cube_admin'
TESTSERVER_SAVE_DIR = '/short/v10/mxh651/test_resources/databases'

#
# test_server instance
#

def test_server():
    """Return the test_server instance."""

    # Using function attribute 'the_server' as persistent storage.

    if not hasattr(test_server, 'the_server'):
        test_server.the_server = Server(name=TESTSERVER_NAME,
                                        host=TESTSERVER_HOST,
                                        port=TESTSERVER_PORT,
                                        user=TESTSERVER_USER,
                                        superuser=TESTSERVER_SUPERUSER,
                                        save_dir=TESTSERVER_SAVE_DIR)
    return test_server.the_server

#
# Server class
#

class Server(object):

    def __init__(self, name, host, port,
                 user, superuser, save_dir, timeout=60):
        self.name = name
        self.host = host
        self.port = port
        self.user = user
        self.superuser = superuser
        self.save_dir = save_dir
        self.timeout = timeout

    def raw_connect_to(self, dbname, user):
        """create a pscopg2.connection to dbname and return it.

        dbname: the database to connect to.
        user: the user for the connection.

        Note that the autocommit attribute for the connection will
        be set to True, turning on autocommit.
        """

        dsn = ("dbname=%s host=%s port=%s user=%s connect_timeout=%s" %
               (dbname, self.host, self.port, user, self.timeout))
        conn = psycopg2.connect(dsn)
        conn.autocommit = True

        return conn

    def create_and_load(self, dbname, save_file):
        pass

    def save(self, dbname, save_file):
        pass

    def drop(self, dbname):
        pass

    def connect_to(self, dbname, schema):
        return None

#
# Unit tests for Server class
#


TEST_CONNECT_DBNAME = "postgres"

class TestServerClass(unittest.TestCase):

    def test_connect(self):
        "Test pscopg2 connection to the server"

        try:
            test_server().raw_connect_to(TEST_CONNECT_DBNAME,
                                         test_server().user)
        except psycopg2.Error as err:
            self.fail("Unable to connect as user '%s'" % test_server().user +
                      ((":\n" + err.pgerr) if err.pgerr else ""))

        try:
            test_server().raw_connect_to(TEST_CONNECT_DBNAME,
                                       test_server().superuser)
        except psycopg2.Error as err:
            self.fail("Unable to connect as superuser '%s'" %
                      test_server().superuser +
                      ((":\n" + err.pgerr) if err.pgerr else ""))


#
# Database class
#

class Database(object):

    def __init__(self, name, server, schema, connection):
        self.name = name
        self.server = server
        self.schema = schema
        self.connection = connection

    def get_column_list(self, table):
        """Return a list of the columns in a database table."""

        curs = self.connection.cursor()
        sql = """-- get_column_list query
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;"""
        curs.execute(sql, (self.schema, table))
        return [tup[0] for tup in curs.fetchall()]

    def get_table_list(self):
        """Return a list of the tables in a database."""

        curs = self.connection.cursor()
        sql = """-- get_table_list query
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name;"""
        curs.execute(sql, (self.schema,))
        return [tup[0] for tup in curs.fetchall()]

    def table_exists(self, table):
        """Returns True if the table exists in the database."""

        return table in self.get_table_list()

    def get_primary_key(self, table):
        """Returns the primary key for a table as a list of columns."""

        curs = self.connection.cursor()
        sql = """-- get_primary_key query
            SELECT column_name FROM information_schema.key_column_usage
            WHERE constraint_schema = %(schema)s AND constraint_name IN
                (SELECT constraint_name
                 FROM information_schema.table_constraints
                 WHERE table_schema = %(schema)s AND table_name = %(table)s AND
                     constraint_type = 'PRIMARY KEY')
            ORDER BY ordinal_position;"""
        curs.execute(sql, {'schema': self.schema, 'table': table})
        return [tup[0] for tup in curs.fetchall()]

    def __getattr__(self, attrname):
        """Delegate other attributes to the psycopg2 connection.

        The cursor() method (to get a new cursor) is the call that is
        needed to access the database, and is accessed through this delegation
        method. The other attributes are also available , but are probably not
        needed, as the connection should be set to autocommit. Use them at
        your own risk.
        """
        return getattr(self.connection, attrname)

#
# Define test suites
#

def the_suite():
    """Returns a test suite of all the tests in this module."""

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestServerClass)
    return suite

#
# Run unit tests if in __main__
#

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(the_suite())
