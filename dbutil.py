
"""dbutil.py - PostgreSQL database utilities for testing.

This module provides easy access to the test database server, and
provides a way to create, load, save and drop databases from this server.

It also provides wrapper classes for psycopg2 database connections that
implement utility queries as methods.
"""

import os
import random
import subprocess
import psycopg2

#
# Setup information for the test server. This might be better off loaded
# from a config file, but this will do for now. The password is kept
# in a .pgpass file to avoid saving it in versioned files. This is likely
# a better solution than recording the password either here or in a config
# file.
#

TESTSERVER_PARAMS = {
    'name': 'test_server',
    'host': '130.56.244.226',
    'port': '6432',
    'user': 'cube_tester',
    'superuser': 'cube_admin',
    'save_dir': '/g/data1/v10/test_resources/databases'}

#
# Database connection constants. These would be better off being defaults
# for items that can be overridden by a configuration file.
#

CONNECT_TIMEOUT = 60
MAINTENANCE_DB = 'postgres'
TEMPLATE_DB = 'template0'
USE_PGBOUNCER = True
PGBOUNCER_DB = 'pgbouncer'

#
# Random string constants. These set the parameters for random strings
# appended to database names by the random_name utility function. The intent
# is to make temporary database names (most likely) unique to avoid clashes.
# The current format is 9 decimal digits.
#

RANDOM_STR_MIN = 1
RANDOM_STR_MAX = 999999999
RANDOM_STR_FORMAT = "%09d"

#
# Server class
#


class Server(object):
    """Abstraction of a database server.

    Gathers all the parameters that describe a server or how to work
    with it, and provides services that use this information."""

    def __init__(self, params):
        self.name = params['name']
        self.host = params['host']
        self.port = params['port']
        self.user = params['user']
        self.superuser = params['superuser']
        self.save_dir = params['save_dir']

    def connect(self, dbname, superuser=False, autocommit=True):
        """create a pscopg2 connection to a database and return it.

        dbname: The database to connect to.
        superuser: Set to True to connect as the superuser, otherwise
            connect as the user.
        autocommit: Set to False to turn off autocommit, otherwise
            autocommit will be turned on."""

        user = (self.superuser if superuser else self.user)
        dsn = ("dbname=%s host=%s port=%s user=%s connect_timeout=%s" %
               (dbname, self.host, self.port, user, CONNECT_TIMEOUT))
        conn = psycopg2.connect(dsn)
        conn.autocommit = autocommit

        return conn

    def load(self, dbname, save_file):
        """Load the contents of a database from a file.

        The database should be empty, and based off template0 or
        equivalent. This method calls the psql command to do the load."""

        save_path = os.path.join(self.save_dir, save_file)
        load_cmd = ["psql",
                    "--dbname=%s" % dbname,
                    "--username=%s" % self.superuser,
                    "--host=%s" % self.host,
                    "--port=%s" % self.port,
                    "--file=%s" % save_path]

        try:
            subprocess.check_output(load_cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as err:
            # Make sure error output is in the error message.
            message = ("%s: problem calling %s:\n%s" %
                       (__name__, err.cmd[0], err.output))
            raise Exception(message)

    def save(self, dbname, save_file):
        """Save the contents of a database to a file.

        This method calls the pg_dump command to do the save. This
        dump is in sql script format so use psql to reload."""

        save_path = os.path.join(self.save_dir, save_file)
        save_cmd = ["pg_dump",
                    "--dbname=%s" % dbname,
                    "--username=%s" % self.superuser,
                    "--host=%s" % self.host,
                    "--port=%s" % self.port,
                    "--file=%s" % save_path]

        try:
            subprocess.check_output(save_cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as err:
            #Make sure error output is in the error message.
            message = ("%s: problem calling %s:\n%s" %
                       (__name__, err.cmd[0], err.output))
            raise Exception(message)

    def drop(self, dbname):
        """Drop the named database.

        Connections are closed explicitly with try/finally blocks,
        since they do not seem to be closed automatically in the
        case of exceptions and this causes problems.

        If pgbouncer is in use a pgbouncer pause command needs to
        be issued before dropping the database. This will wait
        until active transactions are complete."""

        maint_conn = MaintenanceWrapper(
            self.connect(MAINTENANCE_DB, superuser=True))
        try:
            if maint_conn.exists(dbname):
                if USE_PGBOUNCER:
                    bouncer_conn = BouncerWrapper(
                        self.connect(PGBOUNCER_DB, superuser=True))
                    try:
                        bouncer_conn.pause(dbname)
                        maint_conn.drop(dbname)
                    finally:
                        bouncer_conn.close()
                else:
                    maint_conn.drop(dbname)
        finally:
            maint_conn.close()

    def create(self, dbname, save_file):
        """Creates and loads a database from a file.

        This method does a clean create and load of the named database
        from the file 'savefile'. It drops an old database of the same
        name if neccessary.

        Connections are closed explicitly with try/finally blocks,
        since they do not seem to be closed automatically in the
        case of exceptions and this causes problems.

        If pgbouncer is in use a pgbouncer pause command needs to
        be issued before dropping the database. This will wait
        until active transactions are complete. The pgbouncer
        resume command is issued once the database is (re)created.
        This is needed to prevent connection attempts to the new database
        from hanging or returning errors if pgbouncer had pools set
        up on the old database."""

        maint_conn = MaintenanceWrapper(
            self.connect(MAINTENANCE_DB, superuser=True))
        try:
            # Create the database, dropping it first if needed.
            if USE_PGBOUNCER:
                bouncer_conn = BouncerWrapper(
                    self.connect(PGBOUNCER_DB, superuser=True))
                try:
                    if maint_conn.exists(dbname):
                        bouncer_conn.pause(dbname)
                        maint_conn.drop(dbname)
                    maint_conn.create(dbname)
                    bouncer_conn.resume(dbname)
                finally:
                    bouncer_conn.close()
            else:
                if maint_conn.exists(dbname):
                    maint_conn.drop(dbname)
                maint_conn.create(dbname)

            # Load the new database from the save file
            self.load(dbname, save_file)

            # Run ANALYSE on the newly loaded database
            db_conn = ConnectionWrapper(self.connect(dbname, superuser=True))
            try:
                db_conn.analyse()
            finally:
                db_conn.close()

            # All done
        finally:
            maint_conn.close()

#
# Connection wrappers.
#


class ConnectionWrapper(object):
    """Generic connection wrapper, inherited by the specific wrappers.

    This is a wrapper for a psycopg2 database connection. It
    passes on unknown attribute references to the wrapped connection
    using __get_attr__. The specific wrappers that inherit from this
    implement queries and operations on the connection (self.conn)
    as methods.

    Some utility methods are implemented here. database_name is
    useful for testing and error messages. analyse is used after
    a database has been created."""

    def __init__(self, conn):
        self.conn = conn

    def database_name(self):
        """Returns the name of the connected database."""

        sql = ("SELECT catalog_name\n" +
               "FROM information_schema.information_schema_catalog_name;")

        with self.conn.cursor() as curs:
            curs.execute(sql)
            dbname = curs.fetchone()[0]

        return dbname

    def analyse(self):
        """Runs the ANALYSE command on the connected database."""

        with self.conn.cursor() as curs:
            curs.execute("ANALYSE;")

    def __getattr__(self, attrname):
        """Delegate unknown attributes to the psycopg2 connection."""

        return getattr(self.conn, attrname)


class MaintenanceWrapper(ConnectionWrapper):
    """Wrapper for a connection intented for maintenance commands."""

    def exists(self, dbname):
        """Returns True if the named database exists."""

        exists_sql = ("SELECT datname FROM pg_database\n" +
                      "WHERE datname = %(dbname)s;")

        with self.conn.cursor() as curs:
            curs.execute(exists_sql, {'dbname': dbname})
            db_found = bool(curs.fetchone())

        return db_found

    def drop(self, dbname):
        """Drops the named database."""

        drop_sql = "DROP DATABASE %s;" % safe_name(dbname)

        with self.conn.cursor() as curs:
            curs.execute(drop_sql)

    def create(self, dbname):
        """Creates the named database."""

        create_sql = ("CREATE DATABASE %s\n" % safe_name(dbname) +
                      "TEMPLATE %s;" % TEMPLATE_DB)

        with self.conn.cursor() as curs:
            curs.execute(create_sql)


class BouncerWrapper(ConnectionWrapper):
    """Wrapper for a connection to the pgbouncer console pseudo-database.

    Obviously these commands will not work if connected to an ordinary
    database.

    These commands will ignore errors since pgbouncer may
    not know about the database the operations are being done on, but
    the commands have to be run anyway in case it does."""

    def pause(self, dbname):
        """Tells pgbouncer to pause the named database.

        This should cause pgbouncer to disconnect from dbname, first
        waiting for any queries to complete. This allows the database
        to be dropped.
        """

        pause_sql = "PAUSE %s;" % safe_name(dbname)

        with self.conn.cursor() as cur:
            try:
                cur.execute(pause_sql)
            except psycopg2.DatabaseError:
                pass

    def resume(self, dbname):
        """Tells pgbouncer to resume work on the named database.

        If this is not called and the database was previously
        paused then connection attempts will hang or give errors."""

        resume_sql = "RESUME %s;" % safe_name(dbname)

        with self.conn.cursor() as cur:
            try:
                cur.execute(resume_sql)
            except psycopg2.DatabaseError:
                pass


#
# Utility functions
#

def random_name(basename=""):
    """Returns a database name with a 9 digit random number appended."""

    random_str = (RANDOM_STR_FORMAT %
                  random.randint(RANDOM_STR_MIN, RANDOM_STR_MAX))

    return basename + "_" + random_str


def safe_name(dbname):
    """Returns a database name with non letter, digit, _ characters removed."""

    char_list = [c for c in dbname if c.isalnum() or c == '_']

    return "".join(char_list)

#
# Test server instance:
#

TESTSERVER = Server(TESTSERVER_PARAMS)
