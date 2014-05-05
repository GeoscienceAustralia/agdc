
"""dbutil.py - PostgreSQL database utilities for testing.

This module provides easy access to the test database server, and
provides a way to create, load, save and drop databases from this server.

It also provides wrapper classes for psycopg2 database connections that
implement utility queries as methods.
"""
import os
import sys
import logging
import pprint
import random
import subprocess
import re
import psycopg2

#
# Root directory for test resources.
#

TEST_RESOURCES_ROOT = '/g/data1/v10/test_resources'

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
    'superuser': 'cube_admin'
    }

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

    def connect(self, dbname, superuser=False, autocommit=True):
        """Create a pscopg2 connection to a database and return it.

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

    def exists(self, dbname):
        """Returns True if the named database exists on the server."""

        maint_conn = MaintenanceWrapper(
            self.connect(MAINTENANCE_DB, superuser=True))
        try:
            result = maint_conn.exists(dbname)
        finally:
            maint_conn.close()

        return result

    def load(self, dbname, save_dir, save_file):
        """Load the contents of a database from a file.

        The database should be empty, and based off template0 or
        equivalent. This method calls the psql command to do the load."""

        save_path = os.path.join(save_dir, save_file)
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
            for k in range(len(load_cmd)):
                message = message + load_cmd[k]
            raise Exception(message)

    def save(self, dbname, save_dir, save_file):
        """Save the contents of a database to a file.

        This method calls the pg_dump command to do the save. This
        dump is in sql script format so use psql to reload."""

        save_path = os.path.join(save_dir, save_file)
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

    def create(self, dbname, save_dir, save_file):
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
            self.load(dbname, save_dir, save_file)

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
    using __getattr__. The specific wrappers that inherit from this
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


def resources_directory(*names):
    """Returns the path to a test resources directory, creating it if needed.

    The path of the directory is TEST_RESOURCES_ROOT/name1/name2/...
    where name1, name2, ... are the names passed in as parameters.
    """

    test_dir = os.path.join(TEST_RESOURCES_ROOT, *names)

    if not os.path.isdir(test_dir):
        # Allow group permissions on the directory we are about to create
        old_umask = os.umask(0o007)
        # Make the directories
        os.makedirs(test_dir)
        # Put back the old umask
        os.umask(old_umask)

    return test_dir


def version_or_user(version=None, user=None):
    """Returns the version or user for a test resources directory.

    Returns the version string, unless version is 'user', in which case
    the user string is returned instead. Defaults are described below.

    version: The version of the datacube code. This is expected to be either
        'develop', 'user', or a version number. If not given it is taken
        from the DATACUBE_VERSION environment variable. If the DATACUBE_VERSION
        variable is not defined it is taken to be 'user'.
    user: The user name. This is used in place of version if version is 'user'.
        If this is not defined it is taken from the USER environment variable.
    """

    if not version:
        # Using 'not version' rather than 'version is None' here because
        # "" is NOT a valid version.
        version = os.environ.get('DATACUBE_VERSION', 'user')

    if version == 'user':
        if not user:
            # Using 'not user' rather than 'user is None' here because
            # "" is NOT a valid user.
            user = os.environ['USER']
        return user
    else:
        return version


def input_directory(module, suite, version=None, user=None):
    """Returns a path to a test input directory, creating it if needed.

    The path of the directory is
    TEST_RESOURCES_ROOT/version/input/module/suite/. If the version is
    'user' then the user argument takes the place of version in the path.

    module: The name of the module being tested, eg 'dbcompare'.
    suite: The name of the test suite of test class containting the test,
        eg 'TestReporter'.
    version: The version of the datacube code. This is expected to be either
        'develop', 'user', or a version number. If not given it is taken
        from the DATACUBE_VERSION environment variable. If the DATACUBE_VERSION
        variable is not defined it is taken to be 'user'.
    user: The user name. This is used in place of version if version is 'user'.
        If this is not defined it is taken from the USER environment variable.

    The 'input' directory is for input or setup files for tests. The
    files are expected to be named after the test that uses them.
    """

    version = version_or_user(version, user)
    return resources_directory(version, 'input', module, suite)


def output_directory(module, suite, user=None):
    """Returns the path to a test output directory, creating it if needed.

    The path of the directory is TEST_RESOUCES_ROOT/user/output/module/suite/.
    If user is not given, the environment variable USER is used as the
    name of the user.

    module: the name of the module being tested, eg 'dbcompare'
    suite: the name of the test suite or test class containting the test,
        eg 'TestReporter'

    The 'output' directory is for the output of the tests. The files are
    expected to be named after the test that produces them.
    """

    version = version_or_user(version='user', user=user)
    return resources_directory(version, 'output', module, suite)


def expected_directory(module, suite, version=None, user=None):
    """Returns a path to a test expected directory, creating it if needed.

    The path of the directory is
    TEST_RESOURCES_ROOT/version/expected/module/suite/. If the version is
    'user' then the user argument takes the place of version in the path.

    module: The name of the module being tested, eg 'dbcompare'.
    suite: The name of the test suite of test class containting the test,
        eg 'TestReporter'.
    version: The version of the datacube code. This is expected to be either
        'develop', 'user', or a version number. If not given it is taken
        from the DATACUBE_VERSION environment variable. If the DATACUBE_VERSION
        variable is not defined it is taken to be 'user'.
    user: The user name. This is used in place of version if version is 'user'.
        If this is not defined it is taken from the USER environment variable.

    The 'expected' directory is for the expected output of the tests. The
    files are expected to be named after the test that produces them. These
    files are used to automate the tests by comparing output produced against
    expected output.
    """

    version = version_or_user(version, user)
    return resources_directory(version, 'expected', module, suite)


def update_config_file(dbname, input_dir, output_dir, config_file_name):
    """Creates a temporary datacube config file by updating the database name.

    This function returns the path to the updated config file.

    dbname: the name of the database to connect to.
    input_dir: the directory containing the config file template.
    output_dir: the directory in which the updated config file will be written.
    config_file_name: the name of the config file (template and updated).
    """

    template_path = os.path.join(input_dir, config_file_name)
    update_path = os.path.join(output_dir, config_file_name)

    with open(template_path) as template:
        template_str = template.read()

    update_str = re.sub(r'^\s*dbname\s*=\s*.*$', "dbname = " + dbname,
                        template_str, flags=re.MULTILINE)

    with open(update_path, 'w') as update:
        update.write(update_str)

    return update_path


def update_config_file2(parameter_values_dict, input_dir, output_dir,
                        config_file_name):
    """Creates a temporary datacube config file by updating those attributes
    according to the dictionary parameter_values.

    This function returns the path to the updated config file.

    parameter_values_dict: a dictionary of parameter-values to be inserted
        into the template config file
    input_dir: the directory containing the config file template.
    output_dir: the directory in which the updated config file will be written.
    config_file_name: the name of the config file (template and updated).
    """
    template_path = os.path.join(input_dir, config_file_name)
    update_path = os.path.join(output_dir, config_file_name)

    with open(template_path) as template:
        template_str = template.read()

    update_str = template_str
    for param, value in parameter_values_dict.items():
        update_str = re.sub(r'^\s*%s\s*=\s*.*$' % param,
                            "%s = %s" % (param, value),
                            update_str, flags=re.MULTILINE)

    with open(update_path, 'w') as update:
        update.write(update_str)

    return update_path


def create_logger(name, logfile_path=None):
    """Creates a logger object in the datacube style.

    This sets up a logger with handler, formatter, and level defined
    as is usual for the datacube scripts. 'name' is the name of the
    logger, __name__ (the current module) is a typical value.

    If 'logfile_path' is set it is taken as the name of a log file,
    which is opened in write mode and used to create the logger.
    Otherwise sys.stdout is used."""

    if logfile_path:
        console_handler = logging.FileHandler(logfile_path, mode='w')
    else:
        console_handler = logging.StreamHandler(sys.stdout)

    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)

    logger = logging.getLogger(name)
    if logger.level == logging.NOTSET:
        logger.setLevel(logging.DEBUG)  # Default logging level for all modules
        logger.addHandler(console_handler)

    return logger


def log_multiline(log_function, log_text, title=None, prefix=''):
    """Function to log multi-line text.

    This is a clone of the log_multiline function from the ULA3 package.
    It is repeated here to reduce cross-repository dependancies.
    """

    LOGGER.debug('log_multiline(%s, %s, %s, %s) called',
                 log_function, repr(log_text), repr(title), repr(prefix))

    if type(log_text) == str:
        LOGGER.debug('log_text is type str')
        log_list = log_text.splitlines()
    elif type(log_text) == list and type(log_text[0]) == str:
        LOGGER.debug('log_text is type list with first element of type text')
        log_list = log_text
    else:
        LOGGER.debug('log_text is type ' + type(log_text).__name__)
        log_list = pprint.pformat(log_text).splitlines()

    log_function(prefix + '=' * 80)
    if title:
        log_function(prefix + title)
        log_function(prefix + '-' * 80)

    for line in log_list:
        log_function(prefix + line)

    log_function(prefix + '=' * 80)

#
# Module logger object
#

LOGGER = create_logger(__name__)

#
# Test server instance:
#

TESTSERVER = Server(TESTSERVER_PARAMS)
