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
#     * Neither Geoscience Australia nor the names of its contributors may be
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

"""
    Command line interface to test db server utilities.
"""

import argparse
import os
import sys
import logging
import re
import dbutil
from EOtools.execute import execute

#
# Temporary test database pattern
#
# This is the regular expression used to identify a test database.
#
# The current pattern looks for a name containing 'test' and ending
# in an underscore followed by a 9 digit number.
#

TESTDB_PATTERN = r".*test.*_\d{9}$"

#
# Default database file
#
# This is the path to the empty hypercube dump used as a base for newly
# created databases.
#

DEFAULT_DBFILE = os.path.join(dbutil.TEST_RESOURCES_ROOT,
                              'databases/hypercube_empty.sql')

#
# Temporary directory.
#
# This is used for a temporary copy of the config file.
#

#TEMP_DIR = dbutil.temp_directory()
TEMP_DIR = './temp'

#
# Set up logging
#

logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
LOGGER = logging.getLogger()

#
# Argument parser setup functions
#


def command_line_parser():
    """Return the top level parser."""

    description = "Run utility commands on the test database server."
    parser = argparse.ArgumentParser(description=description)
    subparser_factory = parser.add_subparsers(title="subcommands")

    add_create_subcommand(subparser_factory)
    add_save_subcommand(subparser_factory)
    add_drop_subcommand(subparser_factory)
    add_list_subcommand(subparser_factory)
    add_cleanup_subcommand(subparser_factory)
#    add_dbupdate_subcommand(subparser_factory)

    return parser


def add_create_subcommand(subparser_factory):
    """Add a subparser for the create subcommand."""

    create_help = "Create and load a database from an sql dump file."
    subparser = subparser_factory.add_parser('create', help=create_help,
                                             description=create_help)

    dbname_help = "The name of the database to be created."
    subparser.add_argument('dbname', help=dbname_help)

    dbfile_help = ("An sql database dump to be loaded into the new " +
                   "database. If not given, an empty hypercube database " +
                   "will be loaded.")
    subparser.add_argument('dbfile', help=dbfile_help, nargs='?',
                           default=DEFAULT_DBFILE)

    subparser.set_defaults(subcommand=run_create_subcommand)


def add_save_subcommand(subparser_factory):
    """Add a subparser for the save subcommand."""

    save_help = "Save a database to an sql dump file."
    subparser = subparser_factory.add_parser('save', help=save_help,
                                             description=save_help)

    dbname_help = "The name of the database to be saved."
    subparser.add_argument('dbname', help=dbname_help)

    dbfile_help = "The sql dump file to save to."
    subparser.add_argument('dbfile', help=dbfile_help)

    subparser.set_defaults(subcommand=run_save_subcommand)


def add_drop_subcommand(subparser_factory):
    """Add a subparser for the drop subcommand."""

    drop_help = "Drop a database from the test server."
    subparser = subparser_factory.add_parser('drop', help=drop_help,
                                             description=drop_help)

    dbname_help = "The name of the database to drop."
    subparser.add_argument('dbname', help=dbname_help)

    subparser.set_defaults(subcommand=run_drop_subcommand)


def add_list_subcommand(subparser_factory):
    """Add a subparser for the list subcommand."""

    list_help = "List the databases on the test server."
    subparser = subparser_factory.add_parser('list', help=list_help,
                                             description=list_help)

    subparser.set_defaults(subcommand=run_list_subcommand)


def add_cleanup_subcommand(subparser_factory):
    """Add a subparser for the cleanup subcommand."""

    cleanup_help = "Drop all temporary test databases."
    description = (cleanup_help + " Note that running " +
                   "this command may cause tests currently running to fail.")
    subparser = subparser_factory.add_parser('cleanup', help=cleanup_help,
                                             description=description)

    subparser.set_defaults(subcommand=run_cleanup_subcommand)


def add_dbupdate_subcommand(subparser_factory):
    """Add a subparser for the dbupdate subcommand."""

    dbupdate_help = "Run dbupdater.py to catalog a dataset or datasets."
    description = (dbupdate_help + " This will create an acquisition_record " +
                   "and a dataset_record if they do not already exist.")
    subparser = subparser_factory.add_parser('dbupdate', help=dbupdate_help,
                                             description=description)

    dbname_help = "The name of the database to update."
    subparser.add_argument('dbname', help=dbname_help)

    source_dir_help = "The source directory for the datasets."
    subparser.add_argument('source_dir', help=source_dir_help)

    subparser.set_defaults(subcommand=run_dbupdate_subcommand)


#
# Subcommand functions
#


def run_create_subcommand(args):
    """Run the create subcommand."""

    LOGGER.debug("Running create subcommand:")
    LOGGER.debug("    dbname = %s", args.dbname)
    LOGGER.debug("    dbfile = %s", args.dbfile)

    dbutil.TESTSERVER.create(args.dbname, "", args.dbfile)


def run_save_subcommand(args):
    """Run the save subcommand."""

    LOGGER.debug("Running save subcommand:")
    LOGGER.debug("    dbname = %s", args.dbname)
    LOGGER.debug("    dbfile = %s", args.dbfile)

    dbutil.TESTSERVER.save(args.dbname, "", args.dbfile)


def run_drop_subcommand(args):
    """Run the drop subcommand."""

    LOGGER.debug("Running drop subcommand:")
    LOGGER.debug("    dbname = %s", args.dbname)

    dbutil.TESTSERVER.drop(args.dbname)


def run_list_subcommand(dummy_args):
    """Run the list subcommand."""

    LOGGER.debug("Running list subcommand:")

    dblist = dbutil.TESTSERVER.dblist()
    for dbname in sorted(dblist):
        print dbname


def run_cleanup_subcommand(dummy_args):
    """Run the cleanup subcommand."""

    LOGGER.debug("Running cleanup subcommand:")

    dblist = dbutil.TESTSERVER.dblist()
    test_dblist = [db for db in dblist if re.match(TESTDB_PATTERN, db)]

    print "Dropping temporary test databases:"
    if test_dblist:
        for dbname in test_dblist:
            print "    %s" % dbname
            dbutil.TESTSERVER.drop(dbname)
    else:
        print "    nothing to do."

def run_dbupdate_subcommand(args):
    """Run the dbupdate subcommand."""

    raise NotImplementedError


# def run_dbupdate_subcommand(args):
#     """Run the dbupdate subcommand."""

#     LOGGER.debug("Running dbupdate subcommand:")
#     LOGGER.debug("    dbname = %s", args.dbname)
#     LOGGER.debug("    source_dir = %s", args.source_dir)

#     config_file_name = dbutil.random_name("test_datacube") + ".conf"
#     config_file_path = dbutil.make_config_file(args.dbname, TEMP_DIR,
#                                                config_file_name)

#     dbupdater_cmd = ["python",
#                      "dbupdater.py",
#                      "--debug",
#                      "--config=%s" % config_file_path,
#                      "--source=%s" % args.source_dir,
#                      "--removedblist",
#                      "--followsymlinks"]
#     result = execute(dbupater_cmd, shell=False)

#
# Main program
#

if __name__ == '__main__':
    ARGS = command_line_parser().parse_args()
    ARGS.subcommand(ARGS)
