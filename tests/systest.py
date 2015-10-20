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

"""
    systest.py - system test runner.
"""

import os
import sys
import errno
import re
import ConfigParser
import argparse
import json

import psycopg2

from agdc import dbutil
from eotools.execute import execute

#
# Constants
#

DEFAULT_DATACUBE_CONFIG_TEMPLATE = os.path.join(dbutil.TEST_RESOURCES_ROOT,
                                                'config/template.conf')
DEFAULT_DATACUBE_CONFIG_NAME = 'datacube.conf'

#
# Systest Class
#


class Systest(object):
    """Class for a system test case."""
    # pylint: disable=too-many-instance-attributes
    def __init__(self, test_name, config):

        self.test_name = test_name
        self.result = ''
        self.error_message = None

        try:
            self.test_dir = self.create_dir('.', test_name)

            self.scenes_dir = self.create_dir(self.test_dir, 'scenes')
            self.remove_old_links()
            if config.has_option(test_name, 'scenes'):
                scene_list = self.link_scenes(test_name, config)
            else:
                scene_list = None

            # export the list of scenes as environment variables for use by
            # called shell script
            for iscene in range(len(scene_list)):
                os.environ['SCENE_DIR%d' %iscene] = scene_list[iscene]
            os.environ['Nscenes'] = str(len(scene_list))

            os.environ['SYSTEST_DIR'] = self.test_dir

            self.temp_dir = self.create_dir(self.test_dir, 'temp')
            self.tile_dir = self.create_dir(self.test_dir, 'tiles')


            self.dbname = dbutil.random_name(test_name)
            self.load_initial_database(test_name, config)
            self.make_datacube_config(test_name, config)

            self.command = None
            if config.has_option(test_name, 'command'):
                self.command = config.get(test_name, 'command')

            os.environ['DATACUBE_ROOT'] = \
                config.get(test_name, 'datacube_root')

            self.logfile = self.open_logfile(test_name)

        except AssertionError as e:
            self.result = 'ERROR'
            self.error_message = e.message


    @staticmethod
    def create_dir(parent_dir, dir_name):
        """Create a directory (if it does not already exist)."""

        dir_path = os.path.abspath(os.path.join(parent_dir, dir_name))
        try:
            os.mkdir(dir_path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise AssertionError('Unable to create directory %s: %s' %
                                     (dir_path, e.strerror))
        return dir_path

    def remove_old_links(self):
        """Clear out any old symbolic links from the scenes directory."""

        for file_name in os.listdir(self.scenes_dir):
            file_path = os.path.join(self.scenes_dir, file_name)
            if os.path.islink(file_path):
                try:
                    os.remove(file_path)
                except IOError as e:
                    raise AssertionError('Unable to remove symbolic link' +
                                         '%s: %s' % (file_path, e.strerror))

    def link_scenes(self, test_name, config):
        """Make symbolic links to the listed scenes."""

        scene_list = json.loads(config.get(test_name, 'scenes'))
        for scene_path in scene_list:

            if not os.path.exists(scene_path):
                raise AssertionError('Scene %s does not exist.' % scene_path)

            scene_path = os.path.abspath(scene_path)
            scene_name = os.path.basename(re.sub(r'[/\\]$', '', scene_path))
            link_path = os.path.join(self.scenes_dir, scene_name)

            try:
                os.symlink(scene_path, link_path)
            except IOError as e:
                raise AssertionError('Unable to make link to scene %s: %s' %
                                     (scene_path, e.strerror))
        return scene_list

    def make_datacube_config(self, test_name, config):
        """Make a datacube config file using a template and test info."""

        template = config.get(test_name, 'datacube_config_template')
        (template_dir, template_name) = os.path.split(template)

        output_name = config.get(test_name, 'datacube_config_name')

        try:
            updates = {'dbname': self.dbname,
                       'temp_dir': self.temp_dir,
                       'tile_root': self.tile_dir}
            dbutil.update_config_file2(updates, template_dir, self.test_dir,
                                       template_name, output_name)

        except IOError as e:
            raise AssertionError('Unable to update datacube config file: %s' %
                                 e.strerror)

    def load_initial_database(self, test_name, config):
        """Depending on the value of initial_database in the system test config
        file, this method does one of two things:
        1. If the config file's initial_database value has suffix ".sql" then
        a database named self.dbname is created from the named .sql file of
        psql commands.
        2. If the config file's initial_database value does not have suffix
        ".sql" then it is taken to be the name of an existing database on to
        which test datasets are loaded."""

        initial_database = config.get(test_name, 'initial_database')

        # initial_database in config file can be an existing database, rather
        # than a file of sql commands from which to create the database
        m = re.match(r'.+\.sql', initial_database)
        if m is None:
            self.dbname = initial_database
            return

        (save_dir, save_file) = os.path.split(initial_database)

        try:
            dbutil.TESTSERVER.create(self.dbname, save_dir, save_file)
        except psycopg2.Error as e:
            raise AssertionError('Unable to create initial database: %s' %
                                 e.pgerror)
    @staticmethod
    def open_logfile(test_name):
        """Open and return a logfile for the test."""

        logfile_path = os.path.join(test_name, test_name + '.log')
        try:
            logfile = open(logfile_path, 'w')
        except IOError as e:
            raise AssertionError('Unable to open logfile %s: %s' %
                                 (logfile_path, e.strerror))
        return logfile

    def run(self):
        """Run the system test."""

        if self.result:
            return self.result

        elif self.command:

            print 'Changing directory:'
            os.chdir(self.test_name)
            print 'Current directory is now:', os.getcwd()
            print ''

            print 'Running command:'
            print self.command
            print ''

            exe_result = execute(self.command)
            self.logfile.write(exe_result['stdout'])
            self.logfile.write(exe_result['stderr'])
            if exe_result['returncode'] != 0:
                self.error_message = exe_result['stderr']
                return 'ERROR'

            os.chdir('..')

            return 'Command run.'

        else:
            return 'No command to run.'

#
# Top level functions
#


def parse_args():
    """Parse and return command line arguments using argparse."""

    argparser = argparse.ArgumentParser(description='System test runner.')
    argparser.add_argument('config_file',
                           help='config file specifiying the system tests.')
    return argparser.parse_args()


def read_config(args):
    """Read and return parsed config file using ConfigParser."""

    script_path = os.path.abspath(sys.argv[0])
    script_dir = os.path.dirname(script_path)
    datacube_root = os.environ.get('DATACUBE_ROOT', script_dir)

    defaults = {
        'run_test': 'true',
        'datacube_root': datacube_root,
        'datacube_config_template': DEFAULT_DATACUBE_CONFIG_TEMPLATE,
        'datacube_config_name': DEFAULT_DATACUBE_CONFIG_NAME
        }

    config = ConfigParser.SafeConfigParser(defaults)
    with open(args.config_file) as fp:
        config.readfp(fp)

    return config


def run_tests():
    """Run the system tests as specified in the config file."""

    args = parse_args()
    config = read_config(args)
    for test_name in config.sections():
        print "Running system test: %s" % test_name
        systest = Systest(test_name, config)
        result = systest.run()
        print "Result: %s" % result
        if result == 'ERROR':
            print systest.error_message
        print ""


def dump_environment():
    """Dump selected environment variables to standard output."""

    print "Envirionment Variables:"
    print ""
    print "PYTHONPATH =", os.environ['PYTHONPATH']
    print ""
    print "PATH =", os.environ['PATH']
    print ""
    print "DATACUBE_ROOT =", os.environ['DATACUBE_ROOT']
    print ""
    print ""


def dump_config(config):
    """Dump the contents of a parsed config file."""

    print "Config File:"
    print ""

    for section in config.sections():
        print "Section: ", section
        print ""

        for (name, value) in config.items(section):
            print "%s: '%s'" % (name, value)

        if config.has_option(section, 'scenes'):
            scenes = json.loads(config.get(section, 'scenes'))
            print "json scenes:", scenes

        print ""

#
# Main program
#


if __name__ == '__main__':
    run_tests()
