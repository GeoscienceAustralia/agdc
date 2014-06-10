"""
    systest.py - system test runner.
"""

import os
import sys
import errno
import ConfigParser
import argparse
import json

#
# Systest Class
#


class Systest(object):
    """Class for a system test case."""

    def __init__(self, test_name, config):

        self.result = ''
        self.exctype = None
        self.excval = None

        try:
            self.test_dir = self.create_dir('.', test_name)
            self.scenes_dir = self.create_dir(self.test_dir, 'scenes')
            self.temp_dir = self.create_dir(self.test_dir, 'temp')
            self.tile_dir = self.create_dir(self.test_dir, 'tiles')

        except AssertionError:
            self.result = 'ERROR'
            (self.exctype, self.excval) = sys.exc_info()[:2]

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

    def run(self):
        """Run the system test."""

        return 'Not Implemented.'

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
        'datacube_root': datacube_root
        }

    config = ConfigParser.SafeConfigParser(defaults)
    config.read(args.config_file)

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

        if config.has_option(section, 'command'):
            command = json.loads(config.get(section, 'command'))
            print "json command:", command

        print ""

#
# Main program
#


if __name__ == '__main__':
    run_tests()
