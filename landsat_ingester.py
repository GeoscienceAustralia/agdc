#!/usr/bin/env python
"""
    landsat_ingester.py - Ingester script for landsat datasets.
"""

import os
import sys
import datetime
import re
import logging
import argparse

import cube_util
from abstract_ingester import AbstractIngester
from landsat_dataset import LandsatDataset

#
# Set up root logger
#
# Note that the logging level of the root logger will be reset to DEBUG
# if the --debug flag is set (by AbstractIngester.__init__). To recieve
# DEBUG level messages from a module do two things:
#    1) set the logging level for the module you are interested in to DEBUG,
#    2) use the --debug flag when running the script.
#

logging.basicConfig(stream=sys.stdout,
                    format='%(message)s',
                    level=logging.INFO)

#
# Set up logger (for this module).
#

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class LandsatIngester(AbstractIngester):
    """Ingester class for Landsat datasets."""

    @staticmethod
    def parse_args():
        """Parse the command line arguments for the ingester.

        Returns an argparse namespace object.
        """
        LOGGER.debug('  Calling parse_args()')

        _arg_parser = argparse.ArgumentParser()

        _arg_parser.add_argument('-C', '--config', dest='config_file',
            default=os.path.join(os.path.dirname(__file__), 'datacube.conf'),
            help='LandsatIngester configuration file')

        _arg_parser.add_argument('-d', '--debug', dest='debug',
            default=False, action='store_const', const=True,
            help='Debug mode flag')

        _arg_parser.add_argument('--source', dest='source_dir',
            required=True,
            help='Source root directory containing datasets')

        follow_symlinks_help = \
            'Follow symbolic links when finding datasets to ingest'
        _arg_parser.add_argument('--followsymlinks',
                                 dest='follow_symbolic_links',
                                 default=False, action='store_const',
                                 const=True, help=follow_symlinks_help)

        fast_filter_help = 'Filter datasets using filename patterns.'
        _arg_parser.add_argument('--fastfilter', dest='fast_filter',
                                 default=False, action='store_const',
                                 const=True, help=fast_filter_help)

        sync_time_help = 'Synchronize parallel ingestions at the given time'\
            ' in seconds after 01/01/1970'
        _arg_parser.add_argument('--synctime', dest='sync_time',
                                 default=None, help=sync_time_help)

        sync_type_help = 'Type of transaction to syncronize with synctime,'\
            + ' one of "cataloging", "tiling", or "mosaicking".'
        _arg_parser.add_argument('--synctype', dest='sync_type',
                                 default=None, help=sync_type_help)

        return _arg_parser.parse_args()

    def find_datasets(self, source_dir):
        """Return a list of path to the datasets under 'source_dir'.

        Datasets are identified as a directory containing a 'scene01'
        subdirectory.

        Datasets are filtered by path, row, and date range if
        fast filtering is on (command line flag)."""

        LOGGER.info('Searching for datasets in %s', source_dir)
        if self.args.follow_symbolic_links:
            command = "find -L %s -name 'scene01' | sort" % source_dir
        else:
            command = "find %s -name 'scene01' | sort" % source_dir
        LOGGER.debug('executing "%s"', command)
        result = cube_util.execute(command)
        assert not result['returncode'], \
            '"%s" failed: %s' % (command, result['stderr'])

        dataset_list = [os.path.abspath(re.sub(r'/scene01$', '', scenedir))
                        for scenedir in result['stdout'].split('\n')
                        if scenedir]

        if self.args.fast_filter:
            dataset_list = self.fast_filter_datasets(dataset_list)

        return dataset_list

    def fast_filter_datasets(self, dataset_list):
        """Filter a list of dataset paths by path/row and date range."""

        new_list = []
        for dataset_path in dataset_list:
            match = re.search(r'_(\d{3})_(\d{3})_(\d{4})(\d{2})(\d{2})$',
                              dataset_path)
            if match:
                (path, row, year, month, day) = map(int, match.groups())

                if self.filter_dataset(path,
                                       row,
                                       datetime.date(year, month, day)):
                    new_list.append(dataset_path)
            else:
                # Note that dataset paths that do not match the pattern
                # are included. They will be filtered on metadata by
                # AbstractIngester.
                new_list.append(dataset_path)

        return new_list

    def open_dataset(self, dataset_path):
        """Create and return a dataset object.

        dataset_path: points to the dataset to be opened and have
           its metadata read.
        """

        return LandsatDataset(dataset_path)

# Start ingest process
if __name__ == "__main__":

    #pylint:disable=invalid-name
    #
    # Top level variables are OK if this is the top level script.
    #

    ingester = LandsatIngester()

    if ingester.args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    ingester.ingest(ingester.args.source_dir)

    ingester.collection.cleanup()
