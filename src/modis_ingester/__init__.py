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
    modis_ingester.py - Ingester script for Modis datasets.
"""

import os
import sys
import datetime
import re
import logging
import argparse

from os.path import basename
from osgeo import gdal
from EOtools.execute import execute
from agdc.abstract_ingester import AbstractIngester
from modis_dataset import ModisDataset

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


class ModisIngester(AbstractIngester):
    """Ingester class for Modis datasets."""

    @staticmethod
    def parse_args():
        """Parse the command line arguments for the ingester.

        Returns an argparse namespace object.
        """
        LOGGER.debug('  Calling parse_args()')

        _arg_parser = argparse.ArgumentParser()

        _arg_parser.add_argument('-C', '--config', dest='config_file',
            # N.B: The following line assumes that this module is under the agdc directory
            default=os.path.join(os.path.dirname(__file__), 'datacube.conf'),
            help='ModisIngester configuration file')

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

        # Get all files of source_dir
        dir_path, child_dirs, files = os.walk(source_dir).next()

        # find all NetCDF files in the source_dir
#        dataset_list = [ os.path.join(dir_path, x) for x in files if (os.path.splitext(x)[1] == ".nc") ]
        dataset_list = [ os.path.join(dir_path, x) for x in files if (x.endswith("float64.nc")) ]

        print dataset_list
        return dataset_list

    def open_dataset(self, dataset_path):
        """Create and return a dataset object.

        dataset_path: points to the dataset to be opened and have
           its metadata read.
        """
	   
        return ModisDataset(dataset_path)
    
    def filter_dataset(self, path, row, date):
        """Return True if the dataset should be included, False otherwise.

        Overridden to allow NULLS for row 
        """
        print "ModisIngester::filter_dataset()"
        (start_date, end_date) = self.get_date_range()
        (min_path, max_path) = self.get_path_range()
        (min_row, max_row) = self.get_row_range()

        include = ((int(max_path) is None or path is None or int(path) <= int(max_path)) and
                   (int(min_path) is None or path is None or int(path) >= int(min_path)) and
                   (end_date is None or date is None or date <= end_date) and
                   (start_date is None or date is None or date >= start_date))

        print "ModisIngester::filter_dataset() DONE"
        return include

    def preprocess_dataset(self, dataset_path):
        """Performs pre-processing on the dataset object.

        dataset_path: points to the dataset to be opened and have
           its metadata read.
        """

        print "dataset_path", dataset_path

        fname = os.path.splitext(basename(dataset_path))[0]
        dataset_dir = os.path.split(dataset_path)[0]

        dataset = gdal.Open(dataset_path, gdal.GA_ReadOnly)
        subDataSets = dataset.GetSubDatasets()
        command_string = 'gdalbuildvrt -separate -overwrite '
        command_string += dataset_dir + '/' + fname
        command_string += '.vrt'

        command_string += ' ' + subDataSets[14][0] # band 1
        command_string += ' ' + subDataSets[15][0] # band 2
        command_string += ' ' + subDataSets[16][0] # band 3
        command_string += ' ' + subDataSets[17][0] # band 4
        command_string += ' ' + subDataSets[18][0] # band 5
        command_string += ' ' + subDataSets[19][0] # band 6
        command_string += ' ' + subDataSets[20][0] # band 7
        command_string += ' ' + subDataSets[13][0] # 500m PQA
        """
        command_string += ' ' + subDataSets[11][0] # band 8
        command_string += ' ' + subDataSets[12][0] # band 9

        command_string += ' ' + subDataSets[3][0] # band 10
        command_string += ' ' + subDataSets[4][0] # band 11
        command_string += ' ' + subDataSets[5][0] # band 12
        command_string += ' ' + subDataSets[6][0] # band 13
        command_string += ' ' + subDataSets[7][0] # band 14
        command_string += ' ' + subDataSets[8][0] # band 15
        command_string += ' ' + subDataSets[9][0] # band 16

        command_string += ' ' + subDataSets[10][0] # band 26
        command_string += ' ' + subDataSets[1][0] # 1km PQA
        """
        result = execute(command_string=command_string)
