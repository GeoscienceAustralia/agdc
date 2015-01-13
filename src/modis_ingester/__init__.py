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

    @classmethod
    def arg_parser(cls):
        """Make a parser for required args."""

        # Extend the default parser
        _arg_parser = super(ModisIngester, cls).arg_parser()

        _arg_parser.add_argument('--source', dest='source_dir',
            required=True,
            help='Source root directory containing datasets')

        return _arg_parser

    def find_datasets(self, source_dir):
        """Return a list of path to the netCDF datasets under 'source_dir' or a single-item list
        if source_dir is a netCDF file path
        """
        
        # Allow an individual netCDF file to be nominated as the source
        if os.path.isfile(source_dir) and source_dir.endswith(".nc"):
            LOGGER.debug('%s is a netCDF file')
            return [source_dir]

        assert os.path.isdir(source_dir), '%s is not a directory' % source_dir
        LOGGER.info('Searching for datasets in %s', source_dir)

        # Get all .nc files under source_dir (all levels)
        dataset_list = []
        for root, _dirs, files in os.walk(source_dir):
            dataset_list += [os.path.join(root, nc) for nc in files if nc.endswith('.nc')]
            
        dataset_list = sorted(dataset_list)
        
        LOGGER.debug('dataset_list = %s', dataset_list)
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
        (start_date, end_date) = self.get_date_range()
        (min_path, max_path) = self.get_path_range()
        (min_row, max_row) = self.get_row_range()

        include = ((int(max_path) is None or path is None or int(path) <= int(max_path)) and
                   (int(min_path) is None or path is None or int(path) >= int(min_path)) and
                   (end_date is None or date is None or date <= end_date) and
                   (start_date is None or date is None or date >= start_date))

        return include

    def preprocess_dataset(self, dataset_list):
        """Performs pre-processing on the dataset_list object.

        dataset_list: list of datasets to be opened and have
           its metadata read.
        """

        temp_dir = self.collection.get_temp_tile_directory()
        vrt_list = []

        for dataset_path in dataset_list:
            fname = os.path.splitext(basename(dataset_path))[0]
            dataset_dir = os.path.split(dataset_path)[0]

            mod09_fname = temp_dir + '/' + fname + '.vrt'
            rbq500_fname = temp_dir + '/' + fname + '_RBQ500.vrt'

            dataset = gdal.Open(dataset_path, gdal.GA_ReadOnly)
            subDataSets = dataset.GetSubDatasets()
            command_string = 'gdalbuildvrt -separate -overwrite '
            command_string += mod09_fname

            command_string += ' ' + subDataSets[1][0] # band 1
            command_string += ' ' + subDataSets[2][0] # band 2
            command_string += ' ' + subDataSets[3][0] # band 3
            command_string += ' ' + subDataSets[4][0] # band 4
            command_string += ' ' + subDataSets[5][0] # band 5
            command_string += ' ' + subDataSets[6][0] # band 6
            command_string += ' ' + subDataSets[7][0] # band 7

            result = execute(command_string=command_string)
            if result['returncode'] != 0:
                raise DatasetError('Unable to perform gdalbuildvrt on bands: ' +
                                   '"%s" failed: %s'\
                                       % (buildvrt_cmd, result['stderr']))

            vrt_list.append(mod09_fname)

            command_string = 'gdalbuildvrt -separate -overwrite '
            command_string += rbq500_fname

            command_string += ' ' + subDataSets[0][0] # 500m PQA

            result = execute(command_string=command_string)
            if result['returncode'] != 0:
                raise DatasetError('Unable to perform gdalbuildvrt on rbq: ' +
                                   '"%s" failed: %s'\
                                       % (buildvrt_cmd, result['stderr']))

            vrt_list.append(rbq500_fname)

        return vrt_list

