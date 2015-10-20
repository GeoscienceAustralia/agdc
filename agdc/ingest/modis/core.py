#!/usr/bin/env python

# ===============================================================================
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
# ===============================================================================

"""
Ingester for Modis datasets.
"""
from __future__ import absolute_import

import os
import logging
from os.path import basename

from osgeo import gdal

from eotools.execute import execute

from agdc.ingest import SourceFileIngester
from agdc.cube_util import DatasetError
from .modis_dataset import ModisDataset

_LOG = logging.getLogger(__name__)


def _is_modis_file(filename):
    """
    Does the given file match a Modis NetCDF file?

    (we could make this more extensive in the future, but it's directly derived from the old find_files() logic.

    :type filename: str
    :rtype: bool
    >>> d = '/g/data/u39/public/data/modis/datacube/mod09-swath/terra/2010/12/31'
    >>> f = 'MOD09_L2.2010365.2300.20130130162407.remapped_swath_500mbands_0.005deg.nc'
    >>> _is_modis_file(f)
    True
    >>> _is_modis_file(os.path.join(d, f))
    True
    >>> _is_modis_file(d)
    False
    """
    basename = os.path.basename(filename).lower()
    return basename.startswith('mod') and filename.endswith(".nc")


class ModisIngester(SourceFileIngester):
    """Ingester class for Modis datasets."""

    def __init__(self, datacube=None, collection=None):
        super(ModisIngester, self).__init__(_is_modis_file, datacube, collection)

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

        # Modis 'path' is actually the Orbit number.
        (min_path, max_path) = self.get_path_range()

        include = ((max_path is None or path is None or int(path) <= int(max_path)) and
                   (min_path is None or path is None or int(path) >= int(min_path)) and
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
                                   '%r failed: %r' % (command_string, result['stderr']))

            vrt_list.append(mod09_fname)

            command_string = 'gdalbuildvrt -separate -overwrite '
            command_string += rbq500_fname

            command_string += ' ' + subDataSets[0][0] # 500m PQA

            result = execute(command_string=command_string)
            if result['returncode'] != 0:
                raise DatasetError('Unable to perform gdalbuildvrt on rbq: ' +
                                   '%r failed: %r' % (command_string, result['stderr']))

            vrt_list.append(rbq500_fname)

        return vrt_list
