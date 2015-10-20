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
MosaicContents: database interface class.

These classes provide an interface between the database and the top-level
ingest algorithm (AbstractIngester and its subclasses). They also provide
the implementation of the database and tile store side of the ingest
process. They are expected to be independent of the structure of any
particular dataset, but will change if the database schema or tile store
format changes.
"""
from __future__ import absolute_import

import logging
import os
import re
import shutil
from eotools.execute import execute
from eotools.utils import log_multiline
from agdc.cube_util import DatasetError, get_file_size_mb, create_directory
from .ingest_db_wrapper import TC_MOSAIC
from osgeo import gdal
import numpy

# Set up logger.
LOGGER = logging.getLogger(__name__)

#
# Constants for PQA mosaic formation:
#

PQA_CONTIGUITY = 256  # contiguity = bit 8

#
# Classes
#


class MosaicContents(object):
    """MosaicContents database interface class.

    This class has 'remove' and 'make_permanent' methods, so can be
    used as a tile_contents object with the collection.Collection and
    collection.Transaction classes.
    """

    def __init__(self, tile_record_list, tile_type_dict,
                 level_name, temp_tile_dir):
        """Create the mosaic contents."""

        assert len(tile_record_list) > 1, \
            "Attempt to make a mosaic out of a single tile."
        assert len(tile_record_list) <= 2, \
            ("Attempt to make a mosaic out of more than 2 tiles.\n" +
             "Handling for this case is not yet implemented.")

        tile_dict = tile_record_list[0]
        tile_type_id = tile_dict['tile_type_id']
        tile_type_info = tile_type_dict[tile_type_id]

        if level_name == 'PQA':
            extension = tile_type_info['file_extension']
        else:
            extension = '.vrt'

        (self.mosaic_temp_path, self.mosaic_final_path) = (
            self.__get_mosaic_paths(tile_dict['tile_pathname'],
                                    extension,
                                    temp_tile_dir))

        if level_name == 'PQA':
            self.__make_mosaic_pqa(tile_record_list,
                                   tile_type_info,
                                   self.mosaic_temp_path)
        else:
            self.__make_mosaic_vrt(tile_record_list,
                                   self.mosaic_temp_path)

        self.mosaic_dict = dict(tile_dict)
        self.mosaic_dict['tile_id'] = None
        self.mosaic_dict['tile_pathname'] = self.mosaic_final_path
        self.mosaic_dict['tile_class_id'] = TC_MOSAIC
        self.mosaic_dict['tile_size'] = (
            get_file_size_mb(self.mosaic_temp_path))

    def remove(self):
        """Remove the temporary mosaic file."""
        if os.path.isfile(self.mosaic_temp_path):
            os.remove(self.mosaic_temp_path)

    def make_permanent(self):
        """Move mosaic tile contents to its permanent location."""

        shutil.move(self.mosaic_temp_path, self.mosaic_final_path)

    def get_output_path(self):
        """Return the final location for the mosaic."""

        return self.mosaic_final_path

    def create_record(self, db):
        """Create a record for the mosaic in the database."""

        db.insert_tile_record(self.mosaic_dict)

    @staticmethod
    def __get_mosaic_paths(tile_pathname, extension, temp_tile_dir):
        """Generate the temporary and final pathnames for the mosaic.

        'tile_pathname' is the path to the first tile in the mosaic.
        'extension' is the extension to use for the mosaic filename.
        Returns a tuple (mosaic_temp_path, mosaic_final_path).
        """

        (tile_dir, tile_basename) = os.path.split(tile_pathname)

        mosaic_final_dir = os.path.join(tile_dir, 'mosaic_cache')
        create_directory(mosaic_final_dir)

        mosaic_temp_dir = os.path.join(temp_tile_dir, 'mosaic_cache')
        create_directory(mosaic_temp_dir)

        mosaic_basename = re.sub(r'\.\w+$', extension, tile_basename)

        mosaic_temp_path = os.path.join(mosaic_temp_dir, mosaic_basename)
        mosaic_final_path = os.path.join(mosaic_final_dir, mosaic_basename)

        return (mosaic_temp_path, mosaic_final_path)

    @staticmethod
    def __make_mosaic_pqa(tile_record_list, tile_type_info, mosaic_path):
        """From the PQA tiles in tile_record_list, create a mosaic tile
        at mosaic_pathname.
        """

        LOGGER.info('Creating PQA mosaic file %s', mosaic_path)

        mosaic_file_list = [tr['tile_pathname'] for tr in tile_record_list]

        template_dataset = gdal.Open(mosaic_file_list[0])

        gdal_driver = gdal.GetDriverByName(tile_type_info['file_format'])

        #Set datatype formats appropriate to Create() and numpy
        gdal_dtype = template_dataset.GetRasterBand(1).DataType
        numpy_dtype = gdal.GetDataTypeName(gdal_dtype)

        mosaic_dataset = gdal_driver.Create(
            mosaic_path,
            template_dataset.RasterXSize,
            template_dataset.RasterYSize,
            1,
            gdal_dtype,
            tile_type_info['format_options'].split(','),
            )

        if not mosaic_dataset:
            raise DatasetError(
                'Unable to open output dataset %s' % mosaic_dataset)

        mosaic_dataset.SetGeoTransform(template_dataset.GetGeoTransform())
        mosaic_dataset.SetProjection(template_dataset.GetProjection())

        #TODO: make vrt here - not really needed for single-layer file
        # if tile_type_info['file_format'] == 'netCDF':
        #     pass

        output_band = mosaic_dataset.GetRasterBand(1)
        # Set all background values of data_array to FFFF (i.e. all ones)
        data_array = numpy.ones(shape=(template_dataset.RasterYSize,
                                       template_dataset.RasterXSize),
                                dtype=numpy_dtype
                                ) * -1
        # Set all background values of no_data_array to 0 (i.e. all zeroes)
        no_data_array = numpy.zeros(shape=(template_dataset.RasterYSize,
                                           template_dataset.RasterXSize),
                                    dtype=numpy_dtype
                                    )
        overall_data_mask = numpy.zeros((mosaic_dataset.RasterYSize,
                                         mosaic_dataset.RasterXSize),
                                        dtype=numpy.bool
                                        )
        del template_dataset

        # Populate data_array with -masked PQA data
        for pqa_dataset_index in range(len(mosaic_file_list)):
            pqa_dataset_path = mosaic_file_list[pqa_dataset_index]
            pqa_dataset = gdal.Open(pqa_dataset_path)
            if not pqa_dataset:
                raise DatasetError('Unable to open %s' % pqa_dataset_path)
            pqa_array = pqa_dataset.ReadAsArray()
            del pqa_dataset
            LOGGER.debug('Opened %s', pqa_dataset_path)

            # Treat contiguous and non-contiguous pixels separately
            # Set all contiguous pixels to true in data_mask
            pqa_data_mask = (pqa_array & PQA_CONTIGUITY).astype(numpy.bool)
            # Expand overall_data_mask to true for any contiguous pixels
            overall_data_mask = overall_data_mask | pqa_data_mask
            # Perform bitwise-and on contiguous pixels in data_array
            data_array[pqa_data_mask] &= pqa_array[pqa_data_mask]
            # Perform bitwise-or on non-contiguous pixels in no_data_array
            no_data_array[~pqa_data_mask] |= pqa_array[~pqa_data_mask]

        # Set all pixels which don't contain data to combined no-data values
        # (should be same as original no-data values)
        data_array[~overall_data_mask] = no_data_array[~overall_data_mask]

        output_band.WriteArray(data_array)
        mosaic_dataset.FlushCache()

    @staticmethod
    def __make_mosaic_vrt(tile_record_list, mosaic_path):
        """From two or more source tiles create a vrt"""

        LOGGER.info('Creating mosaic VRT file %s', mosaic_path)

        source_file_list = [tr['tile_pathname'] for tr in tile_record_list]

        gdalbuildvrt_cmd = ["gdalbuildvrt",
                            "-q",
                            "-overwrite",
                            "%s" % mosaic_path
                            ]
        gdalbuildvrt_cmd.extend(source_file_list)

        result = execute(gdalbuildvrt_cmd, shell=False)

        if result['stdout']:
            log_multiline(LOGGER.info, result['stdout'],
                                    'stdout from %s' % gdalbuildvrt_cmd, '\t')

        if result['stderr']:
            log_multiline(LOGGER.debug, result['stderr'],
                                    'stderr from %s' % gdalbuildvrt_cmd, '\t')

        if result['returncode'] != 0:
            raise DatasetError('Unable to perform gdalbuildvrt: ' +
                               '"%s" failed: %s'
                               % (gdalbuildvrt_cmd, result['stderr']))
