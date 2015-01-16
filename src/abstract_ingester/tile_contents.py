#!/usr/bin/env python

# ===============================================================================
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
# ===============================================================================

"""
TileContents: database interface class.

These classes provide an interface between the database and the top-level
ingest algorithm (AbstractIngester and its subclasses). They also provide
the implementation of the database and tile store side of the ingest
process. They are expected to be independent of the structure of any
particular dataset, but will change if the database schema or tile store
format changes.
"""

import shutil
import logging
import os
import re
from datetime import datetime

from osgeo import gdal
import numpy as np

from EOtools.execute import execute
from EOtools.utils import log_multiline
from agdc.cube_util import DatasetError, create_directory


# Set up LOGGER.
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

#
# Constants for PQA nodata check:
#

PQA_CONTIGUITY = 256  # contiguity = bit 8


class TileContents(object):
    """TileContents database interface class."""
    # pylint: disable=too-many-instance-attributes
    def __init__(self, tile_root, tile_type_info,
                 tile_footprint, provisional_directory, band_stack):
        """Set the tile_footprint over which we want to resample this dataset.

        :type band_stack: AbstractBandstack
        """
        self.tile_type_id = tile_type_info['tile_type_id']
        self.tile_type_info = tile_type_info
        self.tile_footprint = tile_footprint
        self._band_stack = band_stack

        x_index, y_index = tile_footprint
        tile_output_root = os.path.join(
            tile_root,
            tile_type_info['tile_directory'],
            '%s_%s' % (band_stack.dataset_mdd['satellite_tag'],
                       re.sub(r'\W', '', band_stack.dataset_mdd['sensor_name']))
        )

        tile_output_dir = os.path.join(
            tile_output_root,
            re.sub(r'\+', '', ('%+04d_%+04d' % (tile_footprint[0],
                                                tile_footprint[1]))),
            '%04d' % band_stack.dataset_mdd['start_datetime'].year
        )

        self.tile_output_path = os.path.join(
            tile_output_dir,
            '_'.join([band_stack.dataset_mdd['satellite_tag'],
                      re.sub(r'\W', '', band_stack.dataset_mdd['sensor_name']),
                      band_stack.dataset_mdd['processing_level'],
                      re.sub(r'\+', '', '%+04d_%+04d' % (x_index, y_index)),
                      re.sub(':', '-', band_stack.dataset_mdd['start_datetime'].isoformat())
            ]) + tile_type_info['file_extension']
        )
        #Set the provisional tile location to be the same as the vrt created
        #for the scenes
        self._temp_tile_output_path = os.path.join(provisional_directory, os.path.basename(self.tile_output_path))

        # Work-around to allow existing GDAL code to work with netCDF subdatasets as band stacks
        # N.B: file_extension must be set to ".vrt" when used with netCDF
        #TODO: Change all code to use netCDF libraries instead of GDAL for netCDF file handling
        if self.tile_type_info['file_format'] == 'netCDF' and tile_type_info['file_extension'] == '.vrt':
            self.nc_temp_tile_output_path = re.sub('\.vrt$', '.nc', self._temp_tile_output_path)
            self.nc_tile_output_path = re.sub('\.vrt$', '.nc', self.tile_output_path)
        else:
            self.nc_temp_tile_output_path = None
            self.nc_tile_output_path = None

    @property
    def tile_extents(self):
        return _tile_extents(self.tile_footprint, self.tile_type_info)

    def has_data(self):
        return _has_data(self._temp_tile_output_path, self._band_stack)

    def reproject(self):
        """Reproject the scene dataset into tile coordinate reference system
        and extent. This method uses gdalwarp to do the reprojection."""

        # Work-around to allow existing code to work with netCDF subdatasets as GDAL band stacks
        temp_tile_output_path = self.nc_temp_tile_output_path or self._temp_tile_output_path

        _reproject(self.tile_type_info, self.tile_footprint, self._band_stack, temp_tile_output_path)

        # Work-around to allow existing code to work with netCDF subdatasets as GDAL band stacks
        if self.nc_temp_tile_output_path:
            _nc2vrt(self.nc_temp_tile_output_path, temp_tile_output_path)

    def remove(self):
        """Remove tiles that were in coverage but have no data. Also remove
        tiles if we are rolling back the transaction."""
        if os.path.isfile(self._temp_tile_output_path):
            os.remove(self._temp_tile_output_path)

    def make_permanent(self):
        """Move the tile file to its permanent location."""

        source_dir = os.path.abspath(os.path.dirname(self._temp_tile_output_path))
        dest_dir = os.path.abspath(os.path.dirname(self.tile_output_path))

        create_directory(dest_dir)

        # If required, edit paths in re-written .vrt file and move .nc file 
        if self.nc_tile_output_path:
            vrt_file = open(self._temp_tile_output_path, 'r')
            vrt_string = vrt_file.read()
            vrt_file.close()

            vrt_string = vrt_string.replace(source_dir, dest_dir)  # Update all paths in VRT file

            vrt_file = open(self.tile_output_path, 'w')
            vrt_file.write(vrt_string)
            vrt_file.close()

            # Move .nc file
            shutil.move(self.nc_temp_tile_output_path, self.nc_tile_output_path)

        else:  # No .vrt file required - just move the tile file
            shutil.move(self._temp_tile_output_path, self.tile_output_path)

    def get_output_path(self):
        """Return the final location for the tile."""

        return self.tile_output_path


def _tile_extents(tile_footprint, tile_type_info):
    x_origin = tile_type_info['x_origin']
    y_origin = tile_type_info['y_origin']
    x_size = tile_type_info['x_size']
    y_size = tile_type_info['y_size']
    x0 = x_origin + tile_footprint[0] * x_size
    y0 = y_origin + tile_footprint[1] * y_size
    return x0, y0, x0 + x_size, y0 + y_size


def _make_format_spec(tile_type_info):
    format_spec = []
    for format_option in tile_type_info['format_options'].split(','):
        format_spec.extend(["-co", "%s" % format_option])
    return format_spec


def _create_reproject_command(band_stack, first_file_number, nodata_value, temp_tile_output_path, tile_footprint,
                             tile_type_info):

    resampling_method = (
        band_stack.band_dict[first_file_number]['resampling_method']
    )
    if nodata_value is not None:
        # TODO: Check this works for PQA, where
        # band_dict[10]['resampling_method'] == None
        nodata_spec = [
            "-srcnodata",
            "%d" % nodata_value,
            "-dstnodata",
            "%d" % nodata_value
        ]
    else:
        nodata_spec = []

    tile_extents = _tile_extents(tile_footprint, tile_type_info)
    reproject_cmd = [
        "gdalwarp",
        "-q",
        "-of",
        "%s" % tile_type_info['file_format'],
        "-t_srs",
        "%s" % tile_type_info['crs'],
        "-te",
        "%f" % tile_extents[0],
        "%f" % tile_extents[1],
        "%f" % tile_extents[2],
        "%f" % tile_extents[3],
        "-tr",
        "%f" % tile_type_info['x_pixel_size'],
        "%f" % tile_type_info['y_pixel_size'],
        "-tap",
        "-tap",
        "-r",
        "%s" % resampling_method,
    ]
    reproject_cmd.extend(nodata_spec)
    reproject_cmd.extend(_make_format_spec(tile_type_info))
    reproject_cmd.extend([
        "-overwrite",
        "%s" % band_stack.vrt_name,
        "%s" % temp_tile_output_path  # Use locally-defined output path, not class instance value
    ])
    return reproject_cmd


def _reproject(tile_type_info, tile_footprint, band_stack, output_path):

    nodata_value = band_stack.nodata_list[0]

    # Assume resampling method is the same for all bands, this is
    # because resampling_method is per proessing_level
    # TODO assert this is the case
    first_file_number = band_stack.band_dict.keys()[0]
    reproject_cmd = _create_reproject_command(band_stack, first_file_number, nodata_value,
                                             output_path, tile_footprint, tile_type_info)

    command_string = ' '.join(reproject_cmd)

    LOGGER.info('Performing gdalwarp for tile %s', tile_footprint)
    retry = True
    while retry:
        LOGGER.debug('command_string = %s', command_string)
        start_datetime = datetime.now()
        result = execute(command_string)
        LOGGER.debug('gdalwarp time = %s', datetime.now() - start_datetime)

        if result['stdout']:
            log_multiline(LOGGER.debug, result['stdout'], 'stdout from ' + command_string, '\t')

        if result['returncode']:  # Return code is non-zero
            log_multiline(LOGGER.error, result['stderr'], 'stderr from ' + command_string, '\t')

            # Work-around for gdalwarp error writing LZW-compressed GeoTIFFs
            if (result['stderr'].find('LZW') > -1  # LZW-related error
                and tile_type_info['file_format'] == 'GTiff'  # Output format is GeoTIFF
                and 'COMPRESS=LZW' in tile_type_info['format_options']):  # LZW compression requested

                uncompressed_tile_path = output_path + '.tmp'

                # Write uncompressed tile to a temporary path
                command_string = command_string.replace('COMPRESS=LZW', 'COMPRESS=NONE')
                command_string = command_string.replace(output_path, uncompressed_tile_path)

                # Translate temporary uncompressed tile to final compressed tile
                command_string += '; gdal_translate -of GTiff'
                command_string += ' ' + ' '.join(_make_format_spec(tile_type_info))
                command_string += ' %s %s' % (
                    uncompressed_tile_path,
                    output_path
                )

                LOGGER.info('Creating compressed GeoTIFF tile via temporary uncompressed GeoTIFF')
            else:
                raise DatasetError('Unable to perform gdalwarp: ' +
                                   '"%s" failed: %s' % (command_string,
                                                        result['stderr']))

        else:
            retry = False  # No retry on success


def _nc2vrt(nc_path, vrt_path):
    """Create a VRT file to present a netCDF file with multiple subdatasets to GDAL as a band stack"""

    nc_abs_path = os.path.abspath(nc_path)
    vrt_abs_path = os.path.abspath(vrt_path)

    # Create VRT file using absolute pathnames
    nc2vrt_cmd = "gdalbuildvrt -separate -allow_projection_difference -overwrite %s %s" % (
        vrt_abs_path, nc_abs_path)
    LOGGER.debug('nc2vrt_cmd = %s', nc2vrt_cmd)
    result = execute(nc2vrt_cmd)  #, shell=False)
    if result['returncode'] != 0:
        raise DatasetError('Unable to perform gdalbuildvrt: ' +
                           '"%s" failed: %s' % (nc2vrt_cmd,
                                                result['stderr']))


def _has_data(tile_path, band_stack):
    """Check if the reprojection gave rise to a tile with valid data.

    Open the file and check if there is data

    :type tile_path: str
    :type band_stack: AbstractBandStack
    """
    tile_dataset = gdal.Open(tile_path)
    start_datetime = datetime.now()

    if tile_dataset.RasterCount != len(band_stack.band_dict):
        raise DatasetError(
            (
                "Number of layers (%d) in tile file\n %s\n"
                "does not match number of bands "
                "(%d) from database."
            ) % (
                tile_dataset.RasterCount,
                tile_path,
                len(band_stack.band_dict)
            )
        )

    # Convert self.band_stack.band_dict into list of elements sorted by tile_layer
    band_list = [
        band_stack.band_dict[file_number]
        for file_number in sorted(
            band_stack.band_dict.keys(),
            key=lambda f_number: band_stack.band_dict[f_number]['tile_layer']
        )
    ]

    result = False

    # Read each band in individually - will be quicker for non-empty tiles but slower for empty ones
    for band_index in range(tile_dataset.RasterCount):
        band_no = band_index + 1
        band = tile_dataset.GetRasterBand(band_no)
        band_data = band.ReadAsArray()

        # Use DB value: Should actually be the same for all bands in a given processing level
        nodata_val = band_list[band_index]['nodata_value']

        if nodata_val is None:
            # Use value defined in tile dataset (inherited from source dataset)
            nodata_val = band.GetNoDataValue()

        LOGGER.debug('nodata_val = %s for layer %d', nodata_val, band_no)

        if nodata_val is None:
            # Special case for PQA with no no-data value defined
            if (band_stack.band_dict[file_number]['level_name'] == 'PQA'):
                if (np.bitwise_and(band_data, PQA_CONTIGUITY) > 0).any():
                    LOGGER.debug('Tile is not empty: PQA data contains some contiguous data')
                    result = True
                    break
            else:
                # nodata_value of None means all array data is valid
                LOGGER.debug('Tile is not empty: No-data value is not set')
                result = True
                break

        elif (band_data != nodata_val).any():
            LOGGER.debug('Tile is not empty: Some values != %s', nodata_val)
            result = True
            break

    # All comparisons have shown that all band contents are no-data:
    LOGGER.info('Tile ' + ('has data' if result else 'is empty') + '.')
    LOGGER.debug('Empty tile detection time = %s', datetime.now() - start_datetime)
    return result
