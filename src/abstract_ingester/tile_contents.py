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
from EOtools.execute import execute
from agdc.cube_util import DatasetError, create_directory
from osgeo import gdal
import numpy as np

# Set up logger.
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
                 tile_footprint, band_stack):
        """Set the tile_footprint over which we want to resample this dataset.
        """
        self.tile_type_id = tile_type_info['tile_type_id']
        self.tile_type_info = tile_type_info
        self.tile_footprint = tile_footprint
        self.band_stack = band_stack
        x_index, y_index = tile_footprint
        tile_output_root = os.path.join(
            tile_root,
            tile_type_info['tile_directory'],
            '%s_%s' % (band_stack.dataset_mdd['satellite_tag'],
                       re.sub(r'\W', '', band_stack.dataset_mdd['sensor_name'])
                       )
            )

        tile_output_dir = os.path.join(
            tile_output_root,
            re.sub(r'\+', '', ('%+04d_%+04d' % (tile_footprint[0],
                                                tile_footprint[1]
                                                )
                               )
                   ),
            '%04d' % band_stack.dataset_mdd['start_datetime'].year
            )

        self.tile_output_path = os.path.join(
            tile_output_dir,
            '_'.join([band_stack.dataset_mdd['satellite_tag'],
                      re.sub(r'\W', '', band_stack.dataset_mdd['sensor_name']),
                      band_stack.dataset_mdd['processing_level'],
                      re.sub(r'\+', '', '%+04d_%+04d' % (x_index, y_index)),
                      re.sub(':', '-', band_stack.
                             dataset_mdd['start_datetime'].isoformat()
                             )
                      ]) + tile_type_info['file_extension']
            )
        #Set the provisional tile location to be the same as the vrt created
        #for the scenes
        self.temp_tile_output_path = os.path.join(
            os.path.dirname(self.band_stack.vrt_name),
            os.path.basename(self.tile_output_path)
            )
        self.tile_extents = None
        
        # Work-around to allow existing GDAL code to work with netCDF subdatasets as band stacks
        #TODO: Move to netCDF libraries
        self.vrt_required = self.tile_type_info['file_format'] == 'netCDF' and tile_type_info['file_extension'] == '.vrt'

    
    def nc2vrt(self, nc_path, vrt_path):
        """Create a VRT file to present a netCDF file with multiple subdatasets to GDAL as a band stack"""
        
        nc_abs_path = os.path.abspath(nc_path)
        vrt_abs_path = os.path.abspath(vrt_path)
        
        # Create VRT file using absolute pathnames
        nc2vrt_cmd = "gdalbuildvrt -separate -allow_projection_difference -overwrite %s %s" % (vrt_abs_path, nc_abs_path)
        result = execute(nc2vrt_cmd, shell=False)
        if result['returncode'] != 0:
            raise DatasetError('Unable to perform gdalbuildvrt: ' +
                               '"%s" failed: %s' % (nc2vrt_cmd,
                                                    result['stderr']))
            
    
    def reproject(self):
        """Reproject the scene dataset into tile coordinate reference system
        and extent. This method uses gdalwarp to do the reprojection."""
        # pylint: disable=too-many-locals
        x_origin = self.tile_type_info['x_origin']
        y_origin = self.tile_type_info['y_origin']
        x_size = self.tile_type_info['x_size']
        y_size = self.tile_type_info['y_size']
        x_pixel_size = self.tile_type_info['x_pixel_size']
        y_pixel_size = self.tile_type_info['y_pixel_size']
        x0 = x_origin + self.tile_footprint[0] * x_size
        y0 = y_origin + self.tile_footprint[1] * y_size
        tile_extents = (x0, y0, x0 + x_size, y0 + y_size)
        # Make the tile_extents visible to tile_record
        self.tile_extents = tile_extents
        nodata_value = self.band_stack.nodata_list[0]
        #Assume resampling method is the same for all bands, this is
        #because resampling_method is per proessing_level
        #TODO assert this is the case
        first_file_number = self.band_stack.band_dict.keys()[0]
        resampling_method = (
            self.band_stack.band_dict[first_file_number]['resampling_method']
            )
        if nodata_value is not None:
            #TODO: Check this works for PQA, where
            #band_dict[10]['resampling_method'] == None
            nodata_spec = ["-srcnodata",
                           "%d" % nodata_value,
                           "-dstnodata",
                           "%d" % nodata_value
                           ]
        else:
            nodata_spec = []
        format_spec = []
        for format_option in self.tile_type_info['format_options'].split(','):
            format_spec.extend(["-co", "%s" % format_option])
            
        # Work-around to allow existing code to work with netCDF subdatasets as GDAL band stacks
        temp_tile_output_path = re.sub('\.vrt$', '.nc', self.temp_tile_output_path) if self.vrt_required else self.temp_tile_output_path

        
        reproject_cmd = ["gdalwarp",
                         "-q",
                         "-of",
                         "%s" % self.tile_type_info['file_format'],
                         "-t_srs",
                         "%s" % self.tile_type_info['crs'],
                         "-te",
                         "%f" % tile_extents[0],
                         "%f" % tile_extents[1],
                         "%f" % tile_extents[2],
                         "%f" % tile_extents[3],
                         "-tr",
                         "%f" % x_pixel_size,
                         "%f" % y_pixel_size,
                         "-tap",
                         "-tap",
                         "-r",
                         "%s" % resampling_method,
                         ]
        reproject_cmd.extend(nodata_spec)
        reproject_cmd.extend(format_spec)
        reproject_cmd.extend(["-overwrite",
                              "%s" % self.band_stack.vrt_name,
                              "%s" % temp_tile_output_path # Use locally-defined output path, not class instance value
                              ])
        result = execute(reproject_cmd, shell=False)
        if result['returncode'] != 0:
            raise DatasetError('Unable to perform gdalwarp: ' +
                               '"%s" failed: %s' % (reproject_cmd,
                                                    result['stderr']))
            
        # Work-around to allow existing code to work with netCDF subdatasets as GDAL band stacks
        if self.vrt_required:
            self.nc2vrt(temp_tile_output_path, self.temp_tile_output_path)
        
            
    def has_data(self):
        """Check if the reprojection gave rise to a tile with valid data.

        Open the file and check if there is data"""
        tile_dataset = gdal.Open(self.temp_tile_output_path)
        data = tile_dataset.ReadAsArray()
        if len(data.shape) == 2:
            data = data[None, :]
        if data.shape[0] != len(self.band_stack.band_dict):
            raise DatasetError(("Number of layers (%d) in tile file\n %s\n" +
                                "does not match number of bands " +
                                "(%d) from database."
                                ) % (data.shape[0],
                                     self.temp_tile_output_path,
                                     len(self.band_stack.band_dict)
                                     )
                               )
        for file_number in self.band_stack.band_dict:
            nodata_val = self.band_stack.band_dict[file_number]['nodata_value']
            if nodata_val is None:
                if (self.band_stack.band_dict[file_number]['level_name'] ==
                        'PQA'):
                    #Check if any pixel has the contiguity bit set
                    if (np.bitwise_and(data, PQA_CONTIGUITY) > 0).any():
                        return True
                else:
                    #nodata_value of None means all array data is valid
                    return True
            else:
                if (data != nodata_val).any():
                    return True
        #If all comparisons have shown that all array contents are nodata:
        return False

    def remove(self):
        """Remove tiles that were in coverage but have no data. Also remove
        tiles if we are rolling back the transaction."""
        if os.path.isfile(self.temp_tile_output_path):
            os.remove(self.temp_tile_output_path)

    def make_permanent(self):
        """Move the tile file to its permanent location."""

        source_dir = os.path.abspath(os.path.dirname(self.temp_tile_output_path))
        dest_dir = os.path.abspath(os.path.dirname(self.tile_output_path))
        
        create_directory(dest_dir)
        
        # Edit paths in .vrt file and move .nc file if required
        if self.vrt_required:
            vrt_file = open(self.temp_tile_output_path, 'r')
            vrt_string = vrt_file.read()
            vrt_file.close()
            
            vrt_string = vrt_string.replace(source_dir, dest_dir)
            
            vrt_file = open(self.tile_output_path, 'w')
            vrt_file.write(vrt_string)
            vrt_file.close()
            
            # Move .nc file
            shutil.move(re.sub('\.vrt$', '.nc', self.temp_tile_output_path), 
                        re.sub('\.vrt$', '.nc', self.tile_output_path))

        else: # No .vrt file required - just move the tile file
            shutil.move(self.temp_tile_output_path, self.tile_output_path)

    def get_output_path(self):
        """Return the final location for the tile."""

        return self.tile_output_path
