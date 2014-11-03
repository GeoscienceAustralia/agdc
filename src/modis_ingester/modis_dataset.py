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
    modis_dataset.py - dataset class for modis datasets.

    This is the implementation of the AbstractDataset class for modis
    datasets.
"""

import os
import logging
import glob
import re
import datetime
from osgeo import gdal

from EOtools.DatasetDrivers import SceneDataset
from EOtools.execute import execute

from agdc.cube_util import DatasetError
from agdc.abstract_ingester import AbstractDataset
from modis_bandstack import ModisBandstack

#
# Set up logger.
#

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.CRITICAL)

#
# Class definition
#


class ModisDataset(AbstractDataset):
    """Dataset class for modis datasets."""

    # pylint: disable=too-many-public-methods
    #
    # This class provides metadata using acessor functions. This is
    # both straight-forward and allows a docstring to be attached to
    # each to document the definition of the metadata being provided.
    #

    PROCESSING_LEVEL_ALIASES = {
        'Pixel Quality': 'PQA',
        'Fractional Cover': 'FC'
        }

    def __init__(self, dataset_path):
        """Opens the dataset and extracts metadata.

        """

        print "ModisDataset::init()"
        self._netcdf_file = dataset_path

        self._dataset_file = os.path.abspath(dataset_path)
        fileName, fileExtension = os.path.splitext(self._dataset_file)
        self._dataset_path = fileName + ".vrt"

        print self._dataset_path
        self._ds = gdal.Open(self._netcdf_file, gdal.GA_ReadOnly)

        if not self._ds:
            raise DatasetError("Unable to open %s" % self.get_dataset_path())

        self._satellite_tag = "MT"
        self._satellite_sensor = "MODIS-Terra"
        self._processor_level = "MOD09"

        self._dataset_size = os.path.getsize(self._netcdf_file)
        
        LOGGER.debug('Transform = %s', self._ds.GetGeoTransform());
        LOGGER.debug('Projection = %s', self._ds.GetProjection());

        LOGGER.debug('RasterXSize = %s', self._ds.RasterXSize);
        LOGGER.debug('RasterYSize = %s', self._ds.RasterYSize);

        s = execute("ncdump -v InputFileGlobalAttributes %s" % self._netcdf_file)
        s = re.sub(r"\s+", "", s['stdout'])

        self._rangeendingdate = re.search('RANGEENDINGDATE\\\\nNUM_VAL=1\\\\nVALUE=\\\\\"(.*)\\\\\"\\\\nEND_OBJECT=RANGEENDINGDATE', s).groups(1)[0]
        LOGGER.debug('RangeEndingDate = %s', self._rangeendingdate)
        
        self._rangeendingtime = re.search('RANGEENDINGTIME\\\\nNUM_VAL=1\\\\nVALUE=\\\\\"(.*)\\\\\"\\\\nEND_OBJECT=RANGEENDINGTIME', s).groups(1)[0]
        LOGGER.debug('RangeEndingTime = %s', self._rangeendingtime)

        self._rangebeginningdate = re.search('RANGEBEGINNINGDATE\\\\nNUM_VAL=1\\\\nVALUE=\\\\\"(.*)\\\\\"\\\\nEND_OBJECT=RANGEBEGINNINGDATE', s).groups(1)[0]
        LOGGER.debug('RangeBeginningDate = %s', self._rangebeginningdate)
        
        self._rangebeginningtime = re.search('RANGEBEGINNINGTIME\\\\nNUM_VAL=1\\\\nVALUE=\\\\\"(.*)\\\\\"\\\\nEND_OBJECT=RANGEBEGINNINGTIME', s).groups(1)[0]
        LOGGER.debug('RangeBeginningTime = %s', self._rangebeginningtime)

        self.scene_start_datetime = self._rangebeginningdate + " " + self._rangebeginningtime
        self.scene_end_datetime = self._rangeendingdate + " " + self._rangeendingtime

        self._orbitnumber = int(re.search('ORBITNUMBER\\\\nCLASS=\\\\\"1\\\\\"\\\\nNUM_VAL=1\\\\nVALUE=(.*)\\\\nEND_OBJECT=ORBITNUMBER', s).groups(1)[0])
        LOGGER.debug('OrbitNumber = %d', self._orbitnumber)

        self._cloud_cover_percentage = float(re.search('Cloudy:\\\\t(.*)\\\\n\\\\tMixed', s).groups(1)[0])
        LOGGER.debug('CloudCover = %f', self._cloud_cover_percentage)

        self._completion_datetime = re.search('PRODUCTIONDATETIME\\\\nNUM_VAL=1\\\\nVALUE=\\\\\"(.*)Z\\\\\"\\\\nEND_OBJECT=PRODUCTIONDATETIME', s).groups(1)[0]
        LOGGER.debug('ProcessedTime = %s', self._completion_datetime)

        self._metadata = self._ds.GetMetadata('SUBDATASETS')

        print self._metadata['SUBDATASET_1_NAME']
        band1 = gdal.Open(self._metadata['SUBDATASET_1_NAME'])

        # Get Coordinates
        self._width = band1.RasterXSize
        self._height = band1.RasterYSize

        self._gt = band1.GetGeoTransform()
        self._minx = self._gt[0]
        self._miny = self._gt[3] + self._width*self._gt[4] + self._height*self._gt[5]  # from
        self._maxx = self._gt[0] + self._width*self._gt[1] + self._height*self._gt[2]  # from
        self._maxy = self._gt[3]

        LOGGER.debug('min/max x coordinates (%s, %s)',str(self._minx), str(self._maxx))  # min/max x coordinates
        LOGGER.debug('min/max y coordinates (%s, %s)',str(self._miny), str(self._maxy))  # min/max y coordinates

        LOGGER.debug('pixel size (%s, %s)', str(self._gt[1]), str(self._gt[5])) # pixel size

        self._pixelX = self._width
        self._pixelY = self._height

        LOGGER.debug('pixels (%s, %s)', str(self._pixelX), str(self._pixelY)) # pixels

        self._gcp_count = None
        self._mtl_text = None
        self._xml_text = None

        print "ModisDataset::init() DONE"
        AbstractDataset.__init__(self)

    #
    # Methods to extract extra metadata
    #

    def _get_directory_size(self):
        """Calculate the size of the dataset in kB."""

        command = "du -sk %s | cut -f1" % self.get_dataset_path()
        LOGGER.debug('executing "%s"', command)
        result = execute(command)

        if result['returncode'] != 0:
            raise DatasetError('Unable to calculate directory size: ' +
                               '"%s" failed: %s' % (command, result['stderr']))

        LOGGER.debug('stdout = %s', result['stdout'])

        return int(result['stdout'])

    def _get_gcp_count(self):
        """N/A for Modis."""

        return 0

    def _get_mtl_text(self):
        """N/A for Modis."""

        return None

    def _get_xml_text(self):
        """N/A for Modis."""

        return None

    #
    # Metadata accessor methods
    #

    def get_dataset_path(self):
        """The path to the dataset on disk."""
        return self._dataset_path

    def get_satellite_tag(self):
        """A short unique string identifying the satellite."""
        return self._satellite_tag

    def get_sensor_name(self):
        """A short string identifying the sensor.

        The combination of satellite_tag and sensor_name must be unique.
        """
        return self._satellite_sensor

    def get_processing_level(self):
        """A short string identifying the processing level or product.

        The processing level must be unique for each satellite and sensor
        combination.
        """

        return self._processor_level.upper()

    def get_x_ref(self):
        """The x (East-West axis) reference number for the dataset.

        In whatever numbering scheme is used for this satellite.
        """
        return self._orbitnumber

    def get_y_ref(self):
        """N/A for Modis."""

        return None

    def get_start_datetime(self):
        """The start of the acquisition.

        This is a datetime without timezone in UTC.
        """
        
        #2011-01-31 02:35:09.897216
        return datetime.datetime.strptime(self.scene_start_datetime, "%Y-%m-%d %H:%M:%S.%f")

    def get_end_datetime(self):
        """The end of the acquisition.

        This is a datatime without timezone in UTC.
        """

        return datetime.datetime.strptime(self.scene_end_datetime, "%Y-%m-%d %H:%M:%S.%f")

    def get_datetime_processed(self):
        """The date and time when the dataset was processed or created.

        This is used to determine if that dataset is newer than one
        already in the database, and so should replace it.

        It is a datetime without timezone in UTC.
        """
        return self._completion_datetime

    def get_dataset_size(self):
        """The size of the dataset in kilobytes as an integer."""
        return self._dataset_size

    def get_ll_lon(self):
        """The longitude of the lower left corner of the coverage area."""
        return self._minx

    def get_ll_lat(self):
        """The lattitude of the lower left corner of the coverage area."""
        return self._miny

    def get_lr_lon(self):
        """The longitude of the lower right corner of the coverage area."""
        return self._maxx

    def get_lr_lat(self):
        """The lattitude of the lower right corner of the coverage area."""
        return self._miny

    def get_ul_lon(self):
        """The longitude of the upper left corner of the coverage area."""
        return self._minx

    def get_ul_lat(self):
        """The lattitude of the upper left corner of the coverage area."""
        return self._maxy

    def get_ur_lon(self):
        """The longitude of the upper right corner of the coverage area."""
        return self._maxx

    def get_ur_lat(self):
        """The lattitude of the upper right corner of the coverage area."""
        return self._maxy

    def get_projection(self):
        """The coordinate refererence system of the image data."""
        return self._ds.GetMetadata()['NC_GLOBAL#crs']

    def get_ll_x(self):
        """The x coordinate of the lower left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_ll_lon()

    def get_ll_y(self):
        """The y coordinate of the lower left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_ll_lat()

    def get_lr_x(self):
        """The x coordinate of the lower right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_lr_lon()

    def get_lr_y(self):
        """The y coordinate of the lower right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_ll_lat()

    def get_ul_x(self):
        """The x coordinate of the upper left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_ul_lon()

    def get_ul_y(self):
        """The y coordinate of the upper left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_ul_lat()

    def get_ur_x(self):
        """The x coordinate of the upper right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_ur_lon()

    def get_ur_y(self):
        """The y coordinate of the upper right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self.get_ur_lat()

    def get_x_pixels(self):
        """The width of the dataset in pixels."""
        return self._pixelX

    def get_y_pixels(self):
        """The height of the dataset in pixels."""
        return self._pixelY

    def get_gcp_count(self):
        """The number of ground control points?"""
        return self._gcp_count

    def get_mtl_text(self):
        """Text information?"""
        return self._mtl_text

    def get_cloud_cover(self):
        """Percentage cloud cover of the aquisition if available."""
        return self._cloud_cover_percentage

    def get_xml_text(self):
        """XML metadata text for the dataset if available."""
        return self._xml_text

    def get_pq_tests_run(self):
        """The tests run for a Pixel Quality dataset.

        This is a 16 bit integer with the bits acting as flags. 1 indicates
        that the test was run, 0 that it was not.
        """
        return None

    #
    # Methods used for tiling
    #

    def get_geo_transform(self):
        """The affine transform between pixel and geographic coordinates.

        This is a list of six numbers describing a transformation between
        the pixel x and y coordinates and the geographic x and y coordinates
        in dataset's coordinate reference system.

        See http://www.gdal.org/gdal_datamodel for details.
        """
        return self._gt

    def find_band_file(self, file_pattern):
        """Find the file in dataset_dir matching file_pattern and check
        uniqueness.

        Returns the path to the file if found, raises a DatasetError
        otherwise."""

        return self._dataset_path

    def stack_bands(self, band_dict):
        """Creates and returns a band_stack object from the dataset.

        band_dict: a dictionary describing the bands to be included in the
        stack.

        PRE: The numbers in the band list must refer to bands present
        in the dataset. This method (or things that it calls) should
        raise an exception otherwise.

        POST: The object returned supports the band_stack interface
        (described below), allowing the datacube to chop the relevent
        bands into tiles.
        """
        return ModisBandstack(self, band_dict)
