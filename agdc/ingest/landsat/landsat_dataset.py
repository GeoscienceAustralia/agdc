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
    landsat_dataset.py - dataset class for landset (5 and 7) datasets.

    This is the implementation of the AbstractDataset class for landsat
    datasets. At present it only works for level 1 (L1T, ORTHO) and NBAR
    data, as it relys on eotools.drivers.SceneDataset.
"""
from __future__ import absolute_import

import os
import logging
import glob
import re

from eotools.drivers import SceneDataset
from eotools.execute import execute

from agdc.cube_util import DatasetError
from agdc.ingest import AbstractDataset
from .landsat_bandstack import LandsatBandstack

LOGGER = logging.getLogger(__name__)


def _is_a_supported_landsat_image(filename):
    """
    Is the given filename of an image format we support?
    :type filename: str
    :return:

    >>> _is_a_supported_landsat_image('ls7-B10.tif')
    True
    >>> _is_a_supported_landsat_image('ls7-B10.TIF')
    True
    >>> _is_a_supported_landsat_image('ls7-B10.tif.aux.xml')
    False
    >>> _is_a_supported_landsat_image('ls7-B10.nc')
    False
    >>> _is_a_supported_landsat_image('ls7-B10.TIFF')
    True
    """
    # Only tif is currently supported for Landsat.
    return filename.lower().endswith('.tif') or filename.lower().endswith('.tiff')


class LandsatDataset(AbstractDataset):
    """Dataset class for landsat ORTHO and NBAR datasets."""

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

        Most of the metadata is kept in self._ds which is
        a eotools.drivers.SceneDataset object. Some extra metadata is
        extracted and kept the instance attributes.
        """

        self._dataset_path = dataset_path
        LOGGER.info('Opening Dataset %s', self._dataset_path)
        
        self._ds = SceneDataset(default_metadata_required=False, utm_fix=True)
        self._ds = self._ds.Open(self.get_dataset_path())
        if not self._ds:
            raise DatasetError("Unable to open %s" % self.get_dataset_path())

        #
        # Cache extra metadata in instance attributes.
        #

        self._dataset_size = self._get_directory_size()

        if self.get_processing_level() in ['ORTHO', 'L1T', 'MAP']:
            LOGGER.debug('Dataset %s is Level 1', self.get_dataset_path())
            self._gcp_count = self._get_gcp_count()
            self._mtl_text = self._get_mtl_text()
        else:
            self._gcp_count = None
            self._mtl_text = None

        self._xml_text = self._get_xml_text()

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
        """Count the gcp (only for level 1 datasets)."""

        gcp_pattern = os.path.join(self.get_dataset_path(), 'scene01',
                                   '*_GCP.txt')

        return self._extract_from_file(gcp_pattern, 'GCP.txt',
                                       self._extract_gcp_count)

    def _get_mtl_text(self):
        """Extract the mtl text (only for level 1 datasets)."""

        mtl_pattern = os.path.join(self.get_dataset_path(), 'scene01',
                                   '*_MTL.txt')
        return self._extract_from_file(mtl_pattern, 'MTL.txt',
                                       self._extract_text)

    def _get_xml_text(self):
        """Extract the XML metadata text (if any)."""

        xml_pattern = os.path.join(self.get_dataset_path(), 'metadata.xml')
        return self._extract_from_file(xml_pattern, 'metadata.xml',
                                       self._extract_text)

    @staticmethod
    def _extract_from_file(file_pattern, file_description, extract_function):
        """Extract metadata from a file.

        Returns the result of running extract_function on the opened
        file, or None if the file cannot be found. file_pattern is a
        glob pattern for the file: the first file found is used.
        file_description is a description of the file for logging and
        error messages."""

        try:
            md_path = glob.glob(file_pattern)[0]

            md_file = open(md_path)
            metadata = extract_function(md_file)
            md_file.close()

        except IndexError:  # File not found
            metadata = None
            LOGGER.debug('No %s file found.', file_description)

        except IOError:  # Open failed
            raise DatasetError('Unable to open %s file.' % file_description)

        return metadata

    @staticmethod
    def _extract_text(md_file):
        """Dump the text from a metadata file."""
        return md_file.read()

    @staticmethod
    def _extract_gcp_count(md_file):
        """Extract the gcp count from a metadata file.

        Count the number of lines consisting of 8 numbers with
        the first number being positive."""

        return len([line for line in md_file.readlines()
                    if re.match(r'\d+(\s+-?\d+\.?\d*){7}', line)])

    #
    # Metadata accessor methods
    #

    def get_dataset_path(self):
        """The path to the dataset on disk."""
        return self._dataset_path

    def get_satellite_tag(self):
        """A short unique string identifying the satellite."""
        return self._ds.satellite.TAG

    def get_sensor_name(self):
        """A short string identifying the sensor.

        The combination of satellite_tag and sensor_name must be unique.
        """
        return self._ds.satellite.sensor

    def get_processing_level(self):
        """A short string identifying the processing level or product.

        The processing level must be unique for each satellite and sensor
        combination.
        """

        level = self._ds.processor_level
        if level in self.PROCESSING_LEVEL_ALIASES:
            level = self.PROCESSING_LEVEL_ALIASES[level]

        return level.upper()

    def get_x_ref(self):
        """The x (East-West axis) reference number for the dataset.

        In whatever numbering scheme is used for this satellite.
        """
        return self._ds.path_number

    def get_y_ref(self):
        """The y (North-South axis) reference number for the dataset.

        In whatever numbering scheme is used for this satellite.
        """
        return self._ds.row_number

    def get_start_datetime(self):
        """The start of the acquisition.

        This is a datetime without timezone in UTC.
        """

        # Use the alternate time if available (from EODS_DATASET metadata).
        try:
            start_dt = self._ds.scene_alt_start_datetime
        except AttributeError:
            start_dt = None

        # Othewise use the original time (calcualted from scene_centre_time).
        if start_dt is None:
            start_dt = self._ds.scene_start_datetime

        return start_dt

    def get_end_datetime(self):
        """The end of the acquisition.

        This is a datatime without timezone in UTC.
        """

        # Use the alternate time if available (from EODS_DATASET metadata).
        try:
            end_dt = self._ds.scene_alt_end_datetime
        except AttributeError:
            end_dt = None

        # Othewise use the original time (calcualted from scene_centre_time).
        if end_dt is None:
            end_dt = self._ds.scene_end_datetime

        return end_dt

    def get_datetime_processed(self):
        """The date and time when the dataset was processed or created.

        This is used to determine if that dataset is newer than one
        already in the database, and so should replace it.

        It is a datetime without timezone in UTC.
        """
        return self._ds.completion_datetime

    def get_dataset_size(self):
        """The size of the dataset in kilobytes as an integer."""
        return self._dataset_size

    def get_ll_lon(self):
        """The longitude of the lower left corner of the coverage area."""
        return self._ds.ll_lon

    def get_ll_lat(self):
        """The lattitude of the lower left corner of the coverage area."""
        return self._ds.ll_lat

    def get_lr_lon(self):
        """The longitude of the lower right corner of the coverage area."""
        return self._ds.lr_lon

    def get_lr_lat(self):
        """The lattitude of the lower right corner of the coverage area."""
        return self._ds.lr_lat

    def get_ul_lon(self):
        """The longitude of the upper left corner of the coverage area."""
        return self._ds.ul_lon

    def get_ul_lat(self):
        """The lattitude of the upper left corner of the coverage area."""
        return self._ds.ul_lat

    def get_ur_lon(self):
        """The longitude of the upper right corner of the coverage area."""
        return self._ds.ur_lon

    def get_ur_lat(self):
        """The lattitude of the upper right corner of the coverage area."""
        return self._ds.ur_lat

    def get_projection(self):
        """The coordinate refererence system of the image data."""
        return self._ds.GetProjection()

    def get_ll_x(self):
        """The x coordinate of the lower left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self._ds.ll_x

    def get_ll_y(self):
        """The y coordinate of the lower left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self._ds.ll_y

    def get_lr_x(self):
        """The x coordinate of the lower right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self._ds.lr_x

    def get_lr_y(self):
        """The y coordinate of the lower right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self._ds.lr_y

    def get_ul_x(self):
        """The x coordinate of the upper left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self._ds.ul_x

    def get_ul_y(self):
        """The y coordinate of the upper left corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self._ds.ul_y

    def get_ur_x(self):
        """The x coordinate of the upper right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self._ds.ur_x

    def get_ur_y(self):
        """The y coordinate of the upper right corner of the coverage area.

        This is according to the projection returned by get_projection.
        """
        return self._ds.ur_y

    def get_x_pixels(self):
        """The width of the dataset in pixels."""
        return self._ds.image_pixels

    def get_y_pixels(self):
        """The height of the dataset in pixels."""
        return self._ds.image_lines

    def get_gcp_count(self):
        """The number of ground control points?"""
        return self._gcp_count

    def get_mtl_text(self):
        """Text information?"""
        return self._mtl_text

    def get_cloud_cover(self):
        """Percentage cloud cover of the aquisition if available."""
        return self._ds.cloud_cover_percentage

    def get_xml_text(self):
        """XML metadata text for the dataset if available."""
        return self._xml_text

    def get_pq_tests_run(self):
        """The tests run for a Pixel Quality dataset.

        This is a 16 bit integer with the bits acting as flags. 1 indicates
        that the test was run, 0 that it was not.
        """

        # None value provided for pq_tests_run value in case PQA metadata
        # extraction fails due to out of date version of SceneDataset.
        # This should be a temporary measure.
        try:
            pq_tests_run = self._ds.pq_tests_run
        except AttributeError:
            pq_tests_run = None

        return pq_tests_run

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
        return self._ds.GetGeoTransform()

    def find_band_file(self, file_pattern):
        """Find the file in dataset_dir matching file_pattern and check
        uniqueness.

        Returns the path to the file if found, raises a DatasetError
        otherwise."""

        dataset_dir = os.path.join(self.metadata_dict['dataset_path'],
                                   'scene01')
        if not os.path.isdir(dataset_dir):
            raise DatasetError('%s is not a valid directory' % dataset_dir)
        filelist = [filename for filename in os .listdir(dataset_dir)
                    if re.match(file_pattern, filename) and _is_a_supported_landsat_image(filename)]
        if not len(filelist) == 1:
            raise DatasetError('Unable to find unique match ' +
                               'for file pattern %s' % file_pattern)

        return os.path.join(dataset_dir, filelist[0])

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
        return LandsatBandstack(self, band_dict)
