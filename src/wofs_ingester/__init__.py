#!/usr/bin/env python

# ===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright
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
Ingestion of water tiles.
"""

import logging
import argparse
import os
from osgeo import gdal
from datetime import datetime

from agdc.abstract_ingester import AbstractIngester, AbstractDataset


_LOG = logging.getLogger(__name__)


def _is_water_file(f):
    """
    Is this the filename of a water file?
    :type f: str
    :rtype: bool

    >>> _is_water_file('LS7_ETM_WATER_144_-037_2007-11-09T23-59-30.500467.tif')
    True
    >>> _is_water_file('createWaterExtents_r3450_3752.log')
    False
    >>> _is_water_file('LC81130742014337LGN00_B1.tif')
    False
    >>> _is_water_file('LS8_OLITIRS_OTH_P51_GALPGS01-032_113_074_20141203')
    False
    >>> # We only currently care about the Tiffs:
    >>> _is_water_file('LS_WATER_150_-022_1987-05-27T23-23-00.443_2014-03-10T23-55-40.796.nc')
    False
    """
    return 'WATER' in f and f.endswith('.tif')


def _find_water_files(source_path):
    """
    Find water tif files in the given path.

    This may be a directory to search, or a single image.

    :type source_path: str
    :return: A list of absolute paths
    :rtype: list of str
    """
    # Allow an individual file to be supplied as the source
    if os.path.isfile(source_path) and source_path.endswith(".tif"):
        _LOG.debug('%r is a single tiff file', source_path)
        return [source_path]

    assert os.path.isdir(source_path), '%s is not a directory' % source_path

    dataset_list = []
    for root, _dirs, files in os.walk(source_path):
        dataset_list += [os.path.join(root, f) for f in files if _is_water_file(f)]

    return sorted(dataset_list)


class WofsIngester(AbstractIngester):
    """Ingester class for Modis datasets."""

    @staticmethod
    def parse_args():
        """Parse the command line arguments for the ingester.

        Returns an argparse namespace object.
        """

        # Most of this logic is almost identical to other ingester types. Could we share more of this code?
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

        sync_time_help = 'Synchronize parallel ingestions at the given time' \
                         ' in seconds after 01/01/1970'
        _arg_parser.add_argument('--synctime', dest='sync_time',
                                 default=None, help=sync_time_help)

        sync_type_help = 'Type of transaction to syncronize with synctime,' \
                         + ' one of "cataloging", "tiling", or "mosaicking".'
        _arg_parser.add_argument('--synctype', dest='sync_type',
                                 default=None, help=sync_type_help)

        return _arg_parser.parse_args()

    def find_datasets(self, source_path):
        """Return a list of path to the netCDF datasets under 'source_dir' or a single-item list
        if source_dir is a netCDF file path
        """

        dataset_list = _find_water_files(source_path)

        _LOG.debug('%s dataset found: %r', len(dataset_list), dataset_list)
        return dataset_list

    def open_dataset(self, dataset_path):
        """Create and return a dataset object.
        :type: dataset_path: str
        """
        return WofsDataset(dataset_path)

    def filter_dataset(self, path, row, date):
        """Return True if the dataset should be included, False otherwise.

        TODO: Should we include path/row filtering where applicable? Do we always have it?
        """

        start_date, end_date = self.get_date_range()

        include = ((end_date is None or date is None or date <= end_date) and
                   (start_date is None or date is None or date >= start_date))

        return include


def _get_extent_gdal(dataset):
    """ Get the corner coordinates for a gdal Dataset
    :type dataset: gdal.Dataset
    :rtype: list of (list of float)
    :return: List of four corner coords: ul, ll, lr, ur
    """
    return _get_extent(dataset.GetGeoTransform(), dataset.RasterXSize, dataset.RasterYSize)


def _get_extent(gt, cols, rows):
    """ Return the corner coordinates from a geotransform

    :param gt: geotransform
    :type gt: C{tuple/list}
    :param cols: number of columns in the dataset
    :type cols: int
    :param rows: number of rows in the dataset
    :type rows: int
    :rtype: list of (list of float)
    :return: List of four corner coords: ul, ll, lr, ur

    >>> gt = (144.0, 0.00025, 0.0, -36.0, 0.0, -0.00025)
    >>> cols = 4000
    >>> rows = 4000
    >>> _get_extent(gt, cols, rows)
    [[144.0, -36.0], [144.0, -37.0], [145.0, -37.0], [145.0, -36.0]]
    """
    ext = []
    xarr = [0, cols]
    yarr = [0, rows]

    for px in xarr:
        for py in yarr:
            x = gt[0] + (px * gt[1]) + (py * gt[2])
            y = gt[3] + (px * gt[4]) + (py * gt[5])
            ext.append([x, y])
        yarr.reverse()
    return ext


def get_file_size(path):
    """ File size in KBs.
    :type path: str
    :rtype: int
    """
    if os.path.isdir(path):
        raise NotImplementedError('Directory size not yet supported: {!r}'.format(path))

    return os.path.getsize(path) / 1024


class WofsDataset(AbstractDataset):
    """
    Water extent tile.
    """

    def __init__(self, dataset_path):
        #: :type: gdal.Dataset
        self._path = dataset_path
        self._ds = gdal.Open(dataset_path)

        self._md = self._ds.GetMetadata_Dict()

        self._ul, self._ll, self._lr, self._ur = _get_extent_gdal(self._ds)

        super(WofsDataset, self).__init__()

    def get_processing_level(self):
        return 'WATER'

    def get_x_pixels(self):
        return self._ds.RasterXSize

    def get_y_pixels(self):
        return self._ds.RasterYSize

    def get_datetime_processed(self):
        t = os.path.getctime(self._path)
        return datetime.utcfromtimestamp(t)

    def get_geo_transform(self):
        return self._ds.GetGeoTransform()

    def get_x_ref(self):
        pass  # N/A? We could extract the path for landsat datasets.

    def get_y_ref(self):
        pass  # N/A? We could extract the row for landsat datasets.

    def get_satellite_tag(self):
        return self._md.get('satellite_tag')

    def get_gcp_count(self):
        return int(self._md['gcp_count'])

    def get_dataset_size(self):
        return get_file_size(self._path)

    def get_xml_text(self):
        return None  # N/A?

    def get_dataset_path(self):
        return self._path

    def get_sensor_name(self):
        sensor_name = self._md.get('sensor_name')

        # FIXME: Hard-coded correction.
        # This difference is common across our systems. Maybe add an 'alias' column to sensor table?
        if sensor_name == 'ETM':
            return 'ETM+'

        return sensor_name

    def get_mtl_text(self):
        return None  # N/A?

    def _get_date_param(self, param_name):
        start = self._md[param_name]
        return datetime.strptime(start, '%Y-%m-%d %H:%M:%S.%f')

    def get_start_datetime(self):
        return self._get_date_param('start_datetime')

    def get_end_datetime(self):
        return self._get_date_param('end_datetime')

    def _get_int_param(self, param_name):
        val = self._md.get(param_name)
        if not val or val == 'None':
            return None

        return int(val)

    def get_cloud_cover(self):
        return self._get_int_param('cloud_cover')

    def get_projection(self):
        return self._ds.GetProjection()

    def get_ll_lat(self):
        return self._ll[1]

    def get_ll_lon(self):
        return self._ll[0]

    def get_ul_lat(self):
        return self._ul[0]

    def get_ul_lon(self):
        return self._ul[1]

    def get_lr_lat(self):
        return self._lr[0]

    def get_lr_lon(self):
        return self._lr[1]

    def get_ur_lat(self):
        return self._ur[0]

    def get_ur_lon(self):
        return self._ur[1]

    # ?
    get_ll_x = get_ll_lon
    get_ll_y = get_ll_lat
    get_lr_x = get_lr_lon
    get_lr_y = get_lr_lat
    get_ur_x = get_ur_lon
    get_ur_y = get_ur_lat
    get_ul_x = get_ul_lon
    get_ul_y = get_ul_lat

    def find_band_file(self, file_pattern):
        return self._path

    def stack_bands(self, band_dict):
        return None


if __name__ == "__main__":
    import doctest
    doctest.testmod()