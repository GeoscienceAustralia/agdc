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


__author__ = "Simon Oldfield"


import gdal
from gdalconst import *
import numpy


class RasterMetadata:

    transform = None
    projection = None
    no_data_value = None
    data_type = None

    def __init__(self, transform, projection, no_data_value, data_type):
        self.transform = transform
        self.projection = projection
        self.no_data_value = no_data_value
        self.data_type = data_type


def raster_get_metadata(path, band_number):

    raster = gdal.Open(path, GA_ReadOnly)
    assert raster

    band = raster.GetRasterBand(band_number)
    assert band

    metadata = RasterMetadata(raster.GetGeoTransform(), raster.GetProjection(), band.GetNoDataValue(), band.DataType)

    band = raster = None

    return metadata


def raster_get_data(path, band_number):

    """
    Get the data for a band from a raster as a numpy (masked with the no data value) array

    :param path: path to the input raster
    :param band_number: band number (starts at 1)
    :return: numpy (masked with the no data value) array
    """

    raster = gdal.Open(path, GA_ReadOnly)
    assert raster

    band = raster.GetRasterBand(band_number)
    assert band

    data = band.ReadAsArray(0, 0, raster.RasterXSize, raster.RasterYSize)
    data = numpy.ma.masked_equal(data, band.GetNoDataValue())

    band = raster = None

    return data
