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
from datetime import datetime
import math


__author__ = "Simon Oldfield"


import logging
import numpy
import gdal
from gdalconst import *
from enum import Enum
from datacube.api.model import Pq25Bands, Ls57Arg25Bands, Satellite, DatasetType


_log = logging.getLogger(__name__)


# Define PQ mask
#   This represents bits 0-13 set which means:
#       -  0 = band 10 not saturated
#       -  1 = band 20 not saturated
#       -  2 = band 30 not saturated
#       -  3 = band 40 not saturated
#       -  4 = band 50 not saturated
#       -  5 = band 61 not saturated
#       -  6 = band 62 not saturated
#       -  7 = band 70 not saturated
#       -  8 = contiguity ok (i.e. all bands present)
#       -  9 = land (not sea)
#       - 10 = not cloud (ACCA test)
#       - 11 = not cloud (FMASK test)
#       - 12 = not cloud shadow (ACCA test)
#       - 13 = not cloud shadow (FMASK test)

# ### DEPRECATE AND REMOVE THESE IN FAVOUR OF THE ENUM!!!
#
# PQA_MASK = 0x3fff  # This represents bits 0-13 set
# PQA_MASK_CONTIGUITY = 0x01FF
# PQA_MASK_CLOUD = 0x0C00
# PQA_MASK_CLOUD_SHADOW = 0x3000
# PQA_MASK_SEA_WATER = 0x0200
#
# PQ_MASK_CLEAR = 16383               # bits 0 - 13 set
# PQ_MASK_SATURATION = 255            # bits 0 - 7 set
# PQ_MASK_SATURATION_OPTICAL = 159    # bits 0-4 and 7 set
# PQ_MASK_SATURATION_THERMAL = 96     # bits 5,6 set
# PQ_MASK_CONTIGUITY = 256            # bit 8 set
# PQ_MASK_LAND = 512                  # bit 9 set
# PQ_MASK_CLOUD_ACCA = 1024           # bit 10 set
# PQ_MASK_CLOUD_FMASK = 2048          # bit 11 set
# PQ_MASK_CLOUD_SHADOW_ACCA = 4096    # bit 12 set
# PQ_MASK_CLOUD_SHADOW_FMASK = 8192   # bit 13 set

class PqaMask(Enum):
    # PQA_MASK = 0x3fff                   # This represents bits 0-13 set
    # PQA_MASK_CONTIGUITY = 0x01FF
    # PQA_MASK_CLOUD = 0x0C00
    # PQA_MASK_CLOUD_SHADOW = 0x3000
    # PQA_MASK_SEA_WATER = 0x0200

    PQ_MASK_CLEAR = 16383               # bits 0 - 13 set
    PQ_MASK_SATURATION = 255            # bits 0 - 7 set
    PQ_MASK_SATURATION_OPTICAL = 159    # bits 0-4 and 7 set
    PQ_MASK_SATURATION_THERMAL = 96     # bits 5,6 set
    PQ_MASK_CONTIGUITY = 256            # bit 8 set
    PQ_MASK_LAND = 512                  # bit 9 set
    PQ_MASK_CLOUD_ACCA = 1024           # bit 10 set
    PQ_MASK_CLOUD_FMASK = 2048          # bit 11 set
    PQ_MASK_CLOUD_SHADOW_ACCA = 4096    # bit 12 set
    PQ_MASK_CLOUD_SHADOW_FMASK = 8192   # bit 13 set


# Standard no data value
NDV = -999

INT16_MIN = numpy.iinfo(numpy.int16).min
INT16_MAX = numpy.iinfo(numpy.int16).max


def empty_array(shape, dtype=numpy.int16, ndv=-999):

    """
    Return an empty (i.e. filled with the no data value) array of the given shape and data type

    :param shape: shape of the array
    :param dtype: data type of the array (defaults to int32)
    :param ndv: no data value (defaults to -999)
    :return: array
    """

    a = None

    if ndv == 0:
        a = numpy.zeros(shape=shape, dtype=dtype)

    else:
        a = numpy.empty(shape=shape, dtype=dtype)
        a.fill(ndv)

    return a


class DatasetBandMetaData:
    no_data_value = None
    data_type = None

    def __init__(self, no_data_value, data_type):
        self.no_data_value=no_data_value
        self.data_type=data_type


class DatasetMetaData:

    shape = None
    transform = None
    projection = None
    bands = None
    ul = None
    lr = None
    pixel_size_x = None
    pixel_size_y = None

    def __init__(self, shape, transform, projection, bands):
        self.shape = shape
        self.transform = transform
        self.projection = projection
        self.bands = bands
        self.pixel_size_x = self.transform[1]
        self.pixel_size_y = self.transform[5]
        self.ul = (self.transform[0], self.transform[3])
        self.lr = (self.ul[0] + self.pixel_size_x * self.shape[0], self.ul[1] + self.pixel_size_y * self.shape[1])


def get_dataset_metadata(dataset):

    raster = gdal.Open(dataset.path, GA_ReadOnly)
    assert raster

    band_metadata = dict()

    for band in dataset.bands:
        raster_band = raster.GetRasterBand(band.value)
        assert raster_band

        band_metadata[band] = DatasetBandMetaData(raster_band.GetNoDataValue(), raster_band.DataType)

        raster_band = None

    dataset_metadata = DatasetMetaData((raster.RasterXSize, raster.RasterYSize), raster.GetGeoTransform(), raster.GetProjection(), band_metadata)

    raster = None

    return dataset_metadata


def get_dataset_data(dataset, bands=None, x=0, y=0, x_size=None, y_size=None):

    """
    Return one or more bands from a dataset

    :param dataset: The dataset from which to read the band
    :param bands: A list of bands to read from the dataset
    :param x:
    :param y:
    :param x_size:
    :param y_size:
    :return: dictionary of band/data as numpy array
    """

    out = dict()

    raster = gdal.Open(dataset.path, GA_ReadOnly)
    assert raster

    if not x_size:
        x_size = raster.RasterXSize

    if not y_size:
        y_size = raster.RasterYSize

    if not bands:
        bands = dataset.bands

    for b in bands:

        band = raster.GetRasterBand(b.value)
        assert band

        data = band.ReadAsArray(x, y, x_size, y_size)
        out[b] = data

    return out


DEFAULT_PQA_MASK = [PqaMask.PQ_MASK_CLEAR]

def get_dataset_data_with_pq(dataset, pq_dataset, bands=None, x=0, y=0, x_size=None, y_size=None, pq_masks=DEFAULT_PQA_MASK):

    """
    Return one or more bands from the dataset with pixel quality applied

    :param dataset: The dataset from which to read the band
    :param pq_dataset: The pixel quality dataset
    :param bands: A list of bands to read from the dataset
    :param x:
    :param y:
    :param x_size:
    :param y_size:
    :param pq_mask: Required pixel quality mask to apply
    :return: dictionary of band/data as numpy array
    """

    if not bands:
        bands = dataset.bands

    out = get_dataset_data(dataset, bands, x=x, y=y, x_size=x_size, y_size=y_size)

    data_pq = get_dataset_data(pq_dataset, [Pq25Bands.PQ], x=x, y=y, x_size=x_size, y_size=y_size)[Pq25Bands.PQ]

    for band in bands:

        out[band] = apply_pq(out[band], data_pq, pq_masks=pq_masks)

    return out


def apply_pq(dataset, pq, ndv=NDV, pq_masks=DEFAULT_PQA_MASK):

    # Get the PQ mask
    mask = get_pq_mask(pq, pq_masks)

    # Apply the PQ mask to the dataset and fill masked entries with no data value
    return numpy.ma.array(dataset, mask=mask).filled(ndv)


def get_pq_mask(pq, pq_masks=DEFAULT_PQA_MASK):

    """
    Return a pixel quality mask

    :param pq: Pixel Quality dataset
    :param mask: which PQ flags to use
    :return: the PQ mask
    """

    # Consolidate the list of (bit) masks into a single (bit) mask
    pq_mask = consolidate_pq_mask(pq_masks)

    # Mask out values where the requested bits in the PQ value are not set
    return numpy.ma.masked_where(pq & pq_mask != pq_mask, pq).mask


def consolidate_pq_mask(masks):
    mask = 0x0000

    for m in masks:
        mask |= m.value

    return mask


def raster_create(path, data, transform, projection, no_data_value, data_type,
                  # options=["INTERLEAVE=PIXEL", "COMPRESS=DEFLATE", "PREDICTOR=2", "ZLEVEL=9"]):
                  # options=["INTERLEAVE=PIXEL", "COMPRESS=DEFLATE", "PREDICTOR=1", "ZLEVEL=6"]):
                  options=["INTERLEAVE=PIXEL"]):
    """
    Create a raster from a list of numpy arrays

    :param path: path to the output raster
    :param data: list of numpy arrays
    :param transform: geo transform
    :param projection: projection
    :param no_data_value: no data value
    :param data_type: data type
    :param options: raster creation options
    """

    _log.info("creating output raster %s", path)
    _log.debug("filename=%s | shape = %s | bands = %d | data type = %s", path, (numpy.shape(data[0])[0], numpy.shape(data[0])[1]),
               len(data), data_type)

    driver = gdal.GetDriverByName("GTiff")
    assert driver

    dataset = driver.Create(path, numpy.shape(data[0])[1], numpy.shape(data[0])[0],
                            len(data), data_type,
                            options)
    assert dataset

    dataset.SetGeoTransform(transform)
    dataset.SetProjection(projection)

    for i in range(0, len(data)):
        _log.info("Writing band %d", i + 1)
        dataset.GetRasterBand(i + 1).SetNoDataValue(no_data_value)
        dataset.GetRasterBand(i + 1).WriteArray(data[i])
        dataset.GetRasterBand(i + 1).ComputeStatistics(True)

    dataset.FlushCache()

    dataset = None


def propagate_using_selected_pixel(a, b, c, d, ndv=NDV):
    return numpy.where((a == b) & (a != ndv), c, d)


# def calculate_ndvi(red, nir, input_ndv=NDV, output_ndv=INT16_MAX):
#     m_red = numpy.ma.masked_equal(red, input_ndv) #.astype(numpy.float32)
#     m_nir = numpy.ma.masked_equal(nir, input_ndv) #.astype(numpy.float32)
#
#     ndvi = numpy.true_divide(m_nir - m_red, m_nir + m_red)
#     ndvi = (ndvi * 10000).astype(numpy.int16)
#     ndvi = ndvi.filled(output_ndv)
#
#     return ndvi


def calculate_ndvi(red, nir, input_ndv=NDV, output_ndv=NDV):
    m_red = numpy.ma.masked_equal(red, input_ndv)
    m_nir = numpy.ma.masked_equal(nir, input_ndv)

    ndvi = numpy.true_divide(m_nir - m_red, m_nir + m_red)
    ndvi = ndvi.filled(output_ndv)

    return ndvi


class TasselCapIndex(Enum):
    __order__ = "BRIGHTNESS GREENNESS WETNESS"

    BRIGHTNESS = 1
    GREENNESS = 2
    WETNESS = 3
    FOURTH = 4
    FIFTH = 5
    SIXTH = 6


TCI_COEFFICIENTS = {
    Satellite.LS5:
    {
        TasselCapIndex.BRIGHTNESS: {
            Ls57Arg25Bands.BLUE: 0.3037,
            Ls57Arg25Bands.GREEN: 0.2793,
            Ls57Arg25Bands.RED: 0.4743,
            Ls57Arg25Bands.NEAR_INFRARED: 0.5585,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_1: 0.5082,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_2: 0.1863},

        TasselCapIndex.GREENNESS: {
            Ls57Arg25Bands.BLUE: -0.2848,
            Ls57Arg25Bands.GREEN: -0.2435,
            Ls57Arg25Bands.RED: -0.5436,
            Ls57Arg25Bands.NEAR_INFRARED: 0.7243,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_1: 0.0840,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_2: -0.1800},

        TasselCapIndex.WETNESS: {
            Ls57Arg25Bands.BLUE: 0.1509,
            Ls57Arg25Bands.GREEN: 0.1973,
            Ls57Arg25Bands.RED: 0.3279,
            Ls57Arg25Bands.NEAR_INFRARED: 0.3406,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_1: -0.7112,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_2: -0.4572},

        TasselCapIndex.FOURTH: {
            Ls57Arg25Bands.BLUE: -0.8242,
            Ls57Arg25Bands.GREEN: 0.0849,
            Ls57Arg25Bands.RED: 0.4392,
            Ls57Arg25Bands.NEAR_INFRARED: -0.0580,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_1: 0.2012,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_2: -0.2768},

        TasselCapIndex.FIFTH: {
            Ls57Arg25Bands.BLUE: -0.3280,
            Ls57Arg25Bands.GREEN: 0.0549,
            Ls57Arg25Bands.RED: 0.1075,
            Ls57Arg25Bands.NEAR_INFRARED: 0.1855,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_1: -0.4357,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_2: 0.8085},

        TasselCapIndex.SIXTH: {
            Ls57Arg25Bands.BLUE: 0.1084,
            Ls57Arg25Bands.GREEN: -0.9022,
            Ls57Arg25Bands.RED: 0.4120,
            Ls57Arg25Bands.NEAR_INFRARED: 0.0573,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_1: -0.0251,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_2: 0.0238}
    },

    Satellite.LS7:
    {
        TasselCapIndex.BRIGHTNESS: {
            Ls57Arg25Bands.BLUE: 0.3561,
            Ls57Arg25Bands.GREEN: 0.3972,
            Ls57Arg25Bands.RED: 0.3904,
            Ls57Arg25Bands.NEAR_INFRARED: 0.6966,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_1: 0.2286,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_2: 0.1596},

        TasselCapIndex.GREENNESS: {
            Ls57Arg25Bands.BLUE: -0.3344,
            Ls57Arg25Bands.GREEN: -0.3544,
            Ls57Arg25Bands.RED: -0.4556,
            Ls57Arg25Bands.NEAR_INFRARED: 0.6966,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_1: -0.0242,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_2: -0.2630},

        TasselCapIndex.WETNESS: {
            Ls57Arg25Bands.BLUE: 0.2626,
            Ls57Arg25Bands.GREEN: 0.2141,
            Ls57Arg25Bands.RED: 0.0926,
            Ls57Arg25Bands.NEAR_INFRARED: 0.0656,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_1: -0.7629,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_2: -0.5388},

        TasselCapIndex.FOURTH: {
            Ls57Arg25Bands.BLUE: 0.0805,
            Ls57Arg25Bands.GREEN: -0.0498,
            Ls57Arg25Bands.RED: 0.1950,
            Ls57Arg25Bands.NEAR_INFRARED: -0.1327,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_1: 0.5752,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_2: -0.7775},

        TasselCapIndex.FIFTH: {
            Ls57Arg25Bands.BLUE: -0.7252,
            Ls57Arg25Bands.GREEN: -0.0202,
            Ls57Arg25Bands.RED: 0.6683,
            Ls57Arg25Bands.NEAR_INFRARED: 0.0631,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_1: -0.1494,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_2: -0.0274},

        TasselCapIndex.SIXTH: {
            Ls57Arg25Bands.BLUE: 0.4000,
            Ls57Arg25Bands.GREEN: -0.8172,
            Ls57Arg25Bands.RED: 0.3832,
            Ls57Arg25Bands.NEAR_INFRARED: 0.0602,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_1: -0.1095,
            Ls57Arg25Bands.SHORT_WAVE_INFRARED_2: 0.0985}
    }
}


def calculate_tassel_cap_index(bands, coefficients, input_ndv=NDV, output_ndv=numpy.nan):
    """

    :param bands:
    :param coefficients:
    :param input_ndv:
    :param output_ndv:
    :return:
    """
    bands_masked = dict()

    # Drop out no data values - do I need this???

    for b in bands.iterkeys():
        bands_masked[b] = numpy.ma.masked_equal(bands[b], input_ndv).astype(numpy.float32) / 10000

    tci = 0

    for b in [Ls57Arg25Bands.BLUE,
              Ls57Arg25Bands.GREEN,
              Ls57Arg25Bands.RED,
              Ls57Arg25Bands.NEAR_INFRARED,
              Ls57Arg25Bands.SHORT_WAVE_INFRARED_1,
              Ls57Arg25Bands.SHORT_WAVE_INFRARED_2]:

        tci += bands_masked[b] * coefficients[b]

    tci = tci.filled(output_ndv)

    return tci


def calculate_medoid(X, dist=None):
    _log.debug("X is \n%s", X)
    _log.debug("X.ndim is %d", X.ndim)

    if dist is None:
        dist = lambda x, y: numpy.sqrt(numpy.square(x-y).sum())
    if X.ndim == 1:
        return X
    _, n = X.shape
    d = numpy.empty(n)
    for i in range(n):
        d[i] = numpy.sum([dist(X[:, i], X[:, j]) for j in range(n) if j != i])
    return X[:, numpy.argmin(d)]


# def calculate_medoid_simon(X):
#
#     _log.debug("shape of X is %s", numpy.shape(X))
#
#     files, bands, rows, cols = numpy.shape(X)
#     _log.debug("files=%d bands=%d rows=%d cols=%d", files, bands, rows, cols)
#
#     d = numpy.empty(rows, cols)


# def calculate_medoid_flood(X):
#
#     ndx = vectormedian(imagestack)
#     medianimg = selectmedianimage(imagestack, ndx)


def latlon_to_xy(lat, lon, transform):
    """
    Convert lat/lon to x/y for raster
    NOTE: No projection done - assumes raster has native lat/lon projection

    :param lat: latitude
    :param lon: longitude
    :param transform: GDAL GeoTransform
    :return: x, y pair
    """
    # Get the reverse direction GeoTransform
    _, transform = gdal.InvGeoTransform(transform)

    ulx, uly = transform[0], transform[3]
    psx, psy = transform[1], transform[5]

    x = int(math.floor(ulx + psx * lon))
    y = int(math.floor(uly + psy * lat))

    return x, y


def latlon_to_cell(lat, lon):
    """
    Return the cell that contains the given lat/lon pair

    NOTE: x of cell represents min (contained) lon value but y of cell represents max (not contained) lat value
        that is, 120_-20 contains lon values 120->120.99999 but lat values -19->-19.99999
        that is, that is, 120_-20 does NOT contain lat value of -20

    :param lat: latitude
    :param lon: longitude
    :return: cell as x, y pair
    """
    x = int(lon)
    y = int(lat) - 1

    return x, y


# TODO this is a bit of dodginess until the WOFS tiles are ingested
# DO NOT USE THIS IT WON'T STAY!!!!

def extract_fields_from_filename(filename):
    """

    :param filename:
    :return:
    """

    # At the moment I only need this to work for the WOFS WATER extent tile files....
    #  LS5_TM_WATER_120_-021_2004-09-20T01-40-14.409038.tif
    #  LS7_ETM_WATER_120_-021_2006-06-30T01-45-48.187525.tif

    if filename.endswith(".tif"):
        filename = filename[:-len(".tif")]

    elif filename.endswith(".tiff"):
        filename = filename[:-len(".tiff")]

    elif filename.endswith(".vrt"):
        filename = filename[:-len(".vrt")]

    fields = filename.split("_")

    # Satellite
    satellite = Satellite[fields[0]]

    # Dataset Type
    dataset_type = DatasetType[fields[2]]

    # Cell
    x, y = int(fields[3]), int(fields[4])

    # Acquisition Date/Time
    acq_str = fields[5].split(".")[0] # go from "2006-06-30T01-45-48.187525" to "2006-06-30T01-45-48"
    acq_dt = datetime.strptime(acq_str, "%Y-%m-%dT%H-%M-%S")

    return satellite, dataset_type, x, y, acq_dt