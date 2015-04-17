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


__author__ = "Simon Oldfield"


import math
import logging
import numpy
import gdal
import os
from gdalconst import *
from enum import Enum
from datacube.api.model import Pq25Bands, Ls57Arg25Bands, Satellite, DatasetType, Ls8Arg25Bands, Wofs25Bands, NdviBands, \
    get_bands, EviBands, NbrBands, TciBands
from datetime import datetime


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

class PqaMask(Enum):
    PQ_MASK_CLEAR = 16383               # bits 0 - 13 set

    PQ_MASK_SATURATION = 255            # bits 0 - 7 set
    PQ_MASK_SATURATION_OPTICAL = 159    # bits 0-4 and 7 set
    PQ_MASK_SATURATION_THERMAL = 96     # bits 5,6 set

    PQ_MASK_CONTIGUITY = 256            # bit 8 set

    PQ_MASK_LAND = 512                  # bit 9 set

    PQ_MASK_CLOUD = 15360               # bits 10-13

    PQ_MASK_CLOUD_ACCA = 1024           # bit 10 set
    PQ_MASK_CLOUD_FMASK = 2048          # bit 11 set

    PQ_MASK_CLOUD_SHADOW_ACCA = 4096    # bit 12 set
    PQ_MASK_CLOUD_SHADOW_FMASK = 8192   # bit 13 set


class WofsMask(Enum):
    DRY = 0
    NO_DATA = 1
    SATURATION_CONTIGUITY = 2
    SEA_WATER = 4
    TERRAIN_SHADOW = 8
    HIGH_SLOPE = 16
    CLOUD_SHADOW = 32
    CLOUD = 64
    WET = 128


# Standard no data value
NDV = -999

INT16_MIN = numpy.iinfo(numpy.int16).min
INT16_MAX = numpy.iinfo(numpy.int16).max

UINT16_MIN = numpy.iinfo(numpy.uint16).min
UINT16_MAX = numpy.iinfo(numpy.uint16).max

BYTE_MIN = numpy.iinfo(numpy.ubyte).min
BYTE_MAX = numpy.iinfo(numpy.ubyte).max


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

        raster_band.FlushCache()
        del raster_band

    dataset_metadata = DatasetMetaData((raster.RasterXSize, raster.RasterYSize), raster.GetGeoTransform(), raster.GetProjection(), band_metadata)

    raster.FlushCache()
    del raster

    return dataset_metadata


def get_dataset_data(dataset, bands=None, x=0, y=0, x_size=None, y_size=None):

    dataset_types_physical = [
        DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25,
        DatasetType.WATER,
        DatasetType.DSM, DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED, DatasetType.DEM_SMOOTHED]

    dataset_types_virtual_nbar = [
        DatasetType.NDVI,
        DatasetType.EVI,
        DatasetType.NBR,
        DatasetType.TCI
    ]

    # NDVI calculated using RED and NIR from ARG25

    if dataset.dataset_type == DatasetType.NDVI:

        bands = get_bands(DatasetType.ARG25, dataset.satellite)

        band_red = bands[Ls57Arg25Bands.RED.name]
        band_nir = bands[Ls57Arg25Bands.NEAR_INFRARED.name]

        data = read_dataset_data(dataset, bands=[band_red, band_nir])
        data = calculate_ndvi(data[band_red], data[band_nir])

        return {NdviBands.NDVI: data}

    # EVI calculated using RED, BLUE and NIR from ARG25

    elif dataset.dataset_type == DatasetType.EVI:

        bands = get_bands(DatasetType.ARG25, dataset.satellite)

        band_red = bands[Ls57Arg25Bands.RED.name]
        band_blue = bands[Ls57Arg25Bands.BLUE.name]
        band_nir = bands[Ls57Arg25Bands.NEAR_INFRARED.name]

        data = read_dataset_data(dataset, bands=[band_red, band_blue, band_nir])
        data = calculate_evi(data[band_red], data[band_blue], data[band_nir])

        return {EviBands.EVI: data}

    # NBR calculated using NIR and SWIR-2 from ARG25

    elif dataset.dataset_type == DatasetType.NBR:

        bands = get_bands(DatasetType.ARG25, dataset.satellite)

        band_nir = bands[Ls57Arg25Bands.NEAR_INFRARED.name]
        band_swir = bands[Ls57Arg25Bands.SHORT_WAVE_INFRARED_2.name]

        data = read_dataset_data(dataset, bands=[band_nir, band_swir])
        data = calculate_nbr(data[band_nir], data[band_swir])

        return {NbrBands.NBR: data}

    # TCI calculated from ARG25

    elif dataset.dataset_type == DatasetType.TCI:

        bands = get_bands(DatasetType.ARG25, dataset.satellite)

        data = read_dataset_data(dataset, bands=bands)

        out = dict()

        for index in TasselCapIndex:
            out[TciBands[index.name]] = calculate_tassel_cap_index(data, TCI_COEFFICIENTS[dataset.satellite][index])

        return out

    # It is a "physical" dataset so just read it
    else:
        return read_dataset_data(dataset, bands, x, y, x_size, y_size)


def read_dataset_data(dataset, bands=None, x=0, y=0, x_size=None, y_size=None):

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

        band.FlushCache()
        del band

    raster.FlushCache()
    del raster

    return out


DEFAULT_MASK_PQA = [PqaMask.PQ_MASK_CLEAR]


def get_dataset_data_masked(dataset, bands=None, x=0, y=0, x_size=None, y_size=None, ndv=NDV, mask=None):

    """
    Return one or more bands from the dataset with pixel quality applied

    :type dataset: datacube.api.model.Dataset
    :type bands: list[Band]
    :type x: int
    :type y: int
    :type x_size: int
    :type y_size: int
    :type ndv: int
    :type mask: numpy.array
    :rtype: dict[numpy.array]
    """

    if not bands:
        bands = dataset.bands

    out = get_dataset_data(dataset, bands, x=x, y=y, x_size=x_size, y_size=y_size)

    if mask is not None:
        for band in bands:
            out[band] = apply_mask(out[band], mask=mask, ndv=ndv)

    return out


def get_dataset_data_with_pq(dataset, dataset_pqa, bands=None, x=0, y=0, x_size=None, y_size=None, masks_pqa=DEFAULT_MASK_PQA, ndv=NDV):

    """
    Return one or more bands from the dataset with pixel quality applied

    :type dataset: datacube.api.model.Dataset
    :type dataset_pqa: datacube.api.model.Dataset
    :type bands: list[Band]
    :type x: int
    :type y: int
    :type x_size: int
    :type y_size: int
    :type masks_pqa: list[datacube.api.util.PqaMask]
    :rtype: dict[numpy.array]
    """

    if not bands:
        bands = dataset.bands

    mask_pqa = get_mask_pqa(dataset_pqa, x=x, y=y, x_size=x_size, y_size=y_size, pqa_masks=masks_pqa)

    out = get_dataset_data_masked(dataset, bands, x=x, y=y, x_size=x_size, y_size=y_size, mask=mask_pqa, ndv=ndv)

    return out


def apply_mask(data, mask, ndv=NDV):
    return numpy.ma.array(data, mask=mask).filled(ndv)


def get_mask_pqa(pqa, pqa_masks=DEFAULT_MASK_PQA, x=0, y=0, x_size=None, y_size=None, mask=None):

    """
    Return a pixel quality mask

    :param pqa: Pixel Quality dataset
    :param pqa_masks: which PQ flags to use
    :param mask: an optional existing mask to update
    :return: the mask
    """

    # Consolidate the list of (bit) masks into a single (bit) mask
    pqa_mask = consolidate_masks(pqa_masks)

    # Read the PQA dataset
    data = get_dataset_data(pqa, [Pq25Bands.PQ], x=x, y=y, x_size=x_size, y_size=y_size)[Pq25Bands.PQ]

    # Create an empty mask if none provided - just to avoid an if below :)
    if mask is None:
        mask = numpy.ma.make_mask_none(numpy.shape(data))

    # Mask out values where the requested bits in the PQ value are not set
    mask = numpy.ma.mask_or(mask, numpy.ma.masked_where(data & pqa_mask != pqa_mask, data).mask)

    return mask


def consolidate_masks(masks):
    mask = 0x0000

    for m in masks:
        mask |= m.value

    return mask


DEFAULT_MASK_WOFS = [WofsMask.WET]


def get_mask_wofs(wofs, wofs_masks=DEFAULT_MASK_WOFS, x=0, y=0, x_size=None, y_size=None, mask=None):

    """
    Return a WOFS mask

    :param wofs: WOFS dataset
    :param wofs_masks: which WOFS values to mask
    :param mask: an optional existing mask to update
    :return: the mask
    """

    # Read the WOFS dataset
    data = get_dataset_data(wofs, bands=[Wofs25Bands.WATER], x=x, y=y, x_size=x_size, y_size=y_size)[Wofs25Bands.WATER]

    if mask is None:
        mask = numpy.ma.make_mask_none(numpy.shape(data))

    # Mask out values where the WOFS value is one of the requested mask values
    for wofs_mask in wofs_masks:
        mask = numpy.ma.mask_or(mask, numpy.ma.masked_equal(data, wofs_mask.value).mask)

    return mask


# TODO I've dodgied this to get band names in.  Should redo it properly so you pass in a lit of band data structures
# that have a name, the data, the NDV, etc

def raster_create(path, data, transform, projection, no_data_value, data_type,
                  # options=["INTERLEAVE=PIXEL", "COMPRESS=DEFLATE", "PREDICTOR=2", "ZLEVEL=9"]):
                  # options=["INTERLEAVE=PIXEL", "COMPRESS=DEFLATE", "PREDICTOR=1", "ZLEVEL=6"]):
                  options=["INTERLEAVE=PIXEL"],
                  # options=["INTERLEAVE=PIXEL", "COMPRESS=LZW"],
                  # options=["INTERLEAVE=PIXEL", "COMPRESS=LZW", "TILED=YES"],
                  width=None, height=None, dataset_metadata=None, band_ids=None):
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

    _log.debug("creating output raster %s", path)
    _log.debug("filename=%s | shape = %s | bands = %d | data type = %s", path, (numpy.shape(data[0])[0], numpy.shape(data[0])[1]),
               len(data), data_type)

    driver = gdal.GetDriverByName("GTiff")
    assert driver

    width = width or numpy.shape(data[0])[1]
    height = height or numpy.shape(data[0])[0]

    raster = driver.Create(path, width, height, len(data), data_type, options)
    assert raster

    raster.SetGeoTransform(transform)
    raster.SetProjection(projection)

    if dataset_metadata:
        raster.SetMetadata(dataset_metadata)

    for i in range(0, len(data)):
        _log.debug("Writing band %d", i + 1)

        band = raster.GetRasterBand(i + 1)

        if band_ids and len(band_ids) - 1 >= i:
            band.SetDescription(band_ids[i])
        band.SetNoDataValue(no_data_value)
        band.WriteArray(data[i])
        band.ComputeStatistics(True)

        band.FlushCache()
        del band

    raster.FlushCache()
    del raster


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
    """
    Calculate the Normalised Difference Vegetation Index (NDVI) from a Landsat dataset

    NDVI is defined as (NIR - RED) / (NIR + RED)
    """

    red = numpy.ma.masked_equal(red, input_ndv)
    nir = numpy.ma.masked_equal(nir, input_ndv)

    ndvi = numpy.true_divide(nir - red, nir + red)
    ndvi = ndvi.filled(output_ndv)

    return ndvi


def calculate_evi(red, blue, nir, l=1, c1=6, c2=7.5, input_ndv=NDV, output_ndv=NDV):
    """
    Calculate the Enhanced Vegetation Index (EVI) from a Landsat dataset first applying Pixel Quality indicators

    EVI is defined as 2.5 * (NIR - RED) / (NIR + C1 * RED - C2 * BLUE + L)

    Defaults to the standard MODIS EVI of L=1 C1=6 C2=7.5
    """

    red = numpy.ma.masked_equal(red, input_ndv)
    blue = numpy.ma.masked_equal(blue, input_ndv)
    nir = numpy.ma.masked_equal(nir, input_ndv)

    evi = 2.5 * numpy.true_divide(nir - red, nir + c1 * red - c2 * blue + 1)
    evi = evi.filled(output_ndv)

    return evi


def calculate_nbr(nir, swir, input_ndv=NDV, output_ndv=NDV):
    """
    Calculate the Normalised Burn Ratio (NBR) from a Landsat dataset

    NBR is defined as (NIR - SWIR 2) / (NIR + SWIR 2)
    """

    nir = numpy.ma.masked_equal(nir, input_ndv)
    swir = numpy.ma.masked_equal(swir, input_ndv)

    nbr = numpy.true_divide(nir - swir, nir + swir)
    nbr = nbr.filled(output_ndv)

    return nbr


class TasselCapIndex(Enum):
    __order__ = "BRIGHTNESS GREENNESS WETNESS FOURTH FIFTH SIXTH"

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
    },

    Satellite.LS8:
    {
        TasselCapIndex.BRIGHTNESS: {
            Ls8Arg25Bands.BLUE: 0.3029,
            Ls8Arg25Bands.GREEN: 0.2786,
            Ls8Arg25Bands.RED: 0.4733,
            Ls8Arg25Bands.NEAR_INFRARED: 0.5599,
            Ls8Arg25Bands.SHORT_WAVE_INFRARED_1: 0.508,
            Ls8Arg25Bands.SHORT_WAVE_INFRARED_2: 0.1872},

        TasselCapIndex.GREENNESS: {
            Ls8Arg25Bands.BLUE: -0.2941,
            Ls8Arg25Bands.GREEN: -0.2430,
            Ls8Arg25Bands.RED: -0.5424,
            Ls8Arg25Bands.NEAR_INFRARED: 0.7276,
            Ls8Arg25Bands.SHORT_WAVE_INFRARED_1: 0.0713,
            Ls8Arg25Bands.SHORT_WAVE_INFRARED_2: -0.1608},

        TasselCapIndex.WETNESS: {
            Ls8Arg25Bands.BLUE: 0.1511,
            Ls8Arg25Bands.GREEN: 0.1973,
            Ls8Arg25Bands.RED: 0.3283,
            Ls8Arg25Bands.NEAR_INFRARED: 0.3407,
            Ls8Arg25Bands.SHORT_WAVE_INFRARED_1: -0.7117,
            Ls8Arg25Bands.SHORT_WAVE_INFRARED_2: -0.4559},

        TasselCapIndex.FOURTH: {
            Ls8Arg25Bands.BLUE: -0.8239,
            Ls8Arg25Bands.GREEN: 0.0849,
            Ls8Arg25Bands.RED: 0.4396,
            Ls8Arg25Bands.NEAR_INFRARED: -0.058,
            Ls8Arg25Bands.SHORT_WAVE_INFRARED_1: 0.2013,
            Ls8Arg25Bands.SHORT_WAVE_INFRARED_2: -0.2773},

        TasselCapIndex.FIFTH: {
            Ls8Arg25Bands.BLUE: -0.3294,
            Ls8Arg25Bands.GREEN: 0.0557,
            Ls8Arg25Bands.RED: 0.1056,
            Ls8Arg25Bands.NEAR_INFRARED: 0.1855,
            Ls8Arg25Bands.SHORT_WAVE_INFRARED_1: -0.4349,
            Ls8Arg25Bands.SHORT_WAVE_INFRARED_2: 0.8085},

        TasselCapIndex.SIXTH: {
            Ls8Arg25Bands.BLUE: 0.1079,
            Ls8Arg25Bands.GREEN: -0.9023,
            Ls8Arg25Bands.RED: 0.4119,
            Ls8Arg25Bands.NEAR_INFRARED: 0.0575,
            Ls8Arg25Bands.SHORT_WAVE_INFRARED_1: -0.0259,
            Ls8Arg25Bands.SHORT_WAVE_INFRARED_2: 0.0252}
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

    for b in bands:
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
        that is, 120/-20 contains lon values 120->120.99999 but lat values -19->-19.99999
        that is, that is, 120/-20 does NOT contain lat value of -20

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


def intersection(a, b):
    return list(set(a) & set(b))


def union(a, b):
    return list(set(a) | set(b))


def subset(a, b):
    return set(a) <= set(b)


def get_satellite_string(satellites):
    # TODO this assumes everything is Landsat!!!!
    return "LS" + "".join([s.value.replace("LS", "") for s in satellites])


def check_overwrite_remove_or_fail(path, overwrite):

    if os.path.exists(path):
        if overwrite:
            _log.info("Removing existing output file [%s]", path)
            os.remove(path)
        else:
            _log.error("Output file [%s] exists", path)
            raise Exception("File [%s] exists" % path)


def log_mem(s=None):

    if s and len(s) > 0:
        _log.info(s)

    import psutil

    _log.info("Current memory usage is [%s]", psutil.Process().memory_info())
    _log.info("Current memory usage is [%d] MB", psutil.Process().memory_info().rss / 1024 / 1024)

    import resource

    _log.info("Current MAX RSS  usage is [%d] MB", resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)


def date_to_integer(d):
    # Return an integer representing the YYYYMMDD value
    return d.year * 10000 + d.month * 100 + d.day


def get_dataset_filename(dataset, mask_pqa_apply=False, mask_wofs_apply=False):

    filename = dataset.path

    filename = os.path.basename(filename)

    dataset_type_from_string = {
        DatasetType.ARG25: "_NBAR_",
        DatasetType.PQ25: "_PQA_",
        DatasetType.FC25: "_FC_",
        DatasetType.WATER: "_WATER_",
        DatasetType.NDVI: "_NBAR_",
        DatasetType.EVI: "_NBAR_",
        DatasetType.NBR: "_NBAR_",
        DatasetType.TCI: "_NBAR_"
    }[dataset.dataset_type]

    dataset_type_to_string = {
        DatasetType.ARG25: "_NBAR_",
        DatasetType.PQ25: "_PQA_",
        DatasetType.FC25: "_FC_",
        DatasetType.WATER: "_WATER_",
        DatasetType.NDVI: "_NDVI_",
        DatasetType.EVI: "_EVI_",
        DatasetType.NBR: "_NBR_",
        DatasetType.TCI: "_TCI_"
    }[dataset.dataset_type]

    if mask_pqa_apply and mask_wofs_apply:
        dataset_type_to_string += "WITH_PQA_WATER_"

    elif mask_pqa_apply:
        dataset_type_to_string += "WITH_PQA_"

    elif mask_wofs_apply:
        dataset_type_to_string += + "WITH_WATER_"

    filename = filename.replace(dataset_type_from_string, dataset_type_to_string)
    filename = filename.replace(".vrt", ".tif")
    filename = filename.replace(".tiff", ".tif")

    return filename


# TODO
def get_dataset_ndv(dataset):
    return NDV