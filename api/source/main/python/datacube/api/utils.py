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


__author__ = "Simon Oldfield"


import math
import logging
import numpy
import gdal
import os
import gdalconst
from collections import namedtuple
from dateutil.relativedelta import relativedelta
from enum import Enum
from datacube.api.model import Pq25Bands, Ls57Arg25Bands, Satellite, DatasetType, Ls8Arg25Bands, Wofs25Bands, NdviBands, \
    NdwiBands, MndwiBands
from datacube.api.model import get_bands, EviBands, NbrBands, TciBands
from scipy.ndimage import map_coordinates
from eotools.coordinates import convert_coordinates
from datetime import datetime, date


_log = logging.getLogger(__name__)

# gdal.SetCacheMax(1024*1024*1024)


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


class OutputFormat(Enum):
    __order__ = "GEOTIFF ENVI"

    GEOTIFF = "GTiff"
    ENVI = "ENVI"


# Standard no data value
NDV = -999

INT16_MIN = numpy.iinfo(numpy.int16).min
INT16_MAX = numpy.iinfo(numpy.int16).max

UINT16_MIN = numpy.iinfo(numpy.uint16).min
UINT16_MAX = numpy.iinfo(numpy.uint16).max

BYTE_MIN = numpy.iinfo(numpy.ubyte).min
BYTE_MAX = numpy.iinfo(numpy.ubyte).max

NAN = numpy.nan


class PercentileInterpolation(Enum):
    __order__ = "LINEAR LOWER HIGHER NEAREST MIDPOINT"

    LINEAR = "linear"
    LOWER = "lower"
    HIGHER = "higher"
    NEAREST = "nearest"
    MIDPOINT = "midpoint"


def empty_array(shape, dtype=numpy.int16, fill=NDV):

    """
    Return an empty (i.e. filled with the no data value) array of the given shape and data type

    :param shape: shape of the array
    :param dtype: data type of the array (defaults to int32)
    :param fill: no data value (defaults to -999)
    :return: array
    """

    a = None

    if fill == 0:
        a = numpy.zeros(shape=shape, dtype=dtype)

    else:
        a = numpy.empty(shape=shape, dtype=dtype)
        a.fill(fill)

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

    raster = gdal.Open(dataset.path, gdalconst.GA_ReadOnly)
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

    # NDVI calculated using RED and NIR from ARG25

    if dataset.dataset_type == DatasetType.NDVI:

        bands = get_bands(DatasetType.ARG25, dataset.satellite)

        band_red = bands[Ls57Arg25Bands.RED.name]
        band_nir = bands[Ls57Arg25Bands.NEAR_INFRARED.name]

        data = read_dataset_data(dataset, bands=[band_red, band_nir], x=x, y=y, x_size=x_size, y_size=y_size)
        data = calculate_ndvi(data[band_red], data[band_nir])

        return {NdviBands.NDVI: data}

    # NDWI calculated using GREEN and NIR from ARG25

    elif dataset.dataset_type == DatasetType.NDWI:

        bands = get_bands(DatasetType.ARG25, dataset.satellite)

        band_green = bands[Ls57Arg25Bands.GREEN.name]
        band_nir = bands[Ls57Arg25Bands.NEAR_INFRARED.name]

        data = read_dataset_data(dataset, bands=[band_green, band_nir], x=x, y=y, x_size=x_size, y_size=y_size)
        data = calculate_ndwi(data[band_green], data[band_nir])

        return {NdwiBands.NDWI: data}

    # MNDWI calculated using GREEN and SWIR 1 from ARG25

    if dataset.dataset_type == DatasetType.MNDWI:

        bands = get_bands(DatasetType.ARG25, dataset.satellite)

        band_green = bands[Ls57Arg25Bands.GREEN.name]
        band_swir = bands[Ls57Arg25Bands.SHORT_WAVE_INFRARED_1.name]

        data = read_dataset_data(dataset, bands=[band_green, band_swir], x=x, y=y, x_size=x_size, y_size=y_size)
        data = calculate_ndvi(data[band_green], data[band_swir])

        return {MndwiBands.MNDWI: data}

    # EVI calculated using RED, BLUE and NIR from ARG25

    elif dataset.dataset_type == DatasetType.EVI:

        bands = get_bands(DatasetType.ARG25, dataset.satellite)

        band_red = bands[Ls57Arg25Bands.RED.name]
        band_blue = bands[Ls57Arg25Bands.BLUE.name]
        band_nir = bands[Ls57Arg25Bands.NEAR_INFRARED.name]

        data = read_dataset_data(dataset, bands=[band_red, band_blue, band_nir], x=x, y=y, x_size=x_size, y_size=y_size)
        data = calculate_evi(data[band_red], data[band_blue], data[band_nir])

        return {EviBands.EVI: data}

    # NBR calculated using NIR and SWIR-2 from ARG25

    elif dataset.dataset_type == DatasetType.NBR:

        bands = get_bands(DatasetType.ARG25, dataset.satellite)

        band_nir = bands[Ls57Arg25Bands.NEAR_INFRARED.name]
        band_swir = bands[Ls57Arg25Bands.SHORT_WAVE_INFRARED_2.name]

        data = read_dataset_data(dataset, bands=[band_nir, band_swir], x=x, y=y, x_size=x_size, y_size=y_size)
        data = calculate_nbr(data[band_nir], data[band_swir])

        return {NbrBands.NBR: data}

    # TCI calculated from ARG25

    elif dataset.dataset_type == DatasetType.TCI:

        bands = get_bands(DatasetType.ARG25, dataset.satellite)

        data = read_dataset_data(dataset, bands=bands, x=x, y=y, x_size=x_size, y_size=y_size)

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

    raster = gdal.Open(dataset.path, gdalconst.GA_ReadOnly)
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
    Return one or more bands from the dataset with the given mask applied

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


def get_mask_vector_for_cell(x, y, vector_file, vector_layer, vector_feature, width=4000, height=4000,
                             pixel_size_x=0.00025, pixel_size_y=-0.00025):

    """
    Return a mask for the given cell based on the specified feature in the vector file

    :param x: X cell index
    :type x: int
    :param y: X cell
    :type y: int
    :param vector_file: Vector file containing the mask polygon
    :type vector_file: str
    :param vector_layer: Layer name within the vector file
    :type vector_layer: str
    :param vector_feature: Feature id (index starts at 0) within the layer
    :type vector_feature: int
    :param width: Width of the mask
    :type width: int
    :param height: Height of the mask
    :type height: int
    :param pixel_size_x: X pixel size
    :type pixel_size_x: float
    :param pixel_size_y: Y pixel size
    :type pixel_size_y: float

    :return: The mask
    :rtype: numpy.ma.MaskedArray.mask (array of boolean)
    """

    import gdal
    import osr

    driver = gdal.GetDriverByName("MEM")
    assert driver

    raster = driver.Create("", width, height, 1, gdal.GDT_Byte)
    assert raster

    raster.SetGeoTransform((x, pixel_size_x, 0.0, y+1, 0.0, pixel_size_y))

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)

    raster.SetProjection(srs.ExportToWkt())

    _log.debug("Reading feature [%d] from layer [%d] of file [%s]", vector_feature, vector_layer, vector_file)

    import ogr
    from gdalconst import GA_ReadOnly

    vector = ogr.Open(vector_file, GA_ReadOnly)
    assert vector

    # layer = vector.GetLayer()
    # assert layer

    # layer = vector.GetLayerByName(vector_layer)
    # assert layer

    layer = vector.GetLayerByIndex(vector_layer)
    assert layer

    layer.SetAttributeFilter("FID={fid}".format(fid=vector_feature))

    gdal.RasterizeLayer(raster, [1], layer, burn_values=[1])

    del layer

    band = raster.GetRasterBand(1)
    assert band

    data = band.ReadAsArray()
    import numpy

    _log.debug("Read [%s] from memory AOI mask dataset", numpy.shape(data))
    return numpy.ma.masked_not_equal(data, 1, copy=False).mask


def get_dataset_data_stack(tiles, dataset_type, band_name, x=0, y=0, x_size=None, y_size=None, ndv=None,
                           mask_pqa_apply=False, mask_pqa_mask=None):

        data_type = get_dataset_type_data_type(dataset_type)

        stack = numpy.empty((len(tiles), y_size and y_size or 4000, x_size and x_size or 4000), dtype=data_type)

        for index, tile in enumerate(tiles, start=0):

            dataset = tile.datasets[dataset_type]
            assert dataset

            band = dataset.bands[band_name]
            assert band

            _log.info("Stacking band [%s] of [%s]", band.name, dataset.path)

            if not ndv:
                ndv = get_dataset_ndv(dataset)

            pqa = (mask_pqa_apply and DatasetType.PQ25 in tile.datasets) and tile.datasets[DatasetType.PQ25] or None

            if dataset_type not in tile.datasets:
                _log.debug("No [%s] dataset present for [%s] - skipping", dataset_type.name, tile.end_datetime)
                continue

            log_mem("Before get data")

            mask = None

            if pqa:
                mask = get_mask_pqa(pqa, mask_pqa_mask, mask=mask, x=x, y=y, x_size=x_size, y_size=y_size)

            data = get_dataset_data_masked(dataset, bands=[band], mask=mask, ndv=ndv, x=x, y=y, x_size=x_size, y_size=y_size)

            log_mem("After get data")

            stack[index] = data[band]
            del data, pqa

            log_mem("After adding data to stack and deleting it")

        return stack


# TODO generalise/refactor this!!!

def raster_create(path, data, transform, projection, no_data_value, data_type,
                  # options=["INTERLEAVE=PIXEL", "COMPRESS=DEFLATE", "PREDICTOR=2", "ZLEVEL=9"]):
                  # options=["INTERLEAVE=PIXEL", "COMPRESS=DEFLATE", "PREDICTOR=1", "ZLEVEL=6"]):
                  options=["INTERLEAVE=PIXEL"],
                  # options=["INTERLEAVE=PIXEL", "COMPRESS=LZW"],
                  # options=["INTERLEAVE=PIXEL", "COMPRESS=LZW", "TILED=YES"],
                  width=None, height=None, dataset_metadata=None, band_ids=None):
    raster_create_geotiff(path, data, transform, projection, no_data_value, data_type, options, width, height,
                          dataset_metadata, band_ids)


# TODO I've dodgied this to get band names in.  Should redo it properly so you pass in a lit of band data structures
# that have a name, the data, the NDV, etc

def raster_create_geotiff(path, data, transform, projection, no_data_value, data_type,
                  # options=["INTERLEAVE=PIXEL", "COMPRESS=DEFLATE", "PREDICTOR=2", "ZLEVEL=9"]):
                  # options=["INTERLEAVE=PIXEL", "COMPRESS=DEFLATE", "PREDICTOR=1", "ZLEVEL=6"]):
                  # options=["INTERLEAVE=PIXEL"],
                  # options=["INTERLEAVE=PIXEL", "COMPRESS=LZW"],
                  options=["INTERLEAVE=PIXEL", "COMPRESS=LZW", "TILED=YES"],
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


def raster_create_envi(path, data, transform, projection, no_data_value, data_type,
                  # options=["INTERLEAVE=PIXEL", "COMPRESS=DEFLATE", "PREDICTOR=2", "ZLEVEL=9"]):
                  # options=["INTERLEAVE=PIXEL", "COMPRESS=DEFLATE", "PREDICTOR=1", "ZLEVEL=6"]):
                  options=["INTERLEAVE=BSQ"],
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

    driver = gdal.GetDriverByName("ENVI")
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


def calculate_ndwi(green, nir, input_ndv=NDV, output_ndv=NDV):
    """
    Calculate the Normalised Difference Water Index (NDWI) from a Landsat dataset

    NDWI is defined as (GREEN - NIR) / (GREEN + NIR)
    """

    green = numpy.ma.masked_equal(green, input_ndv)
    nir = numpy.ma.masked_equal(nir, input_ndv)

    ndwi = numpy.true_divide(green - nir, green + nir)
    ndwi = ndwi.filled(output_ndv)

    return ndwi


def calculate_mndwi(green, mir, input_ndv=NDV, output_ndv=NDV):
    """
    Calculate the Modified Normalised Difference Water Index (MNDWI) from a Landsat dataset

    MNDWI is defined as (GREEN - MIR) / (GREEN + MIR)
    """

    green = numpy.ma.masked_equal(green, input_ndv)
    mir = numpy.ma.masked_equal(mir, input_ndv)

    mdnwi = numpy.true_divide(green - mir, green + mir)
    mdnwi = mdnwi.filled(output_ndv)

    return mdnwi


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
        bands_masked[b] = numpy.ma.masked_equal(bands[b], input_ndv).astype(numpy.float16)

    tci = 0

    for b in bands:
        if b in coefficients:
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
        _log.debug(s)

    import psutil

    _log.info("Current memory usage is [%s]", psutil.Process().memory_info())
    _log.info("Current memory usage is [%d] MB", psutil.Process().memory_info().rss / 1024 / 1024)

    import resource

    _log.info("Current MAX RSS  usage is [%d] MB", resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)


def date_to_integer(d):
    # Return an integer representing the YYYYMMDD value
    return d.year * 10000 + d.month * 100 + d.day


def get_dataset_filename(dataset, output_format=OutputFormat.GEOTIFF,
                         mask_pqa_apply=False, mask_wofs_apply=False, mask_vector_apply=False):

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
        DatasetType.TCI: "_NBAR_",
        DatasetType.DSM: "DSM_",
        DatasetType.DEM: "DEM_",
        DatasetType.DEM_HYDROLOGICALLY_ENFORCED: "DEM-H_",
        DatasetType.DEM_SMOOTHED: "DEM-S_",
        DatasetType.NDWI: "_NBAR_",
        DatasetType.MNDWI: "_NBAR_"
    }[dataset.dataset_type]

    dataset_type_to_string = {
        DatasetType.ARG25: "_NBAR_",
        DatasetType.PQ25: "_PQA_",
        DatasetType.FC25: "_FC_",
        DatasetType.WATER: "_WATER_",
        DatasetType.NDVI: "_NDVI_",
        DatasetType.EVI: "_EVI_",
        DatasetType.NBR: "_NBR_",
        DatasetType.TCI: "_TCI_",
        DatasetType.DSM: "DSM_",
        DatasetType.DEM: "DEM_",
        DatasetType.DEM_HYDROLOGICALLY_ENFORCED: "DEM_H_",
        DatasetType.DEM_SMOOTHED: "DEM_S_",
        DatasetType.NDWI: "_NDWI_",
        DatasetType.MNDWI: "_MNDWI_",
    }[dataset.dataset_type]

    dataset_type_to_string += ((mask_pqa_apply or mask_wofs_apply or mask_vector_apply) and "WITH_" or "") + \
                              (mask_pqa_apply and "PQA_" or "") + \
                              (mask_wofs_apply and "WATER_" or "") + \
                              (mask_vector_apply and "VECTOR_" or "")

    filename = filename.replace(dataset_type_from_string, dataset_type_to_string)

    ext = {OutputFormat.GEOTIFF: ".tif", OutputFormat.ENVI: ".dat"}[output_format]

    filename = filename.replace(".vrt", ext)
    filename = filename.replace(".tiff", ext)
    filename = filename.replace(".tif", ext)

    return filename

# def get_dataset_filename(self, dataset):
#
#     from datacube.api.workflow import format_date
#     from datacube.api.utils import get_satellite_string
#
#     satellites = get_satellite_string(self.satellites)
#
#     acq_min = format_date(self.acq_min)
#     acq_max = format_date(self.acq_max)
#
#     return os.path.join(self.output_directory,
#         "{satellites}_{dataset}_{x:03d}_{y:04d}_{acq_min}_{acq_max}.tif".format(
#         satellites=satellites,
#         dataset=dataset,
#         x=self.x, y=self.y,
#         acq_min=acq_min,
#         acq_max=acq_max))


# def get_dataset_band_stack_filename(satellites, dataset_type, band, x, y, acq_min, acq_max, season_range=None,
#                                     mask_pqa_apply=False, mask_wofs_apply=False, mask_vector_apply=False,
#                                     output_format=OutputFormat.GEOTIFF
#                                     ):
#
#     filename_template = ""
#
#     if season_range:
#         filename_template = "{satellite}_{dataset}_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{season_start}_{season_end}_{band}_STACK.tif"
#     else:
#         filename_template = "{satellite}_{dataset}_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{band}_STACK.tif"
#
#
#     from datacube.api.workflow import format_date
#     from datacube.api.utils import get_satellite_string
#
#     satellites_str = get_satellite_string(satellites)
#
#     acq_min_str = format_date(acq_min)
#     acq_max_str = format_date(acq_max)
#
#     "{satellite}_{dataset}_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{season_start}_{season_end}_{band}_STACK.tif".format(x=self.x,
#                                                                                                     y=self.y,
#                                                                                                     acq_min=acq_min,
#                                                                                                     acq_max=acq_max,
#                                                                                                     season_start=season_start,
#                                                                                                     season_end=season_end,
#                                                                                                     band=self.band.name
#                                                                                                     ))
#     dataset_type_from_string = {
#         DatasetType.ARG25: "_NBAR_",
#         DatasetType.PQ25: "_PQA_",
#         DatasetType.FC25: "_FC_",
#         DatasetType.WATER: "_WATER_",
#         DatasetType.NDVI: "_NBAR_",
#         DatasetType.EVI: "_NBAR_",
#         DatasetType.NBR: "_NBAR_",
#         DatasetType.TCI: "_NBAR_",
#         DatasetType.DSM: "DSM_"
#     }[dataset.dataset_type]
#
#     dataset_type_to_string = {
#         DatasetType.ARG25: "_NBAR_",
#         DatasetType.PQ25: "_PQA_",
#         DatasetType.FC25: "_FC_",
#         DatasetType.WATER: "_WATER_",
#         DatasetType.NDVI: "_NDVI_",
#         DatasetType.EVI: "_EVI_",
#         DatasetType.NBR: "_NBR_",
#         DatasetType.TCI: "_TCI_",
#         DatasetType.DSM: "DSM_"
#     }[dataset.dataset_type]
#
#     dataset_type_to_string += ((mask_pqa_apply or mask_wofs_apply or mask_vector_apply) and "WITH_" or "") + \
#                               (mask_pqa_apply and "PQA_" or "") + \
#                               (mask_wofs_apply and "WATER_" or "") + \
#                               (mask_vector_apply and "VECTOR_" or "")
#
#     dataset_type_to_string += "STACK_" + band.name + "_"
#
#     filename = filename.replace(dataset_type_from_string, dataset_type_to_string)
#
#     ext = {OutputFormat.GEOTIFF: ".tif", OutputFormat.ENVI: ".dat"}[output_format]
#
#     filename = filename.replace(".vrt", ext)
#     filename = filename.replace(".tiff", ext)
#     filename = filename.replace(".tif", ext)
#
#     return filename


def get_dataset_band_stack_filename(satellites, dataset_type, band, x, y, acq_min, acq_max, season=None,
                                    mask_pqa_apply=False, mask_wofs_apply=False, mask_vector_apply=False,
                                    output_format=OutputFormat.GEOTIFF):

    from datacube.api.workflow import format_date

    satellite_str = get_satellite_string(satellites)

    dataset_type_str = {
        DatasetType.ARG25: "NBAR",
        DatasetType.PQ25: "PQA",
        DatasetType.FC25: "FC",
        DatasetType.WATER: "WATER",
        DatasetType.NDVI: "NDVI",
        DatasetType.EVI: "EVI",
        DatasetType.NBR: "NBR",
        DatasetType.TCI: "TCI",
        DatasetType.DSM: "DSM",
        DatasetType.DEM: "DEM",
        DatasetType.DEM_HYDROLOGICALLY_ENFORCED: "DEM_H",
        DatasetType.DEM_SMOOTHED: "DEM_S",
        DatasetType.NDWI: "NDWI",
        DatasetType.MNDWI: "MNDWI"
    }[dataset_type]

    dataset_type_str += ((mask_pqa_apply or mask_wofs_apply or mask_vector_apply) and "_WITH" or "") + \
                              (mask_pqa_apply and "_PQA" or "") + \
                              (mask_wofs_apply and "_WATER" or "") + \
                              (mask_vector_apply and "_VECTOR" or "")

    ext = {OutputFormat.GEOTIFF: "tif", OutputFormat.ENVI: "dat"}[output_format]

    if season:
        filename_template = "{satellite}_{dataset}_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{season_name}_{season_start}_{season_end}_{band}_STACK.{ext}"

        season_name, (season_start_month, season_start_day), (season_end_month, season_end_day) = season

        _, _, include = build_date_criteria(acq_min, acq_max, season_start_month, season_start_day, season_end_month, season_end_day)

        season_start = "{month}_{day:02d}".format(month=season_start_month.name[:3], day=season_start_day)
        season_end = "{month}_{day:02d}".format(month=season_end_month.name[:3], day=season_end_day)

        filename = filename_template.format(satellite=satellite_str, dataset=dataset_type_str, x=x, y=y,
                                            acq_min=format_date(acq_min), acq_max=format_date(acq_max),
                                            season_name=season.name, season_start=season_start, season_end=season_end,
                                            band=band.name, ext=ext)
    else:
        filename_template = "{satellite}_{dataset}_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{band}_STACK.{ext}"

        filename = filename_template.format(satellite=satellite_str, dataset=dataset_type_str, x=x, y=y,
                                            acq_min=format_date(acq_min), acq_max=format_date(acq_max),
                                            band=band.name, ext=ext)

    return filename


def get_pixel_time_series_filename(satellites, dataset_type, lat, lon, acq_min, acq_max, season=None,
                                    mask_pqa_apply=False, mask_wofs_apply=False, mask_vector_apply=False):

    from datacube.api.workflow import format_date

    satellite_str = get_satellite_string(satellites)

    dataset_type_str = {
        DatasetType.ARG25: "NBAR",
        DatasetType.PQ25: "PQA",
        DatasetType.FC25: "FC",
        DatasetType.WATER: "WATER",
        DatasetType.NDVI: "NDVI",
        DatasetType.EVI: "EVI",
        DatasetType.NBR: "NBR",
        DatasetType.TCI: "TCI",
        DatasetType.DSM: "DSM"
    }[dataset_type]

    dataset_type_str += ((mask_pqa_apply or mask_wofs_apply or mask_vector_apply) and "_WITH" or "") + \
                              (mask_pqa_apply and "_PQA" or "") + \
                              (mask_wofs_apply and "_WATER" or "") + \
                              (mask_vector_apply and "_VECTOR" or "")

    if season:
        filename_template = "{satellite}_{dataset}_{longitude:03.5f}_{latitude:03.5f}_{acq_min}_{acq_max}_{season_name}_{season_start}_{season_end}.csv"

        season_name, (season_start_month, season_start_day), (season_end_month, season_end_day) = season

        _, _, include = build_date_criteria(acq_min, acq_max, season_start_month, season_start_day, season_end_month, season_end_day)

        season_start = "{month}_{day:02d}".format(month=season_start_month.name[:3], day=season_start_day)
        season_end = "{month}_{day:02d}".format(month=season_end_month.name[:3], day=season_end_day)

        filename = filename_template.format(satellite=satellite_str, dataset=dataset_type_str, latitude=lat, longitude=lon,
                                            acq_min=format_date(acq_min), acq_max=format_date(acq_max),
                                            season_name=season.name, season_start=season_start, season_end=season_end)
    else:
        filename_template = "{satellite}_{dataset}_{longitude:03.5f}_{latitude:03.5f}_{acq_min}_{acq_max}.csv"

        filename = filename_template.format(satellite=satellite_str, dataset=dataset_type_str, latitude=lat, longitude=lon,
                                            acq_min=format_date(acq_min), acq_max=format_date(acq_max))

    return filename


def get_dataset_datatype(dataset):
    return get_dataset_type_datatype(dataset.dataset_type)


def get_dataset_type_datatype(dataset_type):
    return {
        DatasetType.ARG25: gdalconst.GDT_Int16,
        DatasetType.PQ25: gdalconst.GDT_Int16,
        DatasetType.FC25: gdalconst.GDT_Int16,
        DatasetType.WATER: gdalconst.GDT_Byte,
        DatasetType.NDVI: gdalconst.GDT_Float32,
        DatasetType.NDWI: gdalconst.GDT_Float32,
        DatasetType.MNDWI: gdalconst.GDT_Float32,
        DatasetType.EVI: gdalconst.GDT_Float32,
        DatasetType.NBR: gdalconst.GDT_Float32,
        DatasetType.TCI: gdalconst.GDT_Float32,
        DatasetType.DSM: gdalconst.GDT_Float32,
        DatasetType.DEM: gdalconst.GDT_Float32,
        DatasetType.DEM_HYDROLOGICALLY_ENFORCED: gdalconst.GDT_Float32,
        DatasetType.DEM_SMOOTHED: gdalconst.GDT_Float32
    }[dataset_type]


def get_dataset_ndv(dataset):
    return get_dataset_type_ndv(dataset.dataset_type)


def get_dataset_type_ndv(dataset_type):
    return {
        DatasetType.ARG25: NDV,
        DatasetType.PQ25: UINT16_MAX,
        DatasetType.FC25: NDV,
        DatasetType.WATER: BYTE_MAX,
        DatasetType.NDVI: NAN,
        DatasetType.NDWI: NAN,
        DatasetType.MNDWI: NAN,
        DatasetType.EVI: NAN,
        DatasetType.NBR: NAN,
        DatasetType.TCI: NAN,
        DatasetType.DSM: NAN,
        DatasetType.DEM: NAN,
        DatasetType.DEM_HYDROLOGICALLY_ENFORCED: NAN,
        DatasetType.DEM_SMOOTHED: NAN
    }[dataset_type]


def get_dataset_type_data_type(dataset_type):
    return {
        DatasetType.ARG25: numpy.int16,
        DatasetType.PQ25: numpy.uint16,
        DatasetType.FC25: numpy.int16,
        DatasetType.WATER: numpy.byte,
        DatasetType.NDVI: numpy.float32,
        DatasetType.NDWI: numpy.float32,
        DatasetType.MNDWI: numpy.float32,
        DatasetType.EVI: numpy.float32,
        DatasetType.NBR: numpy.float32,
        DatasetType.TCI: numpy.float32,
        DatasetType.DSM: numpy.int16
    }[dataset_type]

def get_band_name_union(dataset_type, satellites):

    bands = [b.name for b in get_bands(dataset_type, satellites[0])]

    for satellite in satellites[1:]:
        for b in get_bands(dataset_type, satellite):
            if b.name not in bands:
                bands.append(b.name)

    return bands


def get_band_name_intersection(dataset_type, satellites):

    bands = [b.name for b in get_bands(dataset_type, satellites[0])]

    for satellite in satellites[1:]:
        for band in bands:
            if band not in [b.name for b in get_bands(dataset_type, satellite)]:
                bands.remove(band)

    return bands


def format_date(d):
    if d:
        return d.strftime("%Y_%m_%d")

    return None


def format_date_time(d):
    if d:
        return d.strftime("%Y_%m_%d_%H_%M_%S")

    return None


def extract_feature_geometry_wkb(vector_file, vector_layer=0, vector_feature=0, epsg=4326):

    import ogr
    import osr
    from gdalconst import GA_ReadOnly

    vector = ogr.Open(vector_file, GA_ReadOnly)
    assert vector

    layer = vector.GetLayer(vector_layer)
    assert layer

    feature = layer.GetFeature(vector_feature)
    assert feature

    projection = osr.SpatialReference()
    projection.ImportFromEPSG(epsg)

    geom = feature.GetGeometryRef()

    # Transform if required

    if not projection.IsSame(geom.GetSpatialReference()):
        geom.TransformTo(projection)

    return geom.ExportToWkb()


def maskify_stack(stack, ndv=NDV):
    if numpy.isnan(ndv):
        return numpy.ma.masked_invalid(stack, copy=False)
    return numpy.ma.masked_equal(stack, ndv, copy=False)


def calculate_stack_statistic_count(stack, ndv=NDV, dtype=numpy.int16):

    stack = maskify_stack(stack=stack, ndv=ndv)

    stack_depth, stack_size_y, stack_size_x = numpy.shape(stack)

    stat = empty_array((stack_size_y, stack_size_x), dtype=dtype, fill=stack_depth)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)

    _log.debug("count is [%s]\n%s", numpy.shape(stat), stat)

    return stat


def calculate_stack_statistic_count_observed(stack, ndv=NDV, dtype=numpy.int16):

    stack = maskify_stack(stack=stack, ndv=ndv)

    stat = stack.count(axis=0)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)

    _log.debug("count observed is [%s]\n%s", numpy.shape(stat), stat)

    return stat


def calculate_stack_statistic_min(stack, ndv=NDV, dtype=numpy.int16):

    stack = maskify_stack(stack=stack, ndv=ndv)

    stat = numpy.min(stack, axis=0).filled(ndv)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)

    _log.debug("min is [%s]\n%s", numpy.shape(stat), stat)

    return stat


def calculate_stack_statistic_max(stack, ndv=NDV, dtype=numpy.int16):

    stack = maskify_stack(stack=stack, ndv=ndv)

    stat = numpy.max(stack, axis=0).filled(ndv)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)

    _log.debug("max is [%s]\n%s", numpy.shape(stat), stat)

    return stat


def calculate_stack_statistic_mean(stack, ndv=NDV, dtype=numpy.int16):
    if numpy.isnan(ndv):
        stat = numpy.nanmean(stack, axis=0)
    else:
        stack = maskify_stack(stack=stack, ndv=ndv)
        stat = numpy.mean(stack, axis=0).filled(ndv)

    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)

    _log.debug("mean is [%s]\n%s", numpy.shape(stat), stat)

    return stat


def calculate_stack_statistic_variance(stack, ndv=NDV, dtype=numpy.int16):
    if numpy.isnan(ndv):
        stat = numpy.nanvar(stack, axis=0)
    else:
        stack = maskify_stack(stack=stack, ndv=ndv)
        stat = numpy.var(stack, axis=0).filled(ndv)

    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)
    _log.debug("var is [%s]\n%s", numpy.shape(stat), stat)

    return stat


def calculate_stack_statistic_standard_deviation(stack, ndv=NDV, dtype=numpy.int16):
    if numpy.isnan(ndv):
        stat = numpy.nanstd(stack, axis=0)
    else:
        stack = maskify_stack(stack=stack, ndv=ndv)
        stat = numpy.std(stack, axis=0).filled(ndv)

    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)
    _log.debug("std is [%s]\n%s", numpy.shape(stat), stat)

    return stat


def calculate_stack_statistic_median(stack, ndv=NDV, dtype=numpy.int16):

    stack = maskify_stack(stack=stack, ndv=ndv)

    stat = numpy.median(stack, axis=0).filled(ndv)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)

    _log.debug("median is [%s]\n%s", numpy.shape(stat), stat)

    return stat


def calculate_stack_statistic_percentile(stack, percentile, interpolation=PercentileInterpolation.NEAREST, ndv=NDV, dtype=numpy.int16):

    # stack = maskify_stack(stack=stack, ndv=ndv)
    #
    # # numpy (1.9.2) currently doesn't have masked version of percentile so convert to float and use nanpercentile
    #
    # stack = numpy.ndarray.astype(stack, dtype=numpy.float16, copy=False).filled(numpy.nan)
    #
    # stat = numpy.nanpercentile(stack, percentile, axis=0, interpolation=interpolation.value)
    # stat = numpy.ma.masked_invalid(stat, copy=False)
    # stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False).filled(ndv)
    #
    # _log.debug("max is [%s]\n%s", numpy.shape(stat), stat)
    #
    # return stat

    def do_percentile(data):
        d = data[data != ndv]

        # numpy.percentile has a hissy if the array is empty - aka ALL no data...

        if d.size == 0:
            return ndv
        else:
            return numpy.percentile(a=d, q=percentile, interpolation=interpolation.value)

    if numpy.isnan(ndv):
        stat = numpy.nanpercentile(a=stack, q=percentile, axis=0, interpolation=interpolation.value)
    else:
        stat = numpy.apply_along_axis(do_percentile, axis=0, arr=stack)

    _log.debug("%s is [%s]\n%s", percentile, numpy.shape(stat), stat)

    return stat


def get_mask_aoi_cell(vector_file, vector_layer, vector_feature, x, y, width=4000, height=4000, epsg=4326):

    import gdal
    import osr

    driver = gdal.GetDriverByName("MEM")
    assert driver

    raster = driver.Create("", width, height, 1, gdal.GDT_Byte)
    assert raster

    raster.SetGeoTransform((x, 0.00025, 0.0, y+1, 0.0, -0.00025))

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg)

    raster.SetProjection(srs.ExportToWkt())

    import ogr
    from gdalconst import GA_ReadOnly

    vector = ogr.Open(vector_file, GA_ReadOnly)
    assert vector

    layer = vector.GetLayer(vector_layer)
    assert layer

    # Transform if required

    feature = layer.GetFeature(vector_feature)
    assert feature

    projection = osr.SpatialReference()
    projection.ImportFromEPSG(epsg)

    geom = feature.GetGeometryRef()

    if not projection.IsSame(geom.GetSpatialReference()):
        geom.TransformTo(projection)

    layer.SetAttributeFilter("FID={fid}".format(fid=vector_feature))

    gdal.RasterizeLayer(raster, [1], layer, burn_values=[1])

    del layer

    band = raster.GetRasterBand(1)
    assert band

    data = band.ReadAsArray()
    import numpy

    _log.debug("Read [%s] from memory AOI mask dataset", numpy.shape(data))
    return numpy.ma.masked_not_equal(data, 1, copy=False).mask


def grand_mean(means):

    cumulative_mean = 0
    cumulative_count = 0

    for (mean, count) in means:
        cumulative_mean = cumulative_mean * cumulative_count / (cumulative_count + count) + mean * count / (cumulative_count + count)
        cumulative_count += count

    return cumulative_mean


def combine_means(mean1, count1, mean2, count2):
    return mean1 * count1 / (count1 + count2) + mean2 * count2 / (count1 + count2)


def arbitrary_profile(dataset, xy_points, band=None, cubic=False,
                      from_map=False):
    """
    Get the data associated with an arbitrary set of points that
    define an arbitrary profile/transect, and the pixel locations
    associated with the transect.

    :param dataset:
        An instance of a `DatasetType`.

    :param xy_points:
        A list of (x, y) co-ordinate paris eg [(x, y), (x, y), (x, y)].

    :param band:
        The raster band to read from. Default is to read the first band
        in the list returned by `dataset.bands`.

    :param from_map:
        A boolean indicating whether or not the input co-ordinates
        are real world map coordinates. If set to True, then the input
        xy co-ordinate will be converted to image co-ordinates.

    :return:
        A 1D NumPy array of lenght determined by the distance between
        the xy_points; A tuple (y_index, x_index) containing the
        pixel locations of the transect; and a tuple containing a list
        of start and end co-ordinates for both x and y fields.
        The form of the start and end locations is:
            ([(xstart_1, xend_1),...,(xstart_n-1, xend_n-1)],
             [(ystart_1, yend_1),...,(ystart_n-1, yend_n-1)]).
        This form can be directly used in a call to plot() as follows:
            prf, idx, xy_start_end = arbitrary_profile()
            plot(xy_start_end[0], xy_start_end[1], 'r-')

    :history:
        * 2015-07-07 - Base functionality taken from:
        https://github.com/GeoscienceAustralia/eo-tools/blob/stable/eotools/profiles.py
        and converted to work directly with the agdc-api.
    """
    n_points = len(xy_points)
    if n_points < 2:
        msg = "Minimum number of points is 2, received {}".format(n_points)
        raise ValueError(msg)

    metadata = get_dataset_metadata(dataset)
    geotransform = metadata.transform

    # Convert to image co-ordinates if needed
    if from_map:
        img_xy = convert_coordinates(geotransform, xy_points, to_map=False)
    else:
        img_xy = xy_points

    # Read the image band
    if band is None:
        band = [bnd for bnd in dataset.bands][0]
    img = read_dataset_data(dataset, [band])[band]

    # Initialise the arrays to hold the profile
    profile = numpy.array([], dtype=img.dtype)
    x_idx = numpy.array([], dtype='int')
    y_idx = numpy.array([], dtype='int')
    x_start_end = []
    y_start_end = []

    # Build up the profile by creating a line segment between each point
    for i in range(1, n_points):
        x0, y0 = img_xy[i - 1]
        x1, y1 = img_xy[i]
        x_start_end.append((x0, x1))
        y_start_end.append((y0, y1))

        n_pixels = max(abs(x1 - x0 + 1), abs(y1 - y0 + 1))
        x = numpy.linspace(x0, x1, n_pixels)
        y = numpy.linspace(y0, y1, n_pixels)
        x_idx = numpy.append(x_idx, x)
        y_idx = numpy.append(y_idx, y)

        # How to interpolate, cubic or nearest neighbour
        if cubic:
            transect = map_coordinates(img, (y, x))
            profile = numpy.append(profile, transect)
        else:
            transect = img[y.astype('int'), x.astype('int')]
            profile = numpy.append(profile, transect)

    x_idx = x_idx.astype('int')
    y_idx = y_idx.astype('int')

    return (profile, (y_idx, x_idx), (x_start_end, y_start_end))


def x_calculate_stack_statistic_count(stack, ndv=NDV, dtype=numpy.int16):

    stack = maskify_stack(stack=stack, ndv=ndv)

    stack_depth, stack_size_y, stack_size_x = numpy.shape(stack)

    stat = empty_array((stack_size_y, stack_size_x), dtype=dtype, fill=stack_depth)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)

    _log.debug("count is [%s]\n%s", numpy.shape(stat), stat)

    return stat


def x_calculate_stack_statistic_count_observed(stack, ndv=NDV, dtype=numpy.int16):

    stack = maskify_stack(stack=stack, ndv=ndv)

    stat = stack.count(axis=0)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)

    _log.debug("count observed is [%s]\n%s", numpy.shape(stat), stat)

    return stat


def x_calculate_stack_statistic_min(stack, ndv=NDV, dtype=numpy.int16):

    stack = maskify_stack(stack=stack, ndv=ndv)

    stat = numpy.min(stack, axis=0).filled(ndv)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)

    _log.debug("min is [%s]\n%s", numpy.shape(stat), stat)

    return stat


def x_calculate_stack_statistic_max(stack, ndv=NDV, dtype=numpy.int16):

    stack = maskify_stack(stack=stack, ndv=ndv)

    stat = numpy.max(stack, axis=0).filled(ndv)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)

    _log.debug("max is [%s]\n%s", numpy.shape(stat), stat)

    return stat


def x_calculate_stack_statistic_mean(stack, ndv=NDV, dtype=numpy.int16):

    stack = maskify_stack(stack=stack, ndv=ndv)

    stat = numpy.mean(stack, axis=0).filled(ndv)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)

    _log.debug("max is [%s]\n%s", numpy.shape(stat), stat)

    return stat


def x_calculate_stack_statistic_percentile(stack, percentile, interpolation=PercentileInterpolation.NEAREST, ndv=NDV, dtype=numpy.int16):

    stack = maskify_stack(stack=stack, ndv=ndv)

    # numpy (1.9.2) currently doesn't have masked version of percentile so convert to float and use nanpercentile

    stack = numpy.ndarray.astype(stack, dtype=numpy.float16, copy=False).filled(numpy.nan)

    stat = numpy.nanpercentile(stack, percentile, axis=0, interpolation=interpolation.value)
    stat = numpy.ma.masked_invalid(stat, copy=False)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False).filled(ndv)

    _log.debug("max is [%s]\n%s", numpy.shape(stat), stat)

    return stat


class Month(Enum):
    __order__ = "JANUARY FEBRUARY MARCH APRIL MAY JUNE JULY AUGUST SEPTEMBER OCTOBER NOVEMBER DECEMBER"

    JANUARY = 1
    FEBRUARY = 2
    MARCH = 3
    APRIL = 4
    MAY = 5
    JUNE = 6
    JULY = 7
    AUGUST = 8
    SEPTEMBER = 9
    OCTOBER = 10
    NOVEMBER = 11
    DECEMBER = 12


class Season(Enum):
    __order__ = "SPRING SUMMER AUTUMN WINTER CALENDAR_YEAR FINANCIAL_YEAR APR_TO_SEP"

    SPRING = "SPRING"
    SUMMER = "SUMMER"
    AUTUMN = "AUTUMN"
    WINTER = "WINTER"
    CALENDAR_YEAR = "CALENDAR_YEAR"
    FINANCIAL_YEAR = "FINANCIAL_YEAR"
    APR_TO_SEP = "APR_TO_SEP"


class Quarter(Enum):
    __order__ = "Q1 Q2 Q3 Q4"

    Q1 = "Q1"
    Q2 = "Q2"
    Q3 = "Q3"
    Q4 = "Q4"


SEASONS = {
    Season.SUMMER: ((Month.DECEMBER, 1), (Month.FEBRUARY, 31)),
    Season.AUTUMN: ((Month.MARCH, 1), (Month.MAY, 31)),
    Season.WINTER: ((Month.JUNE, 1), (Month.AUGUST, 31)),
    Season.SPRING: ((Month.SEPTEMBER, 1), (Month.NOVEMBER, 31)),
    Season.FINANCIAL_YEAR: ((Month.JULY, 1), (Month.JUNE, 30)),
    Season.CALENDAR_YEAR: ((Month.JANUARY, 1), (Month.DECEMBER, 31)),
    Season.APR_TO_SEP: ((Month.APRIL, 1), (Month.SEPTEMBER, 31))
}


SatelliteDateCriteria = namedtuple("SatelliteDateCriteria", "satellite acq_min acq_max")

LS7_SLC_OFF_ACQ_MIN = date(2005, 5, 31)
LS7_SLC_OFF_ACQ_MAX = None

LS7_SLC_OFF_EXCLUSION = SatelliteDateCriteria(satellite=Satellite.LS7,
                                              acq_min=LS7_SLC_OFF_ACQ_MIN, acq_max=LS7_SLC_OFF_ACQ_MAX)

LS8_PRE_WRS_2_ACQ_MIN = None
LS8_PRE_WRS_2_ACQ_MAX = date(2013, 4, 10)

LS8_PRE_WRS_2_EXCLUSION = SatelliteDateCriteria(satellite=Satellite.LS8,
                                                acq_min=LS8_PRE_WRS_2_ACQ_MIN, acq_max=LS8_PRE_WRS_2_ACQ_MAX)

DateCriteria = namedtuple("DateCriteria", "acq_min acq_max")


def build_season_date_criteria(acq_min, acq_max, season, seasons=SEASONS, extend=True):

    (month_start, day_start), (month_end, day_end) = seasons[season]

    return build_date_criteria(acq_min, acq_max, month_start, day_start, month_end, day_end, extend=extend)


def build_date_criteria(acq_min, acq_max, month_start, day_start, month_end, day_end, extend=True):

    date_criteria = []

    for year in range(acq_min.year, acq_max.year+1):

        min_dt = date(year, month_start.value, 1) + relativedelta(day=day_start)
        max_dt = date(year, month_end.value, 1) + relativedelta(day=day_end)

        if min_dt > max_dt:
            max_dt = date(year+1, month_end.value, 1) + relativedelta(day=day_end)

        date_criteria.append(DateCriteria(min_dt, max_dt))

        if extend and acq_max < max_dt:
            acq_max = max_dt

    return acq_min, acq_max, date_criteria


def generate_dataset_metadata(x, y, acq_dt, dataset, bands=None,
                              mask_pqa_apply=False, mask_pqa_mask=None, mask_wofs_apply=False, mask_wofs_mask=None):

    return {
        "X_INDEX": "{x:03d}".format(x=x),
        "Y_INDEX": "{y:04d}".format(y=y),
        "DATASET_TYPE": dataset.dataset_type.name,
        "BANDS": " ".join([b.name for b in (bands or dataset.bands)]),
        "ACQUISITION_DATE": "{acq_dt}".format(acq_dt=format_date_time(acq_dt)),
        "SATELLITE": dataset.satellite.name,
        "PIXEL_QUALITY_FILTER": mask_pqa_apply and " ".join([mask.name for mask in mask_pqa_mask]) or "",
        "WATER_FILTER": mask_wofs_apply and " ".join([mask.name for mask in mask_wofs_mask]) or ""
    }


def is_ndv(v, ndv=NDV):

    # Note: can't do if nan == nan!!!

    import numpy

    if numpy.isnan(ndv):
        if numpy.isnan(v):
            return True

    else:
        if v == ndv:
            return True

    return False
