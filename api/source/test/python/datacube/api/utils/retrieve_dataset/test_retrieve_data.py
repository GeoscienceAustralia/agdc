#!/usr/bin/env python

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


import filecmp
import gdal
import logging
import numpy
import pytest
from datacube.api import Satellite, DatasetType, WofsMask
from datacube.api.query import list_tiles_as_list
from datacube.api.utils import get_dataset_data, raster_create_geotiff, get_dataset_ndv, get_dataset_datatype, BYTE_MAX, \
    get_mask_pqa, get_dataset_data_masked, get_mask_wofs
from datacube.api.utils import get_dataset_data_with_pq
from datacube.api.utils import is_ndv, generate_dataset_metadata
from datacube.api.utils import NDV, UINT16_MAX, NAN
from datetime import date
from pathlib import Path


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


_log = logging.getLogger()


flatten = lambda *n: (e for a in n for e in (flatten(*a) if isinstance(a, (tuple, list)) else (a,)))


def get_test_data_path(f=None):

    path = Path(__file__).parent.absolute() / "data"

    if f:
        for x in flatten(f):
            path = path / x

    return str(path)


def get_test_data_path_common(f=None):

    path = Path(__file__).parent.parent.absolute() / "data"

    if f:
        for x in flatten(f):
            path = path / x

    return str(path)


CELL_X = 120
CELL_Y = -25

CELL_GEO_TRANSFORM = (120.75, 0.00025, 0.0, -24.75, 0.0, -0.00025)
CELL_PROJECTION = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]'

X_OFFSET = 2250
Y_OFFSET = 2500
X_SIZE = Y_SIZE = 250

ACQ_LS5 = date(2005, 1, 3)

DATE = "2005-01-03T01-36-51.953063"

YEAR_2014 = 2014

ACQ_MIN_2014 = date(YEAR_2014, 1, 1)
ACQ_MAX_2014 = date(YEAR_2014, 12, 31)

ARG_DATASET_TYPE = DatasetType.ARG25
ARG_NDV = NDV
ARG_DATA_TYPE = gdal.GDT_Int16

PQ_DATASET_TYPE = DatasetType.PQ25
PQ_NDV = UINT16_MAX
PQ_DATA_TYPE = gdal.GDT_Int16

FC_DATASET_TYPE = DatasetType.FC25
FC_NDV = NDV
FC_DATA_TYPE = gdal.GDT_Int16

WOFS_DATASET_TYPE = DatasetType.WATER
WOFS_NDV = BYTE_MAX
WOFS_DATA_TYPE = gdal.GDT_Byte

NDVI_DATASET_TYPE = DatasetType.NDVI
NDVI_NDV = NAN
NDVI_DATA_TYPE = gdal.GDT_Float32

TCI_DATASET_TYPE = DatasetType.TCI
TCI_NDV = NAN
TCI_DATA_TYPE = gdal.GDT_Float32

NDWI_DATASET_TYPE = DatasetType.NDWI
NDWI_NDV = NAN
NDWI_DATA_TYPE = gdal.GDT_Float32

MNDWI_DATASET_TYPE = DatasetType.MNDWI
MNDWI_NDV = NAN
MNDWI_DATA_TYPE = gdal.GDT_Float32

VECTOR_FILE_STATES = get_test_data_path_common("Mainlands.shp")
VECTOR_LAYER_STATES = 0
VECTOR_FEATURE_STATES = 4


@pytest.mark.quick
@pytest.mark.LS5
@pytest.mark.ARG
def test_retrieve_data_ls5_arg(config=None):

    filename = "LS5_TM_NBAR_{x:03d}_{y:04d}_{date}.{x_offset:04d}_{y_offset:04d}.{x_size:04d}x{y_size:04d}.tif".format(x=CELL_X, y=CELL_Y, date=DATE, x_offset=X_OFFSET, y_offset=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    tiles = list_tiles_as_list(x=[CELL_X], y=[CELL_Y],
                               acq_min=ACQ_LS5, acq_max=ACQ_LS5,
                               satellites=[Satellite.LS5],
                               dataset_types=[ARG_DATASET_TYPE],
                               config=config)

    assert len(tiles) == 1

    dataset = tiles[0].datasets[ARG_DATASET_TYPE]

    data = get_dataset_data(dataset=dataset, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    assert(data)
    _log.info("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert(is_ndv(ndv, ARG_NDV))

    data_type = get_dataset_datatype(dataset)
    assert(data_type == ARG_DATA_TYPE)

    metadata = generate_dataset_metadata(x=CELL_X, y=CELL_Y, acq_dt=ACQ_LS5,
                                         dataset=dataset, bands=None,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)

    raster_create_geotiff(filename, [data[b] for b in dataset.bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=metadata, band_ids=[b.name for b in dataset.bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.LS5
@pytest.mark.PQ
def test_retrieve_data_ls5_pq(config=None):

    filename = "LS5_TM_PQA_{x:03d}_{y:04d}_{date}.{x_offset:04d}_{y_offset:04d}.{x_size:04d}x{y_size:04d}.tif".format(x=CELL_X, y=CELL_Y, date=DATE, x_offset=X_OFFSET, y_offset=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    tiles = list_tiles_as_list(x=[CELL_X], y=[CELL_Y],
                               acq_min=ACQ_LS5, acq_max=ACQ_LS5,
                               satellites=[Satellite.LS5],
                               dataset_types=[PQ_DATASET_TYPE],
                               config=config)

    assert len(tiles) == 1

    dataset = tiles[0].datasets[PQ_DATASET_TYPE]

    data = get_dataset_data(dataset=dataset, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    assert(data)
    _log.info("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert(is_ndv(ndv, PQ_NDV))

    data_type = get_dataset_datatype(dataset)
    assert(data_type == PQ_DATA_TYPE)

    metadata = generate_dataset_metadata(x=CELL_X, y=CELL_Y, acq_dt=ACQ_LS5,
                                         dataset=dataset, bands=None,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)

    raster_create_geotiff(filename, [data[b] for b in dataset.bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=metadata, band_ids=[b.name for b in dataset.bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.LS5
@pytest.mark.FC
def test_retrieve_data_ls5_fc(config=None):

    filename = "LS5_TM_FC_{x:03d}_{y:04d}_{date}.{x_offset:04d}_{y_offset:04d}.{x_size:04d}x{y_size:04d}.tif".format(x=CELL_X, y=CELL_Y, date=DATE, x_offset=X_OFFSET, y_offset=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    tiles = list_tiles_as_list(x=[CELL_X], y=[CELL_Y],
                               acq_min=ACQ_LS5, acq_max=ACQ_LS5,
                               satellites=[Satellite.LS5],
                               dataset_types=[FC_DATASET_TYPE],
                               config=config)

    assert len(tiles) == 1

    dataset = tiles[0].datasets[FC_DATASET_TYPE]

    data = get_dataset_data(dataset=dataset, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    assert(data)
    _log.info("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert(is_ndv(ndv, FC_NDV))

    data_type = get_dataset_datatype(dataset)
    assert(data_type == FC_DATA_TYPE)

    metadata = generate_dataset_metadata(x=CELL_X, y=CELL_Y, acq_dt=ACQ_LS5,
                                         dataset=dataset, bands=None,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)

    raster_create_geotiff(filename, [data[b] for b in dataset.bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=metadata, band_ids=[b.name for b in dataset.bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.LS5
@pytest.mark.WOFS
def test_retrieve_data_ls5_wofs(config=None):

    filename = "LS5_TM_WATER_{x:03d}_{y:04d}_{date}.{x_offset:04d}_{y_offset:04d}.{x_size:04d}x{y_size:04d}.tif".format(x=CELL_X, y=CELL_Y, date=DATE, x_offset=X_OFFSET, y_offset=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    tiles = list_tiles_as_list(x=[CELL_X], y=[CELL_Y],
                               acq_min=ACQ_LS5, acq_max=ACQ_LS5,
                               satellites=[Satellite.LS5],
                               dataset_types=[WOFS_DATASET_TYPE],
                               config=config)

    assert len(tiles) == 1

    dataset = tiles[0].datasets[WOFS_DATASET_TYPE]

    data = get_dataset_data(dataset=dataset, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    assert(data)
    _log.info("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert(is_ndv(ndv, WOFS_NDV))

    data_type = get_dataset_datatype(dataset)
    assert(data_type == WOFS_DATA_TYPE)

    metadata = generate_dataset_metadata(x=CELL_X, y=CELL_Y, acq_dt=ACQ_LS5,
                                         dataset=dataset, bands=None,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)

    raster_create_geotiff(filename, [data[b] for b in dataset.bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=metadata, band_ids=[b.name for b in dataset.bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.LS5
@pytest.mark.NDVI
def test_retrieve_data_ls5_ndvi(config=None):

    filename = "LS5_TM_NDVI_{x:03d}_{y:04d}_{date}.{x_offset:04d}_{y_offset:04d}.{x_size:04d}x{y_size:04d}.tif".format(x=CELL_X, y=CELL_Y, date=DATE, x_offset=X_OFFSET, y_offset=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    tiles = list_tiles_as_list(x=[CELL_X], y=[CELL_Y],
                               acq_min=ACQ_LS5, acq_max=ACQ_LS5,
                               satellites=[Satellite.LS5],
                               dataset_types=[NDVI_DATASET_TYPE],
                               config=config)

    assert len(tiles) == 1

    dataset = tiles[0].datasets[NDVI_DATASET_TYPE]

    data = get_dataset_data(dataset=dataset, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    assert(data)
    _log.info("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert(is_ndv(ndv, NDVI_NDV))

    data_type = get_dataset_datatype(dataset)
    assert(data_type == NDVI_DATA_TYPE)

    metadata = generate_dataset_metadata(x=CELL_X, y=CELL_Y, acq_dt=ACQ_LS5,
                                         dataset=dataset, bands=None,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)

    raster_create_geotiff(filename, [data[b] for b in dataset.bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=metadata, band_ids=[b.name for b in dataset.bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.LS5
@pytest.mark.TCI
def test_retrieve_data_ls5_tci(config=None):

    filename = "LS5_TM_TCI_{x:03d}_{y:04d}_{date}.{x_offset:04d}_{y_offset:04d}.{x_size:04d}x{y_size:04d}.tif".format(x=CELL_X, y=CELL_Y, date=DATE, x_offset=X_OFFSET, y_offset=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    tiles = list_tiles_as_list(x=[CELL_X], y=[CELL_Y],
                               acq_min=ACQ_LS5, acq_max=ACQ_LS5,
                               satellites=[Satellite.LS5],
                               dataset_types=[TCI_DATASET_TYPE],
                               config=config)

    assert len(tiles) == 1

    dataset = tiles[0].datasets[TCI_DATASET_TYPE]

    data = get_dataset_data(dataset=dataset, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    assert(data)
    _log.info("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert(is_ndv(ndv, TCI_NDV))

    data_type = get_dataset_datatype(dataset)
    assert(data_type == TCI_DATA_TYPE)

    metadata = generate_dataset_metadata(x=CELL_X, y=CELL_Y, acq_dt=ACQ_LS5,
                                         dataset=dataset, bands=None,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)

    raster_create_geotiff(filename, [data[b] for b in dataset.bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=metadata, band_ids=[b.name for b in dataset.bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.LS5
@pytest.mark.NDWI
def test_retrieve_data_ls5_ndwi(config=None):

    filename = "LS5_TM_NDWI_{x:03d}_{y:04d}_{date}.{x_offset:04d}_{y_offset:04d}.{x_size:04d}x{y_size:04d}.tif".format(x=CELL_X, y=CELL_Y, date=DATE, x_offset=X_OFFSET, y_offset=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    tiles = list_tiles_as_list(x=[CELL_X], y=[CELL_Y],
                               acq_min=ACQ_LS5, acq_max=ACQ_LS5,
                               satellites=[Satellite.LS5],
                               dataset_types=[NDWI_DATASET_TYPE],
                               config=config)

    assert len(tiles) == 1

    dataset = tiles[0].datasets[NDWI_DATASET_TYPE]

    data = get_dataset_data(dataset=dataset, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    assert(data)
    _log.info("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert(is_ndv(ndv, NDWI_NDV))

    data_type = get_dataset_datatype(dataset)
    assert(data_type == NDWI_DATA_TYPE)

    metadata = generate_dataset_metadata(x=CELL_X, y=CELL_Y, acq_dt=ACQ_LS5,
                                         dataset=dataset, bands=None,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)

    raster_create_geotiff(filename, [data[b] for b in dataset.bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=metadata, band_ids=[b.name for b in dataset.bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.LS5
@pytest.mark.MNDWI
def test_retrieve_data_ls5_mndwi(config=None):

    filename = "LS5_TM_MNDWI_{x:03d}_{y:04d}_{date}.{x_offset:04d}_{y_offset:04d}.{x_size:04d}x{y_size:04d}.tif".format(x=CELL_X, y=CELL_Y, date=DATE, x_offset=X_OFFSET, y_offset=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    tiles = list_tiles_as_list(x=[CELL_X], y=[CELL_Y],
                               acq_min=ACQ_LS5, acq_max=ACQ_LS5,
                               satellites=[Satellite.LS5],
                               dataset_types=[MNDWI_DATASET_TYPE],
                               config=config)

    assert len(tiles) == 1

    dataset = tiles[0].datasets[MNDWI_DATASET_TYPE]

    data = get_dataset_data(dataset=dataset, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    assert(data)
    _log.info("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert(is_ndv(ndv, MNDWI_NDV))

    data_type = get_dataset_datatype(dataset)
    assert(data_type == MNDWI_DATA_TYPE)

    metadata = generate_dataset_metadata(x=CELL_X, y=CELL_Y, acq_dt=ACQ_LS5,
                                         dataset=dataset, bands=None,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)

    raster_create_geotiff(filename, [data[b] for b in dataset.bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=metadata, band_ids=[b.name for b in dataset.bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.LS5
@pytest.mark.ARG
@pytest.mark.PQ
def test_retrieve_data_ls5_arg_with_pqa(config=None):

    filename = "LS5_TM_NBAR_WITH_PQA_{x:03d}_{y:04d}_{date}.{x_offset:04d}_{y_offset:04d}.{x_size:04d}x{y_size:04d}.tif".format(x=CELL_X, y=CELL_Y, date=DATE, x_offset=X_OFFSET, y_offset=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    tiles = list_tiles_as_list(x=[CELL_X], y=[CELL_Y],
                               acq_min=ACQ_LS5, acq_max=ACQ_LS5,
                               satellites=[Satellite.LS5],
                               dataset_types=[ARG_DATASET_TYPE, PQ_DATASET_TYPE],
                               config=config)

    assert len(tiles) == 1

    tile = tiles[0]

    assert ARG_DATASET_TYPE in tile.datasets
    dataset = tile.datasets[ARG_DATASET_TYPE]

    assert PQ_DATASET_TYPE in tile.datasets
    pqa = tile.datasets[PQ_DATASET_TYPE]

    data = get_dataset_data_with_pq(dataset=dataset, dataset_pqa=pqa, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    assert(data)
    _log.info("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert(is_ndv(ndv, ARG_NDV))

    data_type = get_dataset_datatype(dataset)
    assert(data_type == ARG_DATA_TYPE)

    metadata = generate_dataset_metadata(x=CELL_X, y=CELL_Y, acq_dt=ACQ_LS5,
                                         dataset=dataset, bands=None,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)

    raster_create_geotiff(filename, [data[b] for b in dataset.bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=metadata, band_ids=[b.name for b in dataset.bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.LS5
@pytest.mark.ARG
@pytest.mark.PQ
def test_retrieve_data_ls5_arg_with_pqa_mask(config=None):

    filename = "LS5_TM_NBAR_WITH_PQA_2_{x:03d}_{y:04d}_{date}.{x_offset:04d}_{y_offset:04d}.{x_size:04d}x{y_size:04d}.tif".format(x=CELL_X, y=CELL_Y, date=DATE, x_offset=X_OFFSET, y_offset=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    tiles = list_tiles_as_list(x=[CELL_X], y=[CELL_Y],
                               acq_min=ACQ_LS5, acq_max=ACQ_LS5,
                               satellites=[Satellite.LS5],
                               dataset_types=[ARG_DATASET_TYPE, PQ_DATASET_TYPE],
                               config=config)

    assert len(tiles) == 1

    tile = tiles[0]

    assert ARG_DATASET_TYPE in tile.datasets
    dataset = tile.datasets[ARG_DATASET_TYPE]

    assert PQ_DATASET_TYPE in tile.datasets
    pqa = tile.datasets[PQ_DATASET_TYPE]

    mask = get_mask_pqa(pqa, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    data = get_dataset_data_masked(dataset=dataset, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE, mask=mask)

    assert(data)
    _log.info("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert(is_ndv(ndv, ARG_NDV))

    data_type = get_dataset_datatype(dataset)
    assert(data_type == ARG_DATA_TYPE)

    metadata = generate_dataset_metadata(x=CELL_X, y=CELL_Y, acq_dt=ACQ_LS5,
                                         dataset=dataset, bands=None,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)

    raster_create_geotiff(filename, [data[b] for b in dataset.bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=metadata, band_ids=[b.name for b in dataset.bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.LS5
@pytest.mark.ARG
@pytest.mark.PQ
@pytest.mark.WATER
def test_retrieve_data_ls5_arg_with_pqa_water_mask_wet(config=None):

    filename = "LS5_TM_NBAR_WITH_PQA_WATER_WET_{x:03d}_{y:04d}_{date}.{x_offset:04d}_{y_offset:04d}.{x_size:04d}x{y_size:04d}.tif".format(x=CELL_X, y=CELL_Y, date=DATE, x_offset=X_OFFSET, y_offset=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    tiles = list_tiles_as_list(x=[CELL_X], y=[CELL_Y],
                               acq_min=ACQ_LS5, acq_max=ACQ_LS5,
                               satellites=[Satellite.LS5],
                               dataset_types=[ARG_DATASET_TYPE, PQ_DATASET_TYPE, WOFS_DATASET_TYPE],
                               config=config)

    assert len(tiles) == 1

    tile = tiles[0]

    assert ARG_DATASET_TYPE in tile.datasets
    dataset = tile.datasets[ARG_DATASET_TYPE]

    assert PQ_DATASET_TYPE in tile.datasets
    pqa = tile.datasets[PQ_DATASET_TYPE]

    assert WOFS_DATASET_TYPE in tile.datasets
    wofs = tile.datasets[WOFS_DATASET_TYPE]

    mask = get_mask_pqa(pqa, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)
    mask = get_mask_wofs(wofs, wofs_masks=[WofsMask.WET], x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE, mask=mask)

    data = get_dataset_data_masked(dataset=dataset, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE, mask=mask)

    assert(data)
    _log.info("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert(is_ndv(ndv, ARG_NDV))

    data_type = get_dataset_datatype(dataset)
    assert(data_type == ARG_DATA_TYPE)

    metadata = generate_dataset_metadata(x=CELL_X, y=CELL_Y, acq_dt=ACQ_LS5,
                                         dataset=dataset, bands=None,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)

    raster_create_geotiff(filename, [data[b] for b in dataset.bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=metadata, band_ids=[b.name for b in dataset.bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.LS5
@pytest.mark.ARG
@pytest.mark.PQ
@pytest.mark.WATER
def test_retrieve_data_ls5_arg_with_pqa_water_mask_dry(config=None):

    filename = "LS5_TM_NBAR_WITH_PQA_WATER_DRY_{x:03d}_{y:04d}_{date}.{x_offset:04d}_{y_offset:04d}.{x_size:04d}x{y_size:04d}.tif".format(x=CELL_X, y=CELL_Y, date=DATE, x_offset=X_OFFSET, y_offset=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    tiles = list_tiles_as_list(x=[CELL_X], y=[CELL_Y],
                               acq_min=ACQ_LS5, acq_max=ACQ_LS5,
                               satellites=[Satellite.LS5],
                               dataset_types=[ARG_DATASET_TYPE, PQ_DATASET_TYPE, WOFS_DATASET_TYPE],
                               config=config)

    assert len(tiles) == 1

    tile = tiles[0]

    assert ARG_DATASET_TYPE in tile.datasets
    dataset = tile.datasets[ARG_DATASET_TYPE]

    assert PQ_DATASET_TYPE in tile.datasets
    pqa = tile.datasets[PQ_DATASET_TYPE]

    assert WOFS_DATASET_TYPE in tile.datasets
    wofs = tile.datasets[WOFS_DATASET_TYPE]

    mask = get_mask_pqa(pqa, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)
    mask = get_mask_wofs(wofs, wofs_masks=[WofsMask.DRY, WofsMask.NO_DATA, WofsMask.SATURATION_CONTIGUITY,
                                           WofsMask.SEA_WATER, WofsMask.TERRAIN_SHADOW, WofsMask.HIGH_SLOPE,
                                           WofsMask.CLOUD_SHADOW, WofsMask.CLOUD],
                         x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE, mask=mask)

    data = get_dataset_data_masked(dataset=dataset, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE, mask=mask)

    assert(data)
    _log.info("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert(is_ndv(ndv, ARG_NDV))

    data_type = get_dataset_datatype(dataset)
    assert(data_type == ARG_DATA_TYPE)

    metadata = generate_dataset_metadata(x=CELL_X, y=CELL_Y, acq_dt=ACQ_LS5,
                                         dataset=dataset, bands=None,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)

    raster_create_geotiff(filename, [data[b] for b in dataset.bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=metadata, band_ids=[b.name for b in dataset.bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))