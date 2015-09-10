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


import filecmp
import logging
from datetime import date
import gdalconst
import numpy
from pathlib import Path
from datacube.api import Satellite, DatasetType, TileType
from datacube.api.model import Cell, Ls8UsgsSrBandBands, Ls8UsgsSrAttrBands
from datacube.api.query import list_cells_to_file, list_tiles_to_file, list_tiles_as_list
from datacube.api.utils import latlon_to_cell, latlon_to_xy, get_dataset_data, get_dataset_ndv, is_ndv
from datacube.api.utils import get_dataset_datatype, generate_dataset_metadata, raster_create_geotiff, get_mask_ls8_cloud_qa
from datacube.api.utils import get_dataset_data_masked


__author__ = "Simon Oldfield"

_log = logging.getLogger()


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

MIN_X = 20
MAX_X = 28

MIN_Y = 10
MAX_Y = 18

MIN_ACQ = date(2013, 1, 1)
MAX_ACQ = date(2013, 12, 31)

SATELLITE_LS578 = [Satellite.LS5, Satellite.LS7, Satellite.LS8]

DATASET_TYPE_SR = [DatasetType.USGS_SR_BAND, DatasetType.USGS_SR_ATTR]


flatten = lambda *n: (e for a in n for e in (flatten(*a) if isinstance(a, (tuple, list)) else (a,)))


def get_test_data_path(f=None):
    path = Path(__file__).parent.absolute() / "data"

    if f:
        for x in flatten(f):
            path = path / x

    return str(path)


def test_list_cells_ls578(config=None):
    filename = "usgs_cells_ls578.csv"

    list_cells_to_file(x=range(MIN_X, MAX_X + 1), y=range(MIN_Y, MAX_Y + 1),
                       acq_min=MIN_ACQ, acq_max=MAX_ACQ,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_SR,
                       filename=filename,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


def test_list_tiles_ls578(config=None):
    filename = "usgs_tiles_ls578.csv"

    list_tiles_to_file(x=range(MIN_X, MAX_X + 1), y=range(MIN_Y, MAX_Y + 1),
                       acq_min=MIN_ACQ, acq_max=MAX_ACQ,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_SR,
                       filename=filename,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


def test_lat_lon_to_cell_usgs():
    # CENTER of 20/10
    # -2250600.000, 3149800.000 -> -126.46197152702, 48.0708631399742
    assert latlon_to_cell(lon=-126.46197152702, lat=48.0708631399742, tile_type=TileType.USGS) == (20, 10)

    # CENTER of 20/12
    # -2250600, 3119800 -> -126.336712108846, 47.8126707927157
    assert latlon_to_cell(lon=-126.336712108846, lat=47.8126707927157, tile_type=TileType.USGS) == (20, 12)

    # CENTER of 22/22
    # -2220600, 2969800 -> -125.35370519859 46.6063235836554
    assert latlon_to_cell(lon=-125.35370519859, lat=46.6063235836554, tile_type=TileType.USGS) == (22, 22)


def test_lat_lon_to_x_y_usgs():
    assert latlon_to_xy(lon=-126.5, lat=48.0, transform=(-2265600.0, 30.0, 0.0, 3164800.0, 0.0, -30.0), tile_type=TileType.USGS) == (327, 717)


CELL = Cell(20, 10)

# ACQ = date(2013, 4, 19)
ACQ = date(2013, 5, 5)

CELL_GEO_TRANSFORM = (-2265600.0, 30.0, 0.0, 3164800.0, 0.0, -30.0)
CELL_PROJECTION = 'PROJCS["Albers",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",29.5],PARAMETER["standard_parallel_2",45.5],PARAMETER["latitude_of_center",23],PARAMETER["longitude_of_center",-96],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]'


def test_retrieve_data_ls8_sr(config=None):

    filename = "LC8_SR_{x:04d}_{y:04d}_{date}.tif".format(x=CELL.x, y=CELL.y, date=ACQ)
    _log.info("filename=[%s]", filename)

    tiles = list_tiles_as_list(x=[CELL.x], y=[CELL.y],
                               acq_min=ACQ, acq_max=ACQ,
                               satellites=[Satellite.LS8],
                               dataset_types=[DatasetType.USGS_SR_BAND],
                               config=config)

    assert len(tiles) == 1

    tile = tiles[0]
    assert tile

    dataset = tile.datasets[DatasetType.USGS_SR_BAND]
    assert dataset

    _log.info("x=%d y=%d acq=%s satellite=%s", tile.x, tile.y, tile.end_datetime, dataset.satellite)
    _log.info("dataset path=%s", dataset.path)

    bands = [Ls8UsgsSrBandBands.RED, Ls8UsgsSrBandBands.GREEN, Ls8UsgsSrBandBands.BLUE]

    # TODO sensor agnostic bands
    # data = get_dataset_data(dataset=dataset, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)
    data = get_dataset_data(dataset=dataset, bands=bands)

    assert data
    _log.debug("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert is_ndv(ndv, -9999)

    data_type = get_dataset_datatype(dataset)
    assert data_type == gdalconst.GDT_Int16

    metadata = generate_dataset_metadata(x=CELL.x, y=CELL.y, acq_dt=ACQ,
                                         dataset=dataset, bands=bands,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)
    assert metadata

    raster_create_geotiff(filename, [data[b] for b in bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=None, band_ids=[b.name for b in bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))


def test_retrieve_data_ls8_sr_cloud_qa(config=None):

    filename = "LC8_SR_CLOUD_QA_{x:04d}_{y:04d}_{date}.tif".format(x=CELL.x, y=CELL.y, date=ACQ)
    _log.info("filename=[%s]", filename)

    tiles = list_tiles_as_list(x=[CELL.x], y=[CELL.y],
                               acq_min=ACQ, acq_max=ACQ,
                               satellites=[Satellite.LS8],
                               dataset_types=[DatasetType.USGS_SR_ATTR],
                               config=config)

    assert len(tiles) == 1

    tile = tiles[0]
    assert tile

    dataset = tile.datasets[DatasetType.USGS_SR_ATTR]
    assert dataset

    _log.info("x=%d y=%d acq=%s satellite=%s", tile.x, tile.y, tile.end_datetime, dataset.satellite)
    _log.info("dataset path=%s", dataset.path)

    bands = [Ls8UsgsSrAttrBands.CLOUD]

    # TODO sensor agnostic bands
    # data = get_dataset_data(dataset=dataset, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)
    data = get_dataset_data(dataset=dataset, bands=bands)

    assert data
    _log.debug("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert is_ndv(ndv, 0)

    data_type = get_dataset_datatype(dataset)
    assert data_type == gdalconst.GDT_Byte

    metadata = generate_dataset_metadata(x=CELL.x, y=CELL.y, acq_dt=ACQ,
                                         dataset=dataset, bands=bands,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)
    assert metadata

    raster_create_geotiff(filename, [data[b] for b in bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=None, band_ids=[b.name for b in bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))


# TODO this is a Cloud QA that doesn't have any cloud...find one that does!

def test_retrieve_data_ls8_sr_with_cloud_qa(config=None):

    filename = "LC8_SR_WITH_CLOUD_QA_{x:04d}_{y:04d}_{date}.tif".format(x=CELL.x, y=CELL.y, date=ACQ)
    _log.info("filename=[%s]", filename)

    tiles = list_tiles_as_list(x=[CELL.x], y=[CELL.y],
                               acq_min=ACQ, acq_max=ACQ,
                               satellites=[Satellite.LS8],
                               dataset_types=[DatasetType.USGS_SR_BAND, DatasetType.USGS_SR_ATTR],
                               config=config)

    assert len(tiles) == 1

    tile = tiles[0]
    assert tile

    cloud_qa = tile.datasets[DatasetType.USGS_SR_ATTR]
    assert cloud_qa

    cloud_qa_mask = get_mask_ls8_cloud_qa(cloud_qa)
    _log.info(numpy.unique(cloud_qa_mask, return_counts=True))

    # assert cloud_qa_mask

    dataset = tile.datasets[DatasetType.USGS_SR_BAND]
    assert dataset

    _log.info("x=%d y=%d acq=%s satellite=%s", tile.x, tile.y, tile.end_datetime, dataset.satellite)
    _log.info("dataset path=%s", dataset.path)

    bands = [Ls8UsgsSrBandBands.RED, Ls8UsgsSrBandBands.GREEN, Ls8UsgsSrBandBands.BLUE]

    # TODO sensor agnostic bands
    # data = get_dataset_data(dataset=dataset, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)
    data = get_dataset_data_masked(dataset=dataset, bands=bands, mask=cloud_qa_mask)

    assert data
    _log.debug("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert is_ndv(ndv, -9999)

    data_type = get_dataset_datatype(dataset)
    assert data_type == gdalconst.GDT_Int16

    metadata = generate_dataset_metadata(x=CELL.x, y=CELL.y, acq_dt=ACQ,
                                         dataset=dataset, bands=bands,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)
    assert metadata

    raster_create_geotiff(filename, [data[b] for b in bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=None, band_ids=[b.name for b in bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))