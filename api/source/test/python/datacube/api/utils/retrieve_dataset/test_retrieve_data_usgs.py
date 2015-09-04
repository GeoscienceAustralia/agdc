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


import logging
import filecmp
import gdalconst
import numpy
from pathlib import Path
from datacube.api.model import Cell, Satellite, DatasetType, Ls8UsgsSrBands
from datacube.api.query import list_tiles_as_list
from datacube.api.utils import get_dataset_data, raster_create_geotiff, get_dataset_ndv, is_ndv, get_dataset_datatype
from datacube.api.utils import generate_dataset_metadata
from datetime import date

__author__ = "Simon Oldfield"


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


_log = logging.getLogger()

CELL = Cell(20, 10)

# ACQ = date(2013, 4, 19)
ACQ = date(2013, 5, 5)

CELL_GEO_TRANSFORM = (-2265600.0, 30.0, 0.0, 3164800.0, 0.0, -30.0)
CELL_PROJECTION = 'PROJCS["Albers",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",29.5],PARAMETER["standard_parallel_2",45.5],PARAMETER["latitude_of_center",23],PARAMETER["longitude_of_center",-96],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]'


flatten = lambda *n: (e for a in n for e in (flatten(*a) if isinstance(a, (tuple, list)) else (a,)))


def get_test_data_path(f=None):
    path = Path(__file__).parent.absolute() / "data"

    if f:
        for x in flatten(f):
            path = path / x

    return str(path)


def test_retrieve_data_ls8_sr(config=None):

    # filename = "LC8_SR_{x:03d}_{y:04d}_{date}.{x_offset:04d}_{y_offset:04d}.{x_size:04d}x{y_size:04d}.tif".format(x=CELL.x, y=CELL.Y, date=ACQ, x_offset=X_OFFSET, y_offset=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)

    filename = "LC8_SR_{x:04d}_{y:04d}_{date}.tif".format(x=CELL.x, y=CELL.y, date=ACQ)
    _log.info("filename=[%s]", filename)

    tiles = list_tiles_as_list(x=[CELL.x], y=[CELL.y],
                               acq_min=ACQ, acq_max=ACQ,
                               satellites=[Satellite.LS8],
                               dataset_types=[DatasetType.USGSSR],
                               config=config)

    assert len(tiles) == 1

    tile = tiles[0]
    assert tile

    dataset = tile.datasets[DatasetType.USGSSR]
    assert dataset

    _log.info("x=%d y=%d acq=%s satellite=%s", tile.x, tile.y, tile.end_datetime, dataset.satellite)

    bands = [Ls8UsgsSrBands.RED, Ls8UsgsSrBands.GREEN, Ls8UsgsSrBands.BLUE]

    # TODO sensor agnostic bands
    # data = get_dataset_data(dataset=dataset, x=X_OFFSET, y=Y_OFFSET, x_size=X_SIZE, y_size=Y_SIZE)
    data = get_dataset_data(dataset=dataset, bands=bands)

    assert(data)
    _log.info("data is [%s]\n%s", numpy.shape(data), data)

    ndv = get_dataset_ndv(dataset)
    assert(is_ndv(ndv, -9999))

    data_type = get_dataset_datatype(dataset)
    assert(data_type == gdalconst.GDT_Int16)

    metadata = generate_dataset_metadata(x=CELL.x, y=CELL.y, acq_dt=ACQ,
                                         dataset=dataset, bands=bands,
                                         mask_pqa_apply=False, mask_pqa_mask=None,
                                         mask_wofs_apply=False, mask_wofs_mask=None)
    assert metadata

    raster_create_geotiff(filename, [data[b] for b in bands], CELL_GEO_TRANSFORM, CELL_PROJECTION, ndv, data_type,
                          dataset_metadata=None, band_ids=[b.name for b in bands])

    assert filecmp.cmp(filename, get_test_data_path(filename))


