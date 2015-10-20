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
#===============================================================================

__author__ = "Simon Oldfield"


from datacube.api.model import Ls57Arg25Bands, DatasetTile
from datacube.api.utils import raster_get_band_data_with_pq, get_dataset_metadata, raster_create, raster_get_band_data, \
    calculate_ndvi, calculate_ndvi_with_pq, latlon_to_xy, latlon_to_cell
import logging
import gdal
import numpy


def main():
    logging.basicConfig(level=logging.DEBUG)

    test_apply_pq()
    test_calculate_ndvi()
    test_calculate_ndvi_with_pq()

def test_apply_pq():

    nbar = DatasetTile(satellite_id="LS5", type_id="ARG25", path="/data/tmp/cube/data/from.calum/LS5_TM_NBAR_150_-034_2004-01-13T23-22-17.088044.tif")
    pq = DatasetTile(satellite_id="LS5", type_id="PQ25", path="/data/tmp/cube/data/from.calum/LS5_TM_PQA_150_-034_2004-01-13T23-22-17.088044.tif")

    metadata = get_dataset_metadata(nbar)

    band_data = raster_get_band_data_with_pq(nbar, Ls57Arg25Bands, pq)

    raster_create("/data/tmp/cube/data/from.calum/LS5_TM_NBAR_PQD_150_-034_2004-01-13T23-22-17.088044.tif",
                  [band_data[b].filled(-999) for b in Ls57Arg25Bands],
                  metadata.transform, metadata.projection, -999, gdal.GDT_Int16)


def test_calculate_ndvi():

    nbar = DatasetTile(satellite_id="LS5", type_id="ARG25", path="/data/tmp/cube/data/from.calum/LS5_TM_NBAR_150_-034_2004-01-13T23-22-17.088044.tif")

    metadata = get_dataset_metadata(nbar)

    band_data = calculate_ndvi(nbar)

    raster_create("/data/tmp/cube/data/unit_test/LS5_TM_NDVI_150_-034_2004-01-13T23-22-17.088044.tif",
                  [band_data.filled(numpy.NaN)],
                  metadata.transform, metadata.projection, numpy.NaN, gdal.GDT_Float32)


def test_calculate_ndvi_with_pq():

    nbar = DatasetTile(satellite_id="LS5", type_id="ARG25", path="/data/tmp/cube/data/from.calum/LS5_TM_NBAR_150_-034_2004-01-13T23-22-17.088044.tif")
    pq = DatasetTile(satellite_id="LS5", type_id="PQ25", path="/data/tmp/cube/data/from.calum/LS5_TM_PQA_150_-034_2004-01-13T23-22-17.088044.tif")

    metadata = get_dataset_metadata(nbar)

    band_data = calculate_ndvi_with_pq(nbar, pq)

    raster_create("/data/tmp/cube/data/unit_test/LS5_TM_NDVI_PQ_150_-034_2004-01-13T23-22-17.088044.tif",
                  [band_data.filled(numpy.NaN)],
                  metadata.transform, metadata.projection, numpy.NaN, gdal.GDT_Float32)


def test_latlon_to_xy():
    # This equates to the 120/-20 cell
    transform = (120.0, 0.00025, 0.0, -19.0, 0.0, -0.00025)

    # TODO

    # Expected outputs:
    #
    #   (120.00000, -20.00000) -> (   0, 4000) which is actually outside the TIF!!!
    #
    #   (120.25000, -19.25000) -> (1000, 1000)
    #   (120.50000, -19.50000) -> (2000, 2000)
    #
    #   (120.00024, -19.00024) -> (   0,    0)
    #   (120.00025, -19.00025) -> (   1,    1)
    #   (120.00026, -19.00026) -> (   1,    1)

    # Should return
    latlon_to_xy(120, -20, transform)


def test_latlon_to_cell():

    # TODO

    # Expected outputs:
    #
    # (120, -19) -> (120, -20)
    # (120.1, -19.1) -> (120, -20)
    # (120.9, -19.9) -> (120, -20)
    # (120, -20) -> (120, -21)


    # Should return
    latlon_to_cell(120, -20)


if __name__ == "__main__":
    main()
