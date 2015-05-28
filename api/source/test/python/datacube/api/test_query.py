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


__author__ = "Simon Oldfield"


import logging
from datacube.api import parse_date_min, parse_date_max, Satellite, DatasetType
from datacube.api.query import list_cells_as_list, list_tiles_as_list
from datacube.api.query import list_cells_vector_file_as_list
from datacube.api.query import MONTHS_BY_SEASON, Season
from datacube.api.query import LS7_SLC_OFF_EXCLUSION, LS7_SLC_OFF_ACQ_MIN
from datacube.api.query import LS8_PRE_WRS_2_EXCLUSION, LS8_PRE_WRS_2_ACQ_MAX


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')


_log = logging.getLogger()


TEST_CELL_X = 120
TEST_CELL_Y = -25
TEST_YEAR = 2005
TEST_YEAR_STR = str(TEST_YEAR)
TEST_MONTHS = MONTHS_BY_SEASON[Season.SUMMER]

TEST_VECTOR_FILE = "Mainlands.shp"
TEST_VECTOR_LAYER = 0
TEST_VECTOR_FEATURE = 4


def test_list_cells_120_020_2005_ls578(config=None):

    cells = list_cells_as_list(x=[TEST_CELL_X], y=[TEST_CELL_Y],
                               acq_min=parse_date_min(TEST_YEAR_STR), acq_max=parse_date_max(TEST_YEAR_STR),
                               satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                               dataset_types=[DatasetType.ARG25],
                               config=config)

    assert(cells and len(list(cells)) > 0)

    for cell in cells:
        _log.info("Found cell xy = %s", cell.xy)
        assert(cell.x == TEST_CELL_X and cell.y == TEST_CELL_Y and cell.xy == (TEST_CELL_X, TEST_CELL_Y))


def test_list_cells_120_020_2005_ls578_no_ls7_slc(config=None):

    cells = list_cells_as_list(x=[TEST_CELL_X], y=[TEST_CELL_Y],
                               acq_min=parse_date_min(TEST_YEAR_STR),
                               acq_max=parse_date_max(TEST_YEAR_STR),
                               satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                               dataset_types=[DatasetType.ARG25],
                               exclude=[LS7_SLC_OFF_EXCLUSION],
                               config=config)

    assert(cells and len(list(cells)) > 0)

    for cell in cells:
        _log.info("Found cell xy = %s", cell.xy)
        assert(cell.x == TEST_CELL_X and cell.y == TEST_CELL_Y and cell.xy == (TEST_CELL_X, TEST_CELL_Y))


def test_list_cells_120_020_2005_ls578_no_ls8_pre_wrs_2(config=None):

    cells = list_cells_as_list(x=[TEST_CELL_X], y=[TEST_CELL_Y],
                               acq_min=parse_date_min(TEST_YEAR_STR), acq_max=parse_date_max(TEST_YEAR_STR),
                               satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                               dataset_types=[DatasetType.ARG25],
                               exclude=[LS8_PRE_WRS_2_EXCLUSION],
                               config=config)

    assert(cells and len(list(cells)) > 0)

    for cell in cells:
        _log.info("Found cell xy = %s", cell.xy)
        assert(cell.x == TEST_CELL_X and cell.y == TEST_CELL_Y and cell.xy == (TEST_CELL_X, TEST_CELL_Y))


def test_list_cells_120_020_2005_ls578_summer(config=None):

    cells = list_cells_as_list(x=[TEST_CELL_X], y=[TEST_CELL_Y],
                               acq_min=parse_date_min(TEST_YEAR_STR), acq_max=parse_date_max(TEST_YEAR_STR),
                               satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                               dataset_types=[DatasetType.ARG25],
                               months=TEST_MONTHS,
                               config=config)

    assert(cells and len(list(cells)) > 0)

    for cell in cells:
        _log.info("Found cell xy = %s", cell.xy)
        assert(cell.x == TEST_CELL_X and cell.y == TEST_CELL_Y and cell.xy == (TEST_CELL_X, TEST_CELL_Y))


def test_list_tiles_120_020_2005_ls578(config=None):

    dataset_types = [DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25]

    tiles = list_tiles_as_list(x=[TEST_CELL_X], y=[TEST_CELL_Y],
                               acq_min=parse_date_min(TEST_YEAR_STR), acq_max=parse_date_max(TEST_YEAR_STR),
                               satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                               dataset_types=dataset_types,
                               config=config)

    assert(tiles and len(list(tiles)) > 0)

    for tile in tiles:
        _log.info("Found tile xy = %s", tile.xy)
        assert(tile.x == TEST_CELL_X and tile.y == TEST_CELL_Y and tile.xy == (TEST_CELL_X, TEST_CELL_Y)
               and tile.end_datetime_year == TEST_YEAR
               and ds in tile.datasets for ds in dataset_types)


def test_list_tiles_120_020_2005_ls578_no_ls7_slc(config=None):

    dataset_types = [DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25]

    tiles = list_tiles_as_list(x=[TEST_CELL_X], y=[TEST_CELL_Y],
                               acq_min=parse_date_min(TEST_YEAR_STR),
                               acq_max=parse_date_max(TEST_YEAR_STR),
                               satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                               dataset_types=dataset_types,
                               exclude=[LS7_SLC_OFF_EXCLUSION],
                               config=config)

    assert(tiles and len(list(tiles)) > 0)

    for tile in tiles:
        _log.info("Found tile xy = %s", tile.xy)

        dataset = tile.datasets[DatasetType.ARG25]
        assert dataset
        _log.info("Found ARG25 dataset [%s]", dataset.path)

        assert(tile.x == TEST_CELL_X and tile.y == TEST_CELL_Y and tile.xy == (TEST_CELL_X, TEST_CELL_Y)
               and tile.end_datetime_year == TEST_YEAR
               and (ds in tile.datasets for ds in dataset_types)
               and (dataset.satellite != Satellite.LS7 or tile.end_datetime.date() <= LS7_SLC_OFF_ACQ_MIN))


def test_list_tiles_120_020_2005_ls578_no_ls8_pre_wrs_2(config=None):

    dataset_types = [DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25]

    tiles = list_tiles_as_list(x=[TEST_CELL_X], y=[TEST_CELL_Y],
                               acq_min=parse_date_min(TEST_YEAR_STR),
                               acq_max=parse_date_max(TEST_YEAR_STR),
                               satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                               dataset_types=dataset_types,
                               exclude=[LS8_PRE_WRS_2_EXCLUSION],
                               config=config)

    assert(tiles and len(list(tiles)) > 0)

    for tile in tiles:
        _log.info("Found tile xy = %s", tile.xy)

        dataset = tile.datasets[DatasetType.ARG25]
        assert dataset
        _log.info("Found ARG25 dataset [%s]", dataset.path)

        assert(tile.x == TEST_CELL_X and tile.y == TEST_CELL_Y and tile.xy == (TEST_CELL_X, TEST_CELL_Y)
               and tile.end_datetime_year == TEST_YEAR
               and (ds in tile.datasets for ds in dataset_types)
               and (dataset.satellite != Satellite.LS8 or tile.end_datetime.date() >= LS8_PRE_WRS_2_ACQ_MAX))


def test_list_tiles_120_020_2005_ls578_summer(config=None):

    dataset_types = [DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25]

    tiles = list_tiles_as_list(x=[TEST_CELL_X], y=[TEST_CELL_Y],
                               acq_min=parse_date_min(TEST_YEAR_STR),
                               acq_max=parse_date_max(TEST_YEAR_STR),
                               satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                               dataset_types=dataset_types,
                               months=TEST_MONTHS,
                               config=config)

    assert(tiles and len(list(tiles)) > 0)

    for tile in tiles:
        _log.info("Found tile xy = %s", tile.xy)
        assert(tile.x == TEST_CELL_X and tile.y == TEST_CELL_Y and tile.xy == (TEST_CELL_X, TEST_CELL_Y)
               and tile.end_datetime_year == TEST_YEAR
               and (ds in tile.datasets for ds in dataset_types)
               and tile.end_datetime_month in [m.value for m in TEST_MONTHS])


# AOI

def test_list_cells_act_2005_ls578(config=None):
    cells = list_cells_vector_file_as_list(vector_file=TEST_VECTOR_FILE,
                                           vector_layer=TEST_VECTOR_LAYER,
                                           vector_feature=TEST_VECTOR_FEATURE,
                                           satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                                           acq_min=parse_date_min(TEST_YEAR_STR),
                                           acq_max=parse_date_max(TEST_YEAR_STR),
                                           dataset_types=[DatasetType.ARG25], config=None)

    assert(cells and len(list(cells)) == 2)

    for cell in cells:
        _log.info("Found cell xy = %s", cell.xy)
        assert((cell.x == 148 or cell.x == 149) and cell.y == -36)


# def test_list_tiles_act_2005_ls578(config=None):
#
#     dataset_types = [DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25]
#
#     tiles = list_tiles_vector_file_as_list(vector_file="Mainlands.shp", vector_layer=0, vector_feature=4,
#                                            acq_min=parse_date_min("2005"), acq_max=parse_date_max("2005"),
#                                            satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
#                                            dataset_types=dataset_types,
#                                            config=config)
#
#     assert(tiles and len(list(tiles)) > 0)
#
#     for tile in tiles:
#         _log.info("Found tile xy = %s", tile.xy)
#         assert((tile.x == 148 or tile.x == 149) and tile.y == -36
#                and tile.end_datetime_year == 2005
#                and (ds in tile.datasets for ds in dataset_types)
#                and tile.end_datetime_month in [m.value for m in MONTHS_BY_SEASON[Season.SUMMER]])


# def test_list_tiles_act_2005_ls578_summer(config=None):
#
#     dataset_types = [DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25]
#
#     tiles = list_tiles_vector_file_as_list(vector_file="Mainlands.shp", vector_layer=0, vector_feature=4,
#                                            acq_min=parse_date_min("2005"), acq_max=parse_date_max("2005"),
#                                            satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
#                                            dataset_types=dataset_types,
#                                            months=MONTHS_BY_SEASON[Season.SUMMER],
#                                            config=config)
#
#     assert(tiles and len(list(tiles)) > 0)
#
#     for tile in tiles:
#         _log.info("Found tile xy = %s", tile.xy)
#         assert((tile.x == 148 or tile.x == 149) and tile.y == -36
#                and tile.end_datetime_year == 2005
#                and (ds in tile.datasets for ds in dataset_types)
#                and tile.end_datetime_month in [m.value for m in MONTHS_BY_SEASON[Season.SUMMER]])

