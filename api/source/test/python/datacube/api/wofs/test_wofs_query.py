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
import logging
import pytest
from datacube.api import parse_date_min, parse_date_max, Satellite, DatasetType
from datacube.api.query import list_cells_to_file, list_tiles_to_file, list_cells_vector_file_to_file
from datacube.api.utils import build_season_date_criteria
from datacube.api.utils import Season, Month
from datacube.api.utils import LS7_SLC_OFF_EXCLUSION, LS8_PRE_WRS_2_EXCLUSION
from datetime import date
from pathlib import Path


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')


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

YEAR_2005 = 2005

ACQ_MIN_2005 = date(YEAR_2005, 1, 1)
ACQ_MAX_2005 = date(YEAR_2005, 12, 31)

YEAR_2000 = 2000
YEAR_2010 = 2010

ACQ_MIN_2000 = date(YEAR_2000, 1, 1)
ACQ_MAX_2010 = date(YEAR_2010, 12, 31)

YEAR_2014 = 2014

ACQ_MIN_2014 = date(YEAR_2014, 1, 1)
ACQ_MAX_2014 = date(YEAR_2014, 12, 31)

VECTOR_FILE_STATES = get_test_data_path_common("Mainlands.shp")
VECTOR_LAYER_STATES = 0
VECTOR_FEATURE_STATES = 4

DATASET_TYPE_WOFS = [DatasetType.WATER]
# DATASET_TYPE_ARG25_FC25_PQ25 = [DatasetType.ARG25, DatasetType.FC25, DatasetType.PQ25]

SATELLITE_LS578 = [Satellite.LS5, Satellite.LS7, Satellite.LS8]

SATELLITE_LS5 = [Satellite.LS5]
SATELLITE_LS7 = [Satellite.LS7]
SATELLITE_LS8 = [Satellite.LS8]

MIN_X = 110
MAX_X = 155

MIN_Y = -45
MAX_Y = -10

MIN_ACQ_MIN = date(1985, 1, 1)
MAX_ACQ_MAX = date(2015, 6, 30)

SEASONS_ARG25_STATS = {
    Season.SUMMER: ((Month.NOVEMBER, 17), (Month.APRIL, 25)),
    Season.AUTUMN: ((Month.FEBRUARY, 16), (Month.JULY, 25)),
    Season.WINTER: ((Month.MAY, 17), (Month.OCTOBER, 25)),
    Season.SPRING: ((Month.AUGUST, 17), (Month.JANUARY, 25))
}


@pytest.mark.medium
@pytest.mark.cells
def test_list_cells_ls578(config=None):

    filename = "cells_ls578_wofs.csv"

    list_cells_to_file(x=range(MIN_X, MAX_X + 1), y=range(MIN_Y, MAX_Y + 1),
                       acq_min=MIN_ACQ_MIN, acq_max=MAX_ACQ_MAX,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.cells
def test_list_cells_120_020_2005_ls578(config=None):

    filename = "cells_120_020_2005_ls578_wofs.csv"

    list_cells_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=ACQ_MIN_2005, acq_max=ACQ_MAX_2005,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.cells
def test_list_cells_120_020_2005_ls5(config=None):

    filename = "cells_120_020_2005_ls5_wofs.csv"

    list_cells_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=ACQ_MIN_2005, acq_max=ACQ_MAX_2005,
                       satellites=SATELLITE_LS5,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.cells
def test_list_cells_120_020_2005_ls7(config=None):

    filename = "cells_120_020_2005_ls7_wofs.csv"

    list_cells_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=ACQ_MIN_2005, acq_max=ACQ_MAX_2005,
                       satellites=SATELLITE_LS7,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.cells
def test_list_cells_120_020_2005_ls8(config=None):

    filename = "cells_120_020_2005_ls8_wofs.csv"

    list_cells_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=ACQ_MIN_2005, acq_max=ACQ_MAX_2005,
                       satellites=SATELLITE_LS8,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.cells
def test_list_cells_120_020_2014_ls578(config=None):

    filename = "cells_120_020_2014_ls578_wofs.csv"

    list_cells_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=ACQ_MIN_2014, acq_max=ACQ_MAX_2014,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.cells
def test_list_cells_120_020_2014_ls5(config=None):

    filename = "cells_120_020_2014_ls5_wofs.csv"

    list_cells_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=ACQ_MIN_2014, acq_max=ACQ_MAX_2014,
                       satellites=SATELLITE_LS5,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.cells
def test_list_cells_120_020_2014_ls7(config=None):

    filename = "cells_120_020_2014_ls7_wofs.csv"

    list_cells_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=ACQ_MIN_2014, acq_max=ACQ_MAX_2014,
                       satellites=SATELLITE_LS7,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.cells
def test_list_cells_120_020_2014_ls8(config=None):

    filename = "cells_120_020_2014_ls8_wofs.csv"

    list_cells_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=ACQ_MIN_2014, acq_max=ACQ_MAX_2014,
                       satellites=SATELLITE_LS8,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.cells
def test_list_cells_120_020_2005_ls578_no_ls7_slc(config=None):

    filename = "cells_120_020_2005_ls578_wofs_no_ls7_slc.csv"

    list_cells_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=ACQ_MIN_2005, acq_max=ACQ_MAX_2005,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       exclude=[LS7_SLC_OFF_EXCLUSION],
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


def test_list_cells_120_020_2005_ls578_no_ls8_pre_wrs_2(config=None):

    filename = "cells_120_020_2005_ls578_wofs_no_ls8_pre_wrs_2.csv"

    list_cells_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=ACQ_MIN_2005, acq_max=ACQ_MAX_2005,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       exclude=[LS8_PRE_WRS_2_EXCLUSION],
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.cells
@pytest.mark.season
def test_list_cells_120_020_2005_ls578_summer(config=None):

    filename = "cells_120_020_2005_ls578_wofs_summer.csv"

    acq_min, acq_max, include = build_season_date_criteria(ACQ_MIN_2005, ACQ_MAX_2005, Season.SUMMER)

    list_cells_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=acq_min, acq_max=acq_max,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       include=include,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.cells
@pytest.mark.season
def test_list_cells_120_020_2000_2010_ls578_summer(config=None):

    filename = "cells_120_020_2000_2010_ls578_wofs_summer.csv"

    acq_min = parse_date_min("2000")
    acq_max = parse_date_max("2010")

    acq_min, acq_max, include = build_season_date_criteria(acq_min, acq_max, Season.SUMMER)

    list_cells_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=acq_min, acq_max=acq_max,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       include=include,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.cells
@pytest.mark.season
def test_list_cells_120_020_2000_2010_ls578_summer_arg25_stats(config=None):

    filename = "cells_120_020_2000_2010_ls578_wofs_summer_arg25_stats.csv"

    acq_min = parse_date_min("2000")
    acq_max = parse_date_max("2010")

    acq_min, acq_max, include = build_season_date_criteria(acq_min, acq_max, Season.SUMMER, seasons=SEASONS_ARG25_STATS)

    list_cells_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=acq_min, acq_max=acq_max,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       include=include,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.tiles
def test_list_tiles_120_020_2005_ls578(config=None):

    filename = "tiles_120_020_2005_ls578_wofs.csv"

    list_tiles_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=ACQ_MIN_2005, acq_max=ACQ_MAX_2005,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.tiles
def test_list_tiles_120_020_2005_ls578_no_ls7_slc(config=None):

    filename = "tiles_120_020_2005_ls578_wofs_no_ls7_slc.csv"

    list_tiles_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=ACQ_MIN_2005, acq_max=ACQ_MAX_2005,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       exclude=[LS7_SLC_OFF_EXCLUSION],
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.tiles
def test_list_tiles_120_020_2005_ls578_no_ls8_pre_wrs_2(config=None):

    filename = "tiles_120_020_2005_ls578_wofs_no_ls8_pre_wrs_2.csv"

    list_tiles_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=ACQ_MIN_2005, acq_max=ACQ_MAX_2005,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       exclude=[LS8_PRE_WRS_2_EXCLUSION],
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.tiles
@pytest.mark.season
def test_list_tiles_120_020_2005_ls578_summer(config=None):

    filename = "tiles_120_020_2005_ls578_wofs_summer.csv"

    acq_min, acq_max, include = build_season_date_criteria(ACQ_MIN_2005, ACQ_MAX_2005, Season.SUMMER)

    list_tiles_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=acq_min,
                       acq_max=acq_max,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       include=include,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.tiles
@pytest.mark.season
def test_list_tiles_120_020_2000_2010_ls578_wofs_summer(config=None):

    filename = "tiles_120_020_2000_2010_ls578_summer.csv"

    acq_min, acq_max, include = build_season_date_criteria(ACQ_MIN_2000, ACQ_MAX_2010, Season.SUMMER)

    list_tiles_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=acq_min,
                       acq_max=acq_max,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       include=include,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


@pytest.mark.quick
@pytest.mark.tiles
@pytest.mark.season
def test_list_tiles_120_020_2000_2010_ls578_summer_arg25_stats(config=None):

    filename = "tiles_120_020_2000_2010_ls578_wofs_summer_arg25_stats.csv"

    acq_min, acq_max, include = build_season_date_criteria(ACQ_MIN_2000, ACQ_MAX_2010, Season.SUMMER, seasons=SEASONS_ARG25_STATS)

    list_tiles_to_file(x=[CELL_X], y=[CELL_Y],
                       acq_min=acq_min,
                       acq_max=acq_max,
                       satellites=SATELLITE_LS578,
                       dataset_types=DATASET_TYPE_WOFS,
                       filename=filename,
                       include=include,
                       config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))


# AOI

@pytest.mark.quick
@pytest.mark.tiles
@pytest.mark.aoi
def test_list_cells_act_2005_ls578(config=None):

    filename = "cells_act_2005_ls578_wofs.csv"

    list_cells_vector_file_to_file(vector_file=VECTOR_FILE_STATES,
                                   vector_layer=VECTOR_LAYER_STATES,
                                   vector_feature=VECTOR_FEATURE_STATES,
                                   satellites=SATELLITE_LS578,
                                   acq_min=ACQ_MIN_2005, acq_max=ACQ_MAX_2005,
                                   dataset_types=DATASET_TYPE_WOFS,
                                   filename=filename,
                                   config=config)

    assert filecmp.cmp(filename, get_test_data_path(filename))
