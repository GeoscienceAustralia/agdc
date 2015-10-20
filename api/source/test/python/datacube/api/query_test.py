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


import logging
import os
import csv
from datacube.api.model import DatasetType, Tile, Cell, Satellite
from datacube.api.query import list_tiles, list_tiles_wkt, list_tiles_to_file, list_cells, list_cells_to_file
from datacube.api.query import MONTHS_BY_SEASON, Season
from datacube.api.query import LS7_SLC_OFF_EXCLUSION, LS8_PRE_WRS_2_EXCLUSION
from datacube.api.query import list_cells_missing, list_cells_missing_to_file, list_tiles_dtm
from datacube.api.workflow import parse_date_min, parse_date_max
from datetime import date


_log = logging.getLogger()


def main():

    do_list_cells_by_xy_single()

    # do_list_cells_by_xy_single_no_ls7_slc_off()
    # do_list_cells_by_xy_single_no_ls7_slc_off_no_ls8_pre_wrs_2()

    do_list_cells_by_xy_single_months()

    # do_list_cells_by_xy_single_csv()

    # do_list_cells_missing_by_xy_single()
    # do_list_cells_missing_by_xy_single_csv()

    # do_list_tiles_by_xy_single()

    # do_list_tiles_by_xy_single_no_ls7_slc_off()
    # do_list_tiles_by_xy_single_no_ls7_slc_off_no_ls8_wrs2()

    # do_list_tiles_by_xy_single_months()

    # do_list_tiles_dtm_by_xy_single(config)

    # do_list_cells_by_xy_multiple(config)
    # do_list_tiles_by_xy_multiple(config)

    # do_list_tiles_by_shape_wkt(config)

    # do_list_tiles_by_xy_single_date_range(config)

    # do_list_tiles_by_xy_single_csv(config)

# Records DB -> model classes


def do_list_cells_by_xy_single(config=None):
    cells = list_cells(x=[120], y=[-25], acq_min=parse_date_min("2005"), acq_max=parse_date_max("2005"),
                       satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                       dataset_types=[DatasetType.ARG25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED, DatasetType.DEM_SMOOTHED],
                       config=config)

    for cell in cells:
        _log.info("Found cell xy = %s", cell.xy)


def do_list_cells_by_xy_single_no_ls7_slc_off(config=None):
    cells = list_cells(x=[120], y=[-25], acq_min=parse_date_min("2005"), acq_max=parse_date_max("2005"),
                       satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                       dataset_types=[DatasetType.ARG25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED, DatasetType.DEM_SMOOTHED],
                       exclude=[LS7_SLC_OFF_EXCLUSION],
                       config=config)

    for cell in cells:
        _log.info("Found cell xy = %s", cell.xy)


def do_list_cells_by_xy_single_no_ls7_slc_off_no_ls8_pre_wrs_2(config=None):
    cells = list_cells(x=[120], y=[-25], acq_min=parse_date_min("2005"), acq_max=parse_date_max("2005"),
                       satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                       dataset_types=[DatasetType.ARG25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED, DatasetType.DEM_SMOOTHED],
                       exclude=[LS7_SLC_OFF_EXCLUSION, LS8_PRE_WRS_2_EXCLUSION],
                       config=config)

    for cell in cells:
        _log.info("Found cell xy = %s", cell.xy)


def do_list_cells_by_xy_single_months(config=None):
    cells = list_cells(x=[120], y=[-25], acq_min=parse_date_min("2005"), acq_max=parse_date_max("2005"),
                       satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                       dataset_types=[DatasetType.ARG25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED, DatasetType.DEM_SMOOTHED],
                       months=MONTHS_BY_SEASON[Season.SUMMER],
                       config=config)

    for cell in cells:
        _log.info("Found cell xy = %s", cell.xy)


def do_list_cells_by_xy_single_csv(config=None):

    filename = os.path.expandvars("$HOME/tmp/cube/cells.csv")

    list_cells_to_file(x=[123], y=[-25], acq_min=parse_date_min("2014-01"), acq_max=parse_date_max("2014-01"),
                       satellites=[Satellite.LS7],
                       dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                       filename=filename,
                       config=config)

    cells = []

    with open(filename, "rb") as f:
        reader = csv.DictReader(f)
        for record in reader:
            _log.debug("Found CSV record [%s]", record)
            cells.append(Cell.from_csv_record(record))

    for cell in cells:
        _log.debug("Found cell xy = %s", cell.xy)


def do_list_cells_missing_by_xy_single(config=None):
    cells = list_cells_missing(x=range(110, 155+1), y=range(-45, -10+1),
                               acq_min=parse_date_min("1980"), acq_max=parse_date_max("2020"),
                               satellites=[Satellite.LS7],
                               # dataset_types=[DatasetType.FC25],
                               # dataset_types=[DatasetType.PQ25],
                               # dataset_types=[DatasetType.FC25, DatasetType.PQ25],
                               dataset_types=[DatasetType.FC25, DatasetType.PQ25, DatasetType.DSM],
                               # dataset_types=[DatasetType.ARG25, DatasetType.PQ25],
                               # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                               # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM],
                               # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM],
                               # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED],
                               # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED, DatasetType.DEM_SMOOTHED],
                               config=config)

    for cell in cells:
        _log.debug("Found cell xy = %s", cell.xy)


def do_list_cells_missing_by_xy_single_csv(config=None):

    filename = os.path.expandvars("$HOME/tmp/cube/cells_missing.csv")

    list_cells_missing_to_file(x=range(110, 155+1), y=range(-45, -10+1),
                               acq_min=parse_date_min("1980"), acq_max=parse_date_max("2020"),
                               satellites=[Satellite.LS7],
                               # dataset_types=[DatasetType.FC25],
                               # dataset_types=[DatasetType.PQ25],
                               # dataset_types=[DatasetType.FC25, DatasetType.PQ25],
                               dataset_types=[DatasetType.FC25, DatasetType.PQ25, DatasetType.DSM],
                               # dataset_types=[DatasetType.ARG25, DatasetType.PQ25],
                               # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                               # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM],
                               # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM],
                               # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED],
                               # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DSM, DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED, DatasetType.DEM_SMOOTHED],
                               filename=filename,
                               config=config)

    cells = []

    with open(filename, "rb") as f:
        reader = csv.DictReader(f)
        for record in reader:
            _log.debug("Found CSV record [%s]", record)
            cells.append(Cell.from_csv_record(record))

    for cell in cells:
        _log.debug("Found cell xy = %s", cell.xy)


def do_list_tiles_by_xy_single():
    _log.info("Testing list_tiles...")
    tiles = list_tiles(x=[120], y=[-20], acq_min=date(2005, 1, 1), acq_max=date(2005, 12, 31),
                       satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                       dataset_types=[DatasetType.ARG25])
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25])
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DEM, DatasetType.NDVI])

    count = {Satellite.LS5: 0, Satellite.LS7: 0, Satellite.LS8: 0}

    for tile in tiles:
        _log.info("Found tile xy = %s acq date = [%s] NBAR = [%s]", tile.xy, tile.end_datetime, tile.datasets[DatasetType.ARG25].path)
        count[tile.datasets[DatasetType.ARG25].satellite] += 1

    _log.info(count)


def do_list_tiles_by_xy_single_no_ls7_slc_off():
    _log.info("Testing list_tiles NO LS7 SLC OFF...")

    from datetime import date

    tiles = list_tiles(x=[120], y=[-20], acq_min=date(2005, 1, 1), acq_max=date(2005, 12, 31),
                       satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                       dataset_types=[DatasetType.ARG25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DEM, DatasetType.NDVI],
                       exclude=[LS7_SLC_OFF_EXCLUSION])

    count = {Satellite.LS5: 0, Satellite.LS7: 0, Satellite.LS8: 0}

    for tile in tiles:
        _log.debug("Found tile xy = %s acq date = [%s] NBAR = [%s]", tile.xy, tile.end_datetime, tile.datasets[DatasetType.ARG25].path)
        count[tile.datasets[DatasetType.ARG25].satellite] += 1

    _log.info(count)


def do_list_tiles_by_xy_single_no_ls7_slc_off_no_ls8_wrs2():
    _log.info("Testing list_tiles NO LS7 SLC OFF...")

    from datetime import date

    exclude = [LS7_SLC_OFF_EXCLUSION, LS8_PRE_WRS_2_EXCLUSION]

    tiles = list_tiles(x=[120], y=[-20], acq_min=date(2005, 1, 1), acq_max=date(2005, 12, 31),
                       satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                       dataset_types=[DatasetType.ARG25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DEM, DatasetType.NDVI],
                       exclude=exclude)

    count = {Satellite.LS5: 0, Satellite.LS7: 0, Satellite.LS8: 0}

    for tile in tiles:
        _log.debug("Found tile xy = %s acq date = [%s] NBAR = [%s]", tile.xy, tile.end_datetime, tile.datasets[DatasetType.ARG25].path)
        count[tile.datasets[DatasetType.ARG25].satellite] += 1

    _log.info(count)


def do_list_tiles_by_xy_single_months():
    _log.info("Testing list_tiles MONTHS...")
    tiles = list_tiles(x=[120], y=[-20], acq_min=date(2005, 1, 1), acq_max=date(2005, 12, 31),
                       satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8],
                       dataset_types=[DatasetType.ARG25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                       # dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.DEM, DatasetType.NDVI],
                       months=MONTHS_BY_SEASON[Season.SUMMER])

    count = {Satellite.LS5: 0, Satellite.LS7: 0, Satellite.LS8: 0}

    for tile in tiles:
        _log.info("Found tile xy = %s acq date = [%s] NBAR = [%s]", tile.xy, tile.end_datetime, tile.datasets[DatasetType.ARG25].path)
        count[tile.datasets[DatasetType.ARG25].satellite] += 1

    _log.info(count)


def do_list_tiles_by_xy_single_to_file(config):
    _log.info("Testing list_tiles...")
    tiles = list_tiles(x=[123], y=[-25], acq_min=date(2002, 1, 1), acq_max=date(2002 ,12, 31),
                       satellites=[Satellite.LS7],
                       dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                       database=config.get_db_database(), user=config.get_db_username(),
                       password=config.get_db_password(),
                       host=config.get_db_host(), port=config.get_db_port())

    for tile in tiles:
        _log.info("Found tile xy = %s acq date = [%s] NBAR = [%s]", tile.xy, tile.end_datetime, tile.datasets[DatasetType.ARG25].path)


def do_list_tiles_dtm_by_xy_single(config):
    _log.info("Testing list_tiles_dtm...")
    tiles = list_tiles_dtm(x=[123], y=[-25],
                           datasets=[DatasetType.DSM, DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED,
                                     DatasetType.DEM_SMOOTHED],
                           database=config.get_db_database(), user=config.get_db_username(),
                           password=config.get_db_password(),
                           host=config.get_db_host(), port=config.get_db_port())

    for tile in tiles:
        _log.info("Found tile xy = %s acq date = [%s] NBAR = [%s]", tile.xy, tile.end_datetime, tile.datasets[DatasetType.DSM].path)


def do_list_tiles_by_xy_multiple(config):
    tiles = list_tiles(x=[124, 125], y=[-25, -24], years=[2002], satellites=["LS7"],
                       dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                       database=config.get_db_database(), user=config.get_db_username(),
                       password=config.get_db_password(),
                       host=config.get_db_host(), port=config.get_db_port())

    for tile in tiles:
        _log.debug("Found tile xy = %s acq date = [%s]", tile.xy, tile.end_datetime)


def do_list_cells_by_xy_multiple(config):
    cells = list_cells(x=[124, 125], y=[-25, -24], years=[2002], satellites=["LS7"],
                       dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                       database=config.get_db_database(), user=config.get_db_username(),
                       password=config.get_db_password(),
                       host=config.get_db_host(), port=config.get_db_port())

    for cell in cells:
        _log.debug("Found cell xy = %s", cell.xy)


def do_list_tiles_by_shape_wkt(config):
    tiles = list_tiles_wkt(wkt="POLYGON((123.5 -24,124.5 -23.5,125.5 -24,124.5 -24.5,123.5 -24))", years=[2002],
                           satellites=["LS7"],
                           datasets=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                           database=config.get_db_database(), user=config.get_db_username(),
                           password=config.get_db_password(),
                           host=config.get_db_host(), port=config.get_db_port())

    for tile in tiles:
        _log.debug("Found tile xy = %s acq date = [%s]", tile.xy, tile.end_datetime)


# Records DB -> CSV -> model classes

def do_list_tiles_by_xy_single_csv(config):

    filename = "/tmp/tiles.csv"

    list_tiles_to_file(x=[123], y=[-25], years=[2002], satellites=["LS7"],
                       dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                       filename=filename,
                       database=config.get_db_database(), user=config.get_db_username(),
                       password=config.get_db_password(),
                       host=config.get_db_host(), port=config.get_db_port())

    tiles = []

    with open(filename, "rb") as f:
        reader = csv.DictReader(f)
        for record in reader:
            _log.debug("Found CSV record [%s]", record)
            tiles.append(Tile.from_csv_record(record))

    for tile in tiles:
        _log.debug("Found tile xy = %s acq date = [%s] NBAR = %s", tile.xy, tile.end_datetime, tile.datasets[DatasetType.ARG25].path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
    main()
