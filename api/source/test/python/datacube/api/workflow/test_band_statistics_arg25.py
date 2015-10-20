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
import pytest
from datetime import date
from datacube.api import parse_date_min, parse_date_max, Season, Satellite, DatasetType, PqaMask, Statistic
from datacube.api.model import Ls57Arg25Bands
from datacube.api.workflow.band_statistics_arg25 import Arg25BandStatisticsWorkflow, EpochParameter, SEASONS
from itertools import product


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


_log = logging.getLogger()


TEST_X = 137
TEST_Y = -28


def test_create_tasks():
    
    TILE_COUNTS = {
        1985: {Season.SUMMER: 34, Season.AUTUMN: 29, Season.WINTER: 36, Season.SPRING: 34},
        1990: {Season.SUMMER: 53, Season.AUTUMN: 65, Season.WINTER: 65, Season.SPRING: 57}
    }

    workflow = Arg25BandStatisticsWorkflow()
    
    workflow.x_min = workflow.x_max = TEST_X
    workflow.y_min = workflow.y_max = TEST_Y

    workflow.acq_min = parse_date_min("1985")
    workflow.acq_max = parse_date_max("1994")

    workflow.epoch = EpochParameter(5, 5)

    workflow.seasons = Season
    # workflow.seasons = [Season.SPRING]

    workflow.satellites = [Satellite.LS5, Satellite.LS7]

    workflow.output_directory = "/tmp"

    workflow.mask_pqa_apply = True
    workflow.mask_pqa_mask = [PqaMask.PQ_MASK_SATURATION, PqaMask.PQ_MASK_CONTIGUITY, PqaMask.PQ_MASK_CLOUD]

    # workflow.local_scheduler = None
    # workflow.workers = None

    workflow.dataset_type = DatasetType.ARG25
    workflow.bands = Ls57Arg25Bands

    workflow.x_chunk_size = 4000
    workflow.y_chunk_size = 4000

    workflow.statistics = [Statistic.PERCENTILE_25, Statistic.PERCENTILE_50, Statistic.PERCENTILE_75]

    from luigi.task import flatten

    tasks = flatten(workflow.create_tasks())

    assert(len(tasks) == len(workflow.seasons) * len(TILE_COUNTS))

    for task in tasks:
        _log.info("task = %s", task)

        for output in flatten(task.output()):
            _log.info("output %s", output.path)

        chunk_tasks = flatten(task.requires())

        assert(len(chunk_tasks) == len(Ls57Arg25Bands))

        for chunk_task in chunk_tasks:
            _log.info("chunk task %s", chunk_task)

            for output in flatten(chunk_task.output()):
                _log.info("output %s", output.path)

            tiles = list(chunk_task.get_tiles())

            _log.info("Found %d tiles", len(tiles))

            assert (len(tiles) == TILE_COUNTS[chunk_task.acq_min.year][chunk_task.season])

            for tile in tiles:
                _log.info("\t%s", tile.end_datetime)


@pytest.mark.fred
def test_create_tasks_1985_2014_6_year_epoch():

    TILE_COUNTS = {
        1985: {Season.SUMMER: 48, Season.AUTUMN: 29, Season.WINTER: 36, Season.SPRING: 34},
        1990: {Season.SUMMER: 67, Season.AUTUMN: 65, Season.WINTER: 65, Season.SPRING: 57},
        1995: {Season.SUMMER: 77, Season.AUTUMN: 65, Season.WINTER: 65, Season.SPRING: 57},
        2000: {Season.SUMMER: 148, Season.AUTUMN: 65, Season.WINTER: 65, Season.SPRING: 57},
        2005: {Season.SUMMER: 166, Season.AUTUMN: 65, Season.WINTER: 65, Season.SPRING: 57},
        2010: {Season.SUMMER: 83, Season.AUTUMN: 65, Season.WINTER: 65, Season.SPRING: 57}
    }

    workflow = Arg25BandStatisticsWorkflow()

    workflow.x_min = workflow.x_max = TEST_X
    workflow.y_min = workflow.y_max = TEST_Y

    workflow.acq_min = parse_date_min("1985")
    workflow.acq_max = parse_date_max("2014")

    workflow.epoch = EpochParameter(5, 6)

    # workflow.seasons = Season
    workflow.seasons = [Season.SUMMER]

    workflow.satellites = [Satellite.LS5, Satellite.LS7]

    workflow.output_directory = "/tmp"

    workflow.mask_pqa_apply = False
    # workflow.mask_pqa_apply = True
    workflow.mask_pqa_mask = [PqaMask.PQ_MASK_SATURATION, PqaMask.PQ_MASK_CONTIGUITY, PqaMask.PQ_MASK_CLOUD]

    workflow.dataset_type = DatasetType.ARG25
    workflow.bands = Ls57Arg25Bands

    workflow.x_chunk_size = 4000
    workflow.y_chunk_size = 4000

    workflow.statistics = [Statistic.PERCENTILE_25, Statistic.PERCENTILE_50, Statistic.PERCENTILE_75]

    from luigi.task import flatten

    tasks = flatten(workflow.create_tasks())

    assert(len(tasks) == len(workflow.seasons) * len(TILE_COUNTS))

    for task in tasks:
        _log.info("task = %s", task)

        for output in flatten(task.output()):
            _log.info("output %s", output.path)

        chunk_tasks = flatten(task.requires())

        assert(len(chunk_tasks) == len(Ls57Arg25Bands))

        for chunk_task in chunk_tasks:
            _log.info("chunk task %s", chunk_task)

            for output in flatten(chunk_task.output()):
                _log.info("output %s", output.path)

            tiles = list(chunk_task.get_tiles())

            _log.info("Found %d tiles", len(tiles))

            assert (len(tiles) == TILE_COUNTS[chunk_task.acq_min.year][chunk_task.season])

            for tile in tiles:
                _log.info("\t%s", tile.end_datetime)


@pytest.mark.epoch
def test_get_epochs_5_5():

    workflow = Arg25BandStatisticsWorkflow()

    workflow.x_min = workflow.x_max = TEST_X
    workflow.y_min = workflow.y_max = TEST_Y

    workflow.acq_min = parse_date_min("1985")
    workflow.acq_max = parse_date_max("2014")

    workflow.epoch = EpochParameter(5, 5)

    workflow.seasons = Season

    epochs = list(workflow.get_epochs())

    EXPECTED_EPOCHS = [
        (date(1985, 1, 1), date(1989, 12, 31)),
        (date(1990, 1, 1), date(1994, 12, 31)),
        (date(1995, 1, 1), date(1999, 12, 31)),
        (date(2000, 1, 1), date(2004, 12, 31)),
        (date(2005, 1, 1), date(2009, 12, 31)),
        (date(2010, 1, 1), date(2014, 12, 31))
    ]

    assert epochs == EXPECTED_EPOCHS


@pytest.mark.epoch
def test_get_epochs_5_6():

    workflow = Arg25BandStatisticsWorkflow()

    workflow.x_min = workflow.x_max = TEST_X
    workflow.y_min = workflow.y_max = TEST_Y

    workflow.acq_min = parse_date_min("1985")
    workflow.acq_max = parse_date_max("2014")

    workflow.epoch = EpochParameter(5, 6)

    workflow.seasons = Season

    epochs = list(workflow.get_epochs())

    EXPECTED_EPOCHS = [
        (date(1985, 1, 1), date(1990, 12, 31)),
        (date(1990, 1, 1), date(1995, 12, 31)),
        (date(1995, 1, 1), date(2000, 12, 31)),
        (date(2000, 1, 1), date(2005, 12, 31)),
        (date(2005, 1, 1), date(2010, 12, 31)),
        (date(2010, 1, 1), date(2014, 12, 31))
    ]

    assert epochs == EXPECTED_EPOCHS


@pytest.mark.george
def test_query():

    workflow = Arg25BandStatisticsWorkflow()

    workflow.x_min = workflow.x_max = TEST_X
    workflow.y_min = workflow.y_max = TEST_Y

    workflow.acq_min = parse_date_min("1985")
    workflow.acq_max = parse_date_max("2014")

    workflow.epoch = EpochParameter(5, 6)

    workflow.seasons = Season
    workflow.seasons = [Season.SUMMER]

    workflow.satellites = [Satellite.LS5, Satellite.LS7]

    workflow.mask_pqa_apply = True
    workflow.mask_pqa_mask = [PqaMask.PQ_MASK_SATURATION, PqaMask.PQ_MASK_CONTIGUITY, PqaMask.PQ_MASK_CLOUD]

    workflow.dataset_type = DatasetType.ARG25
    workflow.bands = Ls57Arg25Bands

    epochs = list(workflow.get_epochs())

    print ""

    print "epochs are", epochs

    for season, epoch in product(workflow.seasons, epochs):
        print season, epoch

        from datacube.api.utils import build_season_date_criteria
        acq_min, acq_max, criteria = build_season_date_criteria(epoch[0], epoch[1], season, seasons=SEASONS, extend=True)

        print acq_min, acq_max, criteria

        from datacube.api.query import list_tiles_as_list
        tiles = list_tiles_as_list(x=[workflow.x_min], y=[workflow.y_min], satellites=workflow.satellites,
                                        acq_min=acq_min, acq_max=acq_max,
                                        dataset_types=[workflow.dataset_type], include=criteria)

        print "Tiles found is ", len(tiles)
