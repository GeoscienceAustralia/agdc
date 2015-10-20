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
from datetime import date
import os
from datacube.api import parse_date_min, parse_date_max, Season, Satellite, PqaMask, DatasetType, Statistic
from datacube.api.model import Ls57Arg25Bands
from datacube.api.workflow.band_statistics_arg25 import Arg25BandStatisticsWorkflow, EpochParameter

__author__ = "Simon Oldfield"

import logging

_log = logging.getLogger()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Change from acq min/max to epoch min/max so would request 1985 to 2010 to get 1985-1990, 1990-1995, ..., 2010-2015


def get_workflow_1985_2014():

    workflow = Arg25BandStatisticsWorkflow()

    workflow.x_min = workflow.x_max = 129
    workflow.y_min = workflow.y_max = -26

    workflow.acq_min = parse_date_min("1985")
    workflow.acq_max = parse_date_max("2014")

    workflow.epoch = EpochParameter(5, 6)

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

    return workflow


def test_get_epochs_1985_2014():

    epochs = list(get_workflow_1985_2014().get_epochs())

    _log.info("epochs = %s", epochs)

    expected_epochs = [
        (date(1985, 1, 1), date(1990, 12, 31)),
        (date(1990, 1, 1), date(1995, 12, 31)),
        (date(1995, 1, 1), date(2000, 12, 31)),
        (date(2000, 1, 1), date(2005, 12, 31)),
        (date(2005, 1, 1), date(2010, 12, 31)),
        (date(2010, 1, 1), date(2014, 12, 31))
    ]

    assert epochs == expected_epochs


def test_get_seasons_1985_2014():

    seasons = list(get_workflow_1985_2014().get_seasons())

    _log.info("seasons = %s", seasons)

    expected_seasons = [s for s in Season]

    assert seasons == expected_seasons


def test_create_tasks_1985_2014():

    tasks = list(get_workflow_1985_2014().create_tasks())

    _log.info("tasks = %s", tasks)

    for task in tasks:
        _log.info("output=[%s]", os.path.basename(task.output().path))


def get_workflow_1990_1995():

    workflow = Arg25BandStatisticsWorkflow()

    workflow.x_min = workflow.x_max = 129
    workflow.y_min = workflow.y_max = -26

    workflow.acq_min = parse_date_min("1990")
    workflow.acq_max = parse_date_max("1995")

    workflow.epoch = EpochParameter(5, 6)

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

    return workflow


def test_get_epochs_1990_1995():

    epochs = list(get_workflow_1990_1995().get_epochs())

    _log.info("epochs = %s", epochs)

    expected_epochs = [
        (date(1990, 1, 1), date(1995, 12, 31)),
    ]

    assert epochs == expected_epochs


def test_get_seasons_1990_1995():

    seasons = list(get_workflow_1990_1995().get_seasons())

    _log.info("seasons = %s", seasons)

    expected_seasons = [s for s in Season]

    assert seasons == expected_seasons


def test_create_tasks_1990_1995():

    tasks = list(get_workflow_1990_1995().create_tasks())

    _log.info("tasks = %s", tasks)

    for task in tasks:
        _log.info("output=[%s]", os.path.basename(task.output().path))
