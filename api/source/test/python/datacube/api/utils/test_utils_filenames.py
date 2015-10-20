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
from datacube.api import OutputFormat, Satellite, DatasetType, parse_date_min, parse_date_max
from datacube.api.model import Ls57Arg25Bands
from datacube.api.tool import SeasonParameter
from datacube.api.utils import get_dataset_band_stack_filename, Season, SEASONS, Month

_log = logging.getLogger()


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')


def test_get_band_stack_filename():

    assert get_dataset_band_stack_filename(
        satellites=[Satellite.LS5], dataset_type=DatasetType.ARG25,
        band=Ls57Arg25Bands.BLUE, x=120, y=-20, acq_min=parse_date_min("2000"), acq_max=parse_date_max("2005"),
        mask_pqa_apply=False, mask_wofs_apply=False, mask_vector_apply=False,
        output_format=OutputFormat.GEOTIFF) == "LS5_NBAR_120_-020_2000_01_01_2005_12_31_BLUE_STACK.tif"

    assert get_dataset_band_stack_filename(
        satellites=[Satellite.LS5, Satellite.LS7], dataset_type=DatasetType.ARG25,
        band=Ls57Arg25Bands.BLUE, x=120, y=-20, acq_min=parse_date_min("2000"), acq_max=parse_date_max("2005"),
        mask_pqa_apply=False, mask_wofs_apply=False, mask_vector_apply=False,
        output_format=OutputFormat.GEOTIFF) == "LS57_NBAR_120_-020_2000_01_01_2005_12_31_BLUE_STACK.tif"

    assert get_dataset_band_stack_filename(
        satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8], dataset_type=DatasetType.ARG25,
        band=Ls57Arg25Bands.BLUE, x=120, y=-20, acq_min=parse_date_min("2000"), acq_max=parse_date_max("2005"),
        mask_pqa_apply=False, mask_wofs_apply=False, mask_vector_apply=False,
        output_format=OutputFormat.GEOTIFF) == "LS578_NBAR_120_-020_2000_01_01_2005_12_31_BLUE_STACK.tif"


def test_get_band_stack_filename_seasons_bom():

    season = Season.SUMMER
    season_start = SEASONS[season][0]
    season_end = SEASONS[season][1]

    season = SeasonParameter(season.name,
                             (season_start[0], season_start[1]),
                             (season_end[0], season_end[1]))

    assert get_dataset_band_stack_filename(
        satellites=[Satellite.LS5], dataset_type=DatasetType.ARG25,
        band=Ls57Arg25Bands.BLUE, x=120, y=-20,
        acq_min=parse_date_min("2000"), acq_max=parse_date_max("2005"), season=season,
        mask_pqa_apply=False, mask_wofs_apply=False, mask_vector_apply=False,
        output_format=OutputFormat.GEOTIFF) == "LS5_NBAR_120_-020_2000_01_01_2005_12_31_SUMMER_DEC_01_FEB_31_BLUE_STACK.tif"

    assert get_dataset_band_stack_filename(
        satellites=[Satellite.LS5, Satellite.LS7], dataset_type=DatasetType.ARG25,
        band=Ls57Arg25Bands.BLUE, x=120, y=-20, season=season,
        acq_min=parse_date_min("2000"), acq_max=parse_date_max("2005"),
        mask_pqa_apply=False, mask_wofs_apply=False, mask_vector_apply=False,
        output_format=OutputFormat.GEOTIFF) == "LS57_NBAR_120_-020_2000_01_01_2005_12_31_SUMMER_DEC_01_FEB_31_BLUE_STACK.tif"

    assert get_dataset_band_stack_filename(
        satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8], dataset_type=DatasetType.ARG25,
        band=Ls57Arg25Bands.BLUE, x=120, y=-20,
        acq_min=parse_date_min("2000"), acq_max=parse_date_max("2005"), season=season,
        mask_pqa_apply=False, mask_wofs_apply=False, mask_vector_apply=False,
        output_format=OutputFormat.GEOTIFF) == "LS578_NBAR_120_-020_2000_01_01_2005_12_31_SUMMER_DEC_01_FEB_31_BLUE_STACK.tif"


def test_get_band_stack_filename_seasons_ord_wet():

    season = SeasonParameter("WET",
                             (Month.NOVEMBER, 1),
                             (Month.MARCH, 31))

    assert get_dataset_band_stack_filename(
        satellites=[Satellite.LS5], dataset_type=DatasetType.ARG25,
        band=Ls57Arg25Bands.BLUE, x=120, y=-20,
        acq_min=parse_date_min("2000"), acq_max=parse_date_max("2005"), season=season,
        mask_pqa_apply=False, mask_wofs_apply=False, mask_vector_apply=False,
        output_format=OutputFormat.GEOTIFF) == "LS5_NBAR_120_-020_2000_01_01_2005_12_31_WET_NOV_01_MAR_31_BLUE_STACK.tif"

    assert get_dataset_band_stack_filename(
        satellites=[Satellite.LS5, Satellite.LS7], dataset_type=DatasetType.ARG25,
        band=Ls57Arg25Bands.BLUE, x=120, y=-20, season=season,
        acq_min=parse_date_min("2000"), acq_max=parse_date_max("2005"),
        mask_pqa_apply=False, mask_wofs_apply=False, mask_vector_apply=False,
        output_format=OutputFormat.GEOTIFF) == "LS57_NBAR_120_-020_2000_01_01_2005_12_31_WET_NOV_01_MAR_31_BLUE_STACK.tif"

    assert get_dataset_band_stack_filename(
        satellites=[Satellite.LS5, Satellite.LS7, Satellite.LS8], dataset_type=DatasetType.ARG25,
        band=Ls57Arg25Bands.BLUE, x=120, y=-20,
        acq_min=parse_date_min("2000"), acq_max=parse_date_max("2005"), season=season,
        mask_pqa_apply=False, mask_wofs_apply=False, mask_vector_apply=False,
        output_format=OutputFormat.GEOTIFF) == "LS578_NBAR_120_-020_2000_01_01_2005_12_31_WET_NOV_01_MAR_31_BLUE_STACK.tif"

