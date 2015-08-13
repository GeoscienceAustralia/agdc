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

