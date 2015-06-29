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
import os
from datacube.api import parse_date_min, parse_date_max, Season, Satellite, PqaMask, DatasetType, Statistic
from datacube.api.model import Ls57Arg25Bands
from datacube.api.workflow.band_statistics_arg25 import Arg25BandStatisticsWorkflow

__author__ = "Simon Oldfield"

import logging

_log = logging.getLogger()

def main():

    workflow = Arg25BandStatisticsWorkflow()

    workflow.x_min = 125
    workflow.x_max = 126

    workflow.y_min = -35
    workflow.y_max = -34

    workflow.acq_min = parse_date_min("1985")
    workflow.acq_max = parse_date_max("2014")

    workflow.epoch = 5

    workflow.seasons = Season
    # workflow.seasons = [Season.SPRING]

    workflow.satellites = [Satellite.LS5, Satellite.LS7]

    workflow.output_directory = "/Users/simon/tmp/cube/output/test/arg25_stats_tasks"
    workflow.output_directory = "/Users/simon/tmp/cube/output/test/arg25_stats_tasks/ARG25_125_126_-035_-034_1985_2014_SUMMER_AUTUMN_WINTER_SPRING"

    workflow.mask_pqa_apply = True
    workflow.mask_pqa_mask = [PqaMask.PQ_MASK_SATURATION, PqaMask.PQ_MASK_CONTIGUITY, PqaMask.PQ_MASK_CLOUD]

    # workflow.local_scheduler = None
    # workflow.workers = None

    workflow.dataset_type = [DatasetType.ARG25]
    workflow.bands = Ls57Arg25Bands

    workflow.x_chunk_size = 1000
    workflow.y_chunk_size = 1000

    workflow.statistics = [Statistic.PERCENTILE_25, Statistic.PERCENTILE_50, Statistic.PERCENTILE_75]

    from luigi.task import flatten

    tasks = flatten(workflow.create_tasks())

    print tasks

    for task in tasks:
        _log.info("task = %s", task)

        path = os.path.join(workflow.output_directory, task.output().path.replace("_STATS.tif", ""))
        os.makedirs(path)

        for output in flatten(task.output()):
            _log.info("output %s", output.path)
            # print output.path.replace("_STATS.tif", "")

        chunk_tasks = flatten(task.requires())

        for chunk_task in chunk_tasks:
            _log.info("chunk task %s", chunk_task)

            for output in flatten(chunk_task.output()):
                _log.info("output %s", output.path)
                # print "\t" + output.path.replace(".npy", "")
                os.makedirs(os.path.join(path, output.path.replace(".npy", "")))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    main()
