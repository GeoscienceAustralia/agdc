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


import argparse
import logging
import luigi
import os
import sys
from datacube.api import writeable_dir, satellite_arg, Satellite, dataset_type_arg, DatasetType, Season, season_arg
from datacube.api import parse_date_min, parse_date_max
from datacube.api.query import list_cells_as_list, list_tiles_to_file
from datacube.api.utils import build_season_date_criteria, SEASONS
from datacube.api.workflow2 import Task


__author__ = "Simon Oldfield"

_log = logging.getLogger()


class PrepareCsvFiles(object):

    def __init__(self, name="Prepare CSV Files"):

        self.name = name

        self.parser = argparse.ArgumentParser(prog=sys.argv[0], description=self.name)

        self.x_min = None
        self.x_max = None

        self.y_min = None
        self.y_max = None

        self.acq_min = None
        self.acq_max = None

        self.seasons = None

        self.satellites = None

        self.output_directory = None

        self.mask_pqa_apply = None
        self.mask_wofs_apply = None

        self.include_ls7_slc_off = None
        self.include_ls8_pre_wrs2 = None

        self.dataset_type = None

        self.local_scheduler = None
        self.workers = None

    def setup_arguments(self):

        self.parser.add_argument("--x-min", help="X index of tiles", action="store", dest="x_min", type=int,
                                 choices=range(110, 155 + 1), required=True, metavar="110 ... 155")

        self.parser.add_argument("--x-max", help="X index of tiles", action="store", dest="x_max", type=int,
                                 choices=range(110, 155 + 1), required=True, metavar="110 ... 155")

        self.parser.add_argument("--y-min", help="Y index of tiles", action="store", dest="y_min", type=int,
                                 choices=range(-45, -10 + 1), required=True, metavar="-45 ... -10")

        self.parser.add_argument("--y-max", help="Y index of tiles", action="store", dest="y_max", type=int,
                                 choices=range(-45, -10 + 1), required=True, metavar="-45 ... -10")

        self.parser.add_argument("--output-directory", help="output directory", action="store", dest="output_directory",
                                 type=writeable_dir, required=True)

        self.parser.add_argument("--acq-min", help="Acquisition Date", action="store", dest="acq_min", type=str,
                                 default="1985")

        self.parser.add_argument("--acq-max", help="Acquisition Date", action="store", dest="acq_max", type=str,
                                 default="2014")

        self.parser.add_argument("--satellite", help="The satellite(s) to include", action="store", dest="satellites",
                                 type=satellite_arg, nargs="+", choices=Satellite,
                                 default=[Satellite.LS5, Satellite.LS7],
                                 metavar=" ".join([ts.name for ts in Satellite]))

        self.parser.add_argument("--mask-pqa-apply", help="Apply PQA mask", action="store_true", dest="mask_pqa_apply",
                                 default=True)

        self.parser.add_argument("--mask-wofs-apply", help="Apply WOFS mask", action="store_true",
                                 dest="mask_wofs_apply",
                                 default=False)

        group = self.parser.add_mutually_exclusive_group()

        group.add_argument("--quiet", help="Less output", action="store_const", dest="log_level", const=logging.WARN)
        group.add_argument("--verbose", help="More output", action="store_const", dest="log_level", const=logging.DEBUG)

        self.parser.set_defaults(log_level=logging.INFO)

        self.parser.add_argument("--dataset-type", help="The type of dataset to process", action="store",
                                 dest="dataset_type",
                                 type=dataset_type_arg,
                                 choices=[DatasetType.ARG25],  # required=True,
                                 default=DatasetType.ARG25,
                                 metavar=" ".join([dt.name for dt in [DatasetType.ARG25]]))

        self.parser.add_argument("--season", help="The seasons for which to produce statistics", action="store",
                                 default=Season,  # required=True,
                                 dest="season", type=season_arg, nargs="+",
                                 metavar=" ".join([s.name for s in Season]))

        self.parser.add_argument("--no-ls7-slc-off",
                                 help="Exclude LS7 SLC OFF datasets",
                                 action="store_false", dest="include_ls7_slc_off", default=True)

        self.parser.add_argument("--no-ls8-pre-wrs2",
                                 help="Exclude LS8 PRE-WRS2 datasets",
                                 action="store_false", dest="include_ls8_pre_wrs2", default=True)

        self.parser.add_argument("--local-scheduler", help="Use local luigi scheduler rather than MPI",
                                 action="store_true",
                                 dest="local_scheduler", default=True)

        self.parser.add_argument("--workers", help="Number of worker tasks", action="store", dest="workers", type=int,
                                 default=16)

    def process_arguments(self, args):

        self.x_min = args.x_min
        self.x_max = args.x_max

        self.y_min = args.y_min
        self.y_max = args.y_max

        self.output_directory = args.output_directory

        self.acq_min = parse_date_min(args.acq_min)
        self.acq_max = parse_date_max(args.acq_max)

        self.satellites = args.satellites

        self.seasons = args.season

        self.mask_pqa_apply = args.mask_pqa_apply
        self.mask_wofs_apply = args.mask_wofs_apply

        self.include_ls7_slc_off = args.include_ls7_slc_off
        self.include_ls8_pre_wrs2 = args.include_ls8_pre_wrs2

        self.local_scheduler = args.local_scheduler
        self.workers = args.workers

        _log.setLevel(args.log_level)

        self.dataset_type = args.dataset_type

    def log_arguments(self):

        _log.info("""
        x = {x_min:03d} to {x_max:03d}
        y = {y_min:04d} to {y_max:04d}
        """.format(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max))

        _log.info("""
        acq = {acq_min} to {acq_max}
        satellites = {satellites}
        output directory = {output_directory}
        PQA mask = {pqa_mask}
        local scheduler = {local_scheduler}
        workers = {workers}
        """.format(acq_min=self.acq_min, acq_max=self.acq_max,
                   satellites=" ".join([ts.name for ts in self.satellites]),
                   output_directory=self.output_directory,
                   pqa_mask=self.mask_pqa_apply,
                   local_scheduler=self.local_scheduler, workers=self.workers))

        _log.info("""
        datasets to retrieve = {dataset_type}
        seasons = {seasons}
        """.format(dataset_type=self.dataset_type.name,
                   seasons=" ".join([s.name for s in self.seasons])))

    def create_tasks(self):
        x_list = range(self.x_min, self.x_max + 1)
        y_list = range(self.y_min, self.y_max + 1)

        dataset_types = [self.dataset_type]

        if self.mask_pqa_apply:
            dataset_types.append(DatasetType.PQ25)

        for season in self.seasons:
            _log.info("acq_min=[%s] acq_max=[%s] season=[%s]", self.acq_min, self.acq_max, season.name)

            acq_min_extended, acq_max_extended, criteria = build_season_date_criteria(self.acq_min, self.acq_max,
                                                                                      season, seasons=SEASONS,
                                                                                      extend=True)

            _log.info("\tacq_min_extended=[%s], acq_max_extended=[%s], criteria=[%s]", acq_min_extended, acq_max_extended, criteria)

            for cell in list_cells_as_list(x=x_list, y=y_list, satellites=self.satellites,
                                           acq_min=acq_min_extended, acq_max=acq_max_extended,
                                           dataset_types=dataset_types, include=criteria):
                _log.info("\t%3d %4d", cell.x, cell.y)
                yield self.create_task(x=cell.x, y=cell.y, acq_min=acq_min_extended, acq_max=acq_max_extended, dataset_types=dataset_types, include=criteria, season=season)

    def create_task(self, x, y, acq_min, acq_max, dataset_types, include, season):
        _log.info("Creating task for %s %s %s %s %s", x, y, acq_min, acq_max, season)
        return CsvTask(x=x, y=y, satellites=self.satellites, acq_min=acq_min, acq_max=acq_max, dataset_types=dataset_types, include=include, season=season, dir=self.output_directory)

    def run(self):

        self.setup_arguments()
        self.process_arguments(self.parser.parse_args())
        self.log_arguments()

        luigi.build(self.create_tasks(), local_scheduler=self.local_scheduler, workers=self.workers)


class CsvTask(Task):

    x = luigi.IntParameter()
    y = luigi.IntParameter()

    satellites = luigi.Parameter(is_list=True)

    acq_min = luigi.DateParameter()
    acq_max = luigi.DateParameter()

    season = luigi.Parameter()

    dataset_types = luigi.Parameter(is_list=True)

    include = luigi.Parameter(is_list=True)

    dir = luigi.Parameter()

    def output(self):
        # return FileDependencyTask(path=os.path.join(self.dir, "datasets_{x:03}_{y:04d}_{season}.csv".format(x=self.x, y=self.y, season=self.season.name)))
        return luigi.LocalTarget(os.path.join(self.dir, "datasets_{x:03}_{y:04d}_{season}.csv".format(x=self.x, y=self.y, season=self.season.name)))

    def run(self):

        list_tiles_to_file(x=[self.x], y=[self.y], satellites=self.satellites, acq_min=self.acq_min, acq_max=self.acq_max, dataset_types=self.dataset_types, filename=self.output().path, include=self.include)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    PrepareCsvFiles().run()
