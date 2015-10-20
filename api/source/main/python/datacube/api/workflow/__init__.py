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


from __future__ import print_function

__author__ = "Simon Oldfield"

import abc
import argparse
import logging
import luigi
import os
import sys
from datacube.api import writeable_dir, satellite_arg, pqa_mask_arg, wofs_mask_arg, parse_date_min, parse_date_max
from datacube.api.model import Satellite, Cell, DatasetType, Tile
from datacube.api.utils import PqaMask, get_satellite_string, WofsMask, format_date


_log = logging.getLogger()


class Workflow(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, name):

        self.name = name

        self.parser = argparse.ArgumentParser(prog=sys.argv[0], description=self.name)

        self.x_min = None
        self.x_max = None

        self.y_min = None
        self.y_max = None

        self.acq_min = None
        self.acq_max = None

        self.satellites = None

        self.output_directory = None

        self.csv = None

        self.dummy = None

        self.mask_pqa_apply = None
        self.mask_pqa_mask = None

        self.mask_wofs_apply = None
        self.mask_wofs_mask = None

        self.local_scheduler = None
        self.workers = None

    def setup_arguments(self):

        # TODO get the combinations of mutually exclusive arguments right

        self.parser.add_argument("--output-directory", help="output directory", action="store", dest="output_directory",
                                 type=writeable_dir, required=True)

        self.parser.add_argument("--x-min", help="X index of tiles", action="store", dest="x_min", type=int,
                                 choices=range(110, 155 + 1), required=True, metavar="110 ... 155")
        self.parser.add_argument("--x-max", help="X index of tiles", action="store", dest="x_max", type=int,
                                 choices=range(110, 155 + 1), required=True, metavar="110 ... 155")

        self.parser.add_argument("--y-min", help="Y index of tiles", action="store", dest="y_min", type=int,
                                 choices=range(-45, -10 + 1), required=True, metavar="-45 ... -10")
        self.parser.add_argument("--y-max", help="Y index of tiles", action="store", dest="y_max", type=int,
                                 choices=range(-45, -10 + 1), required=True, metavar="-45 ... -10")

        self.parser.add_argument("--acq-min", help="Acquisition Date", action="store", dest="acq_min", type=str,
                                 default="1980")
        self.parser.add_argument("--acq-max", help="Acquisition Date", action="store", dest="acq_max", type=str,
                                 default="2020")

        self.parser.add_argument("--satellite", help="The satellite(s) to include", action="store", dest="satellite",
                                 type=satellite_arg, nargs="+", choices=Satellite,
                                 default=[Satellite.LS5, Satellite.LS7],
                                 metavar=" ".join([s.name for s in Satellite]))

        self.parser.add_argument("--mask-pqa-apply", help="Apply PQA mask", action="store_true", dest="mask_pqa_apply",
                                 default=False)
        self.parser.add_argument("--mask-pqa-mask", help="The PQA mask to apply", action="store", dest="mask_pqa_mask",
                                 type=pqa_mask_arg, nargs="+", choices=PqaMask, default=[PqaMask.PQ_MASK_CLEAR],
                                 metavar=" ".join([s.name for s in PqaMask]))

        self.parser.add_argument("--mask-wofs-apply", help="Apply WOFS mask", action="store_true",
                                 dest="mask_wofs_apply",
                                 default=False)
        self.parser.add_argument("--mask-wofs-mask", help="The WOFS mask to apply", action="store",
                                 dest="mask_wofs_mask",
                                 type=wofs_mask_arg, nargs="+", choices=WofsMask, default=[WofsMask.WET],
                                 metavar=" ".join([s.name for s in WofsMask]))

        self.parser.add_argument("--csv", help="Get cell/dataset info from pre-created CSV rather than querying DB",
                                 action="store_true", dest="csv", default=False)

        self.parser.add_argument("--dummy", help="Dummy run", action="store_true", dest="dummy", default=False)

        self.parser.add_argument("--local-scheduler", help="Use local luigi scheduler rather than MPI",
                                 action="store_true",
                                 dest="local_scheduler", default=False)

        self.parser.add_argument("--workers", help="Number of worker tasks", action="store", dest="workers", type=int,
                                 default=1)

        group = self.parser.add_mutually_exclusive_group()

        group.add_argument("--quiet", help="Less output", action="store_const", dest="log_level", const=logging.WARN)
        group.add_argument("--verbose", help="More output", action="store_const", dest="log_level", const=logging.DEBUG)

        self.parser.set_defaults(log_level=logging.INFO)

    def process_arguments(self, args):

        self.output_directory = args.output_directory

        self.x_min = args.x_min
        self.x_max = args.x_max

        self.y_min = args.y_min
        self.y_max = args.y_max

        self.acq_min = parse_date_min(args.acq_min)
        self.acq_max = parse_date_max(args.acq_max)

        self.satellites = args.satellite

        self.csv = args.csv

        self.dummy = args.dummy

        self.mask_pqa_apply = args.mask_pqa_apply
        self.mask_pqa_mask = args.mask_pqa_mask

        self.mask_wofs_apply = args.mask_wofs_apply
        self.mask_wofs_mask = args.mask_wofs_mask

        self.local_scheduler = args.local_scheduler
        self.workers = args.workers

        _log.setLevel(args.log_level)

    def log_arguments(self):

        _log.info("""
        x = {x_min:03d} to {x_max:03d}
        y = {y_min:04d} to {y_max:04d}
        acq = {acq_min} to {acq_max}
        satellites = {satellites}
        output directory = {output_directory}
        csv = {csv}
        dummy = {dummy}
        PQA mask = {pqa_mask}
        WOFS mask = {wofs_mask}
        local scheduler = {local_scheduler}
        workers = {workers}
        """.format(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                   acq_min=self.acq_min, acq_max=self.acq_max,
                   satellites=" ".join([s.name for s in self.satellites]), output_directory=self.output_directory,
                   csv=self.csv,
                   dummy=self.dummy,
                   pqa_mask=self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
                   wofs_mask=self.mask_wofs_apply and " ".join([mask.name for mask in self.mask_wofs_mask]) or "",
                   local_scheduler=self.local_scheduler, workers=self.workers))

    @abc.abstractmethod
    def create_summary_tasks(self):

        raise Exception("Abstract method should be overridden")

    def run(self):

        self.setup_arguments()
        self.process_arguments(self.parser.parse_args())
        self.log_arguments()

        if self.local_scheduler:
            luigi.build(self.create_summary_tasks(), local_scheduler=self.local_scheduler, workers=self.workers)

        else:
            import luigi.contrib.mpi as mpi
            mpi.run(self.create_summary_tasks())


class Task(luigi.Task):

    __metaclass__ = abc.ABCMeta

    # TODO this checks both outputs and sub-tasks - don't necessarily want to do both all the time?
    # That is, if all outputs are there do we care about the dependent tasks?

    def complete(self):
        from luigi.task import flatten

        for output in flatten(self.output()):
            if not output.exists():
                return False

        for dep in flatten(self.deps()):
            if not dep.complete():
                return False

        return True


class SummaryTask(Task):

    __metaclass__ = abc.ABCMeta

    x_min = luigi.IntParameter()
    x_max = luigi.IntParameter()

    y_min = luigi.IntParameter()
    y_max = luigi.IntParameter()

    acq_min = luigi.DateParameter()
    acq_max = luigi.DateParameter()

    satellites = luigi.Parameter(is_list=True)

    output_directory = luigi.Parameter()

    csv = luigi.BooleanParameter()

    dummy = luigi.BooleanParameter()

    mask_pqa_apply = luigi.BooleanParameter()
    mask_pqa_mask = luigi.Parameter()

    mask_wofs_apply = luigi.BooleanParameter()
    mask_wofs_mask = luigi.Parameter()

    def get_cells(self):

        # get list of cells from CSV

        if self.csv:
            return list(self.get_cells_from_csv())

        # get list of cells from DB

        else:
            return list(self.get_cells_from_db())

    def get_cells_from_csv(self):

        if os.path.isfile(self.get_cell_csv_filename()):
            with open(self.get_cell_csv_filename(), "rb") as f:
                import csv

                reader = csv.DictReader(f)
                for record in reader:
                    _log.debug("Found CSV record [%s]", record)
                    yield Cell.from_csv_record(record)

    def get_cell_csv_filename(self):

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        # TODO other distinguishing characteristics (e.g. dataset type(s))

        return os.path.join(
            self.output_directory,
            "cells_{satellites}_{x_min:03d}_{x_max:03d}_{y_min:04d}_{y_max:04d}_{acq_min}_{acq_max}.csv".format(
                satellites=get_satellite_string(self.satellites), x_min=self.x_min,
                x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                acq_min=acq_min, acq_max=acq_max
            ))

    def get_cells_from_db(self):

        from datacube.api.query import list_cells

        x_list = range(self.x_min, self.x_max + 1)
        y_list = range(self.y_min, self.y_max + 1)

        for cell in list_cells(x=x_list, y=y_list, acq_min=self.acq_min, acq_max=self.acq_max,
                               satellites=[satellite for satellite in self.satellites],
                               dataset_types=self.get_dataset_types()):
            yield cell

    def requires(self):

        if self.csv:
            yield CellListCsvTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                  acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                  dataset_types=self.get_dataset_types(), path=self.get_cell_csv_filename())

        # yield [self.create_cell_tasks(x=cell.x, y=cell.y) for cell in self.get_cells()]
        for cell in self.get_cells():
            yield self.create_cell_tasks(x=cell.x, y=cell.y)

    @abc.abstractmethod
    def create_cell_tasks(self, x, y):

        raise Exception("Abstract method should be overridden")

    @staticmethod
    def get_dataset_types():

        return [DatasetType.ARG25, DatasetType.PQ25]


class CellTask(Task):

    __metaclass__ = abc.ABCMeta

    x = luigi.IntParameter()
    y = luigi.IntParameter()

    acq_min = luigi.DateParameter()
    acq_max = luigi.DateParameter()

    satellites = luigi.Parameter(is_list=True)

    output_directory = luigi.Parameter()

    csv = luigi.BooleanParameter()

    dummy = luigi.BooleanParameter()

    mask_pqa_apply = luigi.BooleanParameter()
    mask_pqa_mask = luigi.Parameter()

    mask_wofs_apply = luigi.BooleanParameter()
    mask_wofs_mask = luigi.Parameter()

    def get_tiles(self):

        # get list of tiles from CSV

        if self.csv:
            return list(self.get_tiles_from_csv())

        # get list of tiles from DB

        else:
            return list(self.get_tiles_from_db())

    def get_tiles_from_csv(self):

        if os.path.isfile(self.get_tile_csv_filename()):
            with open(self.get_tile_csv_filename(), "rb") as f:
                import csv

                reader = csv.DictReader(f)
                for record in reader:
                    _log.debug("Found CSV record [%s]", record)
                    yield Tile.from_csv_record(record)

    def get_tile_csv_filename(self):

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        # TODO other distinguishing characteristics (e.g. dataset types)

        return os.path.join(
            self.output_directory,
            "tiles_{satellites}_{x_min:03d}_{x_max:03d}_{y_min:04d}_{y_max:04d}_{acq_min}_{acq_max}.csv".format(
                satellites=get_satellite_string(self.satellites),
                x_min=self.x, x_max=self.x, y_min=self.y, y_max=self.y,
                acq_min=acq_min, acq_max=acq_max
            ))

    def get_tiles_from_db(self):

        from datacube.api.query import list_tiles

        x_list = [self.x]
        y_list = [self.y]

        for tile in list_tiles(x=x_list, y=y_list, acq_min=self.acq_min, acq_max=self.acq_max,
                               satellites=[satellite for satellite in self.satellites],
                               dataset_types=self.get_dataset_types()):
            yield tile

    @staticmethod
    def get_dataset_types():

        return [DatasetType.ARG25, DatasetType.PQ25]


class ComplexParameter(luigi.Parameter):

    def serialize(self, x):
        import cPickle
        if self.is_list:
            return [hash(cPickle.dumps(v)) for v in x]
        return hash(cPickle.dumps(x))


class CellListCsvTask(Task):

    x_min = luigi.IntParameter()
    x_max = luigi.IntParameter()

    y_min = luigi.IntParameter()
    y_max = luigi.IntParameter()

    acq_min = luigi.DateParameter()
    acq_max = luigi.DateParameter()

    satellites = luigi.Parameter(is_list=True)

    dataset_types = luigi.Parameter(is_list=True)

    path = luigi.Parameter()

    def output(self):

        return luigi.LocalTarget(self.path)

    def run(self):
        print("**** ", self.output().path)

        x_list = range(self.x_min, self.x_max + 1)
        y_list = range(self.y_min, self.y_max + 1)

        from datacube.api.query import list_cells_to_file

        from datacube.api.query import SortType

        list_cells_to_file(x=x_list, y=y_list, satellites=list(self.satellites),
                           acq_min=self.acq_min, acq_max=self.acq_max, dataset_types=self.dataset_types,
                           filename=self.output().path, sort=SortType.ASC)


class TileListCsvTask(Task):

    x_min = luigi.IntParameter()
    x_max = luigi.IntParameter()

    y_min = luigi.IntParameter()
    y_max = luigi.IntParameter()

    acq_min = luigi.DateParameter()
    acq_max = luigi.DateParameter()

    satellites = luigi.Parameter(is_list=True)

    dataset_types = luigi.Parameter(is_list=True)

    path = luigi.Parameter()

    def output(self):

        return luigi.LocalTarget(self.path)

    def run(self):
        print("****", self.output().path)

        x_list = range(self.x_min, self.x_max + 1)
        y_list = range(self.y_min, self.y_max + 1)

        from datacube.api.query import list_tiles_to_file, SortType

        list_tiles_to_file(x=x_list, y=y_list, satellites=list(self.satellites),
                           acq_min=self.acq_min, acq_max=self.acq_max, dataset_types=self.dataset_types,
                           filename=self.output().path, sort=SortType.ASC)
