#!/usr/bin/env python

# ===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
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


import abc
import argparse
import logging
import luigi
import os
import sys
from datacube.api.model import Satellite, Cell, DatasetType, Tile
from datacube.api.utils import PqaMask, get_satellite_string, WofsMask


_log = logging.getLogger()


def satellite_arg(s):
    if s in [s.name for s in Satellite]:
        return Satellite[s]
    raise argparse.ArgumentTypeError("{0} is not a supported satellite".format(s))


def pqa_mask_arg(s):
    if s in [m.name for m in PqaMask]:
        return PqaMask[s]
    raise argparse.ArgumentTypeError("{0} is not a supported PQA mask".format(s))


def wofs_mask_arg(s):
    if s in [m.name for m in WofsMask]:
        return WofsMask[s]
    raise argparse.ArgumentTypeError("{0} is not a supported WOFS mask".format(s))


def dataset_type_arg(s):
    if s in [t.name for t in DatasetType]:
        return DatasetType[s]
    raise argparse.ArgumentTypeError("{0} is not a supported dataset type".format(s))


def writeable_dir(prospective_dir):
    if not os.path.exists(prospective_dir):
        raise argparse.ArgumentTypeError("{0} doesn't exist".format(prospective_dir))

    if not os.path.isdir(prospective_dir):
        raise argparse.ArgumentTypeError("{0} is not a directory".format(prospective_dir))

    if not os.access(prospective_dir, os.W_OK):
        raise argparse.ArgumentTypeError("{0} is not writeable".format(prospective_dir))

    return prospective_dir


def readable_dir(prospective_dir):
    if not os.path.exists(prospective_dir):
        raise argparse.ArgumentTypeError("{0} doesn't exist".format(prospective_dir))

    if not os.path.isdir(prospective_dir):
        raise argparse.ArgumentTypeError("{0} is not a directory".format(prospective_dir))

    if not os.access(prospective_dir, os.R_OK):
        raise argparse.ArgumentTypeError("{0} is not readable".format(prospective_dir))

    return prospective_dir


def dummy(path):
    _log.debug("Creating dummy output %s" % path)
    import os

    if not os.path.exists(path):
        with open(path, "w") as f:
            pass


def parse_date_min(s):
    from datetime import datetime

    if s:
        if len(s) == len("YYYY"):
            return datetime.strptime(s, "%Y").date()

        elif len(s) == len("YYYY-MM"):
            return datetime.strptime(s, "%Y-%m").date()

        elif len(s) == len("YYYY-MM-DD"):
            return datetime.strptime(s, "%Y-%m-%d").date()

    return None


def parse_date_max(s):
    from datetime import date, datetime
    import calendar

    if s:
        if len(s) == len("YYYY"):
            d = datetime.strptime(s, "%Y").date()
            d = d.replace(month=12, day=31)
            return d

        elif len(s) == len("YYYY-MM"):
            d = datetime.strptime(s, "%Y-%m").date()

            first, last = calendar.monthrange(d.year, d.month)
            d = d.replace(day=last)
            return d

        elif len(s) == len("YYYY-MM-DD"):
            d = datetime.strptime(s, "%Y-%m-%d").date()
            return d

    return None


def format_date(d):
    from datetime import datetime

    if d:
        return datetime.strftime(d, "%Y_%m_%d")

    return None


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
                   csv=self.csv, dummy=self.dummy,
                   pqa_mask=self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
                   wofs_mask=self.mask_wofs_apply and " ".join([mask.name for mask in self.mask_wofs_mask]) or "",
                   local_scheduler=self.local_scheduler, workers=self.workers))

    @abc.abstractmethod
    def create_tasks(self):

        raise Exception("Abstract method should be overridden")

    def run(self):

        self.setup_arguments()
        self.process_arguments(self.parser.parse_args())
        self.log_arguments()

        if self.local_scheduler:
            luigi.build(self.create_tasks(), local_scheduler=self.local_scheduler, workers=self.workers)

        else:
            import luigi.contrib.mpi as mpi
            mpi.run(self.create_tasks())


class Task(luigi.Task):

    __metaclass__ = abc.ABCMeta

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
            # If the CSV file hasn't been created yet then we stop

            if not os.path.isfile(self.get_cell_csv_filename()):
                _log.error("CELL LIST CSV file [%s] present!!!!", self.get_cell_csv_filename())
                raise Exception("CELL LIST CSV file [{filename}] present!!!!".format(filename=self.get_cell_csv_filename()))

            return list(self.get_cells_from_csv())

        # get list of cells from DB

        else:
            return list(self.get_cells_from_db())

    def get_cells_from_csv(self):

        with open(self.get_cell_csv_filename(), "rb") as f:
            import csv

            reader = csv.DictReader(f)
            for record in reader:
                _log.debug("Found CSV record [%s]", record)
                yield Cell.from_csv_record(record)

    def get_cell_csv_filename(self):

        from datacube.api.workflow import parse_date_min, parse_date_max

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        return os.path.join(
            self.output_directory,
            "cells_{satellites}_{x_min:03d}_{x_max:03d}_{y_min:04d}_{y_max:04d}_{acq_min}_{acq_max}.csv".format(
                satellites=get_satellite_string(self.satellites), x_min=self.x_min,
                x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                acq_min=acq_min, acq_max=acq_max
            ))

    def get_cells_from_db(self):

        from datacube.config import Config
        from datacube.api.query import list_cells

        config = Config(os.path.expanduser("~/.datacube/config"))
        _log.debug(config.to_str())

        x_list = range(self.x_min, self.x_max + 1)
        y_list = range(self.y_min, self.y_max + 1)

        for cell in list_cells(x=x_list, y=y_list, acq_min=self.acq_min, acq_max=self.acq_max,
                               satellites=[satellite for satellite in self.satellites],
                               dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                               database=config.get_db_database(),
                               user=config.get_db_username(),
                               password=config.get_db_password(),
                               host=config.get_db_host(), port=config.get_db_port()):
            yield cell

    def requires(self):

        return [self.create_cell_task(x=cell.x, y=cell.y) for cell in self.get_cells()]

    @abc.abstractmethod
    def create_cell_task(self, x, y):

        raise Exception("Abstract method should be overridden")


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
            # If the CSV file hasn't been created yet then we stop

            if not os.path.isfile(self.get_tile_csv_filename()):
                _log.error("TILE LIST CSV file [%s] present!!!!", self.get_tile_csv_filename())
                raise Exception("TILE LIST CSV file [{filename}] present!!!!".format(filename=self.get_tile_csv_filename()))

            return list(self.get_tiles_from_csv())

        # get list of tiles from DB

        else:
            return list(self.get_tiles_from_db())

    def get_tiles_from_csv(self):

        with open(self.get_tile_csv_filename(), "rb") as f:
            import csv

            reader = csv.DictReader(f)
            for record in reader:
                _log.debug("Found CSV record [%s]", record)
                yield Tile.from_csv_record(record)

    def get_tile_csv_filename(self):

        from datacube.api.workflow import parse_date_min, parse_date_max

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        return os.path.join(
            self.output_directory,
            "tiles_{satellites}_{x_min:03d}_{x_max:03d}_{y_min:04d}_{y_max:04d}_{acq_min}_{acq_max}.csv".format(
                satellites=get_satellite_string(self.satellites),
                x_min=self.x, x_max=self.x, y_min=self.y, y_max=self.y,
                acq_min=acq_min, acq_max=acq_max
            ))

    def get_tiles_from_db(self):

        from datacube.config import Config
        from datacube.api.query import list_tiles

        config = Config(os.path.expanduser("~/.datacube/config"))
        _log.debug(config.to_str())

        x_list = [self.x]
        y_list = [self.y]

        for tile in list_tiles(x=x_list, y=y_list, acq_min=self.acq_min, acq_max=self.acq_max,
                               satellites=[satellite for satellite in self.satellites],
                               dataset_types=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                               database=config.get_db_database(),
                               user=config.get_db_username(),
                               password=config.get_db_password(),
                               host=config.get_db_host(), port=config.get_db_port()):
            yield tile


class ComplexParameter(luigi.Parameter):

    def serialize(self, x):
        import cPickle
        if self.is_list:
            return [hash(cPickle.dumps(v)) for v in x]
        return hash(cPickle.dumps(x))
