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
# documentation and/or other materials provided with the distribution.
# * Neither Geoscience Australia nor the names of its contributors may be
# used to endorse or promote products derived from this software
# without specific prior written permission.
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
import csv
import logging
import luigi
from datacube.config import Config
from datacube.api.model import DatasetType, Tile
from datacube.api.query import list_cells, list_tiles_to_file, list_tiles, SortType
import os.path


_log = logging.getLogger()


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


class SummaryTask(luigi.Task):
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
    save_input_files = luigi.BooleanParameter()
    apply_pq_filter = luigi.BooleanParameter()

    def requires(self):
        _log.debug("SummaryTask.requires()")

        from datacube.config import Config

        config = Config(os.path.expanduser("~/.datacube/config"))
        _log.debug(config.to_str())

        x_list = range(self.x_min, self.x_max + 1)
        y_list = range(self.y_min, self.y_max + 1)

        return [self.create_cell_task(x=cell.x, y=cell.y) for cell in
                list_cells(x=x_list, y=y_list, acq_min=self.acq_min, acq_max=self.acq_max,
                           satellites=[satellite for satellite in self.satellites],
                           datasets=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                           database=config.get_db_database(),
                           user=config.get_db_username(),
                           password=config.get_db_password(),
                           host=config.get_db_host(), port=config.get_db_port())]

    @abc.abstractmethod
    def create_cell_task(self, x, y):
        pass


class CellTask(luigi.Task):
    __metaclass__ = abc.ABCMeta

    x = luigi.IntParameter()
    y = luigi.IntParameter()

    acq_min = luigi.DateParameter()
    acq_max = luigi.DateParameter()

    satellites = luigi.Parameter(is_list=True)

    output_directory = luigi.Parameter()

    csv = luigi.BooleanParameter()
    dummy = luigi.BooleanParameter()
    save_input_files = luigi.BooleanParameter()
    apply_pq_filter = luigi.BooleanParameter()

    def output(self):
        _log.debug("CellTask.output()")
        return [luigi.LocalTarget(f) for f in self.get_output_paths()]

    def requires(self):
        _log.debug("CellTask.requires()")
        if self.csv:
            return CsvTileListTask(x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                   satellites=self.satellites, output_directory=self.output_directory)
        return []

    @abc.abstractmethod
    def get_output_paths(self):
        pass

    @abc.abstractmethod
    def doit(self):
        pass

    def run(self):
        _log.debug("CellTask.run()")
        if self.dummy:
            [dummy(p.path) for p in self.output()]

        else:
            self.doit()

    def get_tiles(self, sort=SortType.ASC):
        if self.csv:
            return self.get_tiles_csv()
        else:
            return self.get_tiles_db(sort=sort)

    def get_tiles_csv(self):
        with open(self.input().path, "rb") as f:
            reader = csv.DictReader(f)
            for record in reader:
                tile = Tile.from_csv_record(record)

                # # Copy input files if requested
                # if self.save_input_files:
                #     input_file_save_dir = self.output_directory + "_input_files"
                #     for dataset in tile.datasets:
                #         os.link(dataset.path, input_file_save_dir)

                yield tile

    def get_tiles_db(self, sort=SortType.ASC):
        from datacube.config import Config

        config = Config(os.path.expanduser("~/.datacube/config"))
        _log.debug(config.to_str())

        for tile in list_tiles(x=[self.x], y=[self.y], acq_min=self.acq_min, acq_max=self.acq_max,
                               satellites=[satellite for satellite in self.satellites],
                               datasets=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                               sort=sort,
                               database=config.get_db_database(),
                               user=config.get_db_username(),
                               password=config.get_db_password(),
                               host=config.get_db_host(), port=config.get_db_port()):

            # # Copy input files if requested
            # if self.save_input_files:
            #     input_file_save_dir = self.output_directory + "_input_files"
            #     for dataset in tile.datasets:
            #         os.link(dataset.path, input_file_save_dir)

            yield tile


class CsvTileListTask(luigi.Task):
    x = luigi.IntParameter()
    y = luigi.IntParameter()

    year_min = luigi.IntParameter()
    year_max = luigi.IntParameter()

    satellites = luigi.Parameter(is_list=True)

    output_directory = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_directory,
                                              "TILES_{x:03d}_{y:04d}_{year_min:04d}_{year_max:04d}.csv"
                                              .format(x=self.x,
                                                      y=self.y,
                                                      year_min=self.year_min,
                                                      year_max=self.year_max)))

    def run(self):
        config = Config(os.path.expanduser("~/.datacube/config"))
        _log.debug(config.to_str())

        year_list = range(self.year_min, self.year_max + 1)

        list_tiles_to_file(x=[self.x], y=[self.y], years=year_list,
                           satellites=[satellite for satellite in self.satellites],
                           datasets=[DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25],
                           filename=self.output().path,
                           database=config.get_db_database(), user=config.get_db_username(),
                           password=config.get_db_password(),
                           host=config.get_db_host(), port=config.get_db_port())


class Workflow(object):
    __metaclass__ = abc.ABCMeta

    application_name = None

    x_min = None
    x_max = None

    y_min = None
    y_max = None

    acq_min = None
    acq_max = None

    process_min = None
    process_max = None

    ingest_min = None
    ingest_max = None

    satellites = None

    output_directory = None

    csv = None
    dummy = None
    save_input_files = None
    apply_pq_filter = None
    local_scheduler = None

    def __init__(self, application_name):
        self.application_name = application_name

    def parse_arguments(self):
        _log.debug("Workflow.parse_arguments()")

        parser = argparse.ArgumentParser(prog=__name__, description=self.application_name)

        parser.add_argument("--output-directory", help="output directory", action="store", dest="output_directory",
                            type=writeable_dir, required=True)

        parser.add_argument("--x-min", help="X index of tiles", action="store", dest="x_min", type=int,
                            choices=range(110, 155 + 1), required=True)
        parser.add_argument("--x-max", help="X index of tiles", action="store", dest="x_max", type=int,
                            choices=range(110, 155 + 1), required=True)

        parser.add_argument("--y-min", help="Y index of tiles", action="store", dest="y_min", type=int,
                            choices=range(-45, -10 + 1), required=True)
        parser.add_argument("--y-max", help="Y index of tiles", action="store", dest="y_max", type=int,
                            choices=range(-45, -10 + 1), required=True)

        # TODO
        parser.add_argument("--year-min", help="year", action="store", dest="year_min", type=int,
                            choices=range(1987, 2014 + 1),
                            default=1987)
        parser.add_argument("--year-max", help="year", action="store", dest="year_max", type=int,
                            choices=range(1987, 2014 + 1),
                            default=2014)

        parser.add_argument("--acq-min", help="Acquisition Date", action="store", dest="acq_min", type=str)
        parser.add_argument("--acq-max", help="Acquisition Date", action="store", dest="acq_max", type=str)

        parser.add_argument("--process-min", help="Process Date", action="store", dest="process_min", type=str)
        parser.add_argument("--process-max", help="Process Date", action="store", dest="process_max", type=str)

        parser.add_argument("--ingest-min", help="Ingest Date", action="store", dest="ingest_min", type=str)
        parser.add_argument("--ingest-max", help="Ingest Date", action="store", dest="ingest_max", type=str)

        parser.add_argument("--satellites", help="satellites", action="store", dest="satellites", type=str, nargs="+",
                            choices=["LS5", "LS7", "LS8"], default=["LS5", "LS7"])

        group = parser.add_mutually_exclusive_group()

        group.add_argument("--db", help="DB vs CSV", action="store_false", dest="csv", default=False)
        group.add_argument("--csv", help="DB vs CSV", action="store_true", dest="csv", default=True)

        parser.add_argument("--dummy", help="Dummy run", action="store_true", dest="dummy", default=False)

        parser.add_argument("--save-input-files", help="Save input files for future reference", action="store_true",
                            dest="save_input_files", default=False)

        parser.add_argument("--skip-pq", help="Skip applying PQ to datasets", action="store_false", dest="pqfilter",
                            default=True)

        parser.add_argument("--local-scheduler", help="Use local luigi scheduler rather than MPI", action="store_true",
                            dest="local_scheduler", default=False)

        args = parser.parse_args()

        self.output_directory = args.output_directory

        self.x_min = args.x_min
        self.x_max = args.x_max
        self.y_min = args.y_min
        self.y_max = args.y_max

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

        self.acq_min = parse_date_min(args.acq_min)
        self.acq_max = parse_date_max(args.acq_max)

        if args.year_min:
            self.process_min = parse_date_min(str(args.year_min))

        if args.year_max:
            self.process_max = parse_date_max(str(args.year_max))

        self.process_min = parse_date_min(args.process_min)
        self.process_max = parse_date_max(args.process_max)

        # NOTE : overrides the year_min/max params - should make this explicit in the argparse config
        self.ingest_min = parse_date_min(args.ingest_min)
        self.ingest_max = parse_date_max(args.ingest_max)

        self.satellites = args.satellites
        self.csv = args.csv
        self.dummy = args.dummy
        self.save_input_files = args.save_input_files
        self.apply_pq_filter = args.pqfilter
        self.local_scheduler = args.local_scheduler

        _log.debug("""
        x = {x_min:03d} to {x_max:03d}
        y = {y_min:04d} to {y_max:04d}
        acq = {acq_min} to {acq_max}
        process = {process_min} to {process_max}
        ingest = {ingest_min} to {ingest_max}
        satellites = {satellites}
        output directory = {output_directory}
        csv = {csv}
        dummy = {dummy}
        save input files = {save_input_files}
        apply PQ filter = {apply_pq_filter}
        local scheduler = {local_scheduler}
        """.format(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                   acq_min=self.acq_min, acq_max=self.acq_max,
                   process_min=self.process_min, process_max=self.process_max,
                   ingest_min=self.ingest_min, ingest_max=self.ingest_max,
                   satellites=self.satellites, output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                   save_input_files=self.save_input_files, apply_pq_filter=self.apply_pq_filter,
                   local_scheduler=self.local_scheduler))


    @abc.abstractmethod
    def create_tasks(self):
        pass

    def run(self):
        self.parse_arguments()

        if self.local_scheduler:
            luigi.build(self.create_tasks(), local_scheduler=True)

        else:
            import luigi.contrib.mpi as mpi
            mpi.run(self.create_tasks())
