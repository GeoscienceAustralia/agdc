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


import abc
import datacube.api.workflow as workflow
import logging
import luigi
import os
from datacube.api.model import DatasetType, Tile
from datacube.api.utils import get_satellite_string


_log = logging.getLogger()


class Workflow(workflow.Workflow):

    __metaclass__ = abc.ABCMeta

    def __init__(self, name):

        # Call method on super class
        # super(self.__class__, self).__init__(name)
        workflow.Workflow.__init__(self, name)

        self.chunk_size_x = None
        self.chunk_size_y = None

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        workflow.Workflow.setup_arguments(self)

        self.parser.add_argument("--chunk-size-x", help="X chunk size", action="store", dest="chunk_size_x", type=int,
                                 choices=range(1, 4000 + 1), required=True)
        self.parser.add_argument("--chunk-size-y", help="Y chunk size", action="store", dest="chunk_size_y", type=int,
                                 choices=range(1, 4000 + 1), required=True)

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        workflow.Workflow.process_arguments(self, args)

        self.chunk_size_x = args.chunk_size_x
        self.chunk_size_y = args.chunk_size_y

    def log_arguments(self):

        # Call method on super class
        # super(self.__class__, self).log_arguments()
        workflow.Workflow.log_arguments(self)

        _log.info("""
        X chunk size = {chunk_size_x}
        Y chunk size = {chunk_size_y}
        """.format(chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y))

    def create_summary_tasks(self):

        raise Exception("Abstract method should be overridden")


class SummaryTask(workflow.SummaryTask):

    __metaclass__ = abc.ABCMeta

    chunk_size_x = luigi.IntParameter()
    chunk_size_y = luigi.IntParameter()

    @abc.abstractmethod
    def create_cell_tasks(self, x, y):

        raise Exception("Abstract method should be overridden")


class CellTask(workflow.CellTask):

    __metaclass__ = abc.ABCMeta

    chunk_size_x = luigi.IntParameter()
    chunk_size_y = luigi.IntParameter()

    def requires(self):

        # return [self.create_cell_chunk_task(x_offset, y_offset) for x_offset, y_offset in self.get_chunks()]

        for x_offset, y_offset in self.get_chunks():
            yield self.create_cell_chunk_task(x_offset, y_offset)

    def get_chunks(self):

        import itertools

        for x_offset, y_offset in itertools.product(range(0, 4000, self.chunk_size_x), range(0, 4000, self.chunk_size_y)):
            yield x_offset, y_offset

    @abc.abstractmethod
    def create_cell_chunk_task(self, x_offset, y_offset):

        raise Exception("Abstract method should be overridden")


class CellChunkTask(workflow.Task):

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

    x_offset = luigi.IntParameter()
    y_offset = luigi.IntParameter()

    chunk_size_x = luigi.IntParameter()
    chunk_size_y = luigi.IntParameter()

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

        acq_min = workflow.format_date(self.acq_min)
        acq_max = workflow.format_date(self.acq_max)

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
