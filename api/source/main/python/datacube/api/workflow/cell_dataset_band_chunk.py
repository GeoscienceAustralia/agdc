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
from datacube.api.model import Ls57Arg25Bands, get_bands


_log = logging.getLogger()


class Workflow(workflow.Workflow):

    __metaclass__ = abc.ABCMeta

    def __init__(self, name):

        # Call method on super class
        # super(self.__class__, self).__init__(name)
        workflow.Workflow.__init__(self, name)

        self.dataset_type = None
        self.bands = None

        self.chunk_size_x = None
        self.chunk_size_y = None

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        workflow.Workflow.setup_arguments(self)

        self.parser.add_argument("--dataset-type", help="The type of dataset to process", action="store",
                                 dest="dataset_type",
                                 type=workflow.dataset_type_arg,
                                 choices=self.get_supported_dataset_types(), required=True,
                                 metavar=" ".join(
                                     [dataset_type.name for dataset_type in self.get_supported_dataset_types()]))

        self.parser.add_argument("--band", help="The band(s) from the dataset to process", action="store", required=True,
                                 dest="bands", type=str, nargs="+", metavar=" ".join([b.name for b in Ls57Arg25Bands]))

        self.parser.add_argument("--chunk-size-x", help="X chunk size", action="store", dest="chunk_size_x", type=int,
                                 choices=range(1, 4000 + 1), required=True)
        self.parser.add_argument("--chunk-size-y", help="Y chunk size", action="store", dest="chunk_size_y", type=int,
                                 choices=range(1, 4000 + 1), required=True)

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        workflow.Workflow.process_arguments(self, args)

        self.dataset_type = args.dataset_type
        self.bands = args.bands

        # Verify that all the requested satellites have the requested bands

        for satellite in self.satellites:
            if not all(item in [b.name for b in get_bands(self.dataset_type, satellite)] for item in self.bands):
                _log.error("Requested bands [%s] not ALL present for satellite [%s]", self.bands, satellite)
                raise Exception("Not all bands present for all satellites")

        self.chunk_size_x = args.chunk_size_x
        self.chunk_size_y = args.chunk_size_y

    def log_arguments(self):

        # Call method on super class
        # super(self.__class__, self).log_arguments()
        workflow.Workflow.log_arguments(self)

        _log.info("""
        dataset to retrieve = {dataset_type}
        bands = {bands}
        X chunk size = {chunk_size_x}
        Y chunk size = {chunk_size_y}
        """.format(dataset_type=self.dataset_type.name, bands=self.bands,
                   chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y))

    @abc.abstractmethod
    def create_summary_tasks(self):

        raise Exception("Abstract method should be overridden")

    @abc.abstractmethod
    def get_supported_dataset_types(self):

        raise Exception("Abstract method should be overridden")


class SummaryTask(workflow.SummaryTask):

    __metaclass__ = abc.ABCMeta

    dataset_type = luigi.Parameter()
    bands = luigi.Parameter(is_list=True)

    chunk_size_x = luigi.IntParameter()
    chunk_size_y = luigi.IntParameter()

    @abc.abstractmethod
    def create_cell_tasks(self, x, y):

        raise Exception("Abstract method should be overridden")


class CellTask(workflow.CellTask):

    __metaclass__ = abc.ABCMeta

    dataset_type = luigi.Parameter()
    bands = luigi.Parameter(is_list=True)

    chunk_size_x = luigi.IntParameter()
    chunk_size_y = luigi.IntParameter()

    def requires(self):

        return [self.create_cell_dataset_band_task(band) for band in self.bands]

    @abc.abstractmethod
    def create_cell_dataset_band_task(self, band):

        raise Exception("Abstract method should be overridden")


class CellDatasetBandTask(workflow.Task):

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

    dataset_type = luigi.Parameter()
    band = luigi.Parameter()

    chunk_size_x = luigi.IntParameter()
    chunk_size_y = luigi.IntParameter()

    def requires(self):

        return [self.create_cell_dataset_band_chunk_task(x_offset, y_offset) for x_offset, y_offset in self.get_chunks()]

    def get_chunks(self):

        import itertools

        for x_offset, y_offset in itertools.product(range(0, 4000, self.chunk_size_x), range(0, 4000, self.chunk_size_y)):
            yield x_offset, y_offset

    @abc.abstractmethod
    def create_cell_dataset_band_chunk_task(self, x_offset, y_offset):

        raise Exception("Abstract method should be overridden")


class CellDatasetBandChunkTask(workflow.Task):

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

    dataset_type = luigi.Parameter()
    band = luigi.Parameter()

    x_offset = luigi.IntParameter()
    y_offset = luigi.IntParameter()

    chunk_size_x = luigi.IntParameter()
    chunk_size_y = luigi.IntParameter()
