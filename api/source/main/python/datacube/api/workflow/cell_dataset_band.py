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
from enum import Enum
import logging
import luigi
import os
from datacube.api.model import Tile, DatasetType
from datacube.api.utils import get_band_name_union, get_band_name_intersection, format_date, get_satellite_string
from datacube.api import dataset_type_arg


_log = logging.getLogger()


class BandListType(Enum):
    __order__ = "EXPLICIT ALL COMMON"

    EXPLICIT = "EXPLICIT"
    ALL = "ALL"
    COMMON = "COMMON"


class Workflow(workflow.Workflow):

    __metaclass__ = abc.ABCMeta

    def __init__(self, name):

        # Call method on super class
        # super(self.__class__, self).__init__(name)
        workflow.Workflow.__init__(self, name)

        self.dataset_type = None
        self.bands = None

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        workflow.Workflow.setup_arguments(self)

        self.parser.add_argument("--dataset-type", help="The type of dataset to process", action="store",
                                 dest="dataset_type",
                                 type=dataset_type_arg,
                                 choices=self.get_supported_dataset_types(), required=True,
                                 metavar=" ".join(
                                     [dataset_type.name for dataset_type in self.get_supported_dataset_types()]))

        group = self.parser.add_mutually_exclusive_group()

        # self.parser.add_argument("--band", help="The band(s) from the dataset to process", action="store", required=True,
        #                          dest="bands", type=str, nargs="+", metavar=" ".join([b.name for b in Ls57Arg25Bands]))

        group.add_argument("--bands-all", help="Retrieve all bands with NULL values where the band is N/A",
                           action="store_const", dest="bands", const=BandListType.ALL)

        group.add_argument("--bands-common", help="Retrieve only bands in common across all satellites",
                           action="store_const", dest="bands", const=BandListType.COMMON)

        self.parser.set_defaults(bands=BandListType.ALL)

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        workflow.Workflow.process_arguments(self, args)

        self.dataset_type = args.dataset_type

        # self.bands = args.bands

        # # Verify that all the requested satellites have the requested bands
        #
        # for satellite in self.satellites:
        #     if not all(item in [b.name for b in get_bands(self.dataset_type, satellite)] for item in self.bands):
        #         _log.error("Requested bands [%s] not ALL present for satellite [%s]", self.bands, satellite)
        #         raise Exception("Not all bands present for all satellites")

        if args.bands == BandListType.ALL:
            self.bands = get_band_name_union(self.dataset_type, self.satellites)
        else:
            self.bands = get_band_name_intersection(self.dataset_type, self.satellites)

    def log_arguments(self):

        # Call method on super class
        # super(self.__class__, self).log_arguments()
        workflow.Workflow.log_arguments(self)

        # _log.info("""
        # dataset to retrieve = {dataset_type}
        # bands = {bands}
        # """.format(dataset_type=self.dataset_type.name, bands=self.bands))

        _log.info("""
        dataset to retrieve = {dataset_type}
        bands to retrieve = {bands}
        """.format(dataset_type=self.dataset_type.name, bands=self.bands))

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

    @abc.abstractmethod
    def create_cell_tasks(self, x, y):

        raise Exception("Abstract method should be overridden")


class CellTask(workflow.CellTask):

    __metaclass__ = abc.ABCMeta

    dataset_type = luigi.Parameter()
    bands = luigi.Parameter(is_list=True)

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

    mask_wofs_apply = luigi.BooleanParameter()
    mask_wofs_mask = luigi.Parameter()

    dataset_type = luigi.Parameter()
    band = luigi.Parameter()

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

        dataset_types = [self.dataset_type]

        if self.mask_pqa_apply:
            dataset_types.append(DatasetType.PQ25)

        if self.mask_wofs_apply:
            dataset_types.append(DatasetType.WATER)

        for tile in list_tiles(x=x_list, y=y_list, acq_min=self.acq_min, acq_max=self.acq_max,
                               satellites=[satellite for satellite in self.satellites],
                               dataset_types=dataset_types):
            yield tile
