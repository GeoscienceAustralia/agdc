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

    def log_arguments(self):

        # Call method on super class
        # super(self.__class__, self).log_arguments()
        workflow.Workflow.log_arguments(self)

        _log.info("""
        dataset to retrieve = {dataset_type}
        bands = {bands}
        """.format(dataset_type=self.dataset_type.name, bands=self.bands))

    @abc.abstractmethod
    def create_tasks(self):

        raise Exception("Abstract method should be overridden")

    @abc.abstractmethod
    def get_supported_dataset_types(self):

        raise Exception("Abstract method should be overridden")


class SummaryTask(workflow.SummaryTask):

    __metaclass__ = abc.ABCMeta

    dataset_type = luigi.Parameter()
    bands = luigi.Parameter(is_list=True)

    @abc.abstractmethod
    def create_cell_task(self, x, y):

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

    dataset_type = luigi.Parameter()
    band = luigi.Parameter()
