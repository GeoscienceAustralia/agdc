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
import luigi
import logging


_log = logging.getLogger()


class Workflow(workflow.Workflow):

    __metaclass__ = abc.ABCMeta

    def __init__(self, name):

        # Call method on super class
        # super(self.__class__, self).__init__(name)
        workflow.Workflow.__init__(self, name)

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        workflow.Workflow.setup_arguments(self)

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        workflow.Workflow.process_arguments(self, args)

    def log_arguments(self):

        # Call method on super class
        # super(self.__class__, self).log_arguments()
        workflow.Workflow.log_arguments(self)

    def create_summary_tasks(self):

        raise Exception("Abstract method should be overridden")


class SummaryTask(workflow.SummaryTask):

    @abc.abstractmethod
    def create_cell_tasks(self, x, y):

        raise Exception("Abstract method should be overridden")


class CellTask(workflow.CellTask):

    __metaclass__ = abc.ABCMeta

    def requires(self):

        if self.csv:
            yield workflow.TileListCsvTask(x_min=self.x, x_max=self.x, y_min=self.y, y_max=self.y,
                                           acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                           dataset_types=self.get_dataset_types(), path=self.get_tile_csv_filename())

        # yield [self.create_tile_tasks(tile=tile) for tile in self.get_tiles()]

        for tile in self.get_tiles():
            yield self.create_tile_tasks(tile=tile)

    @abc.abstractmethod
    def create_tile_tasks(self, tile):

        raise Exception("Abstract method should be overridden")


class TileTask(workflow.Task):

    __metaclass__ = abc.ABCMeta

    tile = workflow.ComplexParameter()

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
