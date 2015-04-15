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


import luigi
import logging
import os
from datacube.api.model import DatasetType
from datacube.api.workflow.cell_dataset_band_chunk import Workflow, SummaryTask, CellTask, CellDatasetBandTask
from datacube.api.workflow.cell_dataset_band_chunk import CellDatasetBandChunkTask


_log = logging.getLogger()


class PixelStatisticsWorkflow(Workflow):

    def get_supported_dataset_types(self):
        return [DatasetType.ARG25, DatasetType.FC25]

    def __init__(self):

        Workflow.__init__(self, name="Pixel Statistics Workflow")

    def create_summary_tasks(self):

        return [PixelStatisticsSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                               acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                               output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                               mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                               dataset_type=self.dataset_type, bands=self.bands,
                                               chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y)]


class PixelStatisticsSummaryTask(SummaryTask):

    def create_cell_task(self, x, y):

        return PixelStatisticsCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max,
                                           satellites=self.satellites,
                                           output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                           mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                           dataset_type=self.dataset_type, bands=self.bands,
                                           chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y)


class PixelStatisticsCellTask(CellTask):

    def create_cell_dataset_band_task(self, band):

        return PixelStatisticsCellDatasetBandTask(x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                                      satellites=self.satellites,
                                                      output_directory=self.output_directory, csv=self.csv,
                                                      dummy=self.dummy,
                                                      mask_pqa_apply=self.mask_pqa_apply,
                                                      mask_pqa_mask=self.mask_pqa_mask,
                                                      dataset_type=self.dataset_type, band=band,
                                                      chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y)


class PixelStatisticsCellDatasetBandTask(CellDatasetBandTask):

    def create_cell_dataset_band_chunk_task(self, x_offset, y_offset):
        
        return PixelStatisticsCellDatasetBandChunkTask(x=self.x, y=self.y, acq_min=self.acq_min,
                                                           acq_max=self.acq_max,
                                                           satellites=self.satellites,
                                                           output_directory=self.output_directory, csv=self.csv,
                                                           dummy=self.dummy,
                                                           mask_pqa_apply=self.mask_pqa_apply,
                                                           mask_pqa_mask=self.mask_pqa_mask,
                                                           dataset_type=self.dataset_type, band=self.band,
                                                           chunk_size_x=self.chunk_size_x,
                                                           chunk_size_y=self.chunk_size_y,
                                                           x_offset=x_offset, y_offset=y_offset)

    def output(self):

        from datacube.api.workflow import format_date
        from datacube.api.utils import get_satellite_string

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        filename = "{satellites}_OUTPUT_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{band}.tif".format(
            satellites=get_satellite_string(self.satellites), x=self.x, y=self.y, acq_min=acq_min, acq_max=acq_max,
            band=self.band
        )

        filename = os.path.join(self.output_directory, filename)

        return luigi.LocalTarget(filename)

    def run(self):

        print "****", self.output().path

        # For now just create an empty output
        from datacube.api.workflow import dummy
        dummy(self.output().path)


class PixelStatisticsCellDatasetBandChunkTask(CellDatasetBandChunkTask):

    def output(self):

        from datacube.api.workflow import format_date
        from datacube.api.utils import get_satellite_string

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        filename = "{satellites}_OUTPUT_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{band}_{ulx:04d}_{uly:04d}_{lrx:04d}_{lry:04d}.tif".format(
            satellites=get_satellite_string(self.satellites), x=self.x, y=self.y, acq_min=acq_min, acq_max=acq_max,
            band=self.band,
            ulx=self.x_offset, uly=self.y_offset,
            lrx=(self.x_offset + self.chunk_size_x),
            lry=(self.y_offset + self.chunk_size_y)
        )

        filename = os.path.join(self.output_directory, filename)

        return luigi.LocalTarget(filename)

    def run(self):

        print "****", self.output().path

        # For now just create an empty output
        from datacube.api.workflow import dummy
        dummy(self.output().path)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    PixelStatisticsWorkflow().run()