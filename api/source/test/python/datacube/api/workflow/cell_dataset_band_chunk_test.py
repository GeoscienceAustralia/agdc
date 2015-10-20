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


import luigi
import logging
import os
from datacube.api.model import DatasetType
from datacube.api.workflow.cell_dataset_band_chunk import Workflow, SummaryTask, CellTask, CellDatasetBandTask
from datacube.api.workflow.cell_dataset_band_chunk import CellDatasetBandChunkTask


_log = logging.getLogger()


class CellDatasetBandTestWorkflow(Workflow):

    def get_supported_dataset_types(self):
        return [DatasetType.ARG25, DatasetType.FC25]

    def __init__(self):

        Workflow.__init__(self, name="Cell Chunk Workflow Test")

    def create_summary_tasks(self):

        return [CellDatasetBandTestSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                               acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                               output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                               mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                               dataset_type=self.dataset_type, bands=self.bands,
                                               chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y)]


class CellDatasetBandTestSummaryTask(SummaryTask):

    def create_cell_tasks(self, x, y):

        return CellDatasetBandTestCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max,
                                           satellites=self.satellites,
                                           output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                           mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                           dataset_type=self.dataset_type, bands=self.bands,
                                           chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y)


class CellDatasetBandTestCellTask(CellTask):

    def create_cell_dataset_band_task(self, band):

        return CellDatasetBandTestCellDatasetBandTask(x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                                      satellites=self.satellites,
                                                      output_directory=self.output_directory, csv=self.csv,
                                                      dummy=self.dummy,
                                                      mask_pqa_apply=self.mask_pqa_apply,
                                                      mask_pqa_mask=self.mask_pqa_mask,
                                                      dataset_type=self.dataset_type, band=band,
                                                      chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y)


class CellDatasetBandTestCellDatasetBandTask(CellDatasetBandTask):

    def create_cell_dataset_band_chunk_task(self, x_offset, y_offset):

        return CellDatasetBandTestCellDatasetBandChunkTask(x=self.x, y=self.y, acq_min=self.acq_min,
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


class CellDatasetBandTestCellDatasetBandChunkTask(CellDatasetBandChunkTask):

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

    CellDatasetBandTestWorkflow().run()