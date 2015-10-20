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


import gdal
import luigi
import logging
import os
from datacube.api.utils import get_dataset_data_with_pq, raster_create, get_dataset_metadata, NDV
from datacube.api.model import DatasetType, Ls57Arg25Bands
from datacube.api.workflow.tile import Workflow, SummaryTask, CellTask, TileTask


class NbarPqaWorkflow(Workflow):

    def __init__(self):

        Workflow.__init__(self, name="NBAR with PQA Workflow Test")

    def create_summary_tasks(self):

        return [NbarPqaSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                   acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                   output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                   mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask)]


class NbarPqaSummaryTask(SummaryTask):

    def create_cell_tasks(self, x, y):

        return NbarPqaCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                               output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                               mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask)


class NbarPqaCellTask(CellTask):

    def create_tile_tasks(self, tile):

        return NbarPqaTileTask(tile=tile, x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                               satellites=self.satellites,
                               output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                               mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask)


class NbarPqaTileTask(TileTask):

    def output(self):

        filename = self.tile.datasets[DatasetType.ARG25].path

        filename = os.path.basename(filename)

        filename = filename.replace("_NBAR_", "_NBAR_PQA_")
        filename = filename.replace(".vrt", ".tif")
        filename = filename.replace(".tiff", ".tif")

        filename = os.path.join(self.output_directory, filename)

        return luigi.LocalTarget(filename)

    def run(self):

        print "****", self.output().path

        metadata = get_dataset_metadata(self.tile.datasets[DatasetType.ARG25])

        data = get_dataset_data_with_pq(self.tile.datasets[DatasetType.ARG25],
                                        self.tile.datasets[DatasetType.PQ25])

        raster_create(self.output().path, [data[b] for b in Ls57Arg25Bands],
                      metadata.transform, metadata.projection, NDV, gdal.GDT_Int16)


if __name__ == '__main__':
    # logging.basicConfig(level=logging.INFO)
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    NbarPqaWorkflow().run()
