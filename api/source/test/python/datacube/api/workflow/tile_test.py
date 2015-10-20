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
from datacube.api.workflow.tile import Workflow, SummaryTask, CellTask, TileTask


class TileTestWorkflow(Workflow):

    def __init__(self):

        Workflow.__init__(self, name="Dataset Workflow Test")

    def create_summary_tasks(self):

        return [TileTestSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                    acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                    output_directory=self.output_directory,
                                    csv=self.csv, dummy=self.dummy,
                                    mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                    mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask)]


class TileTestSummaryTask(SummaryTask):

    def create_cell_tasks(self, x, y):

        return TileTestCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                output_directory=self.output_directory,
                                csv=self.csv, dummy=self.dummy,
                                mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask)


class TileTestCellTask(CellTask):

    def create_tile_tasks(self, tile):

        return TileTestTileTask(tile=tile, x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                satellites=self.satellites,
                                output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask)


class TileTestTileTask(TileTask):

    def output(self):

        filename = self.tile.datasets[DatasetType.ARG25].path

        filename = os.path.basename(filename)

        filename = filename.replace("_NBAR_", "_TEST_")
        filename = filename.replace(".vrt", ".tif")
        filename = filename.replace(".tiff", ".tif")

        filename = os.path.join(self.output_directory, filename)

        return luigi.LocalTarget(filename)

    def run(self):

        # NBAR is self.tile.datasets[DatasetType.ARG25]
        # PQA  is self.tile.datasets[DatasetType.PQ25]
        # FC  is self.tile.datasets[DatasetType.FC25]

        # get data with get_dataset_data() ...

        print "****", self.output().path

        # For now just create an empty output
        from datacube.api.workflow import dummy
        dummy(self.output().path)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    TileTestWorkflow().run()