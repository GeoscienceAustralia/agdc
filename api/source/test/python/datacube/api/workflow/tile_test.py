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
from datacube.api.workflow.tile import Workflow, SummaryTask, CellTask, TileTask


class TileTestWorkflow(Workflow):

    def __init__(self):

        Workflow.__init__(self, name="Dataset Workflow Test")

    def create_summary_tasks(self):

        return [TileTestSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                    acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                    output_directory=self.output_directory,
                                    csv=self.csv, csv_generate=self.csv_generate, dummy=self.dummy,
                                    mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                    mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask)]


class TileTestSummaryTask(SummaryTask):

    def create_cell_task(self, x, y):

        return TileTestCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                output_directory=self.output_directory,
                                csv=self.csv, csv_generate=self.csv_generate, dummy=self.dummy,
                                mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask)


class TileTestCellTask(CellTask):

    def create_tile_task(self, tile):

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