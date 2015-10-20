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
import numpy
import os
from datacube.api.model import DatasetType
from datacube.api.workflow.tile import Workflow, SummaryTask, CellTask, TileTask
from datacube.api.utils import NDV, log_mem, get_mask_pqa, get_dataset_data_masked, get_dataset_metadata, \
    calculate_tassel_cap_index, TCI_COEFFICIENTS, TasselCapIndex, raster_create


_log = logging.getLogger()


class WetnessWorkflow(Workflow):

    def __init__(self):

        Workflow.__init__(self, name="Wetness in the Landscape Workflow")

    def create_summary_tasks(self):

        return [WetnessSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                   acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                   output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                   mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask)]


class WetnessSummaryTask(SummaryTask):

    def create_cell_tasks(self, x, y):

        return WetnessCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                               output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                               mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask)


class WetnessCellTask(CellTask):

    def create_tile_tasks(self, tile):

        return WetnessTileTask(tile=tile, x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                               satellites=self.satellites,
                               output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                               mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask)


class WetnessTileTask(TileTask):

    def output(self):

        nbar = self.tile.datasets[DatasetType.ARG25]

        filename = os.path.basename(nbar.path)
        filename = filename.replace("NBAR", "WETNESS")
        filename = filename.replace(".vrt", ".tif")
        filename = os.path.join(self.output_directory, filename)

        return luigi.LocalTarget(filename)

    def run(self):

        ndv = NDV

        nbar = self.tile.datasets[DatasetType.ARG25]

        _log.info("Processing tile [%s]", nbar.path)

        # Apply PQA if specified

        pqa = None

        if self.mask_pqa_apply and DatasetType.PQ25 in self.tile.datasets:
            pqa = self.tile.datasets[DatasetType.PQ25]

        mask = None

        log_mem("Before get PQA mask")

        if pqa:
            mask = get_mask_pqa(pqa, self.mask_pqa_mask)

        data = get_dataset_data_masked(nbar, mask=mask, ndv=ndv)

        log_mem("After get data (masked)")

        metadata = get_dataset_metadata(nbar)

        data = calculate_tassel_cap_index(data, coefficients=TCI_COEFFICIENTS[nbar.satellite][TasselCapIndex.WETNESS])

        raster_create(self.output().path, [data], metadata.transform, metadata.projection, numpy.nan, gdal.GDT_Float32)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    WetnessWorkflow().run()