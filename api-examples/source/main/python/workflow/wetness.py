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
# documentation and/or other materials provided with the distribution.
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