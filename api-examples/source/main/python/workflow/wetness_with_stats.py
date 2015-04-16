#!/usr/bin/env python

# ===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
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
from datacube.api.workflow import TileListCsvTask
from datacube.api.workflow.tile import TileTask
from datacube.api.workflow.cell_chunk import Workflow, SummaryTask, CellTask, CellChunkTask
from enum import Enum


_log = logging.getLogger()


class Statistic(Enum):
    __order__ = "COUNT MIN MAX MEAN MEDIAN SUM STANDARD_DEVIATION VARIANCE PERCENTILE_25 PERCENTILE_50 PERCENTILE_75 PERCENTILE_90 PERCENTILE_95"

    COUNT = "COUNT"
    MIN = "MIN"
    MAX = "MAX"
    MEAN = "MEAN"
    MEDIAN = "MEDIAN"
    SUM = "SUM"
    STANDARD_DEVIATION = "STANDARD_DEVIATION"
    VARIANCE = "VARIANCE"
    PERCENTILE_25 = "PERCENTILE_25"
    PERCENTILE_50 = "PERCENTILE_50"
    PERCENTILE_75 = "PERCENTILE_75"
    PERCENTILE_90 = "PERCENTILE_90"
    PERCENTILE_95 = "PERCENTILE_95"


class WetnessWorkflow(Workflow):

    def __init__(self):

        Workflow.__init__(self, name="Wetness In the Landscape - 2015-04-17")

    def create_summary_tasks(self):

        return [WetnessSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                   acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                   output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                   mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                   mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                   chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y)]


class WetnessSummaryTask(SummaryTask):

    def create_cell_tasks(self, x, y):

        return WetnessCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                               output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                               mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                               mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                               chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y)


class WetnessCellTask(CellTask):

    def create_cell_chunk_task(self, x_offset, y_offset):

        return WetnessCellChunkTask(x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                    satellites=self.satellites,
                                    output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                    mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                    mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                    chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y,
                                    x_offset=x_offset, y_offset=y_offset)

    def output(self):

        from datacube.api.workflow import format_date
        from datacube.api.utils import get_satellite_string

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        filename = "{satellites}_WETNESS_STATISTICS_{x:03d}_{y:04d}_{acq_min}_{acq_max}.tif".format(
            satellites=get_satellite_string(self.satellites), x=self.x, y=self.y, acq_min=acq_min, acq_max=acq_max
        )

        filename = os.path.join(self.output_directory, filename)

        return luigi.LocalTarget(filename)

    def run(self):

        # For now just create an empty output

        from luigi.task import flatten
        from datacube.api.workflow import dummy

        for output in flatten(self.output()):
            print "****", output.path
            dummy(output.path)


class WetnessCellChunkTask(CellChunkTask):

    def requires(self):

        if self.csv:
            yield TileListCsvTask(x_min=self.x, x_max=self.x, y_min=self.y, y_max=self.y,
                                  acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                  dataset_types=self.get_dataset_types(), path=self.get_tile_csv_filename())

        yield [self.create_tile_tasks(tile=tile) for tile in self.get_tiles()]

    def create_tile_tasks(self, tile):

        return WetnessTileTask(tile=tile, x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                               satellites=self.satellites,
                               output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                               mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                               mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask)

    @staticmethod
    def get_dataset_types():

        return [DatasetType.TCI]

    def output(self):

        from datacube.api.workflow import format_date
        from datacube.api.utils import get_satellite_string

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        def get_filename(s):
            filename = "{satellites}_WETNESS_{statistic}_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{ulx:04d}_{uly:04d}_{lrx:04d}_{lry:04d}.npy".format(
                satellites=get_satellite_string(self.satellites), statistic=stat.name,
                x=self.x, y=self.y, acq_min=acq_min, acq_max=acq_max,
                ulx=self.x_offset, uly=self.y_offset,
                lrx=(self.x_offset + self.chunk_size_x),
                lry=(self.y_offset + self.chunk_size_y)
            )
            return os.path.join(self.output_directory, filename)

        return [luigi.LocalTarget(get_filename(stat)) for stat in Statistic]

    def run(self):

        # For now just create an empty output

        from luigi.task import flatten
        from datacube.api.workflow import dummy

        for output in flatten(self.output()):
            print "****", output.path
            dummy(output.path)


class WetnessTileTask(TileTask):

    def output(self):

        filename = self.tile.datasets[DatasetType.TCI].path

        filename = os.path.basename(filename)

        filename = filename.replace("_NBAR_", "_WETNESS_")
        filename = filename.replace(".vrt", ".tif")
        filename = filename.replace(".tiff", ".tif")

        filename = os.path.join(self.output_directory, filename)

        return luigi.LocalTarget(filename)

    def run(self):

        # For now just create an empty output

        from luigi.task import flatten
        from datacube.api.workflow import dummy

        for output in flatten(self.output()):
            print "****", output.path
            dummy(output.path)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    WetnessWorkflow().run()