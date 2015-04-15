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
import numpy
import os
from datacube.api.model import DatasetType, Ls57Arg25Bands
from datacube.api.utils import get_dataset_data_with_pq, raster_create, get_dataset_metadata, \
    calculate_tassel_cap_index, TCI_COEFFICIENTS, TasselCapIndex
from datacube.config import Config
import gdal
from datacube.api.raster.utils import raster_get_data, raster_get_metadata
from datacube.api.workflow import Workflow, SummaryTask, CellTask, dummy


_log = logging.getLogger()


class WetnessSummaryTask(SummaryTask):
    def create_cell_task(self, x, y):
        return WetnessCellTask(x=x, y=y, year_min=self.year_min, year_max=self.year_max,
                               satellites=self.satellites, output_directory=self.output_directory, csv=self.csv,
                               dummy=self.dummy)


class WetnessCellTask(CellTask):
    def get_output_paths(self):
        return [os.path.join(
            self.output_directory,
            "LS_WETNESS_{x:03d}_{y:04d}_{year_min:04d}_{year_max:04d}.tif".format(x=self.x, y=self.y,
                                                                                  year_min=self.year_min,
                                                                                  year_max=self.year_max))]

    # # TODO - how to deal with the tile list requirements from the the CSV option????
    #
    # def requires(self):
    #
    #     # Require a wetness dataset for each tile
    #
    #     config = Config(os.path.expanduser("~/.datacube/config"))
    #     _log.debug(config.to_str())
    #
    #     # for tile in self.get_tiles():
    #     #     yield WetnessTileTask(x=tile.x, y=tile.y, output_directory=self.output_directory, arg=tile.datasets[DatasetType.ARG25],
    #     #                           pqa=tile.datasets[DatasetType.PQ25], dummy=self.dummy)
    #
    #     return [WetnessTileTask(x=tile.x, y=tile.y, output_directory=self.output_directory,
    #                             arg=tile.datasets[DatasetType.ARG25],
    #                             pqa=tile.datasets[DatasetType.PQ25], dummy=self.dummy) for tile in self.get_tiles()]

    def doit(self):

        # TODO this is working around the issue of it blowing up?

        # First create the wetness output for each tile
        for tile in self.get_tiles():
            self.create_water_tile(tile)

        ########

        bands = []
        metadata = None

        for water_tile in self.input():
            _log.debug("Processing %s", water_tile.path)

            if not metadata:
                metadata = raster_get_metadata(water_tile.path, 1)
            bands.append(raster_get_data(water_tile.path, 1))

        data = numpy.array([band for band in bands])

        _log.info("Creating summary raster %s", self.output().path)
        raster_create(self.output().path,
                      [numpy.nanmin(data, axis=0), numpy.nanmax(data, axis=0), numpy.nanmean(data, axis=0)],
                      metadata.transform, metadata.projection, numpy.nan, gdal.GDT_Float32)

    def create_water_tile(self, tile):
        arg = tile.datasets[DatasetType.ARG25]
        pqa = tile.datasets[DatasetType.PQ25]

        _log.info("ARG tile [%s]", arg)
        _log.info("PQ tile [%s]", pqa)

        filename = os.path.basename(arg.path)
        filename = filename.replace("NBAR", "WETNESS")
        filename = filename.replace(".vrt", ".tif")
        filename = os.path.join(self.output_directory, filename)

        metadata = get_dataset_metadata(arg)

        data = get_dataset_data_with_pq(arg, Ls57Arg25Bands, pqa)

        # Calculate TCI Wetness

        tci = calculate_tassel_cap_index(data,
                                         coefficients=TCI_COEFFICIENTS[arg.satellite][TasselCapIndex.WETNESS])

        _log.info("TCI shape is %s | min = %s | max = %s", numpy.shape(tci), tci.min(), tci.max())
        raster_create(filename,
                      [tci],
                      metadata.transform, metadata.projection, numpy.nan, gdal.GDT_Float32)

# class WetnessTileTask(luigi.Task):
#     x = luigi.IntParameter()
#     y = luigi.IntParameter()
#
#     output_directory = luigi.Parameter()
#
#     arg = luigi.Parameter()
#     pqa = luigi.Parameter()
#
#     dummy = luigi.BooleanParameter()
#
#     # def requires(self):
#     #     return [luigi.LocalTarget(self.arg), luigi.LocalTarget(self.pqa)]
#
#     def output(self):
#         filename = os.path.basename(self.arg.path)
#         filename = filename.replace("NBAR", "WETNESS")
#         filename = filename.replace(".vrt", ".tif")
#         return luigi.LocalTarget(os.path.join(self.output_directory, filename))
#
#     def run(self):
#
#         if self.dummy:
#             dummy(self.output().path)
#
#         else:
#             self.doit()
#
#     def doit(self):
#         _log.info("ARG tile [%s]", self.arg.path)
#
#         _log.info("PQ tile [%s]", self.pqa.path)
#
#         metadata = get_dataset_metadata(self.arg)
#
#         data = get_dataset_data_with_pq(self.arg, Arg25Bands, self.pqa)
#
#         # Calculate TCI Wetness
#
#         tci = calculate_tassel_cap_index(data,
#                                          coefficients=TCI_COEFFICIENTS[self.arg.satellite][TasselCapIndex.WETNESS])
#
#         _log.info("TCI shape is %s | min = %s | max = %s", numpy.shape(tci), tci.min(), tci.max())
#         raster_create(self.output().path,
#                       [tci],
#                       metadata.transform, metadata.projection, numpy.nan, gdal.GDT_Float32)


class WetnessWorkflow(Workflow):
    def __init__(self):
        Workflow.__init__(self, application_name="Wetness in the Landscape")

    def create_summary_tasks(self):
        return [WetnessSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                   year_min=self.year_min, year_max=self.year_max, satellites=self.satellites,
                                   output_directory=self.output_directory, csv=self.csv, dummy=self.dummy)]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    WetnessWorkflow().run()
