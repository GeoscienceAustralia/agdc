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
# * Neither Geoscience Australia nor the names of its contributors may be
# used to endorse or promote products derived from this software
# without specific prior written permission.
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

import logging
import os
import csv
import gdal
import numpy
from datacube.api.model import DatasetType, Ls57Arg25Bands, Tile, Satellite, Ls8Arg25Bands
from datacube.api.utils import NDV, empty_array, get_dataset_metadata, get_dataset_data_with_pq, raster_create, \
    get_dataset_data
from datacube.api.workflow import SummaryTask, CellTask, Workflow


_log = logging.getLogger()


class LandsatMosaicSummaryTask(SummaryTask):
    def create_cell_task(self, x, y):
        return LandsatMosaicCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max,
                                     satellites=self.satellites, output_directory=self.output_directory, csv=self.csv,
                                     dummy=self.dummy, apply_pq_filter=self.apply_pq_filter)


class LandsatMosaicCellTask(CellTask):
    def get_output_paths(self):
        return [os.path.join(self.output_directory,
                             "LS_NBAR_{x:03d}_{y:04d}_{acq_min}_{acq_max}.tif".format(x=self.x, y=self.y,
                                                                                      acq_min=self.acq_min,
                                                                                      acq_max=self.acq_max))]

    def doit(self):
        shape = (4000, 4000)
        no_data_value = NDV

        best_pixel_data = dict()

        # TODO
        if Satellite.LS8.value in self.satellites:
            bands = Ls8Arg25Bands
        else:
            bands = Ls57Arg25Bands

        for band in bands:
            best_pixel_data[band] = empty_array(shape=shape, dtype=numpy.int16, ndv=no_data_value)

        metadata = None

        for tile in self.get_tiles():
            # Get ARG25 dataset

            dataset = tile.datasets[DatasetType.ARG25]
            _log.info("Processing ARG tile [%s]", dataset.path)

            if not metadata:
                metadata = get_dataset_metadata(dataset)

            band_data = None

            if self.apply_pq_filter:
                band_data = get_dataset_data_with_pq(dataset, tile.datasets[DatasetType.PQ25])
            else:
                band_data = get_dataset_data(dataset)

            for band in bands:
                data = band_data[band]
                # _log.debug("data = \n%s", data)

                # Replace any NO DATA best pixels with data pixels
                # TODO should I explicitly do the AND data is not NO DATA VALUE?
                best_pixel_data[band] = numpy.where(best_pixel_data[band] == no_data_value, data, best_pixel_data[band])
                # _log.debug("best pixel = \n%s", best_pixel_data[band])

            still_no_data = numpy.any(numpy.array([best_pixel_data[b] for b in bands]) == no_data_value)
            # _log.debug("still no data pixels = %s", still_no_data)

            if not still_no_data:
                break

        raster_create(self.output()[0].path, [best_pixel_data[b] for b in bands],
                      metadata.transform, metadata.projection, NDV, gdal.GDT_Int16)


class LandsatMosaicWorkflow(Workflow):
    def __init__(self):
        Workflow.__init__(self, application_name="Landsat Mosaic")

    def create_tasks(self):
        return [LandsatMosaicSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                         acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                         output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                         apply_pq_filter=self.apply_pq_filter)]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    LandsatMosaicWorkflow().run()