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
import calendar
import numpy
import gdal
from gdalconst import GDT_Int16
from datacube.api.model import DatasetType, Fc25Bands, Ls57Arg25Bands, Satellite, Pq25Bands
from datacube.api.utils import NDV, empty_array, INT16_MIN, get_dataset_data, get_pq_mask, get_dataset_metadata, \
    get_dataset_data_with_pq, calculate_ndvi, propagate_using_selected_pixel, raster_create, PQA_MASK, PQ_MASK_CLEAR, \
    PQ_MASK_SATURATION_OPTICAL, PQ_MASK_SATURATION_THERMAL, PQ_MASK_CONTIGUITY, PQ_MASK_LAND, PQ_MASK_CLOUD_ACCA, \
    PQ_MASK_CLOUD_FMASK, PQ_MASK_CLOUD_SHADOW_ACCA, PQ_MASK_CLOUD_SHADOW_FMASK
from datacube.api.workflow import Workflow, SummaryTask, CellTask


_log = logging.getLogger()


class ObservationCountSummaryTask(SummaryTask):
    def create_cell_tasks(self, x, y):
        return ObservationCountCellTask(x=x, y=y, year_min=self.year_min, year_max=self.year_max,
                                satellites=self.satellites, output_directory=self.output_directory, csv=self.csv,
                                dummy=self.dummy)


class ObservationCountCellTask(CellTask):
    def get_output_paths(self):
        return [os.path.join(self.output_directory, self.get_dataset_filename("COUNT"))]

    def get_dataset_filename(self, dataset):
        return "LS_{dataset}_{x:03d}_{y:04d}_{year_min:04d}_{year_max:04d}.tif".format(dataset=dataset,
                                                                                       x=self.x, y=self.y,
                                                                                       year_min=self.year_min,
                                                                                       year_max=self.year_max)

    def doit(self):

        shape = (4000, 4000)

        masks = [PQ_MASK_CLEAR,
                 PQ_MASK_SATURATION_OPTICAL,
                 PQ_MASK_SATURATION_THERMAL,
                 PQ_MASK_CONTIGUITY,
                 PQ_MASK_LAND,
                 PQ_MASK_CLOUD_ACCA,
                 PQ_MASK_CLOUD_FMASK,
                 PQ_MASK_CLOUD_SHADOW_ACCA,
                 PQ_MASK_CLOUD_SHADOW_FMASK]

        observation_count = empty_array(shape=shape, dtype=numpy.int16, ndv=0)

        observation_count_clear = dict()

        for mask in masks:
            observation_count_clear[mask] = empty_array(shape=shape, dtype=numpy.int16, ndv=0)

        metadata = None

        for tile in self.get_tiles():

            # Get the PQ mask

            pq = tile.datasets[DatasetType.PQ25]
            data = get_dataset_data(pq, [Pq25Bands.PQ])[Pq25Bands.PQ]

            #
            # Count any pixels that are no NDV - don't think we should actually have any but anyway
            #

            # Mask out any no data pixels - should actually be none but anyway
            pq = numpy.ma.masked_equal(data, NDV)

            # Count the data pixels - i.e. pixels that were NOT masked out
            observation_count += numpy.where(data.mask, 0, 1)

            #
            # Count and pixels that are not masked due to pixel quality
            #

            for mask in masks:
                # Apply the particular pixel mask
                pqm = numpy.ma.masked_where(numpy.bitwise_and(data, mask) != mask, data)

                # Count the pixels that were not masked out
                observation_count_clear[mask] += numpy.where(pqm.mask, 0, 1)

            if not metadata:
                metadata = get_dataset_metadata(pq)

        # Create the output datasets

        # Observation Count

        raster_create(self.output()[0].path, [observation_count] + [observation_count_clear[mask] for mask in masks],
                      metadata.transform, metadata.projection, NDV, GDT_Int16)


class ObservationCountWorkflow(Workflow):
    def __init__(self):
        Workflow.__init__(self, application_name="Observation Count")

    def create_summary_tasks(self):
        return [ObservationCountSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                    year_min=self.year_min, year_max=self.year_max, satellites=self.satellites,
                                    output_directory=self.output_directory, csv=self.csv, dummy=self.dummy)]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    ObservationCountWorkflow().run()