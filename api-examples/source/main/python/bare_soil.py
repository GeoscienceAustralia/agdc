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


import logging
import os
import calendar
import numpy
import gdal
from datacube.api.model import DatasetType, Fc25Bands, Ls57Arg25Bands, Satellite, Pq25Bands
from datacube.api.utils import NDV, empty_array, INT16_MIN, get_dataset_data, get_pq_mask, get_dataset_metadata, \
    get_dataset_data_with_pq, calculate_ndvi, propagate_using_selected_pixel, raster_create
from datacube.api.workflow import Workflow, SummaryTask, CellTask


_log = logging.getLogger()


class BareSoilSummaryTask(SummaryTask):
    def create_cell_tasks(self, x, y):
        _log.debug("BareSoilSummaryTask.create_cell_task()")
        return BareSoilCellTask(x=x, y=y, year_min=self.year_min, year_max=self.year_max,
                                satellites=self.satellites, output_directory=self.output_directory, csv=self.csv,
                                dummy=self.dummy)


class BareSoilCellTask(CellTask):
    def get_output_paths(self):
        _log.debug("BareSoilCellTask.get_output_paths()")
        return [os.path.join(self.output_directory, f) for f in
                [self.get_dataset_filename(dataset) for dataset in ["FC", "NBAR", "SAT", "YEAR", "MONTH", "EPOCH"]]]

    def get_dataset_filename(self, dataset):
        return "LS_{dataset}_{x:03d}_{y:04d}_{year_min:04d}_{year_max:04d}.tif".format(dataset=dataset,
                                                                                       x=self.x, y=self.y,
                                                                                       year_min=self.year_min,
                                                                                       year_max=self.year_max)

    def doit(self):

        _log.debug("Bare Soil Cell Task - doit()")
        shape = (4000, 4000)
        no_data_value = NDV

        best_pixel_fc = dict()

        for band in Fc25Bands:
            best_pixel_fc[band] = empty_array(shape=shape, dtype=numpy.int16, ndv=INT16_MIN)

        best_pixel_nbar = dict()

        for band in Ls57Arg25Bands:
            best_pixel_nbar[band] = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)

        best_pixel_satellite = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)
        best_pixel_year = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)
        best_pixel_month = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)
        best_pixel_epoch = empty_array(shape=shape, dtype=numpy.int32, ndv=NDV)

        current_satellite = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)
        current_year = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)
        current_month = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)
        current_epoch = empty_array(shape=shape, dtype=numpy.int32, ndv=NDV)

        SATELLITE_DATA_VALUES = {Satellite.LS5: 5, Satellite.LS7: 7, Satellite.LS8: 8}

        metadata_nbar = None
        metadata_fc = None

        for tile in self.get_tiles():
            # Get the PQ mask

            pq = tile.datasets[DatasetType.PQ25]
            data_pq = get_dataset_data(pq, [Pq25Bands.PQ])[Pq25Bands.PQ]

            mask_pq = get_pq_mask(data_pq)

            # Get NBAR dataset

            nbar = tile.datasets[DatasetType.ARG25]
            _log.info("Processing NBAR tile [%s]", nbar.path)

            if not metadata_nbar:
                metadata_nbar = get_dataset_metadata(nbar)

            data_nbar = get_dataset_data_with_pq(nbar, Ls57Arg25Bands, tile.datasets[DatasetType.PQ25])

            # Get the NDVI mask

            red = data_nbar[Ls57Arg25Bands.RED]
            nir = data_nbar[Ls57Arg25Bands.NEAR_INFRARED]

            ndvi_data = calculate_ndvi(red, nir)

            ndvi_data = numpy.ma.masked_equal(ndvi_data, NDV)
            ndvi_data = numpy.ma.masked_outside(ndvi_data, 0, 0.3, copy=False)

            mask_ndvi = ndvi_data.mask

            # Get FC25 dataset

            fc = tile.datasets[DatasetType.FC25]
            _log.info("Processing FC tile [%s]", fc.path)

            if not metadata_fc:
                metadata_fc = get_dataset_metadata(fc)

            _log.debug("metadata fc is %s", metadata_fc)

            data_fc = get_dataset_data(fc, Fc25Bands)

            data_bare_soil = data_fc[Fc25Bands.BS]
            data_bare_soil = numpy.ma.masked_equal(data_bare_soil, -999)
            data_bare_soil = numpy.ma.masked_outside(data_bare_soil, 0, 8000)
            data_bare_soil.mask = (data_bare_soil.mask | mask_pq | mask_ndvi)
            data_bare_soil = data_bare_soil.filled(NDV)

            # Compare the bare soil value from this dataset to the current "best" value
            best_pixel_fc[Fc25Bands.BS] = numpy.fmax(best_pixel_fc[Fc25Bands.BS], data_bare_soil)

            # Now update the other best pixel datasets/bands to grab the pixels we just selected

            for band in Ls57Arg25Bands:
                best_pixel_nbar[band] = propagate_using_selected_pixel(best_pixel_fc[Fc25Bands.BS],
                                                                       data_bare_soil,
                                                                       data_nbar[band],
                                                                       best_pixel_nbar[band])

            for band in [Fc25Bands.PV, Fc25Bands.NPV, Fc25Bands.ERROR]:
                best_pixel_fc[band] = propagate_using_selected_pixel(best_pixel_fc[Fc25Bands.BS],
                                                                     data_bare_soil,
                                                                     data_fc[band],
                                                                     best_pixel_fc[band])

            # And now the other "provenance" data

            current_satellite.fill(SATELLITE_DATA_VALUES[fc.satellite])

            best_pixel_satellite = propagate_using_selected_pixel(best_pixel_fc[Fc25Bands.BS],
                                                                  data_bare_soil,
                                                                  current_satellite,
                                                                  best_pixel_satellite)

            current_year.fill(tile.end_datetime_year)

            best_pixel_year = propagate_using_selected_pixel(best_pixel_fc[Fc25Bands.BS],
                                                             data_bare_soil,
                                                             current_year,
                                                             best_pixel_year)

            current_month.fill(tile.end_datetime_month)

            best_pixel_month = propagate_using_selected_pixel(best_pixel_fc[Fc25Bands.BS],
                                                              data_bare_soil,
                                                              current_month,
                                                              best_pixel_month)

            current_epoch.fill(calendar.timegm(tile.end_datetime.timetuple()))

            best_pixel_epoch = propagate_using_selected_pixel(best_pixel_fc[Fc25Bands.BS],
                                                              data_bare_soil,
                                                              current_epoch,
                                                              best_pixel_epoch)

        # Create the output datasets

        # FC composite

        raster_create(self.get_dataset_filename("FC"),
                      [best_pixel_fc[b] for b in Fc25Bands],
                      metadata_fc.transform, metadata_fc.projection, metadata_fc.bands[Fc25Bands.BS].no_data_value,
                      metadata_fc.bands[Fc25Bands.BS].data_type)

        # NBAR composite

        raster_create(self.get_dataset_filename("NBAR"),
                      [best_pixel_nbar[b] for b in Ls57Arg25Bands],
                      metadata_nbar.transform, metadata_nbar.projection,
                      metadata_nbar.bands[Ls57Arg25Bands.BLUE].no_data_value,
                      metadata_nbar.bands[Ls57Arg25Bands.BLUE].data_type)

        # "Provenance" composites

        raster_create(self.get_dataset_filename("SAT"),
                      [best_pixel_satellite],
                      metadata_nbar.transform, metadata_nbar.projection, no_data_value,
                      gdal.GDT_Int16)

        raster_create(self.get_dataset_filename("YEAR"),
                      [best_pixel_year],
                      metadata_nbar.transform, metadata_nbar.projection, no_data_value,
                      gdal.GDT_Int16)

        raster_create(self.get_dataset_filename("MONTH"),
                      [best_pixel_month],
                      metadata_nbar.transform, metadata_nbar.projection, no_data_value,
                      gdal.GDT_Int16)

        raster_create(self.get_dataset_filename("EPOCH"),
                      [best_pixel_epoch],
                      metadata_nbar.transform, metadata_nbar.projection, no_data_value,
                      gdal.GDT_Int32)


class BareSoilWorkflow(Workflow):
    def __init__(self):
        Workflow.__init__(self, application_name="Bare Soil")

    def create_summary_tasks(self):
        _log.debug("BareSoilWorkflow.create_tasks()")
        return [BareSoilSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                    year_min=self.year_min, year_max=self.year_max, satellites=self.satellites,
                                    output_directory=self.output_directory, csv=self.csv, dummy=self.dummy)]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    BareSoilWorkflow().run()