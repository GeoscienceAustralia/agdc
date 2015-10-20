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
from datacube.api.query import SortType


__author__ = "Simon Oldfield"


import calendar
import logging
import os
import gdal
import numpy
from datacube.api.model import DatasetType, Ls57Arg25Bands, Satellite, Ls8Arg25Bands
from datacube.api.utils import NDV, empty_array, get_dataset_metadata, get_dataset_data_with_pq, raster_create, \
    get_dataset_data
from datacube.api.workflow import SummaryTask, CellTask, Workflow


_log = logging.getLogger()


class LandsatMosaicSummaryTask(SummaryTask):
    def create_cell_tasks(self, x, y):
        return LandsatMosaicCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max,
                                     satellites=self.satellites, output_directory=self.output_directory, csv=self.csv,
                                     dummy=self.dummy, save_input_files=self.save_input_files, apply_pq_filter=self.apply_pq_filter)


class LandsatMosaicCellTask(CellTask):
    def get_output_paths(self):
        return [self.get_output_path(dataset=dataset)
                #for dataset in ["NBAR", "SAT", "EPOCH"]]
                for dataset in ["NBAR", "SAT", "DATE"]]

    def get_output_path(self, dataset):
        return os.path.join(self.output_directory,
                            "LS_{dataset}_{x:03d}_{y:04d}_{acq_min}_{acq_max}.tif".format(dataset=dataset, x=self.x,
                                                                                          y=self.y,
                                                                                          acq_min=self.acq_min,
                                                                                          acq_max=self.acq_max))

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

        best_pixel_satellite = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)
        # best_pixel_epoch = empty_array(shape=shape, dtype=numpy.int32, ndv=NDV)
        best_pixel_date = empty_array(shape=shape, dtype=numpy.int32, ndv=NDV)

        current_satellite = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)
        # current_epoch = empty_array(shape=shape, dtype=numpy.int32, ndv=NDV)
        current_date = empty_array(shape=shape, dtype=numpy.int32, ndv=NDV)

        metadata = None

        SATELLITE_DATA_VALUES = {Satellite.LS5: 5, Satellite.LS7: 7, Satellite.LS8: 8}

        for tile in self.get_tiles(sort=SortType.DESC):
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

            # Create the provenance datasets

            # NOTE: need to do this BEFORE selecting the pixel since it is actually using the fact that the
            # selected pixel currently doesn't have a value

            # NOTE: band values are propagated "as a job lot" so can just check any band

            # TODO better way than just saying....RED....?
            band = bands.RED

            # Satellite

            current_satellite.fill(SATELLITE_DATA_VALUES[dataset.satellite])
            best_pixel_satellite = numpy.where(best_pixel_data[band] == no_data_value, current_satellite, best_pixel_satellite)

            # # Epoch dataset
            #
            # current_epoch.fill(calendar.timegm(tile.end_datetime.timetuple()))
            # best_pixel_epoch = numpy.where(best_pixel_data[band] == no_data_value, current_epoch, best_pixel_epoch)

            # Date dataset (20150101)

            current_date.fill(tile.end_datetime.year * 10000 + tile.end_datetime.month * 100 + tile.end_datetime.day)
            best_pixel_date = numpy.where(best_pixel_data[band] == no_data_value, current_date, best_pixel_date)

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

        # Now want to mask out values in the provenance datasets if we haven't actually got a value

        # TODO better way than just saying....RED....?
        band = bands.RED

        mask = numpy.ma.masked_equal(best_pixel_data[band], NDV).mask

        best_pixel_satellite = numpy.ma.array(best_pixel_satellite, mask=mask).filled(NDV)
        # best_pixel_epoch = numpy.ma.array(best_pixel_epoch, mask=mask).fill(NDV)
        best_pixel_date = numpy.ma.array(best_pixel_date, mask=mask).filled(NDV)

        # Composite NBAR dataset

        raster_create(self.get_output_path("NBAR"), [best_pixel_data[b] for b in bands],
                      metadata.transform, metadata.projection, NDV, gdal.GDT_Int16)

        # Provenance (satellite) dataset

        raster_create(self.get_output_path("SAT"),
                      [best_pixel_satellite],
                      metadata.transform, metadata.projection, no_data_value,
                      gdal.GDT_Int16)

        # # Provenance (epoch) dataset
        #
        # raster_create(self.get_output_path("EPOCH"),
        #               [best_pixel_epoch],
        #               metadata.transform, metadata.projection, no_data_value,
        #               gdal.GDT_Int32)

        # Provenance (day of month) dataset

        raster_create(self.get_output_path("DATE"),
                      [best_pixel_date],
                      metadata.transform, metadata.projection, no_data_value,
                      gdal.GDT_Int32)


class LandsatMosaicWorkflow(Workflow):
    def __init__(self):
        Workflow.__init__(self, application_name="Landsat Mosaic")

    def create_summary_tasks(self):
        return [LandsatMosaicSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                         acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                         output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                         save_input_files=self.save_input_files, apply_pq_filter=self.apply_pq_filter)]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    LandsatMosaicWorkflow().run()