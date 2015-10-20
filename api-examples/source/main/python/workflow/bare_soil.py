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
import logging
import luigi
import numpy
import os
from datacube.api.model import DatasetType, Fc25Bands, Ls57Arg25Bands, Satellite
from datacube.api.utils import NDV, empty_array, get_mask_pqa, get_dataset_data_masked, calculate_ndvi, get_mask_wofs
from datacube.api.utils import propagate_using_selected_pixel, get_dataset_metadata, raster_create, date_to_integer
from datacube.api.workflow.cell import Workflow, SummaryTask, CellTask


_log = logging.getLogger()


class BareSoilWorkflow(Workflow):

    def __init__(self):

        Workflow.__init__(self, name="Bare Soil Workflow")

    def create_summary_tasks(self):

        return [BareSoilSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                    acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                    output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                    mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                    mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask)]


class BareSoilSummaryTask(SummaryTask):

    def create_cell_tasks(self, x, y):

        return BareSoilCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask)


class BareSoilCellTask(CellTask):

    @staticmethod
    def get_dataset_types():

        return [DatasetType.ARG25, DatasetType.PQ25, DatasetType.FC25, DatasetType.NDVI]

    def output(self):

        def get_filenames():
            return [self.get_dataset_filename(d) for d in ["FC", "NBAR", "SAT", "DATE"]]

        return [luigi.LocalTarget(filename) for filename in get_filenames()]

    def get_dataset_filename(self, dataset):

        from datacube.api.workflow import format_date
        from datacube.api.utils import get_satellite_string

        satellites = get_satellite_string(self.satellites)

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        return os.path.join(self.output_directory,
            "{satellites}_{dataset}_{x:03d}_{y:04d}_{acq_min}_{acq_max}.tif".format(
            satellites=satellites,
            dataset=dataset,
            x=self.x, y=self.y,
            acq_min=acq_min,
            acq_max=acq_max))

    def run(self):

        shape = (4000, 4000)
        no_data_value = NDV

        best_pixel_fc = dict()

        for band in Fc25Bands:
            # best_pixel_fc[band] = empty_array(shape=shape, dtype=numpy.int16, ndv=INT16_MIN)
            best_pixel_fc[band] = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)

        best_pixel_nbar = dict()

        for band in Ls57Arg25Bands:
            best_pixel_nbar[band] = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)

        best_pixel_satellite = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)
        best_pixel_date = empty_array(shape=shape, dtype=numpy.int32, ndv=NDV)

        current_satellite = empty_array(shape=shape, dtype=numpy.int16, ndv=NDV)
        current_date = empty_array(shape=shape, dtype=numpy.int32, ndv=NDV)

        SATELLITE_DATA_VALUES = {Satellite.LS5: 5, Satellite.LS7: 7, Satellite.LS8: 8}

        metadata_nbar = None
        metadata_fc = None

        for tile in self.get_tiles():

            pqa = tile.datasets[DatasetType.PQ25]
            nbar = tile.datasets[DatasetType.ARG25]
            fc = tile.datasets[DatasetType.FC25]
            wofs = DatasetType.WATER in tile.datasets and tile.datasets[DatasetType.WATER] or None

            _log.info("Processing [%s]", fc.path)

            data = dict()

            # Create an initial "no mask" mask

            mask = numpy.ma.make_mask_none((4000, 4000))
            # _log.info("### mask is [%s]", mask[1000][1000])

            # Add the PQA mask if we are doing PQA masking

            if self.mask_pqa_apply:
                mask = get_mask_pqa(pqa, pqa_masks=self.mask_pqa_mask, mask=mask)
                # _log.info("### mask PQA is [%s]", mask[1000][1000])

            # Add the WOFS mask if we are doing WOFS masking

            if self.mask_wofs_apply and wofs:
                mask = get_mask_wofs(wofs, wofs_masks=self.mask_wofs_mask, mask=mask)
                # _log.info("### mask PQA is [%s]", mask[1000][1000])

            # Get NBAR dataset

            data[DatasetType.ARG25] = get_dataset_data_masked(nbar, mask=mask)
            # _log.info("### NBAR/RED is [%s]", data[DatasetType.ARG25][Ls57Arg25Bands.RED][1000][1000])

            # Get the NDVI dataset

            data[DatasetType.NDVI] = calculate_ndvi(data[DatasetType.ARG25][Ls57Arg25Bands.RED],
                                                    data[DatasetType.ARG25][Ls57Arg25Bands.NEAR_INFRARED])
            # _log.info("### NDVI is [%s]", data[DatasetType.NDVI][1000][1000])

            # Add the NDVI value range mask (to the existing mask)

            mask = self.get_mask_range(data[DatasetType.NDVI], min_val=0.0, max_val=0.3, mask=mask)
            # _log.info("### mask NDVI is [%s]", mask[1000][1000])

            # Get FC25 dataset

            data[DatasetType.FC25] = get_dataset_data_masked(fc, mask=mask)
            # _log.info("### FC/BS is [%s]", data[DatasetType.FC25][Fc25Bands.BARE_SOIL][1000][1000])

            # Add the bare soil value range mask (to the existing mask)

            mask = self.get_mask_range(data[DatasetType.FC25][Fc25Bands.BARE_SOIL], min_val=0, max_val=8000, mask=mask)
            # _log.info("### mask BS is [%s]", mask[1000][1000])

            # Apply the final mask to the FC25 bare soil data

            data_bare_soil = numpy.ma.MaskedArray(data=data[DatasetType.FC25][Fc25Bands.BARE_SOIL], mask=mask).filled(NDV)
            # _log.info("### bare soil is [%s]", data_bare_soil[1000][1000])

            # Compare the bare soil value from this dataset to the current "best" value

            best_pixel_fc[Fc25Bands.BARE_SOIL] = numpy.fmax(best_pixel_fc[Fc25Bands.BARE_SOIL], data_bare_soil)
            # _log.info("### best pixel bare soil is [%s]", best_pixel_fc[Fc25Bands.BARE_SOIL][1000][1000])

            # Now update the other best pixel datasets/bands to grab the pixels we just selected

            for band in Ls57Arg25Bands:
                best_pixel_nbar[band] = propagate_using_selected_pixel(best_pixel_fc[Fc25Bands.BARE_SOIL],
                                                                       data_bare_soil,
                                                                       data[DatasetType.ARG25][band],
                                                                       best_pixel_nbar[band])

            for band in [Fc25Bands.PHOTOSYNTHETIC_VEGETATION, Fc25Bands.NON_PHOTOSYNTHETIC_VEGETATION, Fc25Bands.UNMIXING_ERROR]:
                best_pixel_fc[band] = propagate_using_selected_pixel(best_pixel_fc[Fc25Bands.BARE_SOIL],
                                                                     data_bare_soil,
                                                                     data[DatasetType.FC25][band],
                                                                     best_pixel_fc[band])

            # And now the other "provenance" data

            # Satellite "provenance" data

            current_satellite.fill(SATELLITE_DATA_VALUES[fc.satellite])

            best_pixel_satellite = propagate_using_selected_pixel(best_pixel_fc[Fc25Bands.BARE_SOIL],
                                                                  data_bare_soil,
                                                                  current_satellite,
                                                                  best_pixel_satellite)

            # Date "provenance" data

            current_date.fill(date_to_integer(tile.end_datetime))

            best_pixel_date = propagate_using_selected_pixel(best_pixel_fc[Fc25Bands.BARE_SOIL],
                                                             data_bare_soil,
                                                             current_date,
                                                             best_pixel_date)

            # Grab the metadata from the input datasets for use later when creating the output datasets

            if not metadata_nbar:
                metadata_nbar = get_dataset_metadata(nbar)

            if not metadata_fc:
                metadata_fc = get_dataset_metadata(fc)

        # Create the output datasets

        # FC composite

        raster_create(self.get_dataset_filename("FC"),
                      [best_pixel_fc[b] for b in Fc25Bands],
                      metadata_fc.transform, metadata_fc.projection,
                      metadata_fc.bands[Fc25Bands.BARE_SOIL].no_data_value,
                      metadata_fc.bands[Fc25Bands.BARE_SOIL].data_type)

        # NBAR composite

        raster_create(self.get_dataset_filename("NBAR"),
                      [best_pixel_nbar[b] for b in Ls57Arg25Bands],
                      metadata_nbar.transform, metadata_nbar.projection,
                      metadata_nbar.bands[Ls57Arg25Bands.BLUE].no_data_value,
                      metadata_nbar.bands[Ls57Arg25Bands.BLUE].data_type)

        # Satellite "provenance" composites

        raster_create(self.get_dataset_filename("SAT"),
                      [best_pixel_satellite],
                      metadata_nbar.transform, metadata_nbar.projection, no_data_value,
                      gdal.GDT_Int16)

        # Date "provenance" composites

        raster_create(self.get_dataset_filename("DATE"),
                      [best_pixel_date],
                      metadata_nbar.transform, metadata_nbar.projection, no_data_value,
                      gdal.GDT_Int32)

    @staticmethod
    def get_mask_range(input_data, min_val, max_val, mask=None, ndv=NDV):

        # Create an empty mask if none provided - just to avoid an if below :)
        if mask is None:
            mask = numpy.ma.make_mask_none(numpy.shape(input_data))

        # Mask out any no data values
        data = numpy.ma.masked_equal(input_data, ndv)

        # Mask out values outside the given range
        mask = numpy.ma.mask_or(mask, numpy.ma.masked_outside(data, min_val, max_val, copy=False).mask)

        return mask


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    BareSoilWorkflow().run()