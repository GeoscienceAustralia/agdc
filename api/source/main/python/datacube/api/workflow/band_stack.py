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
from datacube.api.utils import get_dataset_metadata, get_dataset_datatype, get_dataset_ndv, get_mask_pqa, get_mask_wofs, \
    get_dataset_data_masked, format_date


__author__ = "Simon Oldfield"


import logging
import luigi
import os
from datacube.api.workflow.cell_dataset_band import Workflow, SummaryTask, CellTask, CellDatasetBandTask
from datacube.api.model import dataset_type_database, dataset_type_derived_nbar, DatasetType


_log = logging.getLogger()


class BandStackWorkflow(Workflow):

    def __init__(self):

        Workflow.__init__(self, name="Band Stack")

    def create_summary_tasks(self):

        return [BandStackSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                     acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                     output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                     mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                     mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                     dataset_type=self.dataset_type, bands=self.bands)]

    def get_supported_dataset_types(self):
        return dataset_type_database + dataset_type_derived_nbar


class BandStackSummaryTask(SummaryTask):

    def create_cell_tasks(self, x, y):

        return BandStackCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                 output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                 mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                 mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                 dataset_type=self.dataset_type, bands=self.bands)


class BandStackCellTask(CellTask):

    def create_cell_dataset_band_task(self, band):

        return BandStackCellDatasetBandTask(x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                            satellites=self.satellites,
                                            output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                            mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                            mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                            dataset_type=self.dataset_type, band=band)


class BandStackCellDatasetBandTask(CellDatasetBandTask):

    def output(self):

        from datacube.api.workflow import format_date
        from datacube.api.utils import get_satellite_string

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        dataset_type_string = {
            DatasetType.ARG25: "NBAR",
            DatasetType.PQ25: "PQA",
            DatasetType.FC25: "FC",
            DatasetType.WATER: "WATER",
            DatasetType.NDVI: "NDVI",
            DatasetType.EVI: "EVI",
            DatasetType.NBR: "NBR",
            DatasetType.TCI: "TCI",
            DatasetType.DSM: "DSM"
        }[self.dataset_type]

        if self.mask_pqa_apply and self.mask_wofs_apply:
            dataset_type_string += "_WITH_PQA_WATER"

        elif self.mask_pqa_apply:
            dataset_type_string += "_WITH_PQA"

        elif self.mask_wofs_apply:
            dataset_type_string += + "_WITH_WATER"

        filename = "{satellites}_{dataset_type}_STACK_{band}_{x:03d}_{y:04d}_{acq_min}_{acq_max}.tif".format(
            satellites=get_satellite_string(self.satellites), dataset_type=dataset_type_string, band=self.band,
            x=self.x, y=self.y, acq_min=acq_min, acq_max=acq_max
        )

        filename = os.path.join(self.output_directory, filename)

        return luigi.LocalTarget(filename)

    def run(self):

        # TODO move the dicking around with bands stuff into utils?

        import gdal

        driver = raster = None
        metadata = None
        data_type = ndv = None

        tiles = self.get_tiles()
        _log.info("Total tiles found [%d]", len(tiles))

        _log.info("Creating stack for band [%s]", self.band)

        relevant_tiles = []

        for tile in tiles:

            dataset = self.dataset_type in tile.datasets and tile.datasets[self.dataset_type] or None

            if not dataset:
                _log.info("No applicable [%s] dataset for [%s]", self.dataset_type.name, tile.end_datetime)
                continue

            if self.band in [b.name for b in tile.datasets[self.dataset_type].bands]:
                relevant_tiles.append(tile)

        _log.info("Total tiles for band [%s] is [%d]", self.band, len(relevant_tiles))

        for index, tile in enumerate(relevant_tiles, start=1):

            dataset = tile.datasets[self.dataset_type]
            assert dataset

            band = dataset.bands[self.band]
            assert band

            pqa = (self.mask_pqa_apply and DatasetType.PQ25 in tile.datasets) and tile.datasets[DatasetType.PQ25] or None
            wofs = (self.mask_wofs_apply and DatasetType.WATER in tile.datasets) and tile.datasets[DatasetType.WATER] or None

            if self.dataset_type not in tile.datasets:
                _log.debug("No [%s] dataset present for [%s] - skipping", self.dataset_type.name, tile.end_datetime)
                continue

            filename = self.output().path

            if not metadata:
                metadata = get_dataset_metadata(dataset)
                assert metadata

            if not data_type:
                data_type = get_dataset_datatype(dataset)
                assert data_type

            if not ndv:
                ndv = get_dataset_ndv(dataset)
                assert ndv

            if not driver:

                # if self.output_format == OutputFormat.GEOTIFF:
                #     driver = gdal.GetDriverByName("GTiff")
                # elif self.output_format == OutputFormat.ENVI:
                #     driver = gdal.GetDriverByName("ENVI")

                driver = gdal.GetDriverByName("GTiff")
                assert driver

            if not raster:

                # if self.output_format == OutputFormat.GEOTIFF:
                #     raster = driver.Create(filename, metadata.shape[0], metadata.shape[1], len(tiles), data_type, options=["BIGTIFF=YES", "INTERLEAVE=BAND"])
                # elif self.output_format == OutputFormat.ENVI:
                #     raster = driver.Create(filename, metadata.shape[0], metadata.shape[1], len(tiles), data_type, options=["INTERLEAVE=BSQ"])

                raster = driver.Create(filename, metadata.shape[0], metadata.shape[1], len(tiles), data_type, options=["BIGTIFF=YES", "INTERLEAVE=BAND"])
                assert raster

                # NOTE: could do this without the metadata!!
                raster.SetGeoTransform(metadata.transform)
                raster.SetProjection(metadata.projection)

            raster.SetMetadata(self.generate_raster_metadata())

            mask = None

            if pqa:
                mask = get_mask_pqa(pqa, self.mask_pqa_mask, mask=mask)

            if wofs:
                mask = get_mask_wofs(wofs, self.mask_wofs_mask, mask=mask)

            _log.info("Stacking [%s] band data from [%s] with PQA [%s] and PQA mask [%s] and WOFS [%s] and WOFS mask [%s] to [%s]",
                      band.name, dataset.path,
                      pqa and pqa.path or "",
                      pqa and self.mask_pqa_mask or "",
                      wofs and wofs.path or "", wofs and self.mask_wofs_mask or "",
                      filename)

            data = get_dataset_data_masked(dataset, mask=mask, ndv=ndv)

            _log.debug("data is [%s]", data)

            stack_band = raster.GetRasterBand(index)

            stack_band.SetDescription(os.path.basename(dataset.path))
            stack_band.SetNoDataValue(ndv)
            stack_band.WriteArray(data[band])
            stack_band.ComputeStatistics(True)
            stack_band.SetMetadata({"ACQ_DATE": format_date(tile.end_datetime), "SATELLITE": dataset.satellite.name})

            stack_band.FlushCache()
            del stack_band

        if raster:
            raster.FlushCache()
            del raster
            raster = None

    def generate_raster_metadata(self):
        return {
            "X_INDEX": "{x:03d}".format(x=self.x),
            "Y_INDEX": "{y:04d}".format(y=self.y),
            "DATASET_TYPE": self.dataset_type.name,
            "ACQUISITION_DATE": "{acq_min} to {acq_max}".format(acq_min=format_date(self.acq_min), acq_max=format_date(self.acq_max)),
            "SATELLITES": " ".join([s.name for s in self.satellites]),
            "PIXEL_QUALITY_FILTER": self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
            "WATER_FILTER": self.mask_wofs_apply and " ".join([mask.name for mask in self.mask_wofs_mask]) or ""
        }


def format_date_time(d):
    from datetime import datetime

    if d:
        return datetime.strftime(d, "%Y-%m-%d %H:%M:%S")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    BandStackWorkflow().run()