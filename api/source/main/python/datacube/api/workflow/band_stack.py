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
import luigi
import os
from datacube.api import output_format_arg
from datacube.api.model import dataset_type_database, dataset_type_derived_nbar, DatasetType
from datacube.api.utils import get_dataset_metadata, get_dataset_datatype, get_dataset_ndv, get_mask_pqa, get_mask_wofs
from datacube.api.utils import get_dataset_data_masked, format_date, OutputFormat
from datacube.api.workflow.cell_dataset_band import Workflow, SummaryTask, CellTask, CellDatasetBandTask


_log = logging.getLogger()


class BandStackWorkflow(Workflow):

    def __init__(self):

        Workflow.__init__(self, name="Band Stack")

        self.output_format = None

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        Workflow.setup_arguments(self)

        self.parser.add_argument("--output-format", help="The format of the output dataset",
                                 action="store",
                                 dest="output_format",
                                 type=output_format_arg,
                                 choices=OutputFormat, default=OutputFormat.GEOTIFF,
                                 metavar=" ".join([f.name for f in OutputFormat]))

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        Workflow.process_arguments(self, args)

        self.output_format = args.output_format

    def log_arguments(self):

        # Call method on super class
        # super(self.__class__, self).log_arguments()
        Workflow.log_arguments(self)

        _log.info("""
        output format = {output_format}
        """.format(output_format=self.output_format.name))

    def create_summary_tasks(self):

        return [BandStackSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                     acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                     output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                     mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                     mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                     dataset_type=self.dataset_type, bands=self.bands,
                                     output_format=self.output_format)]

    def get_supported_dataset_types(self):
        return dataset_type_database + dataset_type_derived_nbar


class BandStackSummaryTask(SummaryTask):

    output_format = luigi.Parameter()

    def create_cell_tasks(self, x, y):

        return BandStackCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                 output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                 mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                 mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                 dataset_type=self.dataset_type, bands=self.bands, output_format=self.output_format)


class BandStackCellTask(CellTask):

    output_format = luigi.Parameter()

    def create_cell_dataset_band_task(self, band):

        return BandStackCellDatasetBandTask(x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                            satellites=self.satellites,
                                            output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                            mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                            mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                            dataset_type=self.dataset_type, band=band, output_format=self.output_format)


class BandStackCellDatasetBandTask(CellDatasetBandTask):

    output_format = luigi.Parameter()

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

        ext = {OutputFormat.GEOTIFF: "tif", OutputFormat.ENVI: "dat"}[self.output_format]

        filename = "{satellites}_{dataset_type}_STACK_{band}_{x:03d}_{y:04d}_{acq_min}_{acq_max}.{ext}".format(
            satellites=get_satellite_string(self.satellites), dataset_type=dataset_type_string, band=self.band,
            x=self.x, y=self.y, acq_min=acq_min, acq_max=acq_max, ext=ext)

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

                if self.output_format == OutputFormat.GEOTIFF:
                    driver = gdal.GetDriverByName("GTiff")
                elif self.output_format == OutputFormat.ENVI:
                    driver = gdal.GetDriverByName("ENVI")

                assert driver

            if not raster:

                if self.output_format == OutputFormat.GEOTIFF:
                    raster = driver.Create(filename, metadata.shape[0], metadata.shape[1], len(tiles), data_type, options=["BIGTIFF=YES", "INTERLEAVE=BAND"])
                elif self.output_format == OutputFormat.ENVI:
                    raster = driver.Create(filename, metadata.shape[0], metadata.shape[1], len(tiles), data_type, options=["INTERLEAVE=BSQ"])

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
            raster = None
            del raster

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
