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
import luigi
import logging
import os
from datacube.api.utils import raster_create, get_dataset_metadata, get_mask_pqa, get_mask_wofs
from datacube.api.utils import get_dataset_data_masked, get_dataset_filename, get_dataset_ndv
from datacube.api.model import DatasetType
from datacube.api.workflow.tile import Workflow, SummaryTask, CellTask, TileTask
from datacube.api import workflow
from datacube.api.workflow import dataset_type_arg


_log = logging.getLogger()


class RetrieveDatasetWorkflow(Workflow):

    def __init__(self):

        Workflow.__init__(self, name="Retrieve Dataset Workflow")

        self.dataset_type = None

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        Workflow.setup_arguments(self)

        self.parser.add_argument("--dataset-type", help="The type of dataset to process", action="store",
                                 dest="dataset_type",
                                 type=dataset_type_arg,
                                 choices=self.get_supported_dataset_types(), required=True,
                                 metavar=" ".join(
                                     [dataset_type.name for dataset_type in self.get_supported_dataset_types()]))

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        Workflow.process_arguments(self, args)

        self.dataset_type = args.dataset_type

    def log_arguments(self):

        # Call method on super class
        # super(self.__class__, self).log_arguments()
        workflow.Workflow.log_arguments(self)

        _log.info("""
        dataset to retrieve = {dataset_type}
        """.format(dataset_type=self.dataset_type.name))

    def create_summary_tasks(self):

        return [RetrieveDatasetSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                           acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                           output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                           mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                           mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                           dataset_type=self.dataset_type)]

    @staticmethod
    def get_supported_dataset_types():
        return [DatasetType.ARG25, DatasetType.FC25, DatasetType.PQ25,
                DatasetType.WATER,
                DatasetType.NDVI, DatasetType.EVI, DatasetType.NBR, DatasetType.TCI]


class RetrieveDatasetSummaryTask(SummaryTask):

    dataset_type = luigi.Parameter()

    def create_cell_tasks(self, x, y):

        return RetrieveDatasetCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                       output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                       mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                       mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                       dataset_type=self.dataset_type)


class RetrieveDatasetCellTask(CellTask):

    dataset_type = luigi.Parameter()

    def create_tile_tasks(self, tile):

        return RetrieveDatasetTileTask(tile=tile, x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                       satellites=self.satellites,
                                       output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                       mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                       mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                       dataset_type=self.dataset_type)


class RetrieveDatasetTileTask(TileTask):

    dataset_type = luigi.Parameter()

    def output(self):

        dataset = self.tile.datasets[self.dataset_type]

        filename = get_dataset_filename(dataset,
                                        mask_pqa_apply=self.mask_pqa_apply,
                                        mask_wofs_apply=self.mask_wofs_apply)

        filename = os.path.join(self.output_directory, filename)

        return luigi.LocalTarget(filename)

    def run(self):

        print "****", self.output().path

        dataset = self.tile.datasets[self.dataset_type]

        metadata = get_dataset_metadata(dataset)

        mask = None

        # If doing PQA masking then get PQA mask

        if self.mask_pqa_apply and DatasetType.PQ25 in self.tile.datasets:
            mask = get_mask_pqa(self.tile.datasets[DatasetType.PQ25], self.mask_pqa_mask, mask=mask)

        # If doing WOFS masking then get WOFS mask

        if self.mask_wofs_apply and DatasetType.WATER in self.tile.datasets:
            mask = get_mask_wofs(self.tile.datasets[DatasetType.WATER], self.mask_wofs_mask, mask=mask)

        # TODO - no data value and data type
        ndv = get_dataset_ndv(dataset)

        data = get_dataset_data_masked(dataset, mask=mask, ndv=ndv)

        raster_create(self.output().path, [data[b] for b in dataset.bands],
                      metadata.transform, metadata.projection, ndv, gdal.GDT_Int16,
                      dataset_metadata=self.generate_raster_metadata(dataset),
                      band_ids=[b.name for b in dataset.bands])

    def generate_raster_metadata(self, dataset):
        return {
            "X_INDEX": "{x:03d}".format(x=self.x),
            "Y_INDEX": "{y:04d}".format(y=self.y),
            "DATASET_TYPE": self.dataset_type.name,
            "ACQUISITION_DATE": "{acq}".format(acq=self.tile.end_datetime),
            "SATELLITE": dataset.satellite.name,
            "PIXEL_QUALITY_FILTER": self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
            "WATER_FILTER": self.mask_wofs_apply and " ".join([mask.name for mask in self.mask_wofs_mask]) or ""
        }


if __name__ == '__main__':
    # logging.basicConfig(level=logging.INFO)
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    RetrieveDatasetWorkflow().run()
