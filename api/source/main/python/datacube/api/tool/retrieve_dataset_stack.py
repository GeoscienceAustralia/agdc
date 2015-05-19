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


import logging
import os
from datacube.api import dataset_type_arg, writeable_dir, output_format_arg
from datacube.api.model import DatasetType
from datacube.api.tool import CellTool
from datacube.api.utils import get_mask_pqa, get_mask_wofs, get_dataset_data_masked, format_date, OutputFormat, \
    get_mask_vector_for_cell
from datacube.api.utils import get_dataset_band_stack_filename
from datacube.api.utils import get_band_name_union, get_band_name_intersection
from datacube.api.utils import get_dataset_ndv, get_dataset_datatype, get_dataset_metadata
from enum import Enum


_log = logging.getLogger()


class BandListType(Enum):
    __order__ = "EXPLICIT ALL COMMON"

    EXPLICIT = "EXPLICIT"
    ALL = "ALL"
    COMMON = "COMMON"


class RetrieveDatasetStackTool(CellTool):

    def __init__(self, name):

        # Call method on super class
        # super(self.__class__, self).__init__(name)
        CellTool.__init__(self, name)

        self.dataset_type = None
        self.bands = None

        self.output_directory = None
        self.overwrite = None
        self.list_only = None

        self.output_format = None

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        CellTool.setup_arguments(self)

        self.parser.add_argument("--dataset-type", help="The type(s) of dataset to retrieve",
                                 action="store",
                                 dest="dataset_type",
                                 type=dataset_type_arg,
                                 choices=self.get_supported_dataset_types(), default=DatasetType.ARG25, required=True,
                                 metavar=" ".join([s.name for s in self.get_supported_dataset_types()]))

        group = self.parser.add_mutually_exclusive_group()

        # TODO explicit list of bands
        # group.add_argument("--bands", help="List of bands to retrieve", action="store")

        group.add_argument("--bands-all", help="Retrieve all bands with NULL values where the band is N/A",
                           action="store_const", dest="bands", const=BandListType.ALL)

        group.add_argument("--bands-common", help="Retrieve only bands in common across all satellites",
                           action="store_const", dest="bands", const=BandListType.COMMON)

        self.parser.set_defaults(bands=BandListType.ALL)

        self.parser.add_argument("--output-directory", help="Output directory", action="store", dest="output_directory",
                                 type=writeable_dir, required=True)

        self.parser.add_argument("--overwrite", help="Over write existing output file", action="store_true",
                                 dest="overwrite", default=False)

        self.parser.add_argument("--list-only",
                                 help="List the datasets that would be retrieved rather than retrieving them",
                                 action="store_true", dest="list_only", default=False)

        self.parser.add_argument("--output-format", help="The format of the output dataset",
                                 action="store",
                                 dest="output_format",
                                 type=output_format_arg,
                                 choices=OutputFormat, default=OutputFormat.GEOTIFF,
                                 metavar=" ".join([f.name for f in OutputFormat]))

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        CellTool.process_arguments(self, args)

        self.dataset_type = args.dataset_type

        if args.bands == BandListType.ALL:
            self.bands = get_band_name_union(self.dataset_type, self.satellites)
        else:
            self.bands = get_band_name_intersection(self.dataset_type, self.satellites)

        self.output_directory = args.output_directory
        self.overwrite = args.overwrite
        self.list_only = args.list_only

        self.output_format = args.output_format

    def log_arguments(self):

        # Call method on super class
        # super(self.__class__, self).log_arguments()
        CellTool.log_arguments(self)

        _log.info("""
        datasets to retrieve = {dataset_type}
        bands to retrieve = {bands}
        output directory = {output}
        over write existing = {overwrite}
        list only = {list_only}
        output format = {output_format}
        """.format(dataset_type=self.dataset_type.name,
                   bands=self.bands,
                   output=self.output_directory,
                   overwrite=self.overwrite,
                   list_only=self.list_only,
                   output_format=self.output_format.name))

    def get_tiles(self):

        return list(self.get_tiles_from_db())

    def get_tiles_from_db(self):

        from datacube.api.query import list_tiles

        x_list = [self.x]
        y_list = [self.y]

        dataset_types = [self.dataset_type]

        if self.mask_pqa_apply and DatasetType.PQ25 not in dataset_types:
            dataset_types.append(DatasetType.PQ25)

        if self.mask_wofs_apply and DatasetType.WATER not in dataset_types:
            dataset_types.append(DatasetType.WATER)

        for tile in list_tiles(x=x_list, y=y_list,
                               acq_min=self.acq_min, acq_max=self.acq_max,
                               satellites=[satellite for satellite in self.satellites],
                               dataset_types=dataset_types):
            yield tile

    def go(self):

        # If we are applying a vector mask then calculate it not (once as it is the same for all tiles)

        mask = None

        if self.mask_vector_apply:
            mask = get_mask_vector_for_cell(self.x, self.y, self.mask_vector_file, self.mask_vector_layer, self.mask_vector_feature)

        # TODO move the dicking around with bands stuff into utils?

        import gdal

        driver = raster = None
        metadata = None
        data_type = ndv = None

        tiles = self.get_tiles()
        _log.info("Total tiles found [%d]", len(tiles))

        for band_name in self.bands:
            _log.info("Creating stack for band [%s]", band_name)

            relevant_tiles = []

            for tile in tiles:

                dataset = self.dataset_type in tile.datasets and tile.datasets[self.dataset_type] or None

                if not dataset:
                    _log.info("No applicable [%s] dataset for [%s]", self.dataset_type.name, tile.end_datetime)
                    continue

                if band_name in [b.name for b in tile.datasets[self.dataset_type].bands]:
                    relevant_tiles.append(tile)

            _log.info("Total tiles for band [%s] is [%d]", band_name, len(relevant_tiles))

            for index, tile in enumerate(relevant_tiles, start=1):

                dataset = tile.datasets[self.dataset_type]
                assert dataset

                band = dataset.bands[band_name]
                assert band

                if self.list_only:
                    _log.info("Would stack band [%s] from dataset [%s]", band.name, dataset.path)
                    continue

                pqa = (self.mask_pqa_apply and DatasetType.PQ25 in tile.datasets) and tile.datasets[DatasetType.PQ25] or None
                wofs = (self.mask_wofs_apply and DatasetType.WATER in tile.datasets) and tile.datasets[DatasetType.WATER] or None

                if self.dataset_type not in tile.datasets:
                    _log.debug("No [%s] dataset present for [%s] - skipping", self.dataset_type.name, tile.end_datetime)
                    continue

                filename = os.path.join(self.output_directory,
                                        get_dataset_band_stack_filename(dataset, band,
                                                                        output_format=self.output_format,
                                                                        mask_pqa_apply=self.mask_pqa_apply,
                                                                        mask_wofs_apply=self.mask_wofs_apply,
                                                                        mask_vector_apply=self.mask_vector_apply))

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

                # mask = None

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

    return None
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    RetrieveDatasetStackTool("Retrieve Dataset Stack").run()