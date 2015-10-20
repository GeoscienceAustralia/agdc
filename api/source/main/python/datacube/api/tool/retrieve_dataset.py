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
from datacube.api import dataset_type_arg, writeable_dir, output_format_arg, OutputFormat, BandListType
from datacube.api.model import DatasetType
from datacube.api.utils import LS7_SLC_OFF_EXCLUSION, LS8_PRE_WRS_2_EXCLUSION, build_date_criteria
from datacube.api.tool import CellTool
from datacube.api.utils import get_mask_pqa, get_mask_wofs, get_dataset_data_masked
from datacube.api.utils import get_band_name_union, get_band_name_intersection
from datacube.api.utils import get_mask_vector_for_cell
from datacube.api.utils import raster_create_geotiff, raster_create_envi
from datacube.api.utils import get_dataset_filename, get_dataset_ndv, get_dataset_datatype, get_dataset_metadata


_log = logging.getLogger()


class RetrieveDatasetTool(CellTool):

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
                                 # nargs="+",
                                 choices=self.get_supported_dataset_types(), default=DatasetType.ARG25, required=True,
                                 metavar=" ".join([s.name for s in self.get_supported_dataset_types()]))

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

        group = self.parser.add_mutually_exclusive_group()

        group.add_argument("--band", help="The band(s) to retrieve", action="store", dest="bands", type=str, nargs="+")

        group.add_argument("--bands-all", help="Retrieve all bands with NULL values where the band is not present",
                           action="store_const", dest="bands", const=BandListType.ALL)

        group.add_argument("--bands-common", help="Retrieve only bands in common across all satellites",
                           action="store_const", dest="bands", const=BandListType.COMMON)

        self.parser.set_defaults(bands=BandListType.ALL)

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        CellTool.process_arguments(self, args)

        self.dataset_type = args.dataset_type

        if args.bands == BandListType.ALL:
            self.bands = get_band_name_union(self.dataset_type, self.satellites)
        elif args.bands == BandListType.COMMON:
            self.bands = get_band_name_intersection(self.dataset_type, self.satellites)
        else:
            self.bands = []
            potential_bands = get_band_name_union(self.dataset_type, self.satellites)
            for band in args.bands:
                if band in potential_bands:
                    self.bands.append(band)

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

        exclude = None

        if not self.include_ls8_pre_wrs2 or not self.include_ls8_pre_wrs2:
            exclude = []

            if not self.include_ls7_slc_off:
                exclude.append(LS7_SLC_OFF_EXCLUSION)

            if not self.include_ls8_pre_wrs2:
                exclude.append(LS8_PRE_WRS_2_EXCLUSION)

        include = None

        acq_min = self.acq_min
        acq_max = self.acq_max

        if self.season:
            season_name, (season_start_month, season_start_day), (season_end_month, season_end_day) = self.season

            acq_min, acq_max, include = build_date_criteria(acq_min, acq_max, season_start_month, season_start_day, season_end_month, season_end_day)

        for tile in list_tiles(x=x_list, y=y_list,
                               acq_min=acq_min, acq_max=acq_max,
                               satellites=[satellite for satellite in self.satellites],
                               dataset_types=dataset_types, exclude=exclude, include=include):
            yield tile

    def go(self):

        # If we are applying a vector mask then calculate it (once as it is the same for all tiles)

        mask_vector = None

        if self.mask_vector_apply:
            mask_vector = get_mask_vector_for_cell(self.x, self.y, self.mask_vector_file, self.mask_vector_layer,
                                                   self.mask_vector_feature)

        for tile in self.get_tiles():

            pqa = (self.mask_pqa_apply and DatasetType.PQ25 in tile.datasets) and tile.datasets[DatasetType.PQ25] or None
            wofs = (self.mask_wofs_apply and DatasetType.WATER in tile.datasets) and tile.datasets[DatasetType.WATER] or None

            dataset = self.dataset_type in tile.datasets and tile.datasets[self.dataset_type] or None

            if not dataset:
                _log.info("No applicable [%s] dataset for [%s]", self.dataset_type.name, tile.end_datetime)
                continue

            if self.list_only:
                _log.info("Would retrieve dataset [%s]", tile.datasets[self.dataset_type].path)
                continue

            filename = os.path.join(self.output_directory,
                                    get_dataset_filename(dataset,
                                                         output_format=self.output_format,
                                                         mask_pqa_apply=self.mask_pqa_apply,
                                                         mask_wofs_apply=self.mask_wofs_apply,
                                                         mask_vector_apply=self.mask_vector_apply))

            retrieve_data(tile.x, tile.y, tile.end_datetime, dataset, self.bands, pqa, self.mask_pqa_mask,
                          wofs, self.mask_wofs_mask, filename, self.output_format, self.overwrite, mask=mask_vector)


def retrieve_data(x, y, acq_dt, dataset, band_names, pqa, pqa_masks, wofs, wofs_masks, path, output_format,
                  overwrite=False, data_type=None, ndv=None, mask=None):

    _log.info("Retrieving data from [%s] bands [%s] with pq [%s] and pq mask [%s] and wofs [%s] and wofs mask [%s] to [%s] file [%s]",
              dataset.path,
              band_names,
              pqa and pqa.path or "",
              pqa and pqa_masks or "",
              wofs and wofs.path or "", wofs and wofs_masks or "",
              output_format.name, path)

    if os.path.exists(path) and not overwrite:
        _log.error("Output file [%s] exists", path)
        raise Exception("Output file [%s] already exists" % path)

    metadata = get_dataset_metadata(dataset)

    # mask = None

    if pqa:
        mask = get_mask_pqa(pqa, pqa_masks, mask=mask)

    if wofs:
        mask = get_mask_wofs(wofs, wofs_masks, mask=mask)

    bands = []

    for b in dataset.bands:
        if b.name in band_names:
            bands.append(b)

    ndv = ndv or get_dataset_ndv(dataset)

    data = get_dataset_data_masked(dataset, bands=bands, mask=mask, ndv=ndv)

    _log.debug("data is [%s]", data)

    data_type = data_type or get_dataset_datatype(dataset)

    dataset_info = generate_raster_metadata(x, y, acq_dt, dataset, bands,
                                            pqa is not None, pqa_masks,
                                            wofs is not None, wofs_masks)

    band_info = [b.name for b in bands]

    if output_format == OutputFormat.GEOTIFF:
        raster_create_geotiff(path, [data[b] for b in bands], metadata.transform, metadata.projection, ndv,
                              data_type, dataset_metadata=dataset_info, band_ids=band_info)

    elif output_format == OutputFormat.ENVI:
        raster_create_envi(path, [data[b] for b in bands], metadata.transform, metadata.projection, ndv,
                           data_type, dataset_metadata=dataset_info, band_ids=band_info)


def generate_raster_metadata(x, y, acq_dt, dataset, bands,
                             mask_pqa_apply=False, mask_pqa_mask=None, mask_wofs_apply=False, mask_wofs_mask=None):
    return {
        "X_INDEX": "{x:03d}".format(x=x),
        "Y_INDEX": "{y:04d}".format(y=y),
        "DATASET_TYPE": dataset.dataset_type.name,
        "BANDS": " ".join([b.name for b in bands]),
        "ACQUISITION_DATE": "{acq_dt}".format(acq_dt=format_date_time(acq_dt)),
        "SATELLITE": dataset.satellite.name,
        "PIXEL_QUALITY_FILTER": mask_pqa_apply and " ".join([mask.name for mask in mask_pqa_mask]) or "",
        "WATER_FILTER": mask_wofs_apply and " ".join([mask.name for mask in mask_wofs_mask]) or ""
    }


def format_date_time(d):
    from datetime import datetime

    if d:
        return datetime.strftime(d, "%Y-%m-%d %H:%M:%S")

    return None


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    RetrieveDatasetTool("Retrieve Dataset").run()