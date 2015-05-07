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
from datacube.api import dataset_type_arg, writeable_dir, output_format_arg, OutputFormat
from datacube.api.model import DatasetType
from datacube.api.tool import CellTool
from datacube.api.utils import intersection, get_mask_pqa, get_mask_wofs, get_dataset_data_masked
from datacube.api.utils import get_mask_vector_for_cell
from datacube.api.utils import raster_create_geotiff, raster_create_envi
from datacube.api.utils import get_dataset_filename, get_dataset_ndv, get_dataset_datatype, get_dataset_metadata


_log = logging.getLogger()


class RetrieveDatasetTool(CellTool):

    def __init__(self, name):

        # Call method on super class
        # super(self.__class__, self).__init__(name)
        CellTool.__init__(self, name)

        self.dataset_types = None

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
                                 nargs="+",
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

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        CellTool.process_arguments(self, args)

        self.dataset_types = args.dataset_type

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
        output directory = {output}
        over write existing = {overwrite}
        list only = {list_only}
        output format = {output_format}
        """.format(dataset_type=" ".join([d.name for d in self.dataset_types]),
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

        dataset_types = [d for d in self.dataset_types]

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

        # If we are applying a vector mask then calculate it (once as it is the same for all tiles)

        mask = None

        if self.mask_vector_apply:
            mask = get_mask_vector_for_cell(self.x, self.y,
                                            self.mask_vector_file, self.mask_vector_layer, self.mask_vector_feature)

        for tile in self.get_tiles():

            if self.list_only:
                _log.info("Would retrieve datasets [%s]", "\n".join([tile.datasets[t].path for t in
                                                                     intersection(self.dataset_types,
                                                                                  [d for d in tile.datasets])]))
                continue

            pqa = (self.mask_pqa_apply and DatasetType.PQ25 in tile.datasets) and tile.datasets[DatasetType.PQ25] or None
            wofs = (self.mask_wofs_apply and DatasetType.WATER in tile.datasets) and tile.datasets[DatasetType.WATER] or None

            for dataset_type in self.dataset_types:

                if dataset_type not in tile.datasets:
                    _log.debug("No [%s] dataset present for [%s] - skipping", dataset_type.name, tile.end_datetime)
                    continue

                dataset = tile.datasets[dataset_type]

                filename = os.path.join(self.output_directory,
                                        get_dataset_filename(dataset,
                                                             output_format=self.output_format,
                                                             mask_pqa_apply=self.mask_pqa_apply,
                                                             mask_wofs_apply=self.mask_wofs_apply,
                                                             mask_vector_apply=self.mask_vector_apply))

                retrieve_data(tile.x, tile.y, tile.end_datetime, dataset, pqa, self.mask_pqa_mask,
                              wofs, self.mask_wofs_mask, filename, self.output_format, self.overwrite, mask=mask)


def retrieve_data(x, y, acq_dt, dataset, pqa, pqa_masks, wofs, wofs_masks, path, output_format, overwrite=False,
                  data_type=None, ndv=None, mask=None):

    _log.info("Retrieving data from [%s] with pq [%s] and pq mask [%s] and wofs [%s] and wofs mask [%s] to [%s] file [%s]",
              dataset.path,
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

    data = get_dataset_data_masked(dataset, mask=mask, ndv=ndv)

    _log.debug("data is [%s]", data)

    data_type = data_type or get_dataset_datatype(dataset)
    ndv = ndv or get_dataset_ndv(dataset)

    dataset_info = generate_raster_metadata(x, y, acq_dt, dataset,
                                            pqa is not None, pqa_masks,
                                            wofs is not None, wofs_masks)

    band_info = [b.name for b in dataset.bands]

    if output_format == OutputFormat.GEOTIFF:
        raster_create_geotiff(path, [data[b] for b in dataset.bands], metadata.transform, metadata.projection, ndv,
                              data_type,
                              dataset_metadata=dataset_info, band_ids=band_info)

    elif output_format == OutputFormat.ENVI:
        raster_create_envi(path, [data[b] for b in dataset.bands], metadata.transform, metadata.projection, ndv,
                           data_type,
                           dataset_metadata=dataset_info, band_ids=band_info)


def generate_raster_metadata(x, y, acq_dt, dataset,
                             mask_pqa_apply=False, mask_pqa_mask=None, mask_wofs_apply=False, mask_wofs_mask=None):
    return {
        "X_INDEX": "{x:03d}".format(x=x),
        "Y_INDEX": "{y:04d}".format(y=y),
        "DATASET_TYPE": dataset.dataset_type.name,
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