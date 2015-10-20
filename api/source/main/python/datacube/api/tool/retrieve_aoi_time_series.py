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


import csv
import logging
import os
from datacube.api import dataset_type_arg, writeable_dir, BandListType
from datacube.api.tool import AoiTool
from datacube.api.utils import log_mem, intersection, get_mask_pqa, get_mask_wofs, get_dataset_data, NDV
from datacube.api.utils import get_band_name_union, get_band_name_intersection
from datacube.api.utils import get_dataset_metadata
from datacube.api.model import DatasetType, Satellite
from functools import reduce

_log = logging.getLogger()


class RetrieveAoiTimeSeries(AoiTool):

    def __init__(self):

        # Call method on super class
        # super(self.__class__, self).__init__("Retrieve AOI Time Series")
        AoiTool.__init__(self, "Retrieve AOI Time Series")

        self.dataset_type = None
        self.bands = None
        self.output_directory = None
        self.overwrite = None
        self.list_only = None
        self.output_no_data = None

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        AoiTool.setup_arguments(self)

        self.parser.add_argument("--dataset-type", help="The types of dataset to retrieve", action="store",
                                 dest="dataset_type", type=dataset_type_arg,
                                 choices=self.get_supported_dataset_types(), default=DatasetType.ARG25,
                                 metavar=" ".join([s.name for s in self.get_supported_dataset_types()]))

        group = self.parser.add_mutually_exclusive_group()

        group.add_argument("--band", help="The band(s) to retrieve", action="store", dest="bands", type=str, nargs="+")

        group.add_argument("--bands-all", help="Retrieve all bands with NULL values where the band is N/A",
                           action="store_const", dest="bands", const=BandListType.ALL)

        group.add_argument("--bands-common", help="Retrieve only bands in common across all satellites",
                           action="store_const", dest="bands", const=BandListType.COMMON)

        self.parser.set_defaults(bands=BandListType.ALL)

        self.parser.add_argument("--output-directory", help="Output directory", action="store", dest="output_directory",
                                 type=writeable_dir)

        self.parser.add_argument("--overwrite", help="Over write existing output file", action="store_true",
                                 dest="overwrite", default=False)

        self.parser.add_argument("--list-only", help="Just list datasets that would be processed", action="store_true",
                                 dest="list_only", default=False)

        self.parser.add_argument("--hide-no-data", help="Don't output records that are completely no data value(s)",
                                 action="store_false", dest="output_no_data", default=True)

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        AoiTool.process_arguments(self, args)

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
        self.output_no_data = args.output_no_data

    def log_arguments(self):

        # Call method on super class
        # super(self.__class__, self).log_arguments()
        AoiTool.log_arguments(self)

        _log.info("""
        dataset type = {dataset_type}
        bands to retrieve = {bands}
        output directory = {output}
        over write existing = {overwrite}
        list only = {list_only}
        output no data values = {output_no_data}
        """.format(dataset_type=self.dataset_type.name,
                   bands=self.bands,
                   output=self.output_directory,
                   overwrite=self.overwrite,
                   list_only=self.list_only,
                   output_no_data=self.output_no_data,
                   ))

    def go(self):

        import numpy
        from datacube.api.query import list_cells_as_list, list_tiles_as_list
        from datacube.config import Config

        x_min, x_max, y_max, y_min = self.extract_bounds_from_vector()
        _log.debug("The bounds are [%s]", (x_min, x_max, y_min, y_max))

        cells_vector = self.extract_cells_from_vector()
        _log.debug("Intersecting cells_vector are [%d] [%s]", len(cells_vector), cells_vector)

        config = Config()
        _log.debug(config.to_str())

        x_list = range(x_min, x_max + 1)
        y_list = range(y_min, y_max + 1)

        _log.debug("x = [%s] y=[%s]", x_list, y_list)

        cells_db = list()

        for cell in list_cells_as_list(x=x_list, y=y_list, acq_min=self.acq_min, acq_max=self.acq_max,
                                       satellites=[satellite for satellite in self.satellites],
                                       dataset_types=[self.dataset_type]):
            cells_db.append((cell.x, cell.y))

        _log.debug("Cells from DB are [%d] [%s]", len(cells_db), cells_db)

        cells = intersection(cells_vector, cells_db)
        _log.debug("Combined cells are [%d] [%s]", len(cells), cells)

        for (x, y) in cells:
            _log.info("Processing cell [%3d/%4d]", x, y)

            tiles = list_tiles_as_list(x=x_list, y=y_list, acq_min=self.acq_min, acq_max=self.acq_max,
                                       satellites=[satellite for satellite in self.satellites],
                                       dataset_types=[self.dataset_type])

            _log.info("There are [%d] tiles", len(tiles))

            if self.list_only:
                for tile in tiles:
                    _log.info("Would process [%s]", tile.datasets[self.dataset_type].path)
                continue

            # Calculate the mask for the cell

            mask_aoi = self.get_mask_aoi_cell(x, y)

            pixel_count = 4000 * 4000

            pixel_count_aoi = (mask_aoi == False).sum()

            _log.debug("mask_aoi is [%s]\n[%s]", numpy.shape(mask_aoi), mask_aoi)

            metadata = None

            with self.get_output_file() as csv_file:

                csv_writer = csv.writer(csv_file)

                import operator

                header = reduce(operator.add, [["DATE", "INSTRUMENT", "# PIXELS", "# PIXELS IN AOI"]] + [
                    ["%s - # DATA PIXELS" % band_name,
                     "%s - # DATA PIXELS AFTER PQA" % band_name,
                     "%s - # DATA PIXELS AFTER PQA WOFS" % band_name,
                     "%s - # DATA PIXELS AFTER PQA WOFS AOI" % band_name,
                     "%s - MIN" % band_name, "%s - MAX" % band_name, "%s - MEAN" % band_name] for band_name in self.bands])

                csv_writer.writerow(header)

                for tile in tiles:

                    _log.info("Processing tile [%s]", tile.datasets[self.dataset_type].path)

                    if self.list_only:
                        continue

                    if not metadata:
                        metadata = get_dataset_metadata(tile.datasets[self.dataset_type])

                    # Apply PQA if specified

                    pqa = None
                    mask_pqa = None

                    if self.mask_pqa_apply and DatasetType.PQ25 in tile.datasets:
                        pqa = tile.datasets[DatasetType.PQ25]
                        mask_pqa = get_mask_pqa(pqa, self.mask_pqa_mask)

                    _log.debug("mask_pqa is [%s]\n[%s]", numpy.shape(mask_pqa), mask_pqa)

                    # Apply WOFS if specified

                    wofs = None
                    mask_wofs = None

                    if self.mask_wofs_apply and DatasetType.WATER in tile.datasets:
                        wofs = tile.datasets[DatasetType.WATER]
                        mask_wofs = get_mask_wofs(wofs, self.mask_wofs_mask)

                    _log.debug("mask_wofs is [%s]\n[%s]", numpy.shape(mask_wofs), mask_wofs)

                    dataset = tile.datasets[self.dataset_type]

                    bands = []

                    dataset_band_names = [b.name for b in dataset.bands]

                    for b in self.bands:
                        if b in dataset_band_names:
                            bands.append(dataset.bands[b])

                    data = get_dataset_data(tile.datasets[self.dataset_type], bands=bands)
                    _log.debug("data is [%s]\n[%s]", numpy.shape(data), data)

                    pixel_count_data = dict()
                    pixel_count_data_pqa = dict()
                    pixel_count_data_pqa_wofs = dict()
                    pixel_count_data_pqa_wofs_aoi = dict()
                    mmin = dict()
                    mmax = dict()
                    mmean = dict()

                    for band_name in self.bands:

                        # Add "zeroed" entries for non-present bands - should only be if outputs for those bands have been explicitly requested

                        if band_name not in dataset_band_names:
                            pixel_count_data[band_name] = 0
                            pixel_count_data_pqa[band_name] = 0
                            pixel_count_data_pqa_wofs[band_name] = 0
                            pixel_count_data_pqa_wofs_aoi[band_name] = 0
                            mmin[band_name] = numpy.ma.masked
                            mmax[band_name] = numpy.ma.masked
                            mmean[band_name] = numpy.ma.masked
                            continue

                        band = dataset.bands[band_name]

                        data[band] = numpy.ma.masked_equal(data[band], NDV)
                        _log.debug("masked data is [%s] [%d]\n[%s]", numpy.shape(data), numpy.ma.count(data), data)

                        pixel_count_data[band_name] = numpy.ma.count(data[band])

                        if pqa:
                            data[band].mask = numpy.ma.mask_or(data[band].mask, mask_pqa)
                            _log.debug("PQA masked data is [%s] [%d]\n[%s]", numpy.shape(data[band]), numpy.ma.count(data[band]), data[band])

                        pixel_count_data_pqa[band_name] = numpy.ma.count(data[band])

                        if wofs:
                            data[band].mask = numpy.ma.mask_or(data[band].mask, mask_wofs)
                            _log.debug("WOFS masked data is [%s] [%d]\n[%s]", numpy.shape(data[band]), numpy.ma.count(data[band]), data[band])

                        pixel_count_data_pqa_wofs[band_name] = numpy.ma.count(data[band])

                        data[band].mask = numpy.ma.mask_or(data[band].mask, mask_aoi)
                        _log.debug("AOI masked data is [%s] [%d]\n[%s]", numpy.shape(data[band]), numpy.ma.count(data[band]), data[band])

                        pixel_count_data_pqa_wofs_aoi[band_name] = numpy.ma.count(data[band])

                        mmin[band_name] = numpy.ma.min(data[band])
                        mmax[band_name] = numpy.ma.max(data[band])
                        mmean[band_name] = numpy.ma.mean(data[band])

                        # Convert the mean to an int...taking into account masking....

                        if not numpy.ma.is_masked(mmean[band_name]):
                            mmean[band_name] = mmean[band_name].astype(numpy.int16)

                    pixel_count_data_pqa_wofs_aoi_all_bands = reduce(operator.add, pixel_count_data_pqa_wofs_aoi.itervalues())

                    if pixel_count_data_pqa_wofs_aoi_all_bands == 0 and not self.output_no_data:
                        _log.info("Skipping dataset with no non-masked data values in ANY band")
                        continue

                    row = reduce(
                        operator.add,
                            [[tile.end_datetime,
                              self.decode_satellite_as_instrument(tile.datasets[self.dataset_type].satellite),
                              pixel_count, pixel_count_aoi]] +

                            [[pixel_count_data[band_name], pixel_count_data_pqa[band_name],
                              pixel_count_data_pqa_wofs[band_name], pixel_count_data_pqa_wofs_aoi[band_name],
                              mmin[band_name], mmax[band_name], mmean[band_name]] for band_name in self.bands])

                    csv_writer.writerow(row)

    @staticmethod
    def decode_satellite_as_instrument(satellite):

        instruments = {Satellite.LS5: "TM", Satellite.LS7: "ETM", Satellite.LS8: "OLI"}

        if satellite in instruments:
            return instruments[satellite]

        return "N/A"

    def get_output_file(self):

        import sys

        if not self.output_directory:
            _log.info("Writing output to standard output")
            return sys.stdout

        # TODO
        # filename = self.get_output_filename(self.dataset_type)
        filename = os.path.join(self.output_directory, "output.csv")

        _log.info("Writing output to %s", filename)

        if os.path.exists(filename) and not self.overwrite:
            _log.error("Output file [%s] exists", filename)
            raise Exception("Output file [%s] already exists" % filename)

        return open(filename, "wb")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    log_mem("Start")

    RetrieveAoiTimeSeries().run()

    log_mem("Finish")
