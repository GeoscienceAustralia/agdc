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


import csv
import logging
import os
import sys
from datacube.api import dataset_type_arg, writeable_dir, BandListType
from datacube.api.model import DatasetType, Wofs25Bands
from datacube.api.tool import Tool
from datacube.api.utils import latlon_to_cell, latlon_to_xy, BYTE_MAX, get_mask_pqa, get_band_name_union
from datacube.api.utils import is_ndv, get_dataset_type_ndv
from datacube.api.utils import LS7_SLC_OFF_EXCLUSION, LS8_PRE_WRS_2_EXCLUSION, build_date_criteria
from datacube.api.utils import get_pixel_time_series_filename
from datacube.api.utils import get_band_name_intersection
from datacube.api.utils import get_mask_wofs, get_dataset_data_masked
from datacube.api.utils import get_dataset_metadata, NDV


__author__ = "Simon Oldfield"

_log = logging.getLogger()


class RetrievePixelTimeSeriesTool(Tool):

    def __init__(self, name):

        # Call method on super class
        # super(self.__class__, self).__init__(name)
        Tool.__init__(self, name)

        self.latitude = None
        self.longitude = None

        self.output_no_data = None

        self.dataset_type = None
        self.bands = None

        self.delimiter = None
        self.output_directory = None
        self.overwrite = None

    def setup_arguments(self):

        # Call method on super class
        # super(self.__class__, self).setup_arguments()
        Tool.setup_arguments(self)

        self.parser.add_argument("--lat", help="Latitude value of pixel", action="store", dest="latitude", type=float,
                                 required=True)
        self.parser.add_argument("--lon", help="Longitude value of pixel", action="store", dest="longitude", type=float,
                                 required=True)

        self.parser.add_argument("--hide-no-data", help="Don't output records that are completely no data value(s)",
                                 action="store_false", dest="output_no_data", default=True)

        self.parser.add_argument("--dataset-type", help="The type of dataset from which values will be retrieved",
                                 action="store",
                                 dest="dataset_type",
                                 type=dataset_type_arg,
                                 choices=self.get_supported_dataset_types(), default=DatasetType.ARG25, required=True,
                                 metavar=" ".join([s.name for s in self.get_supported_dataset_types()]))

        group = self.parser.add_mutually_exclusive_group()

        group.add_argument("--band", help="The band(s) to retrieve", action="store", dest="bands", type=str, nargs="+")

        group.add_argument("--bands-all", help="Retrieve all bands with NULL values where the band is N/A",
                           action="store_const", dest="bands", const=BandListType.ALL)

        group.add_argument("--bands-common", help="Retrieve only bands in common across all satellites",
                           action="store_const", dest="bands", const=BandListType.COMMON)

        self.parser.set_defaults(bands=BandListType.ALL)

        self.parser.add_argument("--delimiter", help="Field delimiter in output file", action="store", dest="delimiter",
                                 type=str, default=",")

        self.parser.add_argument("--output-directory", help="Output directory", action="store", dest="output_directory",
                                 type=writeable_dir)

        self.parser.add_argument("--overwrite", help="Over write existing output file", action="store_true",
                                 dest="overwrite", default=False)

    def process_arguments(self, args):

        # Call method on super class
        # super(self.__class__, self).process_arguments(args)
        Tool.process_arguments(self, args)

        self.latitude = args.latitude
        self.longitude = args.longitude

        self.output_no_data = args.output_no_data

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

        self.delimiter = args.delimiter
        self.output_directory = args.output_directory
        self.overwrite = args.overwrite

    def log_arguments(self):

        # Call method on super class
        # super(self.__class__, self).log_arguments()
        Tool.log_arguments(self)

        _log.info("""
        longitude = {longitude:f}
        latitude = {latitude:f}
        datasets to retrieve = {dataset_type}
        bands to retrieve = {bands}
        output no data values = {output_no_data}
        output = {output}
        over write = {overwrite}
        delimiter = {delimiter}
        """.format(longitude=self.longitude, latitude=self.latitude,
                   dataset_type=self.dataset_type.name,
                   bands=self.bands,
                   output_no_data=self.output_no_data,
                   output=self.output_directory and self.output_directory or "STDOUT",
                   overwrite=self.overwrite,
                   delimiter=self.delimiter))

    def get_tiles(self, x, y):

        return list(self.get_tiles_from_db(x=x, y=y))

    def get_tiles_from_db(self, x, y):

        from datacube.api.query import list_tiles

        x_list = [x]
        y_list = [y]

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

        cell_x, cell_y = latlon_to_cell(self.latitude, self.longitude)

        _log.info("cell is %d %d", cell_x, cell_y)

        ndv = get_dataset_type_ndv(self.dataset_type)

        with self.get_output_file(self.dataset_type, self.overwrite) as csv_file:

            csv_writer = csv.writer(csv_file, delimiter=self.delimiter)

            # Output a HEADER

            csv_writer.writerow(["SATELLITE", "ACQUISITION DATE"] + self.bands)

            for tile in self.get_tiles(x=cell_x, y=cell_y):

                if self.dataset_type not in tile.datasets:
                    _log.debug("No [%s] dataset present for [%s] - skipping", self.dataset_type.name, tile.end_datetime)
                    continue

                dataset = tile.datasets[self.dataset_type]
                pqa = (self.mask_pqa_apply and DatasetType.PQ25 in tile.datasets) and tile.datasets[DatasetType.PQ25] or None
                wofs = (self.mask_wofs_apply and DatasetType.WATER in tile.datasets) and tile.datasets[DatasetType.WATER] or None

                data = retrieve_pixel_value(dataset, pqa, self.mask_pqa_mask, wofs, self.mask_wofs_mask, self.latitude, self.longitude, ndv=ndv)

                if has_data(dataset.bands, data, no_data_value=ndv) or self.output_no_data:
                    csv_writer.writerow([dataset.satellite.name, format_date_time(tile.end_datetime)] +
                                        decode_data(self.dataset_type, dataset, self.bands, data))

    def get_output_file(self, dataset_type, overwrite=False):

        if not self.output_directory:
            _log.info("Writing output to standard output")
            return sys.stdout

        filename = self.get_output_filename(dataset_type)

        _log.info("Writing output to %s", filename)

        if os.path.exists(filename) and not overwrite:
            _log.error("Output file [%s] exists", filename)
            raise Exception("Output file [%s] already exists" % filename)

        return open(self.get_output_filename(dataset_type), "wb")

    def get_output_filename(self, dataset_type):
        return get_pixel_time_series_filename(satellites=self.satellites, dataset_type=dataset_type,
                                              lat=self.latitude, lon=self.longitude, acq_min=self.acq_min,
                                              acq_max=self.acq_max,
                                              season=self.season,
                                              mask_pqa_apply=self.mask_pqa_apply, mask_wofs_apply=self.mask_wofs_apply,
                                              mask_vector_apply=False)


def has_data(bands, data, no_data_value=NDV):

    for value in [data[band][0][0] for band in bands]:

        if not is_ndv(value, no_data_value):
            return True

    return False


def decode_data(dataset_type, dataset, bands, data):

    if dataset_type == DatasetType.WATER:
        return [decode_wofs_water_value(data[Wofs25Bands.WATER][0][0]), str(data[Wofs25Bands.WATER][0][0])]

    values = list()

    dataset_band_names = [b.name for b in dataset.bands]

    for b in bands:

        if b in dataset_band_names:
            values.append(str(data[dataset.bands[b]][0, 0]))
        else:
            values.append("")

    return values


def retrieve_pixel_value(dataset, pqa, pqa_masks, wofs, wofs_masks, latitude, longitude, ndv=NDV):

    _log.debug(
        "Retrieving pixel value(s) at lat=[%f] lon=[%f] from [%s] with pqa [%s] and paq mask [%s] and wofs [%s] and wofs mask [%s]",
        latitude, longitude, dataset.path, pqa and pqa.path or "", pqa and pqa_masks or "",
        wofs and wofs.path or "", wofs and wofs_masks or "")

    metadata = get_dataset_metadata(dataset)

    x, y = latlon_to_xy(latitude, longitude, metadata.transform)

    _log.info("Retrieving value at x=[%d] y=[%d] from %s", x, y, dataset.path)

    x_size = y_size = 1

    mask = None

    if pqa:
        mask = get_mask_pqa(pqa, pqa_masks, x=x, y=y, x_size=x_size, y_size=y_size)

    if wofs:
        mask = get_mask_wofs(wofs, wofs_masks, x=x, y=y, x_size=x_size, y_size=y_size, mask=mask)

    data = get_dataset_data_masked(dataset, x=x, y=y, x_size=x_size, y_size=y_size, mask=mask, ndv=ndv)

    _log.debug("data is [%s]", data)

    return data


# A WaterTile stores 1 data layer encoded as unsigned BYTE values as described in the WaterConstants.py file.
#
# Note - legal (decimal) values are:
#
#        0:  no water in pixel
#        1:  no data (one or more bands) in source NBAR image
#    2-127:  pixel masked for some reason (refer to MASKED bits)
#      128:  water in pixel
#
# Values 129-255 are illegal (i.e. if bit 7 set, all others must be unset)
#
#
# WATER_PRESENT             (dec 128) bit 7: 1=water present, 0=no water if all other bits zero
# MASKED_CLOUD              (dec 64)  bit 6: 1=pixel masked out due to cloud, 0=unmasked
# MASKED_CLOUD_SHADOW       (dec 32)  bit 5: 1=pixel masked out due to cloud shadow, 0=unmasked
# MASKED_HIGH_SLOPE         (dec 16)  bit 4: 1=pixel masked out due to high slope, 0=unmasked
# MASKED_TERRAIN_SHADOW     (dec 8)   bit 3: 1=pixel masked out due to terrain shadow or low incident angle, 0=unmasked
# MASKED_SEA_WATER          (dec 4)   bit 2: 1=pixel masked out due to being over sea, 0=unmasked
# MASKED_NO_CONTIGUITY      (dec 2)   bit 1: 1=pixel masked out due to lack of data contiguity, 0=unmasked
# NO_DATA                   (dec 1)   bit 0: 1=pixel masked out due to NO_DATA in NBAR source, 0=valid data in NBAR
# WATER_NOT_PRESENT         (dec 0)          All bits zero indicated valid observation, no water present


def decode_wofs_water_value(value):

    # values = {
    #     0: "Dry|7",
    #     1: "No Data|0",
    #     2: "Saturation/Contiguity|1",
    #     4: "Sea Water|2",
    #     8: "Terrain Shadow|3",
    #     16: "High Slope|4",
    #     32: "Cloud Shadow|5",
    #     64: "Cloud|6",
    #     128: "Wet|8"
    #     }

    values = {
        0: "Dry",
        1: "No Data",
        2: "Saturation/Contiguity",
        4: "Sea Water",
        8: "Terrain Shadow",
        16: "High Slope",
        32: "Cloud Shadow",
        64: "Cloud",
        128: "Wet",
        BYTE_MAX: "--"
        }

    return values[value]


def format_date_time(d):
    from datetime import datetime

    if d:
        return datetime.strftime(d, "%Y-%m-%d %H:%M:%S")

    return None


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    RetrievePixelTimeSeriesTool("Retrieve Pixel Time Series").run()