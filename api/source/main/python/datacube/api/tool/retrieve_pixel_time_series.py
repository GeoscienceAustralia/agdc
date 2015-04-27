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


import csv
import logging
import os
import sys
from enum import Enum
from datacube.api import dataset_type_arg, writeable_dir
from datacube.api.model import DatasetType, Wofs25Bands, Satellite, dataset_type_database, NdviBands, get_bands
from datacube.api.model import dataset_type_derived_nbar
from datacube.api.query import list_tiles
from datacube.api.tool import Tool
from datacube.api.utils import latlon_to_cell, latlon_to_xy, UINT16_MAX, BYTE_MAX, get_mask_pqa, get_band_name_union
from datacube.api.utils import get_band_name_intersection
from datacube.api.utils import calculate_tassel_cap_index, TasselCapIndex, TCI_COEFFICIENTS
from datacube.api.utils import get_mask_wofs, get_dataset_data_masked, calculate_ndvi, calculate_evi, calculate_nbr
from datacube.api.utils import union, get_dataset_metadata, NDV


_log = logging.getLogger()


class BandListType(Enum):
    __order__ = "EXPLICIT ALL COMMON"

    EXPLICIT = "EXPLICIT"
    ALL = "ALL"
    COMMON = "COMMON"


class TimeSeriesRetrievalWorkflow(Tool):

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

        # TODO explicit list of bands
        # group.add_argument("--bands", help="List of bands to retrieve", action="store")

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
        else:
            self.bands = get_band_name_intersection(self.dataset_type, self.satellites)

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

        if self.mask_pqa_apply:
            dataset_types.append(DatasetType.PQ25)

        if self.mask_wofs_apply:
            dataset_types.append(DatasetType.WATER)

        for tile in list_tiles(x=x_list, y=y_list,
                               acq_min=self.acq_min, acq_max=self.acq_max,
                               satellites=[satellite for satellite in self.satellites],
                               dataset_types=dataset_types):
            yield tile

    def go(self):

        cell_x, cell_y = latlon_to_cell(self.latitude, self.longitude)

        # TODO - PQ is UNIT16 and WOFS is BYTE (others are INT16) and so -999 NDV doesn't work
        ndv = NDV

        if self.dataset_type == DatasetType.PQ25:
            ndv = UINT16_MAX

        elif self.dataset_type == DatasetType.WATER:
            ndv = BYTE_MAX

        # elif self.dataset_type == DatasetType.TCI:
        #     ndv = numpy.nan

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

        if dataset_type == DatasetType.WATER:
            return os.path.join(self.output_directory,"LS_WOFS_{longitude:03.5f}_{latitude:03.5f}_{acq_min}_{acq_max}.csv".format(latitude=self.latitude,
                                                                                              longitude=self.longitude,
                                                                                              acq_min=self.acq_min,
                                                                                              acq_max=self.acq_max))
        satellite_str = ""

        if Satellite.LS5 in self.satellites or Satellite.LS7 in self.satellites or Satellite.LS8 in self.satellites:
            satellite_str += "LS"

        if Satellite.LS5 in self.satellites:
            satellite_str += "5"

        if Satellite.LS7 in self.satellites:
            satellite_str += "7"

        if Satellite.LS8 in self.satellites:
            satellite_str += "8"

        dataset_str = ""

        if dataset_type == DatasetType.ARG25:
            dataset_str += "NBAR"

        elif dataset_type == DatasetType.PQ25:
            dataset_str += "PQA"

        elif dataset_type == DatasetType.FC25:
            dataset_str += "FC"

        elif dataset_type == DatasetType.WATER:
            dataset_str += "WOFS"

        if self.mask_pqa_apply and self.mask_wofs_apply:
            dataset_str += "_WITH_PQA_WATER"

        elif self.mask_pqa_apply:
            dataset_str += "_WITH_PQA"

        elif self.mask_wofs_apply:
            dataset_str += "_WITH_WATER"

        return os.path.join(self.output_directory,
                            "{satellite}_{dataset}_{longitude:03.5f}_{latitude:03.5f}_{acq_min}_{acq_max}.csv".format(satellite=satellite_str, dataset=dataset_str, latitude=self.latitude,
                                                                                          longitude=self.longitude,
                                                                                          acq_min=self.acq_min,
                                                                                          acq_max=self.acq_max))


def has_data(bands, data, no_data_value=NDV):
    for value in [data[band][0][0] for band in bands]:
        if value != no_data_value:
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

    _log.debug("Retrieving value at x=[%d] y=[%d]", x, y)

    x_size = y_size = 1

    mask = None

    if pqa:
        mask = get_mask_pqa(pqa, pqa_masks, x=x, y=y, x_size=x_size, y_size=y_size)

    if wofs:
        mask = get_mask_wofs(wofs, wofs_masks, x=x, y=y, x_size=x_size, y_size=y_size, mask=mask)

    data = get_dataset_data_masked(dataset, x=x, y=y, x_size=x_size, y_size=y_size, mask=mask, ndv=ndv)

    _log.debug("data is [%s]", data)

    return data


def retrieve_pixel_value_derived_nbar(dataset_type, nbar, pqa, pqa_masks, wofs, wofs_masks, latitude, longitude, ndv=NDV):
    _log.debug(
        "Retrieving pixel value(s) at lat=[%f] lon=[%f] from [%s] derived from [%s] with pqa [%s] and paq mask [%s] and wofs [%s] and wofs mask [%s]",
        latitude, longitude, dataset_type.name, nbar.path, pqa and pqa.path or "", pqa and pqa_masks or "",
        wofs and wofs.path or "", wofs and wofs_masks or "")

    metadata = get_dataset_metadata(nbar)

    x, y = latlon_to_xy(latitude, longitude, metadata.transform)

    _log.debug("Retrieving value at x=[%d] y=[%d]", x, y)

    mask = None

    if pqa:
        mask = get_mask_pqa(pqa, pqa_masks, x=x, y=y, x_size=1, y_size=1)

    if wofs:
        mask = get_mask_wofs(wofs, wofs_masks, x=x, y=y, x_size=1, y_size=1, mask=mask)

    data = get_dataset_data_masked(nbar, x=x, y=y, x_size=1, y_size=1, mask=mask, ndv=ndv)

    if dataset_type == DatasetType.NDVI:
        data = calculate_ndvi(data[nbar.bands.RED], data[nbar.bands.NEAR_INFRARED])

    elif dataset_type == DatasetType.EVI:
        data = calculate_evi(data[nbar.bands.RED], data[nbar.bands.BLUE], data[nbar.bands.NEAR_INFRARED])

    elif dataset_type == DatasetType.NBR:
        data = calculate_nbr(data[nbar.bands.NEAR_INFRARED], data[nbar.bands.SHORT_WAVE_INFRARED_2])

    elif dataset_type == DatasetType.TCI:
        # tct = dict()
        #
        # # TODO calculate_tassel_cap_index should do this
        # for index in TasselCapIndex:
        #     tct[index] = calculate_tassel_cap_index(data, coefficients=TCI_COEFFICIENTS[nbar.satellite][index])[DatasetType.TCI]
        #
        # data = tct
        data = calculate_tassel_cap_index(data, coefficients=TCI_COEFFICIENTS[nbar.satellite][TasselCapIndex.WETNESS])

    _log.debug("data is [%s]", data)

    return {dataset_type.name: data}


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

    TimeSeriesRetrievalWorkflow("Time Series Retrieval").run()