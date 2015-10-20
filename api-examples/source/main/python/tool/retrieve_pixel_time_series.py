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


import argparse
import csv
import glob
import logging
import os
import sys
from datacube.api.model import DatasetType, DatasetTile, Wofs25Bands, Satellite, dataset_type_database, \
    dataset_type_filesystem, dataset_type_derived_nbar
from datacube.api.query import list_tiles
from datacube.api.utils import latlon_to_cell, latlon_to_xy, PqaMask, UINT16_MAX
from datacube.api.utils import get_dataset_data, get_dataset_data_with_pq, get_dataset_metadata
from datacube.api.utils import extract_fields_from_filename, NDV
from datacube.api.workflow import writeable_dir
from datacube.config import Config


_log = logging.getLogger()


def satellite_arg(s):
    if s in Satellite._member_names_:
        return Satellite[s]
    raise argparse.ArgumentTypeError("{0} is not a supported satellite".format(s))


def pqa_mask_arg(s):
    if s in PqaMask._member_names_:
        return PqaMask[s]
    raise argparse.ArgumentTypeError("{0} is not a supported PQA mask".format(s))


def dataset_type_arg(s):
    if s in DatasetType._member_names_:
        return DatasetType[s]
    raise argparse.ArgumentTypeError("{0} is not a supported dataset type".format(s))


class TimeSeriesRetrievalWorkflow():

    application_name = None

    latitude = None
    longitude = None

    acq_min = None
    acq_max = None

    process_min = None
    process_max = None

    ingest_min = None
    ingest_max = None

    satellites = None

    apply_pqa_filter = None
    pqa_mask = None

    output_no_data = None

    dataset_type = None

    delimiter = None
    output_directory = None
    overwrite = None

    def __init__(self, application_name):
        self.application_name = application_name

    def parse_arguments(self):
        parser = argparse.ArgumentParser(prog=__name__, description=self.application_name)

        group = parser.add_mutually_exclusive_group()

        group.add_argument("--quiet", help="Less output", action="store_const", dest="log_level", const=logging.WARN)
        group.add_argument("--verbose", help="More output", action="store_const", dest="log_level", const=logging.DEBUG)

        parser.set_defaults(log_level=logging.INFO)

        parser.add_argument("--lat", help="Latitude value of pixel", action="store", dest="latitude", type=float, required=True)
        parser.add_argument("--lon", help="Longitude value of pixel", action="store", dest="longitude", type=float, required=True)

        parser.add_argument("--acq-min", help="Acquisition Date", action="store", dest="acq_min", type=str)
        parser.add_argument("--acq-max", help="Acquisition Date", action="store", dest="acq_max", type=str)

        # parser.add_argument("--process-min", help="Process Date", action="store", dest="process_min", type=str)
        # parser.add_argument("--process-max", help="Process Date", action="store", dest="process_max", type=str)
        #
        # parser.add_argument("--ingest-min", help="Ingest Date", action="store", dest="ingest_min", type=str)
        # parser.add_argument("--ingest-max", help="Ingest Date", action="store", dest="ingest_max", type=str)

        parser.add_argument("--satellite", help="The satellite(s) to include", action="store", dest="satellite",
                            type=satellite_arg, nargs="+", choices=Satellite, default=[Satellite.LS5, Satellite.LS7], metavar=" ".join([s.name for s in Satellite]))

        parser.add_argument("--apply-pqa", help="Apply PQA mask", action="store_true", dest="apply_pqa", default=False)
        parser.add_argument("--pqa-mask", help="The PQA mask to apply", action="store", dest="pqa_mask",
                            type=pqa_mask_arg, nargs="+", choices=PqaMask, default=[PqaMask.PQ_MASK_CLEAR], metavar=" ".join([s.name for s in PqaMask]))

        parser.add_argument("--hide-no-data", help="Don't output records that are completely no data value(s)", action="store_false", dest="output_no_data", default=True)

        supported_dataset_types = dataset_type_database + dataset_type_filesystem + dataset_type_derived_nbar

        # For now only only one type of dataset per customer
        parser.add_argument("--dataset-type", help="The type of dataset from which values will be retrieved", action="store",
                            dest="dataset_type",
                            type=dataset_type_arg,
                            #nargs="+",
                            choices=supported_dataset_types, default=DatasetType.ARG25, required=True, metavar=" ".join([s.name for s in supported_dataset_types]))

        parser.add_argument("--delimiter", help="Field delimiter in output file", action="store", dest="delimiter", type=str, default=",")

        parser.add_argument("--output-directory", help="Output directory", action="store", dest="output_directory",
                            type=writeable_dir)

        parser.add_argument("--overwrite", help="Over write existing output file", action="store_true", dest="overwrite", default=False)

        args = parser.parse_args()

        _log.setLevel(args.log_level)

        self.latitude = args.latitude
        self.longitude = args.longitude

        def parse_date_min(s):
            from datetime import datetime

            if s:
                if len(s) == len("YYYY"):
                    return datetime.strptime(s, "%Y").date()

                elif len(s) == len("YYYY-MM"):
                    return datetime.strptime(s, "%Y-%m").date()

                elif len(s) == len("YYYY-MM-DD"):
                    return datetime.strptime(s, "%Y-%m-%d").date()

            return None

        def parse_date_max(s):
            from datetime import datetime
            import calendar

            if s:
                if len(s) == len("YYYY"):
                    d = datetime.strptime(s, "%Y").date()
                    d = d.replace(month=12, day=31)
                    return d

                elif len(s) == len("YYYY-MM"):
                    d = datetime.strptime(s, "%Y-%m").date()

                    first, last = calendar.monthrange(d.year, d.month)
                    d = d.replace(day=last)
                    return d

                elif len(s) == len("YYYY-MM-DD"):
                    d = datetime.strptime(s, "%Y-%m-%d").date()
                    return d

            return None

        self.acq_min = parse_date_min(args.acq_min)
        self.acq_max = parse_date_max(args.acq_max)

        # self.process_min = parse_date_min(args.process_min)
        # self.process_max = parse_date_max(args.process_max)
        #
        # self.ingest_min = parse_date_min(args.ingest_min)
        # self.ingest_max = parse_date_max(args.ingest_max)

        self.satellites = args.satellite

        self.apply_pqa_filter = args.apply_pqa
        self.pqa_mask = args.pqa_mask

        self.output_no_data = args.output_no_data

        self.dataset_type = args.dataset_type

        self.delimiter = args.delimiter
        self.output_directory = args.output_directory
        self.overwrite = args.overwrite

        _log.info("""
        longitude = {longitude:f}
        latitude = {latitude:f}
        acq = {acq_min} to {acq_max}
        process = {process_min} to {process_max}
        ingest = {ingest_min} to {ingest_max}
        satellites = {satellites}
        apply PQA filter = {apply_pqa_filter}
        PQA mask = {pqa_mask}
        datasets to retrieve = {dataset_type}
        output no data values = {output_no_data}
        output = {output}
        over write = {overwrite}
        delimiter = {delimiter}
        """.format(longitude=self.longitude, latitude=self.latitude,
                   acq_min=self.acq_min, acq_max=self.acq_max,
                   process_min=self.process_min, process_max=self.process_max,
                   ingest_min=self.ingest_min, ingest_max=self.ingest_max,
                   satellites=self.satellites,
                   apply_pqa_filter=self.apply_pqa_filter, pqa_mask=self.pqa_mask,
                   dataset_type=decode_dataset_type(self.dataset_type),
                   output_no_data=self.output_no_data,
                   output=self.output_directory and self.output_directory or "STDOUT",
                   overwrite=self.overwrite,
                   delimiter=self.delimiter))

    def run(self):
        self.parse_arguments()

        config = Config()
        _log.debug(config.to_str())

        cell_x, cell_y = latlon_to_cell(self.latitude, self.longitude)

        # TODO once WOFS is in the cube

        if self.dataset_type in dataset_type_database:

            # TODO - PQ is UNIT16 (others are INT16) and so -999 NDV doesn't work
            ndv = self.dataset_type == DatasetType.PQ25 and UINT16_MAX or NDV

            headered = False

            with self.get_output_file(self.dataset_type, self.overwrite) as csv_file:

                csv_writer = csv.writer(csv_file, delimiter=self.delimiter)

                for tile in list_tiles(x=[cell_x], y=[cell_y], acq_min=self.acq_min, acq_max=self.acq_max,
                                       satellites=[satellite for satellite in self.satellites],
                                       dataset_types=[self.dataset_type],
                                       database=config.get_db_database(),
                                       user=config.get_db_username(),
                                       password=config.get_db_password(),
                                       host=config.get_db_host(), port=config.get_db_port()):

                    # Output a HEADER
                    if not headered:
                        header_fields = ["SATELLITE", "ACQUISITION DATE"] + [b.name for b in tile.datasets[self.dataset_type].bands]
                        csv_writer.writerow(header_fields)
                        headered = True

                    pqa = None

                    # Apply PQA if specified

                    if self.apply_pqa_filter:
                        pqa = tile.datasets[DatasetType.PQ25]

                    data = retrieve_pixel_value(tile.datasets[self.dataset_type], pqa, self.pqa_mask, self.latitude, self.longitude, ndv=ndv)
                    _log.debug("data is [%s]", data)
                    if has_data(tile.datasets[self.dataset_type], data, no_data_value=ndv) or self.output_no_data:
                        csv_writer.writerow([tile.datasets[self.dataset_type].satellite.value, str(tile.end_datetime)] + decode_data(tile.datasets[self.dataset_type], data))

        elif self.dataset_type == DatasetType.WATER:
            base = "/g/data/u46/wofs/water_f7q/extents/{x:03d}_{y:04d}/LS*_WATER_{x:03d}_{y:04d}_*.tif".format(x=cell_x, y=cell_y)

            headered = False

            with self.get_output_file(self.dataset_type, self.overwrite) as csv_file:

                csv_writer = csv.writer(csv_file, delimiter=self.delimiter)

                for f in glob.glob(base):
                    _log.debug(" *** Found WOFS file [%s]", f)

                    satellite, dataset_type, x, y, acq_dt = extract_fields_from_filename(os.path.basename(f))

                    if acq_dt.date() < self.acq_min or acq_dt.date() > self.acq_max:
                        continue

                    dataset = DatasetTile.from_path(f)
                    _log.debug("Found dataset [%s]", dataset)

                    # Output a HEADER
                    if not headered:
                        header_fields = ["SATELLITE", "ACQUISITION DATE"] + [b.name for b in dataset.bands]
                        csv_writer.writerow(header_fields)
                        headered = True

                    data = retrieve_pixel_value(dataset, None, None, self.latitude, self.longitude)
                    _log.debug("data is [%s]", data)

                    # TODO
                    if True or self.output_no_data:
                        csv_writer.writerow([satellite.value, str(acq_dt), decode_wofs_water_value(data[Wofs25Bands.WATER][0][0]), str(data[Wofs25Bands.WATER][0][0])])

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

        if self.apply_pqa_filter:
            dataset_str += "_WITH_PQA"

        return os.path.join(self.output_directory,
                            "{satellite}_{dataset}_{longitude:03.5f}_{latitude:03.5f}_{acq_min}_{acq_max}.csv".format(satellite=satellite_str, dataset=dataset_str, latitude=self.latitude,
                                                                                          longitude=self.longitude,
                                                                                          acq_min=self.acq_min,
                                                                                          acq_max=self.acq_max))


def decode_dataset_type(dataset_type):
    return {DatasetType.ARG25: "Surface Reflectance",
              DatasetType.PQ25: "Pixel Quality",
              DatasetType.FC25: "Fractional Cover",
              DatasetType.WATER: "WOFS Woffle"}[dataset_type]


def has_data(dataset, data, no_data_value=NDV):
    for value in [data[band][0][0] for band in dataset.bands]:
        if value != no_data_value:
            return True

    return False


def decode_data(dataset, data):

    return [str(data[band][0][0]) for band in dataset.bands]


def retrieve_pixel_value(dataset, pq, pq_masks, latitude, longitude, ndv=NDV):
    _log.debug("Retrieving pixel value(s) at lat=[%f] lon=[%f] from [%s] with pq [%s] and pq mask [%s]", latitude, longitude, dataset.path, pq and pq.path or "", pq and pq_masks or "")

    metadata = get_dataset_metadata(dataset)
    x, y = latlon_to_xy(latitude, longitude, metadata.transform)

    _log.debug("Retrieving value at x=[%d] y=[%d]", x, y)

    data = None

    if pq:
        data = get_dataset_data_with_pq(dataset, pq, x=x, y=y, x_size=1, y_size=1, pq_masks=pq_masks, ndv=ndv)
    else:
        data = get_dataset_data(dataset, x=x, y=y, x_size=1, y_size=1)

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
        128: "Wet"
        }

    return values[value]


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    TimeSeriesRetrievalWorkflow("Time Series Retrieval").run()