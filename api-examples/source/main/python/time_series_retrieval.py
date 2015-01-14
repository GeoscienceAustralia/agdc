#!/usr/bin/env python

# ===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
# * Neither Geoscience Australia nor the names of its contributors may be
# used to endorse or promote products derived from this software
# without specific prior written permission.
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


import argparse
import csv
import glob
import logging
import os
from datacube.api.model import DatasetType, DatasetTile, Wofs25Bands, BANDS, Satellite
from datacube.api.query import list_tiles
from datacube.api.utils import latlon_to_cell, get_dataset_metadata, latlon_to_xy, get_dataset_data_with_pq, \
    get_dataset_data, extract_fields_from_filename, NDV
from datacube.api.workflow import writeable_dir
from datacube.config import Config


_log = logging.getLogger()


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

    apply_pq_filter = None
    output_no_data = None

    nbar = None
    pqa = None
    fc = None
    wofs = None

    delimiter = None
    output_directory = None

    def __init__(self, application_name):
        self.application_name = application_name

    def parse_arguments(self):
        _log.debug("Workflow.parse_arguments()")

        parser = argparse.ArgumentParser(prog=__name__, description=self.application_name)

        parser.add_argument("--lat", help="Latitude value of pixel", action="store", dest="latitude", type=float, required=True)
        parser.add_argument("--lon", help="Longitude value of pixel", action="store", dest="longitude", type=float, required=True)

        parser.add_argument("--acq-min", help="Acquisition Date", action="store", dest="acq_min", type=str)
        parser.add_argument("--acq-max", help="Acquisition Date", action="store", dest="acq_max", type=str)

        parser.add_argument("--process-min", help="Process Date", action="store", dest="process_min", type=str)
        parser.add_argument("--process-max", help="Process Date", action="store", dest="process_max", type=str)

        parser.add_argument("--ingest-min", help="Ingest Date", action="store", dest="ingest_min", type=str)
        parser.add_argument("--ingest-max", help="Ingest Date", action="store", dest="ingest_max", type=str)

        parser.add_argument("--satellites", help="satellites", action="store", dest="satellites", type=str, nargs="+",
                            choices=["LS5", "LS7", "LS8"], default=["LS5", "LS7"])

        parser.add_argument("--skip-pq", help="Don't apply PQA mask", action="store_false", dest="pqfilter", default=True)
        parser.add_argument("--hide-no-data", help="Don't output records that are completely no data value(s)", action="store_false", dest="output_no_data", default=True)

        # For the moment making these mutually exclusive - i.e. you can only do one at a time - just for simplicity
        group = parser.add_mutually_exclusive_group()

        group.add_argument("--nbar", help="Retrieve NBAR values", action="store_true", dest="nbar", default=False)
        group.add_argument("--pqa", help="Retrieve PQA values", action="store_true", dest="pqa", default=False)
        group.add_argument("--fc", help="Retrieve FC values", action="store_true", dest="fc", default=False)
        group.add_argument("--wofs", help="Retrieve WOFS values", action="store_true", dest="wofs", default=False)

        parser.add_argument("--delimiter", help="Field delimiter in output file", action="store", dest="delimiter", type=str, default=",")

        parser.add_argument("--output-directory", help="Output directory", action="store", dest="output_directory",
                            type=writeable_dir, required=True)

        args = parser.parse_args()

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
            from datetime import date, datetime
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

        self.process_min = parse_date_min(args.process_min)
        self.process_max = parse_date_max(args.process_max)

        self.ingest_min = parse_date_min(args.ingest_min)
        self.ingest_max = parse_date_max(args.ingest_max)

        self.satellites = args.satellites
        self.apply_pq_filter = args.pqfilter
        self.output_no_data = args.output_no_data

        self.nbar = args.nbar
        self.pqa = args.pqa
        self.fc = args.fc
        self.wofs = args.wofs

        self.delimiter = args.delimiter
        self.output_directory = args.output_directory

        _log.debug("""
        longitude = {longitude:f}
        latitude = {latitude:f}
        acq = {acq_min} to {acq_max}
        process = {process_min} to {process_max}
        ingest = {ingest_min} to {ingest_max}
        satellites = {satellites}
        apply PQ filter = {apply_pq_filter}
        retrieve NBAR = {nbar}
        retrieve PQA = {pqa}
        retrieve FC = {fc}
        retrieve WOFS = {wofs}
        output no data values = {output_no_data}
        output directory = {output_directory}
        delimiter = {delimiter}
        """.format(longitude=self.longitude, latitude=self.latitude,
                   acq_min=self.acq_min, acq_max=self.acq_max,
                   process_min=self.process_min, process_max=self.process_max,
                   ingest_min=self.ingest_min, ingest_max=self.ingest_max,
                   satellites=self.satellites, apply_pq_filter=self.apply_pq_filter,
                   nbar=self.nbar, pqa=self.pqa, fc=self.fc, wofs=self.wofs,
                   output_no_data=self.output_no_data, output_directory=self.output_directory,
                   delimiter=self.delimiter))

    def run(self):
        self.parse_arguments()

        config = Config(os.path.expanduser("~/.datacube/config"))
        _log.debug(config.to_str())

        cell_x, cell_y = latlon_to_cell(self.latitude, self.longitude)

        dataset = None

        if self.nbar:
            dataset = DatasetType.ARG25

        if self.pqa:
            dataset = DatasetType.PQ25

        if self.fc:
            dataset = DatasetType.FC25

        # TODO once WOFS is in the cube

        if dataset:

            _log.debug("***** [%s]", self.get_output_filename(dataset))

            headered = False

            with open(self.get_output_filename(dataset), "wb") as csv_file:

                csv_writer = csv.writer(csv_file, delimiter=self.delimiter)

                for tile in list_tiles(x=[cell_x], y=[cell_y], acq_min=self.acq_min, acq_max=self.acq_max,
                                       satellites=[satellite for satellite in self.satellites],
                                       datasets=[dataset],
                                       database=config.get_db_database(),
                                       user=config.get_db_username(),
                                       password=config.get_db_password(),
                                       host=config.get_db_host(), port=config.get_db_port()):

                    # Output a HEADER
                    if not headered:
                        header_fields = ["SATELLITE", "ACQUISITION DATE"] + [b.name for b in tile.datasets[dataset].bands]
                        csv_writer.writerow(header_fields)
                        headered = True

                    pqa = None

                    # Apply PQA if specified - but NOT if retrieving PQA data itself - that's crazy talk!!!

                    if self.apply_pq_filter and dataset != DatasetType.PQ25:
                        pqa = tile.datasets[DatasetType.PQ25]

                    data = retrieve_pixel_value(tile.datasets[dataset], pqa, self.latitude, self.longitude)
                    _log.info("data is [%s]", data)
                    if has_data(tile.datasets[dataset], data) or self.output_no_data:
                        csv_writer.writerow([tile.datasets[dataset].satellite.value, str(tile.end_datetime)] + decode_data(tile.datasets[dataset], data))

        if self.wofs:
            base = "/g/data/u46/wofs/water_f7q/extents/{x:03d}_{y:04d}/LS*_WATER_{x:03d}_{y:04d}_*.tif".format(x=cell_x, y=cell_y)

            headered = False

            _log.debug("***** [%s]", self.get_output_filename_wofs())

            with open(self.get_output_filename_wofs(), "wb") as csv_file:

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

                    data = retrieve_pixel_value(dataset, None, self.latitude, self.longitude)
                    _log.info("data is [%s]", data)

                    # TODO
                    if True or self.output_no_data:
                        csv_writer.writerow([satellite.value, str(acq_dt), decode_wofs_water_value(data[Wofs25Bands.WATER][0][0]), str(data[Wofs25Bands.WATER][0][0])])


    def get_output_filename(self, dataset):

        satellite_str = ""

        if Satellite.LS5.value in self.satellites or Satellite.LS7.value in self.satellites or Satellite.LS8.value in self.satellites:
            satellite_str += "LS"

        if Satellite.LS5.value in self.satellites:
            satellite_str += "5"

        if Satellite.LS7.value in self.satellites:
            satellite_str += "7"

        if Satellite.LS8.value in self.satellites:
            satellite_str += "8"

        dataset_str = ""

        if dataset == DatasetType.ARG25:
            dataset_str += "NBAR"

        elif dataset == DatasetType.PQ25:
            dataset_str += "PQA"

        elif dataset == DatasetType.FC25:
            dataset_str += "FC"

        elif dataset == DatasetType.WATER:
            dataset_str += "WOFS"

        if self.apply_pq_filter and dataset != DatasetType.PQ25:
            dataset_str += "_WITH_PQA"

        return os.path.join(self.output_directory,
                            "{satellite}_{dataset}_{longitude:03.5f}_{latitude:03.5f}_{acq_min}_{acq_max}.csv".format(satellite=satellite_str, dataset=dataset_str, latitude=self.latitude,
                                                                                          longitude=self.longitude,
                                                                                          acq_min=self.acq_min,
                                                                                          acq_max=self.acq_max))

    def get_output_filename_wofs(self):
        return os.path.join(self.output_directory,"LS_WOFS_{longitude:03.5f}_{latitude:03.5f}_{acq_min}_{acq_max}.csv".format(latitude=self.latitude,
                                                                                          longitude=self.longitude,
                                                                                          acq_min=self.acq_min,
                                                                                          acq_max=self.acq_max))


def has_data(dataset, data, no_data_value=NDV):
    for value in [data[band][0][0] for band in dataset.bands]:
        if value != no_data_value:
            return True

    return False


def decode_data(dataset, data):

    return [str(data[band][0][0]) for band in dataset.bands]


def retrieve_pixel_value(dataset, pq, latitude, longitude):
    _log.info("Retrieving pixel value(s) at lat=[%f] lon=[%f] from [%s] with pq [%s]", latitude, longitude, dataset.path, pq and pq.path or "")

    metadata = get_dataset_metadata(dataset)
    x, y = latlon_to_xy(latitude, longitude, metadata.transform)

    _log.debug("Retrieving value at x=[%d] y=[%d]", x, y)

    data = None

    if pq:
        data = get_dataset_data_with_pq(dataset, pq, x=x, y=y, x_size=1, y_size=1)
    else:
        data = get_dataset_data(dataset, x=x, y=y, x_size=1, y_size=1)

    _log.info("data is [%s]", data)

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
    logging.basicConfig(level=logging.DEBUG)

    TimeSeriesRetrievalWorkflow("Time Series Retrieval").run()