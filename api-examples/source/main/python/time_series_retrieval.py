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
import argparse
import glob
import os
from datacube.api.model import DatasetType, DatasetTile, Wofs25Bands
from datacube.api.query import list_tiles
from datacube.api.utils import latlon_to_cell, get_dataset_metadata, latlon_to_xy, get_dataset_data_with_pq, \
    get_dataset_data, extract_fields_from_filename
from datacube.config import Config


__author__ = "Simon Oldfield"


import logging


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

    arg = None
    pqa = None
    fc = None
    wofs = None

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

        parser.add_argument("--skip-pq", help="Skip applying PQ to datasets", action="store_false", dest="pqfilter",
                            default=True)

        parser.add_argument("--arg", help="Retrieve NBAR values", action="store_true", dest="arg", default=False)
        parser.add_argument("--pqa", help="Retrieve PQA values", action="store_true", dest="pqa", default=False)
        parser.add_argument("--fc", help="Retrieve FC values", action="store_true", dest="fc", default=False)
        parser.add_argument("--wofs", help="Retrieve WOFS values", action="store_true", dest="wofs", default=False)

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

        self.arg = args.arg
        self.pqa = args.pqa
        self.fc = args.fc
        self.wofs = args.wofs

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
        """.format(longitude=self.longitude, latitude=self.latitude,
                   acq_min=self.acq_min, acq_max=self.acq_max,
                   process_min=self.process_min, process_max=self.process_max,
                   ingest_min=self.ingest_min, ingest_max=self.ingest_max,
                   satellites=self.satellites, apply_pq_filter=self.apply_pq_filter,
                   nbar=self.arg, pqa=self.pqa, fc=self.fc, wofs=self.wofs))

    def run(self):
        self.parse_arguments()

        config = Config(os.path.expanduser("~/.datacube/config"))
        _log.debug(config.to_str())

        cell_x, cell_y = latlon_to_cell(self.latitude, self.longitude)

        datasets = []

        if self.arg:
            datasets.append(DatasetType.ARG25)

        if self.pqa:
            datasets.append(DatasetType.PQ25)

        if self.fc:
            datasets.append(DatasetType.FC25)

        # TODO once WOFS is in the cube

        if len(datasets) > 0:
            for tile in list_tiles(x=[cell_x], y=[cell_y], acq_min=self.acq_min, acq_max=self.acq_max,
                                   satellites=[satellite for satellite in self.satellites],
                                   datasets=datasets,
                                   database=config.get_db_database(),
                                   user=config.get_db_username(),
                                   password=config.get_db_password(),
                                   host=config.get_db_host(), port=config.get_db_port()):

                for dataset in datasets:
                    data = retrieve_pixel_value(tile.datasets[dataset], self.apply_pq_filter and tile.datasets[dataset] or None, self.latitude, self.longitude)
                    _log.info("data is [%s]", data)

        if self.wofs:
            base = "/g/data/u46/wofs/water_f7q/extents/{x:03d}_{y:04d}/LS*_WATER_{x:03d}_{y:04d}_*.tif".format(x=cell_x, y=cell_y)

            for f in glob.glob(base):
                _log.debug(" *** Found WOFS file [%s]", f)
                dataset = DatasetTile.from_path(f)
                _log.debug("Found dataset [%s]", dataset)
                data = retrieve_pixel_value(dataset, None, self.latitude, self.longitude)
                _log.info("data is [%s]", data)
                # TODO
                satellite, dataset_type, x, y, acq_dt = extract_fields_from_filename(os.path.basename(f))
                print "%s|%s" % (acq_dt, decode_wofs_water_value(data[Wofs25Bands.WATER][0][0]))


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

    values = {
        0: "Dry|7",
        1: "No Data|0",
        2: "Saturation/Contiguity|1",
        4: "Sea Water|2",
        8: "Terrain Shadow|3",
        16: "High Slope|4",
        32: "Cloud Shadow|5",
        64: "Cloud|6",
        128: "Wet|8"
        }

    return values[value]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    TimeSeriesRetrievalWorkflow("Time Series Retrieval").run()