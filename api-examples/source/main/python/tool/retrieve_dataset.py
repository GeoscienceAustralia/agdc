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
import gdal


__author__ = "Simon Oldfield"


import argparse
import csv
import glob
import logging
import os
import sys
from datacube.api.model import DatasetType, DatasetTile, Wofs25Bands, Satellite, dataset_type_database, \
    dataset_type_derived_nbar
from datacube.api.query import list_tiles
from datacube.api.utils import latlon_to_cell, latlon_to_xy, PqaMask, raster_create, intersection, calculate_ndvi
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


class DatasetRetrievalWorkflow():

    application_name = None

    x = None
    y = None

    acq_min = None
    acq_max = None

    process_min = None
    process_max = None

    ingest_min = None
    ingest_max = None

    satellites = None

    apply_pqa_filter = None
    pqa_mask = None

    dataset_types = None

    output_directory = None
    overwrite = None
    list_only = None

    def __init__(self, application_name):
        self.application_name = application_name

    def parse_arguments(self):
        parser = argparse.ArgumentParser(prog=__name__, description=self.application_name)

        group = parser.add_mutually_exclusive_group()

        group.add_argument("--quiet", help="Less output", action="store_const", dest="log_level", const=logging.WARN)
        group.add_argument("--verbose", help="More output", action="store_const", dest="log_level", const=logging.DEBUG)

        parser.set_defaults(log_level=logging.INFO)

        parser.add_argument("--x", help="x grid reference", action="store", dest="x", type=int, choices=range(110, 155+1), required=True)
        parser.add_argument("--y", help="y grid reference", action="store", dest="y", type=int, choices=range(-45, -10+1), required=True)

        parser.add_argument("--acq-min", help="Acquisition Date", action="store", dest="acq_min", type=str, required=True)
        parser.add_argument("--acq-max", help="Acquisition Date", action="store", dest="acq_max", type=str, required=True)

        parser.add_argument("--process-min", help="Process Date", action="store", dest="process_min", type=str)
        parser.add_argument("--process-max", help="Process Date", action="store", dest="process_max", type=str)

        parser.add_argument("--ingest-min", help="Ingest Date", action="store", dest="ingest_min", type=str)
        parser.add_argument("--ingest-max", help="Ingest Date", action="store", dest="ingest_max", type=str)

        parser.add_argument("--satellite", help="The satellite(s) to include", action="store", dest="satellite",
                            type=satellite_arg, nargs="+", choices=Satellite, default=[Satellite.LS5, Satellite.LS7])

        parser.add_argument("--apply-pqa", help="Apply PQA mask", action="store_true", dest="apply_pqa", default=False)
        parser.add_argument("--pqa-mask", help="The PQA mask to apply", action="store", dest="pqa_mask",
                            type=pqa_mask_arg, nargs="+", choices=PqaMask, default=[PqaMask.PQ_MASK_CLEAR])

        parser.add_argument("--dataset-type", help="The types of dataset to retrieve", action="store",
                            dest="dataset_type",
                            type=dataset_type_arg,
                            nargs="+",
                            choices=DatasetType, default=DatasetType.ARG25)

        parser.add_argument("--output-directory", help="Output directory", action="store", dest="output_directory",
                            type=writeable_dir, required=True)

        parser.add_argument("--overwrite", help="Over write existing output file", action="store_true", dest="overwrite", default=False)

        parser.add_argument("--list-only", help="List the datasets that would be retrieved rather than retrieving them", action="store_true", dest="list_only", default=False)

        args = parser.parse_args()

        _log.setLevel(args.log_level)

        self.x = args.x
        self.y = args.y

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

        self.process_min = parse_date_min(args.process_min)
        self.process_max = parse_date_max(args.process_max)

        self.ingest_min = parse_date_min(args.ingest_min)
        self.ingest_max = parse_date_max(args.ingest_max)

        self.satellites = args.satellite

        self.apply_pqa_filter = args.apply_pqa
        self.pqa_mask = args.pqa_mask

        self.dataset_types = args.dataset_type

        self.output_directory = args.output_directory
        self.overwrite = args.overwrite
        self.list_only = args.list_only

        _log.info("""
        x = {x:03d}
        y = {y:04d}
        acq = {acq_min} to {acq_max}
        process = {process_min} to {process_max}
        ingest = {ingest_min} to {ingest_max}
        satellites = {satellites}
        apply PQA filter = {apply_pqa_filter}
        PQA mask = {pqa_mask}
        datasets to retrieve = {dataset_type}
        output directory = {output}
        over write existing = {overwrite}
        list only = {list_only}
        """.format(x=self.x, y=self.y,
                   acq_min=self.acq_min, acq_max=self.acq_max,
                   process_min=self.process_min, process_max=self.process_max,
                   ingest_min=self.ingest_min, ingest_max=self.ingest_max,
                   satellites=self.satellites,
                   apply_pqa_filter=self.apply_pqa_filter, pqa_mask=self.pqa_mask,
                   dataset_type=[decode_dataset_type(t) for t in self.dataset_types],
                   output=self.output_directory,
                   overwrite=self.overwrite,
                   list_only=self.list_only))

    def run(self):
        self.parse_arguments()

        config = Config(os.path.expanduser("~/.datacube/config"))
        _log.debug(config.to_str())

        # TODO once WOFS is in the cube

        for tile in list_tiles(x=[self.x], y=[self.y], acq_min=self.acq_min, acq_max=self.acq_max,
                               satellites=[satellite for satellite in self.satellites],
                               datasets=intersection(self.dataset_types, dataset_type_database),
                               database=config.get_db_database(),
                               user=config.get_db_username(),
                               password=config.get_db_password(),
                               host=config.get_db_host(), port=config.get_db_port()):

            if self.list_only:
                _log.info("Would retrieve datasets [%s]", [tile.datasets[t].path for t in self.dataset_types])
                continue

            pqa = None

            # Apply PQA if specified - but NOT if retrieving PQA data itself - that's crazy talk!!!

            if self.apply_pqa_filter and self.dataset_types != DatasetType.PQ25:
                pqa = tile.datasets[DatasetType.PQ25]

            for dataset_type in intersection(self.dataset_types, dataset_type_database):
                retrieve_data(tile.datasets[dataset_type], pqa, self.pqa_mask, self.get_output_filename(tile.datasets[dataset_type]), self.overwrite)

            nbar = tile.datasets[DatasetType.ARG25]

            self.generate_derived_nbar(intersection(self.dataset_types, dataset_type_derived_nbar), nbar, pqa, self.pqa_mask, self.overwrite)

    def generate_derived_nbar(self, dataset_types, nbar, pqa, pqa_masks, overwrite=False):
        for dataset_type in dataset_types:
            filename = self.get_output_filename_derived_nbar(nbar, dataset_type)
            _log.info("Generating data from [%s] with pq [%s] and pq mask [%s] to [%s]", nbar.path, pqa and pqa.path or "", pqa and pqa_masks or "", filename)

            metadata = get_dataset_metadata(nbar)

            data = None

            if pqa:
                data = get_dataset_data_with_pq(nbar, pqa, pq_masks=pqa_masks)
            else:
                data = get_dataset_data(nbar)

            _log.debug("data is [%s]", data)

            if dataset_type == DatasetType.NDVI:
                ndvi = calculate_ndvi(data[nbar.bands.RED], data[nbar.bands.NEAR_INFRARED])

                raster_create(filename, [ndvi], metadata.transform, metadata.projection, NDV, gdal.GDT_Float32)

    def get_output_filename(self, dataset):

        filename = os.path.basename(dataset.path)

        if filename.endswith(".vrt"):
            filename = filename.replace(".vrt", ".tif")

        if self.apply_pqa_filter and dataset.dataset_type != DatasetType.PQ25:
            dataset_type_string = {
                DatasetType.ARG25: "_NBAR_",
                DatasetType.PQ25: "_PQA_",
                DatasetType.FC25: "_FC_"
            }[dataset.dataset_type]

            filename = filename.replace(dataset_type_string, dataset_type_string + "WITH_PQA_")

        return os.path.join(self.output_directory, filename)

    def get_output_filename_derived_nbar(self, nbar, dataset_type):

        filename = os.path.basename(nbar.path)

        if filename.endswith(".vrt"):
            filename = filename.replace(".vrt", ".tif")

        dataset_type_string = {
            DatasetType.NDVI: "_NDVI_"
        }[dataset_type]

        if self.apply_pqa_filter:
            dataset_type_string += "WITH_PQA_"

        filename = filename.replace("_NBAR_", dataset_type_string)

        return os.path.join(self.output_directory, filename)

def decode_dataset_type(dataset_type):
    return {DatasetType.ARG25: "Surface Reflectance",
              DatasetType.PQ25: "Pixel Quality",
              DatasetType.FC25: "Fractional Cover",
              DatasetType.WATER: "WOFS Woffle",
              DatasetType.NDVI: "NDVI"}[dataset_type]


def retrieve_data(dataset, pq, pq_masks, path, overwrite=False):
    _log.info("Retrieving data from [%s] with pq [%s] and pq mask [%s] to [%s]", dataset.path, pq and pq.path or "", pq and pq_masks or "", path)

    if os.path.exists(path) and not overwrite:
        _log.error("Output file [%s] exists", path)
        raise Exception("Output file [%s] already exists" % path)

    data = None

    metadata = get_dataset_metadata(dataset)

    if pq:
        data = get_dataset_data_with_pq(dataset, pq, pq_masks=pq_masks)
    else:
        data = get_dataset_data(dataset)

    _log.debug("data is [%s]", data)

    raster_create(path, [data[b] for b in dataset.bands],
              metadata.transform, metadata.projection, NDV, gdal.GDT_Int16)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    DatasetRetrievalWorkflow("Dataset Retrieval").run()