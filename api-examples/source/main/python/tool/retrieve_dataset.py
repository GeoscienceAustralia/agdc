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
import gdal
import itertools
import logging
import os
import subprocess
from datacube.api.model import DatasetType, Satellite, dataset_type_database, dataset_type_derived_nbar, BANDS
from datacube.api.query import list_tiles
from datacube.api.utils import PqaMask, raster_create, intersection, calculate_ndvi, calculate_evi, calculate_nbr
from datacube.api.utils import get_dataset_data, get_dataset_data_with_pq, get_dataset_metadata, NDV
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


class DatasetRetrievalWorkflow(object):

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

    stack_vrt = None

    def __init__(self, application_name):
        self.application_name = application_name

    def parse_arguments(self):
        parser = argparse.ArgumentParser(prog=__name__, description=self.application_name)

        group = parser.add_mutually_exclusive_group()

        group.add_argument("--quiet", help="Less output", action="store_const", dest="log_level", const=logging.WARN)
        group.add_argument("--verbose", help="More output", action="store_const", dest="log_level", const=logging.DEBUG)

        parser.set_defaults(log_level=logging.INFO)

        parser.add_argument("--x", help="X grid reference", action="store", dest="x", type=int, choices=range(110, 155+1), required=True, metavar="[110 - 155]")
        parser.add_argument("--y", help="Y grid reference", action="store", dest="y", type=int, choices=range(-45, -10+1), required=True, metavar="[-45 - -10]")

        parser.add_argument("--acq-min", help="Acquisition Date", action="store", dest="acq_min", type=str, required=True)
        parser.add_argument("--acq-max", help="Acquisition Date", action="store", dest="acq_max", type=str, required=True)

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

        supported_dataset_types = dataset_type_database + dataset_type_derived_nbar

        parser.add_argument("--dataset-type", help="The types of dataset to retrieve", action="store",
                            dest="dataset_type",
                            type=dataset_type_arg,
                            nargs="+",
                            choices=supported_dataset_types, default=DatasetType.ARG25, metavar=" ".join([s.name for s in supported_dataset_types]))

        parser.add_argument("--output-directory", help="Output directory", action="store", dest="output_directory",
                            type=writeable_dir, required=True)

        parser.add_argument("--overwrite", help="Over write existing output file", action="store_true", dest="overwrite", default=False)

        parser.add_argument("--list-only", help="List the datasets that would be retrieved rather than retrieving them", action="store_true", dest="list_only", default=False)

        # parser.add_argument("--stack-vrt", help="Create a band stack VRT", action="store_true", dest="stack_vrt", default=False)

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

        # self.process_min = parse_date_min(args.process_min)
        # self.process_max = parse_date_max(args.process_max)
        #
        # self.ingest_min = parse_date_min(args.ingest_min)
        # self.ingest_max = parse_date_max(args.ingest_max)

        self.satellites = args.satellite

        self.apply_pqa_filter = args.apply_pqa
        self.pqa_mask = args.pqa_mask

        self.dataset_types = args.dataset_type

        self.output_directory = args.output_directory
        self.overwrite = args.overwrite
        self.list_only = args.list_only

        # self.stack_vrt = args.stack_vrt

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
        stack (VRT) = {stack_vrt}
        """.format(x=self.x, y=self.y,
                   acq_min=self.acq_min, acq_max=self.acq_max,
                   process_min=self.process_min, process_max=self.process_max,
                   ingest_min=self.ingest_min, ingest_max=self.ingest_max,
                   satellites=self.satellites,
                   apply_pqa_filter=self.apply_pqa_filter, pqa_mask=self.pqa_mask,
                   dataset_type=[decode_dataset_type(t) for t in self.dataset_types],
                   output=self.output_directory,
                   overwrite=self.overwrite,
                   list_only=self.list_only,
                   stack_vrt=self.stack_vrt))

    def run(self):
        self.parse_arguments()

        config = Config()
        _log.debug(config.to_str())

        # Clear stack files
        # TODO - filename consistency and safety and so on

        if self.stack_vrt:
            for satellite, dataset_type in itertools.product(self.satellites, self.dataset_types):
                path = os.path.join(self.output_directory, get_filename_file_list(satellite, dataset_type, self.x, self.y))
                check_overwrite_remove_or_fail(path, self.overwrite)

        # TODO once WOFS is in the cube

        for tile in list_tiles(x=[self.x], y=[self.y], acq_min=self.acq_min, acq_max=self.acq_max,
                               satellites=[satellite for satellite in self.satellites],
                               dataset_types=intersection(self.dataset_types, dataset_type_database),
                               database=config.get_db_database(),
                               user=config.get_db_username(),
                               password=config.get_db_password(),
                               host=config.get_db_host(), port=config.get_db_port()):

            if self.list_only:
                _log.info("Would retrieve datasets [%s]", [tile.datasets[t].path for t in intersection(self.dataset_types, dataset_type_database)])
                continue

            pqa = None

            # Apply PQA if specified

            if self.apply_pqa_filter:
                pqa = tile.datasets[DatasetType.PQ25]

            for dataset_type in intersection(self.dataset_types, dataset_type_database):
                retrieve_data(tile.datasets[dataset_type], pqa, self.pqa_mask, self.get_output_filename(tile.datasets[dataset_type]), tile.x, tile.y, self.overwrite, self.stack_vrt)

            nbar = tile.datasets[DatasetType.ARG25]

            self.generate_derived_nbar(intersection(self.dataset_types, dataset_type_derived_nbar), nbar, pqa, self.pqa_mask, self.overwrite)

        # Generate VRT stack
        if self.stack_vrt:
            for satellite, dataset_type in itertools.product(self.satellites, self.dataset_types):
                path = os.path.join(self.output_directory, get_filename_file_list(satellite, dataset_type, self.x, self.y))
                if os.path.exists(path):
                    for band in BANDS[dataset_type, satellite]:
                        path_vrt = os.path.join(self.output_directory, get_filename_stack_vrt(satellite, dataset_type, self.x, self.y, band))
                        _log.info("Generating VRT file [%s] for band [%s]", path_vrt, band)
                        # gdalbuildrt -separate -b <band> -input_file_list <input file> <vrt file>
                        subprocess.call(["gdalbuildvrt", "-separate", "-b", str(band.value), "-input_file_list", path, path_vrt])

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

            elif dataset_type == DatasetType.EVI:
                evi = calculate_evi(data[nbar.bands.RED], data[nbar.bands.BLUE], data[nbar.bands.NEAR_INFRARED])
                raster_create(filename, [evi], metadata.transform, metadata.projection, NDV, gdal.GDT_Float32)

            elif dataset_type == DatasetType.NBR:
                nbr = calculate_nbr(data[nbar.bands.NEAR_INFRARED], data[nbar.bands.SHORT_WAVE_INFRARED_2])
                raster_create(filename, [nbr], metadata.transform, metadata.projection, NDV, gdal.GDT_Float32)

    def get_output_filename(self, dataset):

        filename = os.path.basename(dataset.path)

        if filename.endswith(".vrt"):
            filename = filename.replace(".vrt", ".tif")

        if self.apply_pqa_filter:
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
            DatasetType.NDVI: "_NDVI_",
            DatasetType.EVI: "_EVI_",
            DatasetType.NBR: "_NBR_"
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
              DatasetType.NDVI: "NDVI",
              DatasetType.EVI: "EVI",
              DatasetType.NBR: "Normalised Burn Ratio"}[dataset_type]


def retrieve_data(dataset, pq, pq_masks, path, x, y, overwrite=False, stack=False):
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

    # If we are creating a stack then also add to a file list file...
    if stack:
        path_file_list = os.path.join(os.path.dirname(path), get_filename_file_list(dataset.satellite, dataset.dataset_type, x, y))
        _log.info("Also going to write file list to [%s]", path_file_list)
        with open(path_file_list, "ab") as f:
            print >>f, path


def get_filename_file_list(satellite, dataset_type, x, y):

    satellite_str = "LS"

    if satellite == Satellite.LS8 and dataset_type == DatasetType.ARG25:
        satellite_str += "8"

    dataset_type_str = {
        DatasetType.ARG25: "NBAR",
        DatasetType.PQ25: "PQA",
        DatasetType.FC25: "FC",
        DatasetType.NDVI: "NDVI",
        DatasetType.EVI: "EVI",
        DatasetType.NBR: "NBR"
    }[dataset_type]

    return "{satellite}_{dataset}_{x:03d}_{y:04d}.files.txt".format(satellite=satellite_str, dataset=dataset_type, x=x, y=y)


def get_filename_stack_vrt(satellite, dataset_type, x, y, band):

    satellite_str = "LS"

    if satellite == Satellite.LS8 and dataset_type == DatasetType.ARG25:
        satellite_str += "8"

    dataset_type_str = {
        DatasetType.ARG25: "NBAR",
        DatasetType.PQ25: "PQA",
        DatasetType.FC25: "FC",
        DatasetType.NDVI: "NDVI",
        DatasetType.EVI: "EVI",
        DatasetType.NBR: "NBR"
    }[dataset_type]

    return "{satellite}_{dataset}_{x:03d}_{y:04d}_{band}.vrt".format(satellite=satellite_str, dataset=dataset_type, x=x, y=y, band=band.name)


def check_overwrite_remove_or_fail(path, overwrite):
    if os.path.exists(path):
        if overwrite:
            os.remove(path)
        else:
            raise Exception("File [%s] exists" % path)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    DatasetRetrievalWorkflow("Dataset Retrieval").run()