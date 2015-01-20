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
import numpy


__author__ = "Simon Oldfield"


import argparse
import logging
import os
from datacube.api.model import DatasetType, Satellite, Ls8Arg25Bands
from datacube.api.query import list_tiles
from datacube.api.utils import PqaMask
from datacube.api.utils import get_dataset_data, get_dataset_data_with_pq, NDV
from datacube.api.workflow import writeable_dir
from datacube.config import Config
from enum import Enum


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


def summary_method_arg(s):
    if s in TimeSeriesSummaryMethod._member_names_:
        return TimeSeriesSummaryMethod[s]
    raise argparse.ArgumentTypeError("{0} is not a supported summary method".format(s))


class TimeSeriesSummaryMethod(Enum):
    __order__ = "YOUNGEST_PIXEL COUNT MIN MAX MEAN MEDIAN SUM"

    YOUNGEST_PIXEL = 1
    COUNT = 2
    MIN = 3
    MAX = 4
    MEAN = 5
    MEDIAN = 6
    MEDIAN_NON_INTERPOLATED = 7
    SUM = 8
    STANDARD_DEVIATION = 9
    VARIANCE = 10
    PERCENTILE = 11


class SummariseDatasetTimeSeriesWorkflow():

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

    dataset_type = None

    output_directory = None
    overwrite = None
    list_only = None

    summary_method = None

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
                            #nargs="+",
                            choices=DatasetType, default=DatasetType.ARG25)

        parser.add_argument("--output-directory", help="Output directory", action="store", dest="output_directory",
                            type=writeable_dir, required=True)

        parser.add_argument("--overwrite", help="Over write existing output file", action="store_true", dest="overwrite", default=False)

        parser.add_argument("--list-only", help="List the datasets that would be retrieved rather than retrieving them", action="store_true", dest="list_only", default=False)

        parser.add_argument("--summary-method", help="The summary method to apply", action="store",
                            dest="summary_method",
                            type=summary_method_arg,
                            #nargs="+",
                            choices=TimeSeriesSummaryMethod, required=True)

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

        self.dataset_type = args.dataset_type

        self.output_directory = args.output_directory
        self.overwrite = args.overwrite
        self.list_only = args.list_only

        self.summary_method = args.summary_method

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
        summary method = {summary_method}
        """.format(x=self.x, y=self.y,
                   acq_min=self.acq_min, acq_max=self.acq_max,
                   process_min=self.process_min, process_max=self.process_max,
                   ingest_min=self.ingest_min, ingest_max=self.ingest_max,
                   satellites=self.satellites,
                   apply_pqa_filter=self.apply_pqa_filter, pqa_mask=self.pqa_mask,
                   dataset_type=decode_dataset_type(self.dataset_type),
                   output=self.output_directory,
                   overwrite=self.overwrite,
                   list_only=self.list_only,
                   summary_method=self.summary_method))

    def run(self):
        self.parse_arguments()

        config = Config(os.path.expanduser("~/.datacube/config"))
        _log.debug(config.to_str())

        # TODO once WOFS is in the cube

        stack = dict()

        bands = [Ls8Arg25Bands.COASTAL_AEROSOL]

        for tile in list_tiles(x=[self.x], y=[self.y], acq_min=self.acq_min, acq_max=self.acq_max,
                               satellites=[satellite for satellite in self.satellites],
                               datasets=[self.dataset_type],
                               database=config.get_db_database(),
                               user=config.get_db_username(),
                               password=config.get_db_password(),
                               host=config.get_db_host(), port=config.get_db_port()):

            if self.list_only:
                _log.info("Would summarise dataset [%s]", tile.datasets[self.dataset_type].path)
                continue

            pqa = None

            _log.info("Reading dataset [%s]", tile.datasets[self.dataset_type].path)

            # Apply PQA if specified - but NOT if retrieving PQA data itself - that's crazy talk!!!

            if self.apply_pqa_filter and self.dataset_type != DatasetType.PQ25:
                data = get_dataset_data_with_pq(tile.datasets[self.dataset_type], tile.datasets[DatasetType.PQ25], bands=bands, x=0, y=0, x_size=2000, y_size=2000, pq_masks=self.pqa_mask)

            else:
                data = get_dataset_data(tile.datasets[self.dataset_type], bands=bands, x=0, y=0, x_size=2000, y_size=2000)

            for band in bands:
                if band in stack:
                    stack[band].append(data[band])

                else:
                    stack[band] = [data[band]]

                _log.info("data[%s] has shape [%s] and MB [%s]", band.name, numpy.shape(data[band]), data[band].nbytes/1000/1000)
                _log.info("stack[%s] has [%s] elements", band.name, len(stack[band]))

        # Apply summary method

        masked_stack = dict()

        for band in bands:
            masked_stack[band] = numpy.ma.masked_equal(stack[band], NDV)
            _log.info("masked stack[%s] has shape [%s] and MB [%s]", band.name, numpy.shape(masked_stack[band]), masked_stack[band].nbytes/1000/1000)

            if self.summary_method == TimeSeriesSummaryMethod.MIN:
                masked_summary = numpy.min(masked_stack[band], axis=0)

            elif self.summary_method == TimeSeriesSummaryMethod.MAX:
                masked_summary = numpy.max(masked_stack[band], axis=0)

            elif self.summary_method == TimeSeriesSummaryMethod.MEAN:
                masked_summary = numpy.mean(masked_stack[band], axis=0)

            elif self.summary_method == TimeSeriesSummaryMethod.MEDIAN:
                masked_summary = numpy.median(masked_stack[band], axis=0)

            # aka 50th percentile

            elif self.summary_method == TimeSeriesSummaryMethod.MEDIAN_NON_INTERPOLATED:
                masked_sorted = numpy.ma.sort(masked_stack[band], axis=0)
                masked_percentile_index = numpy.ma.floor(numpy.ma.count(masked_sorted, axis=0) * 0.95).astype(numpy.int16)
                masked_summary = numpy.ma.choose(masked_percentile_index, masked_sorted)

            elif self.summary_method == TimeSeriesSummaryMethod.COUNT:
                masked_summary = masked_stack[band].count(axis=0)

            elif self.summary_method == TimeSeriesSummaryMethod.SUM:
                masked_summary = numpy.sum(masked_stack[band], axis=0)

            elif self.summary_method == TimeSeriesSummaryMethod.STANDARD_DEVIATION:
                masked_summary = numpy.std(masked_stack[band], axis=0)

            elif self.summary_method == TimeSeriesSummaryMethod.VARIANCE:
                masked_summary = numpy.var(masked_stack[band], axis=0)

            # currently 95th percentile

            elif self.summary_method == TimeSeriesSummaryMethod.PERCENTILE:
                masked_sorted = numpy.ma.sort(masked_stack[band], axis=0)
                masked_percentile_index = numpy.ma.floor(numpy.ma.count(masked_sorted, axis=0) * 0.95).astype(numpy.int16)
                masked_summary = numpy.ma.choose(masked_percentile_index, masked_sorted)

            _log.info("masked summary is [%s]", masked_summary)


def decode_dataset_type(dataset_type):
    return {DatasetType.ARG25: "Surface Reflectance",
              DatasetType.PQ25: "Pixel Quality",
              DatasetType.FC25: "Fractional Cover",
              DatasetType.WATER: "WOFS Woffle",
              DatasetType.NDVI: "NDVI",
              DatasetType.EVI: "EVI",
              DatasetType.NBR: "Normalised Burn Ratio"}[dataset_type]


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    SummariseDatasetTimeSeriesWorkflow("Summarise Dataset Time Series").run()