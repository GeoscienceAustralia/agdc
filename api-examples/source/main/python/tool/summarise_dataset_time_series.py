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
from datetime import datetime, timedelta


__author__ = "Simon Oldfield"


import argparse
import gdal
import numpy
from gdalconst import GA_ReadOnly, GA_Update
import logging
import os
import resource
from datacube.api.model import DatasetType, Satellite, get_bands, dataset_type_database
from datacube.api.query import list_tiles_as_list
from datacube.api.utils import PqaMask, get_dataset_metadata, get_dataset_data, get_dataset_data_with_pq, empty_array
from datacube.api.utils import NDV, UINT16_MAX
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
    __order__ = "YOUNGEST_PIXEL OLDEST_PIXEL MEDOID_PIXEL COUNT MIN MAX MEAN MEDIAN MEDIAN_NON_INTERPOLATED SUM STANDARD_DEVIATION VARIANCE PERCENTILE"

    YOUNGEST_PIXEL = 1
    OLDEST_PIXEL = 2
    MEDOID_PIXEL = 3
    COUNT = 4
    MIN = 5
    MAX = 6
    MEAN = 7
    MEDIAN = 8
    MEDIAN_NON_INTERPOLATED = 9
    SUM = 10
    STANDARD_DEVIATION = 11
    VARIANCE = 12
    PERCENTILE = 13


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

    chunk_size_x = None
    chunk_size_y = None

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

        parser.add_argument("--acq-min", help="Acquisition Date (YYYY or YYYY-MM or YYYY-MM-DD)", action="store", dest="acq_min", type=str, required=True)
        parser.add_argument("--acq-max", help="Acquisition Date (YYYY or YYYY-MM or YYYY-MM-DD)", action="store", dest="acq_max", type=str, required=True)

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

        supported_dataset_types = dataset_type_database

        parser.add_argument("--dataset-type", help="The types of dataset to retrieve", action="store",
                            dest="dataset_type",
                            type=dataset_type_arg,
                            #nargs="+",
                            choices=supported_dataset_types, default=DatasetType.ARG25, metavar=" ".join([s.name for s in supported_dataset_types]))

        parser.add_argument("--output-directory", help="Output directory", action="store", dest="output_directory",
                            type=writeable_dir, required=True)

        parser.add_argument("--overwrite", help="Over write existing output file", action="store_true", dest="overwrite", default=False)

        parser.add_argument("--list-only", help="List the datasets that would be retrieved rather than retrieving them", action="store_true", dest="list_only", default=False)

        supported_summary_methods = [
            TimeSeriesSummaryMethod.YOUNGEST_PIXEL,
            TimeSeriesSummaryMethod.OLDEST_PIXEL,
            # TimeSeriesSummaryMethod.MEDOID_PIXEL,
            TimeSeriesSummaryMethod.COUNT,
            TimeSeriesSummaryMethod.MIN,
            TimeSeriesSummaryMethod.MAX,
            TimeSeriesSummaryMethod.MEAN,
            TimeSeriesSummaryMethod.MEDIAN,
            TimeSeriesSummaryMethod.MEDIAN_NON_INTERPOLATED,
            TimeSeriesSummaryMethod.SUM,
            TimeSeriesSummaryMethod.STANDARD_DEVIATION,
            TimeSeriesSummaryMethod.VARIANCE,
            TimeSeriesSummaryMethod.PERCENTILE]

        parser.add_argument("--summary-method", help="The summary method to apply", action="store",
                            dest="summary_method",
                            type=summary_method_arg,
                            #nargs="+",
                            choices=supported_summary_methods, required=True, metavar=" ".join([s.name for s in supported_summary_methods]))

        parser.add_argument("--chunk-size-x", help="Number of X pixels to process at once", action="store", dest="chunk_size_x", type=int, choices=range(0, 4000+1), default=4000, metavar="[1 - 4000]")
        parser.add_argument("--chunk-size-y", help="Number of Y pixels to process at once", action="store", dest="chunk_size_y", type=int, choices=range(0, 4000+1), default=4000, metavar="[1 - 4000]")

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

        self.dataset_type = args.dataset_type

        self.output_directory = args.output_directory
        self.overwrite = args.overwrite
        self.list_only = args.list_only

        self.summary_method = args.summary_method

        self.chunk_size_x = args.chunk_size_x
        self.chunk_size_y = args.chunk_size_y

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
        chunk size = {chunk_size_x:4d} x {chunk_size_y:4d} pixels
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
                   summary_method=self.summary_method,
                   chunk_size_x=self.chunk_size_x,
                   chunk_size_y=self.chunk_size_y))

    def run(self):
        self.parse_arguments()

        config = Config()
        _log.debug(config.to_str())

        path = self.get_output_filename(self.dataset_type)
        _log.info("Output file is [%s]", path)

        if os.path.exists(path):
            if self.overwrite:
                _log.info("Removing existing output file [%s]", path)
                os.remove(path)
            else:
                _log.error("Output file [%s] exists", path)
                raise Exception("Output file [%s] already exists" % path)

        # TODO
        bands = get_bands(self.dataset_type, self.satellites[0])

        # TODO once WOFS is in the cube

        tiles = list_tiles_as_list(x=[self.x], y=[self.y], acq_min=self.acq_min, acq_max=self.acq_max,
                                   satellites=[satellite for satellite in self.satellites],
                                   dataset_types=[self.dataset_type],
                                   database=config.get_db_database(),
                                   user=config.get_db_username(),
                                   password=config.get_db_password(),
                                   host=config.get_db_host(), port=config.get_db_port())

        raster = None
        metadata = None

        # TODO - PQ is UNIT16 (others are INT16) and so -999 NDV doesn't work
        ndv = self.dataset_type == DatasetType.PQ25 and UINT16_MAX or NDV

        _log.debug("Current MAX RSS  usage is [%d] MB",  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)

        import itertools
        for x, y in itertools.product(range(0, 4000, self.chunk_size_x), range(0, 4000, self.chunk_size_y)):

            _log.info("About to read data chunk ({xmin:4d},{ymin:4d}) to ({xmax:4d},{ymax:4d})".format(xmin=x, ymin=y, xmax=x+self.chunk_size_x-1, ymax=y+self.chunk_size_y-1))
            _log.debug("Current MAX RSS  usage is [%d] MB",  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)

            stack = dict()

            for tile in tiles:

                if self.list_only:
                    _log.info("Would summarise dataset [%s]", tile.datasets[self.dataset_type].path)
                    continue

                pqa = None

                _log.debug("Reading dataset [%s]", tile.datasets[self.dataset_type].path)

                if not metadata:
                    metadata = get_dataset_metadata(tile.datasets[self.dataset_type])

                # Apply PQA if specified

                if self.apply_pqa_filter:
                    data = get_dataset_data_with_pq(tile.datasets[self.dataset_type], tile.datasets[DatasetType.PQ25], bands=bands, x=x, y=y, x_size=self.chunk_size_x, y_size=self.chunk_size_y, pq_masks=self.pqa_mask, ndv=ndv)

                else:
                    data = get_dataset_data(tile.datasets[self.dataset_type], bands=bands, x=x, y=y, x_size=self.chunk_size_x, y_size=self.chunk_size_y)

                for band in bands:
                    if band in stack:
                        stack[band].append(data[band])

                    else:
                        stack[band] = [data[band]]

                    _log.debug("data[%s] has shape [%s] and MB [%s]", band.name, numpy.shape(data[band]), data[band].nbytes/1000/1000)
                    _log.debug("stack[%s] has [%s] elements", band.name, len(stack[band]))

            # Apply summary method

            _log.info("Finished reading {count} datasets for chunk ({xmin:4d},{ymin:4d}) to ({xmax:4d},{ymax:4d}) - about to summarise them".format(count=len(tiles), xmin=x, ymin=y, xmax=x+self.chunk_size_x-1, ymax=y+self.chunk_size_y-1))
            _log.debug("Current MAX RSS  usage is [%d] MB",  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)

            masked_stack = dict()

            for band in bands:
                masked_stack[band] = numpy.ma.masked_equal(stack[band], ndv)
                _log.debug("masked_stack[%s] is %s", band.name, masked_stack[band])
                _log.debug("masked stack[%s] has shape [%s] and MB [%s]", band.name, numpy.shape(masked_stack[band]), masked_stack[band].nbytes/1000/1000)
                _log.debug("Current MAX RSS  usage is [%d] MB",  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)

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
                    # TODO Need to artificially create masked array here since it is being expected/filled below!!!
                    masked_summary = numpy.ma.masked_equal(masked_stack[band].count(axis=0), ndv)

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

                elif self.summary_method == TimeSeriesSummaryMethod.YOUNGEST_PIXEL:

                    # TODO the fact that this is band at a time might be problematic.  We really should be considering
                    # all bands at once (that is what the landsat_mosaic logic did).  If PQA is being applied then
                    # it's probably all good but if not then we might get odd results....

                    masked_summary = empty_array(shape=(self.chunk_size_x, self.chunk_size_x), dtype=numpy.int16, ndv=ndv)

                    # Note the reversed as the stack is created oldest first
                    for d in reversed(stack[band]):
                        masked_summary = numpy.where(masked_summary == ndv, d, masked_summary)

                        # If the summary doesn't contain an no data values then we can stop
                        if not numpy.any(masked_summary == ndv):
                            break

                    # TODO Need to artificially create masked array here since it is being expected/filled below!!!
                    masked_summary = numpy.ma.masked_equal(masked_summary, ndv)

                elif self.summary_method == TimeSeriesSummaryMethod.OLDEST_PIXEL:

                    # TODO the fact that this is band at a time might be problematic.  We really should be considering
                    # all bands at once (that is what the landsat_mosaic logic did).  If PQA is being applied then
                    # it's probably all good but if not then we might get odd results....

                    masked_summary = empty_array(shape=(self.chunk_size_x, self.chunk_size_x), dtype=numpy.int16, ndv=ndv)

                    # Note the NOT reversed as the stack is created oldest first
                    for d in stack[band]:
                        masked_summary = numpy.where(masked_summary == ndv, d, masked_summary)

                        # If the summary doesn't contain an no data values then we can stop
                        if not numpy.any(masked_summary == ndv):
                            break

                    # TODO Need to artificially create masked array here since it is being expected/filled below!!!
                    masked_summary = numpy.ma.masked_equal(masked_summary, ndv)

                masked_stack[band] = None
                _log.debug("NONE-ing masked stack[%s]", band.name)
                _log.debug("Current MAX RSS  usage is [%d] MB",  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)

                _log.debug("masked summary is [%s]", masked_summary)
                _log.debug("Current MAX RSS  usage is [%d] MB",  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)

                # Create the output file

                if not os.path.exists(path):
                    _log.info("Creating raster [%s]", path)

                    driver = gdal.GetDriverByName("GTiff")
                    assert driver

                    raster = driver.Create(path, metadata.shape[0], metadata.shape[1], len(bands), gdal.GDT_Int16)
                    assert raster

                    raster.SetGeoTransform(metadata.transform)
                    raster.SetProjection(metadata.projection)

                    for b in bands:
                        raster.GetRasterBand(b.value).SetNoDataValue(ndv)

                _log.info("Writing band [%s] data to raster [%s]", band.name, path)
                _log.debug("Current MAX RSS  usage is [%d] MB",  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)

                raster.GetRasterBand(band.value).WriteArray(masked_summary.filled(ndv), xoff=x, yoff=y)
                raster.GetRasterBand(band.value).ComputeStatistics(True)

                raster.FlushCache()

                masked_summary = None
                _log.debug("NONE-ing the masked summary")
                _log.debug("Current MAX RSS  usage is [%d] MB",  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)

            stack = None
            _log.debug("Just NONE-ed the stack")
            _log.debug("Current MAX RSS  usage is [%d] MB",  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)

        raster = None

        _log.debug("Just NONE'd the raster")
        _log.debug("Current MAX RSS  usage is [%d] MB",  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)

        _log.info("Memory usage was [%d MB]", resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
        _log.info("CPU time used [%s]", timedelta(seconds=int(resource.getrusage(resource.RUSAGE_SELF).ru_utime)))

    def get_output_filename(self, dataset_type):

        if dataset_type == DatasetType.WATER:
            return os.path.join(self.output_directory,
                                "LS_WOFS_SUMMARY_{x:03d}_{y:04d}_{acq_min}_{acq_max}.tif".format(latitude=self.x,
                                                                                         longitude=self.y,
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

        if self.apply_pqa_filter and dataset_type != DatasetType.PQ25:
            dataset_str += "_WITH_PQA"

        return os.path.join(self.output_directory,
                            "{satellite}_{dataset}_SUMMARY_{x:03d}_{y:04d}_{acq_min}_{acq_max}.tif".format(
                                satellite=satellite_str, dataset=dataset_str, x=self.x,
                                y=self.y,
                                acq_min=self.acq_min,
                                acq_max=self.acq_max))


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