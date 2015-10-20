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
import logging
import luigi
import numpy
import osr
import os
import sys
from collections import namedtuple
from datacube.api import parse_date_min, parse_date_max, PqaMask, Statistic, PERCENTILE, writeable_dir
from datacube.api import satellite_arg, pqa_mask_arg, dataset_type_arg, statistic_arg, season_arg
from datacube.api.query import list_cells_as_list, list_tiles_as_list
from datacube.api.model import Satellite, DatasetType, Ls57Arg25Bands, NdviBands
from datacube.api.utils import get_dataset_type_ndv, get_dataset_type_data_type, get_dataset_data_stack, log_mem, build_season_date_criteria
from datacube.api.utils import PercentileInterpolation
from datacube.api.utils import Season, SEASONS
from datacube.api.utils import calculate_stack_statistic_count, calculate_stack_statistic_count_observed
from datacube.api.utils import calculate_stack_statistic_min, calculate_stack_statistic_max
from datacube.api.utils import calculate_stack_statistic_mean, calculate_stack_statistic_percentile
from datacube.api.utils import calculate_stack_statistic_variance, calculate_stack_statistic_standard_deviation
from datacube.api.workflow import Task


_log = logging.getLogger()

EpochParameter = namedtuple('Epoch', ['increment', 'duration'])


def ls57_arg_band_arg(s):
    if s in [t.name for t in Ls57Arg25Bands]:
        return Ls57Arg25Bands[s]
    if s in [t.name for t in NdviBands]:
        return NdviBands[s]
    raise argparse.ArgumentTypeError("{0} is not a supported band".format(s))


def percentile_interpolation_arg(s):
    if s in [t.name for t in PercentileInterpolation]:
        return PercentileInterpolation[s]
    raise argparse.ArgumentTypeError("{0} is not a supported percentile interpolation".format(s))


def create_tasks(args):
    x_list = range(args.x_min, args.x_max + 1)
    y_list = range(args.y_min, args.y_max + 1)

    dataset_types = [args.dataset_type]
    if args.mask_pqa_apply:
        dataset_types.append(DatasetType.PQ25)

    from itertools import product

    if args.file_per_statistic:
        for (season, band, statistic) in product(args.get_seasons(), args.bands, args.statistics):
            acq_min_extended, acq_max_extended, criteria = build_season_date_criteria(args.acq_min, args.acq_max, season,
                                                                                      seasons=SEASONS,
                                                                                      extend=True)
            for cell in list_cells_as_list(x=x_list, y=y_list, satellites=args.satellites,
                                           acq_min=acq_min_extended, acq_max=acq_max_extended,
                                           dataset_types=dataset_types, include=criteria):
                yield Arg25EpochStatisticsTask(x=cell.x, y=cell.y,
                                               acq_min=acq_min_extended, acq_max=acq_max_extended,
                                               season=season,
                                               epochs = list(args.get_epochs()),
                                               satellites=args.satellites,
                                               dataset_type=args.dataset_type,
                                               band=band,
                                               bands=args.bands,
                                               mask_pqa_apply=args.mask_pqa_apply, mask_pqa_mask=args.mask_pqa_mask,
                                               x_chunk_size=args.x_chunk_size, y_chunk_size=args.y_chunk_size,
                                               statistic = statistic,
                                               statistics=args.statistics, interpolation=args.interpolation,
                                               output_directory=args.output_directory)
        return

    for (acq_min, acq_max), season in product(args.get_epochs(), args.get_seasons()):
        _log.debug("acq_min=[%s] acq_max=[%s] season=[%s]", acq_min, acq_max, season.name)

        acq_min_extended, acq_max_extended, criteria = build_season_date_criteria(acq_min, acq_max, season,
                                                                                  seasons=SEASONS,
                                                                                  extend=True)

        _log.debug("\tacq_min_extended=[%s], acq_max_extended=[%s], criteria=[%s]", acq_min_extended, acq_max_extended, criteria)
        for cell in list_cells_as_list(x=x_list, y=y_list, satellites=args.satellites,
                                       acq_min=acq_min_extended, acq_max=acq_max_extended,
                                       dataset_types=dataset_types, include=criteria):
            _log.debug("\t%3d %4d", cell.x, cell.y)
            #yield args.create_task(x=cell.x, y=cell.y, acq_min=acq_min, acq_max=acq_max, season=season)
            _log.debug("Creating task for %s %s %s %s %s", cell.x, cell.y, acq_min, acq_max, season)

            yield Arg25BandStatisticsTask(x=cell.x, y=cell.y,
                                          acq_min=acq_min_extended, acq_max=acq_max_extended, season=season,
                                          satellites=args.satellites,
                                          dataset_type=args.dataset_type, bands=args.bands,
                                          mask_pqa_apply=args.mask_pqa_apply, mask_pqa_mask=args.mask_pqa_mask,
                                          x_chunk_size=args.x_chunk_size, y_chunk_size=args.y_chunk_size,
                                          statistics=args.statistics, interpolation=args.interpolation,
                                          output_directory=args.output_directory)


class Arguments(object):

    def __init__(self, name="Arg25 Band Statistics Workflow"):

        # Workflow.__init__(self, name="Arg25 Band Statistics Workflow")

        self.name = name

        self.parser = argparse.ArgumentParser(prog=sys.argv[0], description=self.name)

        self.x_min = None
        self.x_max = None

        self.y_min = None
        self.y_max = None

        self.acq_min = None
        self.acq_max = None

        self.epoch = None

        self.seasons = None

        self.satellites = None

        self.output_directory = None

        self.mask_pqa_apply = None
        self.mask_pqa_mask = None

        self.local_scheduler = None
        self.workers = None

        self.dataset_type = None
        self.bands = None

        self.x_chunk_size = None
        self.y_chunk_size = None

        self.statistics = None

        self.interpolation = None

        self.file_per_statistic = False

        self.setup_arguments()
        self.process_arguments(self.parser.parse_args())

    def setup_arguments(self):

        # # Call method on super class
        # # super(self.__class__, self).setup_arguments()
        # workflow.Workflow.setup_arguments(self)

        # TODO get the combinations of mutually exclusive arguments right

        # TODO months and time slices are sort of mutually exclusive

        self.parser.add_argument("--x-min", help="X index of tiles", action="store", dest="x_min", type=int,
                                 choices=range(110, 155 + 1), required=True, metavar="110 ... 155")

        self.parser.add_argument("--x-max", help="X index of tiles", action="store", dest="x_max", type=int,
                                 choices=range(110, 155 + 1), required=True, metavar="110 ... 155")

        self.parser.add_argument("--y-min", help="Y index of tiles", action="store", dest="y_min", type=int,
                                 choices=range(-45, -10 + 1), required=True, metavar="-45 ... -10")

        self.parser.add_argument("--y-max", help="Y index of tiles", action="store", dest="y_max", type=int,
                                 choices=range(-45, -10 + 1), required=True, metavar="-45 ... -10")

        self.parser.add_argument("--output-directory", help="output directory", action="store", dest="output_directory",
                                 type=writeable_dir, required=True)

        self.parser.add_argument("--acq-min", help="Acquisition Date", action="store", dest="acq_min", type=str,
                                 default="1985")

        self.parser.add_argument("--acq-max", help="Acquisition Date", action="store", dest="acq_max", type=str,
                                 default="2014")

        self.parser.add_argument("--epoch", help="Epoch increment and duration (e.g. 5 6 means 1985-1990, 1990-1995, etc)",
                                 action="store", dest="epoch", type=int, nargs=2, default=[5, 6])

        self.parser.add_argument("--satellite", help="The satellite(s) to include", action="store", dest="satellites",
                                 type=satellite_arg, nargs="+", choices=Satellite,
                                 default=[Satellite.LS5, Satellite.LS7],
                                 metavar=" ".join([ts.name for ts in Satellite]))

        self.parser.add_argument("--mask-pqa-apply", help="Apply PQA mask", action="store_true", dest="mask_pqa_apply",
                                 default=True)

        self.parser.add_argument("--mask-pqa-mask", help="The PQA mask to apply", action="store", dest="mask_pqa_mask",
                                 type=pqa_mask_arg, nargs="+", choices=PqaMask,
                                 default=[PqaMask.PQ_MASK_SATURATION, PqaMask.PQ_MASK_CONTIGUITY, PqaMask.PQ_MASK_CLOUD],
                                 metavar=" ".join([ts.name for ts in PqaMask]))

        self.parser.add_argument("--local-scheduler", help="Use local luigi scheduler rather than MPI",
                                 action="store_true",
                                 dest="local_scheduler", default=True)

        self.parser.add_argument("--workers", help="Number of worker tasks", action="store", dest="workers", type=int,
                                 default=16)

        self.parser.add_argument("--file-per-statistic", help="Generate one file per statistic", action="store_true", dest="file_per_statistic",
                                 default=False)

        group = self.parser.add_mutually_exclusive_group()

        group.add_argument("--quiet", help="Less output", action="store_const", dest="log_level", const=logging.WARN)
        group.add_argument("--verbose", help="More output", action="store_const", dest="log_level", const=logging.DEBUG)

        self.parser.set_defaults(log_level=logging.INFO)

        self.parser.add_argument("--dataset-type", help="The type of dataset to process", action="store",
                                 dest="dataset_type",
                                 type=dataset_type_arg,
                                 choices=[DatasetType.ARG25, DatasetType.NDVI],  # required=True,
                                 default=DatasetType.ARG25,
                                 metavar=" ".join([dt.name for dt in [DatasetType.ARG25, DatasetType.NDVI]]))

        self.parser.add_argument("--band", help="The band(s) to process", action="store",
                                 default=Ls57Arg25Bands,  # required=True,
                                 dest="bands", type=ls57_arg_band_arg, nargs="+", metavar=" ".join([b.name for b in Ls57Arg25Bands]))

        self.parser.add_argument("--chunk-size-x", help="X chunk size", action="store", dest="chunk_size_x", type=int,
                                 choices=range(1, 4000 + 1),
                                 default=1000,  # required=True
                                 metavar="0 ... 4000"
                                 )

        self.parser.add_argument("--chunk-size-y", help="Y chunk size", action="store", dest="chunk_size_y", type=int,
                                 choices=range(1, 4000 + 1),
                                 default=1000,  # required=True
                                 metavar="0 ... 4000"
                                 )

        self.parser.add_argument("--statistic", help="The statistic(s) to produce", action="store",
                                 default=[Statistic.PERCENTILE_25, Statistic.PERCENTILE_50, Statistic.PERCENTILE_75],  # required=True,
                                 dest="statistic", type=statistic_arg, nargs="+",
                                 metavar=" ".join([s.name for s in Statistic]))

        self.parser.add_argument("--interpolation", help="The interpolation method to use", action="store",
                                 default=PercentileInterpolation.NEAREST,  # required=True,
                                 dest="interpolation", type=percentile_interpolation_arg,
                                 metavar=" ".join([s.name for s in PercentileInterpolation]))

        self.parser.add_argument("--season", help="The seasons for which to produce statistics", action="store",
                                 default=Season,  # required=True,
                                 dest="season", type=season_arg, nargs="+",
                                 metavar=" ".join([s.name for s in Season]))

    def process_arguments(self, args):

        # # Call method on super class
        # # super(self.__class__, self).process_arguments(args)
        # workflow.Workflow.process_arguments(self, args)

        self.x_min = args.x_min
        self.x_max = args.x_max

        self.y_min = args.y_min
        self.y_max = args.y_max

        self.output_directory = args.output_directory

        self.acq_min = parse_date_min(args.acq_min)
        self.acq_max = parse_date_max(args.acq_max)

        self.satellites = args.satellites

        if args.epoch:
            self.epoch = EpochParameter(int(args.epoch[0]), int(args.epoch[1]))

        self.seasons = args.season

        self.mask_pqa_apply = args.mask_pqa_apply
        self.mask_pqa_mask = args.mask_pqa_mask

        self.local_scheduler = args.local_scheduler
        self.workers = args.workers

        _log.setLevel(args.log_level)

        self.dataset_type = args.dataset_type
        self.bands = args.bands

        # # Verify that all the requested satellites have the requested bands
        #
        # for satellite in self.satellites:
        #     if not all(item in [b.name for b in get_bands(self.dataset_type, satellite)] for item in self.bands):
        #         _log.error("Requested bands [%s] not ALL present for satellite [%s]", self.bands, satellite)
        #         raise Exception("Not all bands present for all satellites")

        self.x_chunk_size = args.chunk_size_x
        self.y_chunk_size = args.chunk_size_y

        self.statistics = args.statistic

        self.interpolation = args.interpolation

        self.file_per_statistic = args.file_per_statistic

    def log_arguments(self):

        # # Call method on super class
        # # super(self.__class__, self).log_arguments()
        # workflow.Workflow.log_arguments(self)

        _log.info("""
        x = {x_min:03d} to {x_max:03d}
        y = {y_min:04d} to {y_max:04d}
        """.format(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max))

        _log.info("""
        acq = {acq_min} to {acq_max}
        epoch = {epoch}
        satellites = {satellites}
        output directory = {output_directory}
        PQA mask = {pqa_mask}
        local scheduler = {local_scheduler}
        workers = {workers}
        """.format(acq_min=self.acq_min, acq_max=self.acq_max,
                   epoch="{increment:d} / {duration:d}".format(increment=self.epoch.increment, duration=self.epoch.duration),
                   satellites=" ".join([ts.name for ts in self.satellites]),
                   output_directory=self.output_directory,
                   pqa_mask=self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
                   local_scheduler=self.local_scheduler, workers=self.workers))

        _log.info("""
        dataset to retrieve = {dataset_type}
        bands = {bands}
        X chunk size = {chunk_size_x}
        Y chunk size = {chunk_size_y}
        seasons = {seasons}
        statistics = {statistics}
        interpolation = {interpolation}
        """.format(dataset_type=self.dataset_type.name, bands=" ".join([b.name for b in self.bands]),
                   chunk_size_x=self.x_chunk_size, chunk_size_y=self.y_chunk_size,
                   seasons=" ".join([s.name for s in self.seasons]),
                   statistics=" ".join([s.name for s in self.statistics]),
                   interpolation=self.interpolation.name))

    def get_epochs(self):

        from dateutil.rrule import rrule, YEARLY
        from dateutil.relativedelta import relativedelta

        for dt in rrule(YEARLY, interval=self.epoch.increment, dtstart=self.acq_min, until=self.acq_max):
            acq_min = dt.date()
            acq_max = acq_min + relativedelta(years=self.epoch.duration, days=-1)

            acq_min = max(self.acq_min, acq_min)
            acq_max = min(self.acq_max, acq_max)

            yield acq_min, acq_max

    def get_seasons(self):

        for season in self.seasons:
            yield season


class Arg25EpochStatisticsTask(Task):
    x = luigi.IntParameter()
    y = luigi.IntParameter()

    acq_min = luigi.DateParameter()
    acq_max = luigi.DateParameter()

    season = luigi.Parameter()
    epochs = luigi.Parameter(is_list=True, significant=False)

    satellites = luigi.Parameter(is_list=True)

    dataset_type = luigi.Parameter()

    band = luigi.Parameter()
    bands = luigi.Parameter(is_list=True)

    mask_pqa_apply = luigi.BooleanParameter()
    mask_pqa_mask = luigi.Parameter(is_list=True)

    x_chunk_size = luigi.IntParameter()
    y_chunk_size = luigi.IntParameter()

    statistic = luigi.Parameter()
    statistics = luigi.Parameter(is_list=True)

    interpolation = luigi.Parameter()

    output_directory = luigi.Parameter()

    def output(self):
        from datetime import date

        season = SEASONS[self.season]

        acq_min = date(self.acq_min.year, season[0][0].value, season[0][1]).strftime("%Y%m%d")
        acq_max = self.acq_max.strftime("%Y%m%d")

        season_start = "{month}{day:02d}".format(month=season[0][0].name[:3], day=season[0][1])
        season_end = "{month}{day:02d}".format(month=season[1][0].name[:3], day=season[1][1])

        return luigi.LocalTarget(os.path.join(self.output_directory,
                                              "ARG25_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{season_start}_{season_end}_{band}_{stat}.tif".format(
                                                  x=self.x, y=self.y,
                                                  acq_min=acq_min,
                                                  acq_max=acq_max,
                                                  season_start=season_start,
                                                  season_end=season_end,
                                                  band=self.band.name,
                                                  stat=self.statistic.name
                                              )))

    def requires(self):
        dataset_types = [self.dataset_type]
        if self.mask_pqa_apply:
            dataset_types.append(DatasetType.PQ25)

        for (acq_min, acq_max) in self.epochs:
            _log.debug("acq_min=[%s] acq_max=[%s] season=[%s]", acq_min, acq_max, self.season.name)

            acq_min_extended, acq_max_extended, criteria = build_season_date_criteria(acq_min, acq_max, self.season,
                                                                                      seasons=SEASONS,
                                                                                      extend=True)

            _log.debug("\tacq_min_extended=[%s], acq_max_extended=[%s], criteria=[%s]", acq_min_extended, acq_max_extended, criteria)
            for cell in list_cells_as_list(x=[self.x], y=[self.y], satellites=self.satellites,
                                                acq_min=acq_min_extended, acq_max=acq_max_extended,
                                                dataset_types=dataset_types, include=criteria):
                _log.debug("\t%3d %4d", cell.x, cell.y)
                # yield args.create_task(x=cell.x, y=cell.y, acq_min=acq_min, acq_max=acq_max, season=season)
                _log.debug("Creating task for %s %s %s %s %s", cell.x, cell.y, acq_min, acq_max, self.season)

                yield Arg25BandStatisticsTask(x=cell.x, y=cell.y,
                                              acq_min=acq_min_extended, acq_max=acq_max_extended, season=self.season,
                                              satellites=args.satellites,
                                              dataset_type=args.dataset_type, bands=args.bands,
                                              mask_pqa_apply=args.mask_pqa_apply, mask_pqa_mask=args.mask_pqa_mask,
                                              x_chunk_size=args.x_chunk_size, y_chunk_size=args.y_chunk_size,
                                              statistics=args.statistics, interpolation=args.interpolation,
                                              output_directory=args.output_directory)

    def get_statistic_filename(self, acq_min, acq_max, x_offset, y_offset):
        return os.path.join(self.output_directory, get_statistic_filename(x=self.x, y=self.y, acq_min=acq_min, acq_max=acq_max,
                                      season=self.season, band=self.band, statistic=self.statistic,
                                      x_offset=x_offset, y_offset=y_offset))

    def generate_raster_metadata(self):
        return {
            "X_INDEX": "{x:03d}".format(x=self.x),
            "Y_INDEX": "{y:04d}".format(y=self.y),
            "DATASET_TYPE": "{dataset_type} STATISTICS".format(dataset_type=self.dataset_type.name),
            "ACQUISITION_DATE": "{acq_min} to {acq_max}".format(acq_min=self.acq_min, acq_max=self.acq_max),
            "SEASON": self.season.name,
            "SATELLITE": " ".join([s.name for s in self.satellites]),
            "PIXEL_QUALITY_FILTER": self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
            "BANDS": self.band.name,
            "STATISTICS": self.statistic.name,
            "INTERPOLATION": self.interpolation.name
        }

    def run(self):

        _log.info("*** Aggregating chunk NPY files into TIF")

        ndv = get_dataset_type_ndv(self.dataset_type)

        # TODO

        transform = (self.x, 0.00025, 0.0, self.y+1, 0.0, -0.00025)

        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)

        projection = srs.ExportToWkt()

        driver = gdal.GetDriverByName("GTiff")
        assert driver

        # Create the output TIF

        # TODO

        gdal_type = gdal.GDT_Int16
        if self.dataset_type == DatasetType.NDVI and self.statistic not in [Statistic.COUNT, Statistic.COUNT_OBSERVED]:
            gdal_type = gdal.GDT_Float32

        raster = driver.Create(self.output().path, 4000, 4000, len(self.epochs), gdal_type,
                               options=["INTERLEAVE=BAND", "COMPRESS=LZW", "TILED=YES"])
        assert raster

        # TODO

        raster.SetGeoTransform(transform)
        raster.SetProjection(projection)

        raster.SetMetadata(self.generate_raster_metadata())

        from itertools import product
        from datetime import date

        for index, (acq_min, acq_max) in enumerate(self.epochs, start=1):
            _log.info("Doing band [%s] statistic [%s] which is band number [%s]", self.band.name, self.statistic.name, index)

            acq_min_extended, acq_max_extended, criteria = build_season_date_criteria(acq_min, acq_max, self.season,
                                                                                      seasons=SEASONS,
                                                                                      extend=True)

            band = raster.GetRasterBand(index)
            assert band

            season = SEASONS[self.season]
            acq_min_str = date(acq_min_extended.year, season[0][0].value, season[0][1]).strftime("%Y%m%d")
            acq_max_str = acq_max_extended.strftime("%Y%m%d")

            # TODO
            band.SetNoDataValue(ndv)
            band.SetDescription("{band} {stat} {start}-{end}".format(band=self.band.name, stat=self.statistic.name, start=acq_min_str, end=acq_max_str))

            for x_offset, y_offset in product(range(0, 4000, self.x_chunk_size),
                                              range(0, 4000, self.y_chunk_size)):
                filename = self.get_statistic_filename(acq_min_extended, acq_max_extended, x_offset, y_offset)

                _log.info("Processing chunk [%4d|%4d] for [%s] from [%s]", x_offset, y_offset, self.statistic.name, filename)

                # read the chunk
                try:
                    data = numpy.load(filename)
                except IOError:
                    _log.info("Failed to load chunk")
                    continue

                _log.info("data is [%s]\n[%s]", numpy.shape(data), data)
                _log.info("Writing it to (%d,%d)", x_offset, y_offset)

                # write the chunk to the TIF at the offset
                band.WriteArray(data, x_offset, y_offset)

                band.FlushCache()

            band.ComputeStatistics(True)
            band.FlushCache()

            del band

        raster.FlushCache()
        del raster


class Arg25BandStatisticsTask(Task):

    x = luigi.IntParameter()
    y = luigi.IntParameter()

    acq_min = luigi.DateParameter()
    acq_max = luigi.DateParameter()

    season = luigi.Parameter()

    satellites = luigi.Parameter(is_list=True)

    dataset_type = luigi.Parameter()
    bands = luigi.Parameter(is_list=True)

    mask_pqa_apply = luigi.BooleanParameter()
    mask_pqa_mask = luigi.Parameter(is_list=True)

    x_chunk_size = luigi.IntParameter()
    y_chunk_size = luigi.IntParameter()

    statistics = luigi.Parameter(is_list=True)

    interpolation = luigi.Parameter()

    output_directory = luigi.Parameter()

    def output(self):
        acq_min = self.acq_min.strftime("%Y%m%d")
        acq_max = self.acq_max.strftime("%Y%m%d")

        season = SEASONS[self.season]
        season_start = "{month}{day:02d}".format(month=season[0][0].name[:3], day=season[0][1])
        season_end = "{month}{day:02d}".format(month=season[1][0].name[:3], day=season[1][1])

        return luigi.LocalTarget(os.path.join(self.output_directory,
                                              "ARG25_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{season_start}_{season_end}_STATS.tif".format(
                                                  x=self.x, y=self.y,
                                                  acq_min=acq_min,
                                                  acq_max=acq_max,
                                                  season_start=season_start,
                                                  season_end=season_end
                                              )))

    def requires(self):

        from itertools import product

        _log.info("bands = %s", self.bands)

        for band, (x_offset, y_offset) in product(list(self.bands), self.get_chunks()):
            _log.info("\t\t%04d %04d", x_offset, y_offset)
            yield self.create_chunk_task(band, x_offset, y_offset)

    def create_chunk_task(self, band, x_offset, y_offset):

        yield Arg25BandStatisticsBandChunkTask(x=self.x, y=self.y,
                                               acq_min=self.acq_min, acq_max=self.acq_max, season=self.season,
                                               satellites=self.satellites,
                                               dataset_type=self.dataset_type, band=band,
                                               mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                               x_chunk_size=self.x_chunk_size, y_chunk_size=self.y_chunk_size,
                                               x_offset=x_offset, y_offset=y_offset,
                                               statistics=self.statistics, interpolation=self.interpolation,
                                               output_directory=self.output_directory)

    def get_chunks(self):

        from itertools import product

        for x_offset, y_offset in product(range(0, 4000, self.x_chunk_size), range(0, 4000, self.y_chunk_size)):
            yield x_offset, y_offset

    def get_statistic_filename(self, statistic, x_offset, y_offset, band):
        return os.path.join(self.output_directory, get_statistic_filename(x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                      season=self.season, band=band, statistic=statistic,
                                      x_offset=x_offset, y_offset=y_offset))

    def generate_raster_metadata(self):
        return {
            "X_INDEX": "{x:03d}".format(x=self.x),
            "Y_INDEX": "{y:04d}".format(y=self.y),
            "DATASET_TYPE": "{dataset_type} STATISTICS".format(dataset_type=self.dataset_type.name),
            "ACQUISITION_DATE": "{acq_min} to {acq_max}".format(acq_min=self.acq_min, acq_max=self.acq_max),
            "SEASON": self.season.name,
            "SATELLITE": " ".join([s.name for s in self.satellites]),
            "PIXEL_QUALITY_FILTER": self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
            "BANDS": " ".join([b.name for b in self.bands]),
            "STATISTICS": " ".join([s.name for s in self.statistics]),
            "INTERPOLATION": self.interpolation.name
        }

    def run(self):

        _log.info("*** Aggregating chunk NPY files into TIF")

        ndv = get_dataset_type_ndv(self.dataset_type)

        # TODO

        transform = (self.x, 0.00025, 0.0, self.y+1, 0.0, -0.00025)

        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)

        projection = srs.ExportToWkt()

        driver = gdal.GetDriverByName("GTiff")
        assert driver

        # Create the output TIF

        # TODO

        raster = driver.Create(self.output().path, 4000, 4000, len(self.bands) * len(self.statistics), gdal.GDT_Int16,
                               options=["INTERLEAVE=BAND", "COMPRESS=LZW", "TILED=YES"])
        assert raster

        # TODO

        raster.SetGeoTransform(transform)
        raster.SetProjection(projection)

        raster.SetMetadata(self.generate_raster_metadata())

        from itertools import product

        for index, (b, statistic) in enumerate(product(self.bands, self.statistics), start=1):

            _log.info("Doing band [%s] statistic [%s] which is band number [%s]", b.name, statistic.name, index)

            band = raster.GetRasterBand(index)
            assert band

            # TODO
            band.SetNoDataValue(ndv)
            band.SetDescription("{band} - {stat}".format(band=b.name, stat=statistic.name))

            for x_offset, y_offset in product(range(0, 4000, self.x_chunk_size),
                                              range(0, 4000, self.y_chunk_size)):
                filename = self.get_statistic_filename(statistic, x_offset, y_offset, b)

                _log.info("Processing chunk [%4d|%4d] for [%s] from [%s]", x_offset, y_offset, statistic.name, filename)

                # read the chunk
                data = numpy.load(filename)

                _log.info("data is [%s]\n[%s]", numpy.shape(data), data)
                _log.info("Writing it to (%d,%d)", x_offset, y_offset)

                # write the chunk to the TIF at the offset
                band.WriteArray(data, x_offset, y_offset)

                band.FlushCache()

            band.ComputeStatistics(True)
            band.FlushCache()

            del band

        raster.FlushCache()
        del raster

        # TODO delete .npy files? - can't really as luigi doesn't like it.  not sure what the answer here is?
        # maybe just write these to tmp location?  but this makes it not restartable.  sigh!


class Arg25BandStatisticsBandChunkTask(Task):

    x = luigi.IntParameter()
    y = luigi.IntParameter()

    acq_min = luigi.DateParameter()
    acq_max = luigi.DateParameter()

    season = luigi.Parameter()

    satellites = luigi.Parameter(is_list=True)

    dataset_type = luigi.Parameter()
    band = luigi.Parameter()

    mask_pqa_apply = luigi.BooleanParameter()
    mask_pqa_mask = luigi.Parameter(is_list=True)

    x_chunk_size = luigi.IntParameter()
    y_chunk_size = luigi.IntParameter()

    x_offset = luigi.IntParameter()
    y_offset = luigi.IntParameter()

    statistics = luigi.Parameter(is_list=True)

    interpolation = luigi.Parameter()

    output_directory = luigi.Parameter()

    def output(self):
        return [
            luigi.LocalTarget(self.get_statistic_filename(statistic)) for statistic in self.statistics]

    def get_statistic_filename(self, statistic):
        return os.path.join(self.output_directory, get_statistic_filename(x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                            season=self.season, band=self.band, statistic=statistic,
                            x_offset=self.x_offset, y_offset=self.y_offset))

    def get_tiles(self):
        acq_min, acq_max, criteria = build_season_date_criteria(self.acq_min, self.acq_max, self.season,
                                                                seasons=SEASONS, extend=True)

        _log.info("\tcriteria is %s", criteria)

        dataset_types = [self.dataset_type]

        if self.mask_pqa_apply:
            dataset_types.append(DatasetType.PQ25)

        tiles = list_tiles_as_list(x=[self.x], y=[self.y], satellites=self.satellites,
                                   acq_min=acq_min, acq_max=acq_max,
                                   dataset_types=dataset_types, include=criteria)

        return tiles

    def run(self):

        _log.info("Calculating statistics for chunk")

        ndv = get_dataset_type_ndv(self.dataset_type)
        data_type = get_dataset_type_data_type(self.dataset_type)
        tiles = self.get_tiles()

        stack = get_dataset_data_stack(tiles, self.dataset_type, self.band.name, ndv=ndv,
                                       x=self.x_offset, y=self.y_offset,
                                       x_size=self.x_chunk_size, y_size=self.y_chunk_size,
                                       mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask)

        if len(stack) == 0:
            return

        # TODO get statistics to be generated from command line argument

        if Statistic.COUNT in self.statistics:
            log_mem("Before COUNT")

            # COUNT
            stack_stat = calculate_stack_statistic_count(stack=stack, ndv=ndv)
            numpy.save(self.get_statistic_filename(Statistic.COUNT), stack_stat)
            del stack_stat

        if Statistic.MIN in self.statistics:
            log_mem("Before MIN")

            # MIN
            stack_stat = calculate_stack_statistic_min(stack=stack, ndv=ndv, dtype=data_type)
            numpy.save(self.get_statistic_filename(Statistic.MIN), stack_stat)
            del stack_stat

        if Statistic.MAX in self.statistics:
            log_mem("Before MAX")

            # MAX
            stack_stat = calculate_stack_statistic_max(stack=stack, ndv=ndv, dtype=data_type)
            numpy.save(self.get_statistic_filename(Statistic.MAX), stack_stat)
            del stack_stat

        if Statistic.MEAN in self.statistics:
            log_mem("Before MEAN")

            # MEAN
            stack_stat = calculate_stack_statistic_mean(stack=stack, ndv=ndv, dtype=data_type)
            numpy.save(self.get_statistic_filename(Statistic.MEAN), stack_stat)
            del stack_stat

        if Statistic.VARIANCE in self.statistics:
            log_mem("Before VARIANCE")

            # VARIANCE
            stack_stat = calculate_stack_statistic_variance(stack=stack, ndv=ndv, dtype=data_type)
            numpy.save(self.get_statistic_filename(Statistic.VARIANCE), stack_stat)
            del stack_stat

        if Statistic.STANDARD_DEVIATION in self.statistics:
            log_mem("Before STANDARD_DEVIATION")

            # STANDARD_DEVIATION
            stack_stat = calculate_stack_statistic_standard_deviation(stack=stack, ndv=ndv, dtype=data_type)
            numpy.save(self.get_statistic_filename(Statistic.STANDARD_DEVIATION), stack_stat)
            del stack_stat

        for percentile in PERCENTILE:

            if percentile in self.statistics:
                log_mem("Before {p}".format(p=percentile.name))

                stack_stat = calculate_stack_statistic_percentile(stack=stack, percentile=PERCENTILE[percentile],
                                                                  ndv=ndv, interpolation=self.interpolation)
                numpy.save(self.get_statistic_filename(percentile), stack_stat)
                del stack_stat

        if Statistic.COUNT_OBSERVED in self.statistics:
            log_mem("Before OBSERVED COUNT")

            # COUNT OBSERVED - note the copy=False is modifying the array so this is done last
            stack_stat = calculate_stack_statistic_count_observed(stack=stack, ndv=ndv)
            numpy.save(self.get_statistic_filename(Statistic.COUNT_OBSERVED), stack_stat)
            del stack_stat

        log_mem("DONE")


def get_statistic_filename(x, y, acq_min, acq_max, season, band, statistic, x_offset, y_offset):
    return "{x:03d}_{y:04d}_{year_min:04d}_{year_max:04d}_{season}_{band}_{statistic}_{x_offset:04d}_{y_offset:04d}.npy".format(
        x=x, y=y, year_min=acq_min.year, year_max=acq_max.year, season=season.name, band=band.name,
        statistic=statistic.name, x_offset=x_offset, y_offset=y_offset)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    args = Arguments()
    args.log_arguments()

    luigi.build(create_tasks(args), local_scheduler=args.local_scheduler, workers=args.workers)

