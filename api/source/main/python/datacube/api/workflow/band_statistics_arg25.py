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


import argparse
import gdal
import logging
import luigi
import numpy
import osr
import os
import sys
from collections import namedtuple
from datacube.api import parse_date_min, parse_date_max, Month, PqaMask, Statistic, PERCENTILE, writeable_dir, Season
from datacube.api import satellite_arg, pqa_mask_arg, dataset_type_arg, statistic_arg, season_arg
from datacube.api.query import list_cells_as_list, list_tiles_as_list
from datacube.api.model import Satellite, DatasetType, Ls57Arg25Bands
from datacube.api.utils import get_dataset_type_ndv, get_dataset_data_stack, log_mem, build_season_date_criteria
from datacube.api.utils import PercentileInterpolation
from datacube.api.utils import calculate_stack_statistic_count, calculate_stack_statistic_count_observed
from datacube.api.utils import calculate_stack_statistic_min, calculate_stack_statistic_max
from datacube.api.utils import calculate_stack_statistic_mean, calculate_stack_statistic_percentile
from datacube.api.workflow import Task


_log = logging.getLogger()


STATISTICS = [Statistic.PERCENTILE_25, Statistic.PERCENTILE_50, Statistic.PERCENTILE_75]


SEASONS = {
    Season.SUMMER: ((Month.NOVEMBER, 17), (Month.APRIL, 25)),
    Season.AUTUMN: ((Month.FEBRUARY, 16), (Month.JULY, 25)),
    Season.WINTER: ((Month.MAY, 17), (Month.OCTOBER, 25)),
    Season.SPRING: ((Month.AUGUST, 17), (Month.JANUARY, 25))
}

EpochParameter = namedtuple('Epoch', ['increment', 'duration'])


def ls57_arg_band_arg(s):
    if s in [t.name for t in Ls57Arg25Bands]:
        return Ls57Arg25Bands[s]
    raise argparse.ArgumentTypeError("{0} is not a supported LS57 ARG25 band".format(s))


def percentile_interpolation_arg(s):
    if s in [t.name for t in PercentileInterpolation]:
        return PercentileInterpolation[s]
    raise argparse.ArgumentTypeError("{0} is not a supported percentile interpolation".format(s))


class Arg25BandStatisticsWorkflow(object):

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

        group = self.parser.add_mutually_exclusive_group()

        group.add_argument("--quiet", help="Less output", action="store_const", dest="log_level", const=logging.WARN)
        group.add_argument("--verbose", help="More output", action="store_const", dest="log_level", const=logging.DEBUG)

        self.parser.set_defaults(log_level=logging.INFO)

        self.parser.add_argument("--dataset-type", help="The type of dataset to process", action="store",
                                 dest="dataset_type",
                                 type=dataset_type_arg,
                                 choices=[DatasetType.ARG25],  # required=True,
                                 default=DatasetType.ARG25,
                                 metavar=" ".join([dt.name for dt in [DatasetType.ARG25]]))

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
                                 default=STATISTICS,  # required=True,
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

    def create_tasks(self):

        x_list = range(self.x_min, self.x_max + 1)
        y_list = range(self.y_min, self.y_max + 1)

        dataset_types = [self.dataset_type]

        if self.mask_pqa_apply:
            dataset_types.append(DatasetType.PQ25)

        from itertools import product

        for (acq_min, acq_max), season in product(self.get_epochs(), self.get_seasons()):
            _log.debug("acq_min=[%s] acq_max=[%s] season=[%s]", acq_min, acq_max, season.name)

            acq_min_extended, acq_max_extended, criteria = build_season_date_criteria(acq_min, acq_max, season,
                                                                                      seasons=SEASONS,
                                                                                      extend=True)

            _log.debug("\tacq_min_extended=[%s], acq_max_extended=[%s], criteria=[%s]", acq_min_extended, acq_max_extended, criteria)

            for cell in list_cells_as_list(x=x_list, y=y_list, satellites=self.satellites,
                                                acq_min=acq_min_extended, acq_max=acq_max_extended,
                                                dataset_types=dataset_types, include=criteria):
                _log.debug("\t%3d %4d", cell.x, cell.y)
                yield self.create_task(x=cell.x, y=cell.y, acq_min=acq_min, acq_max=acq_max, season=season)

    def create_task(self, x, y, acq_min, acq_max, season):
        _log.debug("Creating task for %s %s %s %s %s", x, y, acq_min, acq_max, season)
        return Arg25BandStatisticsTask(x=x, y=y,
                                       acq_min=acq_min, acq_max=acq_max, season=season,
                                       satellites=self.satellites,
                                       dataset_type=self.dataset_type, bands=self.bands,
                                       mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                       x_chunk_size=self.x_chunk_size, y_chunk_size=self.y_chunk_size,
                                       statistics=self.statistics, interpolation=self.interpolation,
                                       output_directory=self.output_directory)

    def run(self):

        self.setup_arguments()
        self.process_arguments(self.parser.parse_args())
        self.log_arguments()

        luigi.build(self.create_tasks(), local_scheduler=self.local_scheduler, workers=self.workers)


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
        return get_statistic_filename(x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                      season=self.season, band=band, statistic=statistic,
                                      x_offset=x_offset, y_offset=y_offset)

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
            luigi.LocalTarget(os.path.join(self.output_directory, self.get_statistic_filename(statistic))) for statistic in STATISTICS]

    def get_statistic_filename(self, statistic):
        return get_statistic_filename(x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                      season=self.season, band=self.band, statistic=statistic,
                                      x_offset=self.x_offset, y_offset=self.y_offset)

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
            stack_stat = calculate_stack_statistic_min(stack=stack, ndv=ndv)
            numpy.save(self.get_statistic_filename(Statistic.MIN), stack_stat)
            del stack_stat

        if Statistic.MAX in self.statistics:
            log_mem("Before MAX")

            # MAX
            stack_stat = calculate_stack_statistic_max(stack=stack, ndv=ndv)
            numpy.save(self.get_statistic_filename(Statistic.MAX), stack_stat)
            del stack_stat

        if Statistic.MEAN in self.statistics:
            log_mem("Before MEAN")

            # MEAN
            stack_stat = calculate_stack_statistic_mean(stack=stack, ndv=ndv)
            numpy.save(self.get_statistic_filename(Statistic.MEAN), stack_stat)
            del stack_stat

        for percentile in [Statistic.PERCENTILE_25, Statistic.PERCENTILE_50, Statistic.PERCENTILE_75, Statistic.PERCENTILE_90, Statistic.PERCENTILE_95]:

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

    Arg25BandStatisticsWorkflow().run()
