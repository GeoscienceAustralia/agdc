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


import gdal
import logging
import luigi
import numpy
import osr
from datacube.api import parse_date_min, parse_date_max, Month, PqaMask, Statistic, PERCENTILE
from datacube.api.query import Season, list_cells_as_generator, build_season_date_criteria, list_tiles_as_generator
from datacube.api.model import Satellite, DatasetType, Ls57Arg25Bands
from datacube.api.utils import get_dataset_type_ndv, get_dataset_data_stack, log_mem
from datacube.api.utils import calculate_stack_statistic_count, calculate_stack_statistic_count_observed
from datacube.api.utils import calculate_stack_statistic_min, calculate_stack_statistic_max
from datacube.api.utils import calculate_stack_statistic_mean, calculate_stack_statistic_percentile
from datacube.api.workflow import Task


_log = logging.getLogger()


STATISTICS = [
    Statistic.COUNT, Statistic.COUNT_OBSERVED,
    # Statistic.MIN, Statistic.MAX, Statistic.MEAN,
    Statistic.PERCENTILE_25, Statistic.PERCENTILE_50, Statistic.PERCENTILE_75
]


SEASONS = {
    Season.SUMMER: ((Month.NOVEMBER, 17), (Month.APRIL, 25)),
    Season.AUTUMN: ((Month.FEBRUARY, 16), (Month.JULY, 25)),
    Season.WINTER: ((Month.MAY, 17), (Month.OCTOBER, 25)),
    Season.SPRING: ((Month.AUGUST, 17), (Month.JANUARY, 25))
}


class Arg25BandStatisticsWorkflow(object):

    def __init__(self):

        self.x_min = self.x_max = 140
        self.y_min = self.y_max = -36

        self.epoch = 5

        self.acq_min = parse_date_min("1985")
        self.acq_max = parse_date_max("2014")

        self.seasons = [s for s in Season]

        self.satellites = [Satellite.LS5, Satellite.LS7]

        self.dataset_type = DatasetType.ARG25
        self.bands = [Ls57Arg25Bands.RED, Ls57Arg25Bands.GREEN]

        self.x_chunk_size = 2000
        self.y_chunk_size = 2000

        self.mask_pqa_apply = True
        self.mask_pqa_mask = [PqaMask.PQ_MASK_SATURATION, PqaMask.PQ_MASK_CONTIGUITY, PqaMask.PQ_MASK_CLOUD]

        self.statistics = STATISTICS

    def get_epochs(self):

        from dateutil.rrule import rrule, YEARLY
        from dateutil.relativedelta import relativedelta

        for dt in rrule(YEARLY, interval=self.epoch, dtstart=self.acq_min, until=self.acq_max):
            acq_min = dt.date()
            acq_max = acq_min + relativedelta(years=self.epoch, days=-1)

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
            _log.info("%s %s %s", acq_min, acq_max, season)

            acq_min_extended, acq_max_extended, criteria = build_season_date_criteria(acq_min, acq_max, season,
                                                                                      seasons=SEASONS,
                                                                                      extend=True)

            _log.info("\tcriteria is %s", criteria)

            for cell in list_cells_as_generator(x=x_list, y=y_list, satellites=self.satellites,
                                                acq_min=acq_min_extended, acq_max=acq_max_extended,
                                                dataset_types=dataset_types, include=criteria):
                _log.info("\t%3d %4d", cell.x, cell.y)
                yield self.create_task(x=cell.x, y=cell.y, acq_min=acq_min_extended, acq_max=acq_max_extended, season=season)

    def create_task(self, x, y, acq_min, acq_max, season):
        _log.info("Creating task for %s %s %s %s %s", x, y, acq_min, acq_max, season)
        return Arg25BandStatisticsTask(x=x, y=y,
                                       acq_min=acq_min, acq_max=acq_max, season=season,
                                       satellites=self.satellites,
                                       dataset_type=self.dataset_type, bands=self.bands,
                                       mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                       x_chunk_size=self.x_chunk_size, y_chunk_size=self.y_chunk_size,
                                       statistics=self.statistics)

    def run(self):
        luigi.build(self.create_tasks(), local_scheduler=True, workers=16)


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

    def output(self):
        acq_min = self.acq_min.strftime("%Y%m%d")
        acq_max = self.acq_max.strftime("%Y%m%d")

        season = SEASONS[self.season]
        season_start = "{month}{day:02d}".format(month=season[0][0].name[:3], day=season[0][1])
        season_end = "{month}{day:02d}".format(month=season[1][0].name[:3], day=season[1][1])

        return luigi.LocalTarget(
            "ARG25_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{season_start}_{season_end}_STATS.tif".format(x=self.x, y=self.y,
                                                                                                     acq_min=acq_min,
                                                                                                     acq_max=acq_max,
                                                                                                     season_start=season_start,
                                                                                                     season_end=season_end
                                                                                                     ))

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
                                               statistics=self.statistics)

    def get_chunks(self):

        from itertools import product

        for x_offset, y_offset in product(range(0, 4000, self.x_chunk_size), range(0, 4000, self.y_chunk_size)):
            yield x_offset, y_offset

    def get_statistic_filename(self, statistic, x_offset, y_offset):
        return get_statistic_filename(x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                      season=self.season, band=self.band, statistic=statistic,
                                      x_offset=x_offset, y_offset=y_offset)

    def generate_raster_metadata(self):
        return {
            "X_INDEX": "{x:03d}".format(x=self.x),
            "Y_INDEX": "{y:04d}".format(y=self.y),
            "DATASET_TYPE": "{dataset_type} STATISTICS".format(dataset_type=self.get_dataset_type_string().replace("_", " ")),
            "ACQUISITION_DATE": "{acq_min} to {acq_max}".format(acq_min=self.acq_min, acq_max=self.acq_max),
            "SEASON": self.season.name,
            "SATELLITE": " ".join([s.name for s in self.satellites]),
            "PIXEL_QUALITY_FILTER": self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
            "BANDS": " ".join([b.name for b in self.bands]),
            "STATISTICS": " ".join([s.name for s in self.statistics])
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

        raster = driver.Create(self.output().path, 4000, 4000, len(self.bands) * len(self.statistics), gdal.GDT_Int16)
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

            for x_offset, y_offset in product(range(0, 4000, self.chunk_size_x),
                                              range(0, 4000, self.chunk_size_y)):
                filename = self.get_statistic_filename(statistic, x_offset, y_offset)

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

    def output(self):
        return [
            luigi.LocalTarget(self.get_statistic_filename(statistic)) for statistic in STATISTICS]

    def get_statistic_filename(self, statistic):
        return get_statistic_filename(x=self.x, y=self.y, acq_min=self.acq_min, acq_max=self.acq_max,
                                      season=self.season, band=self.band, statistic=statistic,
                                      x_offset=self.x_offset, y_offset=self.y_offset)

    def run(self):

        _log.info("Calculating statistics for chunk")

        ndv = get_dataset_type_ndv(self.dataset_type)

        acq_min, acq_max, criteria = build_season_date_criteria(self.acq_min, self.acq_max, self.season,
                                                                seasons=SEASONS, extend=True)

        _log.info("\tcriteria is %s", criteria)

        dataset_types = [self.dataset_type]

        if self.mask_pqa_apply:
            dataset_types.append(DatasetType.PQ25)

        tiles = list_tiles_as_generator(x=[self.x], y=[self.y], satellites=self.satellites,
                                        acq_min=acq_min, acq_max=acq_max,
                                        dataset_types=dataset_types, include=criteria)

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
            print "COUNT"
            stack_stat = calculate_stack_statistic_count(stack=stack, ndv=ndv)
            numpy.save(self.get_statistic_filename(Statistic.COUNT), stack_stat)
            del stack_stat

        if Statistic.MIN in self.statistics:
            log_mem("Before MIN")

            # MIN
            print "MIN"
            stack_stat = calculate_stack_statistic_min(stack=stack, ndv=ndv)
            numpy.save(self.get_statistic_filename(Statistic.MIN), stack_stat)
            del stack_stat

        if Statistic.MAX in self.statistics:
            log_mem("Before MAX")

            # MAX
            print "MAX"
            stack_stat = calculate_stack_statistic_max(stack=stack, ndv=ndv)
            numpy.save(self.get_statistic_filename(Statistic.MAX), stack_stat)
            del stack_stat

        if Statistic.MEAN in self.statistics:
            log_mem("Before MEAN")

            # MEAN
            print "MEAN"
            stack_stat = calculate_stack_statistic_mean(stack=stack, ndv=ndv)
            numpy.save(self.get_statistic_filename(Statistic.MEAN), stack_stat)
            del stack_stat

        for percentile in [Statistic.PERCENTILE_25, Statistic.PERCENTILE_50, Statistic.PERCENTILE_75, Statistic.PERCENTILE_90, Statistic.PERCENTILE_95]:

            if percentile in self.statistics:
                log_mem("Before {p}".format(p=percentile.name))

                print "Before {p}".format(p=percentile.name)
                stack_stat = calculate_stack_statistic_percentile(stack=stack, percentile=PERCENTILE[percentile], ndv=ndv)
                numpy.save(self.get_statistic_filename(percentile), stack_stat)
                del stack_stat

        if Statistic.COUNT_OBSERVED in self.statistics:
            log_mem("Before OBSERVED COUNT")

            # COUNT OBSERVED - note the copy=False is modifying the array so this is done last
            print "COUNT OBSERVED"
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
