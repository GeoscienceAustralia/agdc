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
import logging
import numpy
import sys
import os
import gdal
from gdalconst import GA_ReadOnly
from collections import namedtuple
from datacube.api import Statistic, Season, Month, Satellite, DatasetType, pqa_mask_arg, PqaMask, PERCENTILE
from datacube.api import writeable_dir, readable_dir, statistic_arg, season_arg, satellite_arg, dataset_type_arg
from datacube.api import parse_date_min, parse_date_max
from datacube.api.model import Ls57Arg25Bands
from datacube.api.query import list_tiles_as_list
from datacube.api.utils import get_dataset_type_ndv, get_dataset_data_stack, build_season_date_criteria
from datacube.api.utils import PercentileInterpolation
from datacube.api.workflow.band_statistics_arg25 import percentile_interpolation_arg
from datetime import date


_log = logging.getLogger()


Cell = namedtuple("Cell", ["x", "y"])


# TODO this is a bit quick and dirty probably a better way

def cell_arg(s):
    values = [int(x) for x in s.split(",")]

    if len(values) == 2:
        return Cell(values[0], values[1])

    raise argparse.ArgumentTypeError("{0} is not a supported PQA mask".format(s))


STATISTICS = [
    #Statistic.COUNT, Statistic.COUNT_OBSERVED,
    # Statistic.MIN, Statistic.MAX, Statistic.MEAN,
    Statistic.PERCENTILE_25, Statistic.PERCENTILE_50, Statistic.PERCENTILE_75
]


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


class Arg25BandStatisticsValidator(object):

    def __init__(self, name="Arg25 Band Statistics Validator"):

        self.name = name

        self.parser = argparse.ArgumentParser(prog=sys.argv[0], description=self.name)

        self.input_directory = None
        self.output_directory = None

        self.cells = None

        self.acq_min = None
        self.acq_max = None

        self.epoch = None

        self.seasons = None

        self.satellites = None

        self.statistics = None

        self.samples = None

        self.dataset_type = None
        self.bands = None

        self.mask_pqa_apply = None
        self.mask_pqa_mask = None

        self.interpolation = None

    def setup_arguments(self):

        self.parser.add_argument("--acq-min", help="Acquisition Date", action="store", dest="acq_min", type=str,
                                 default="1985")

        self.parser.add_argument("--acq-max", help="Acquisition Date", action="store", dest="acq_max", type=str,
                                 default="2015")

        self.parser.add_argument("--epoch", help="Epoch increment and duration (e.g. 5 6 means 1985-1990, 1990-1995, etc)",
                                 action="store", dest="epoch", type=int, nargs=2, default=[5, 6])

        self.parser.add_argument("--satellite", help="The satellite(s) to include", action="store", dest="satellites",
                                 type=satellite_arg, nargs="+", choices=Satellite,
                                 default=[Satellite.LS5, Satellite.LS7],
                                 metavar=" ".join([ts.name for ts in Satellite]))

        self.parser.add_argument("--input-directory", help="input directory", action="store", dest="input_directory",
                                 type=readable_dir, required=True)

        self.parser.add_argument("--output-directory", help="output directory", action="store", dest="output_directory",
                                 type=writeable_dir, required=True)

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

        self.parser.add_argument("--samples", help="Number of (random) pixel samples (per cell per season per epoch)",
                                 action="store", dest="samples", type=int, default=10)

        self.parser.add_argument("--dataset-type", help="The type of dataset to process", action="store",
                                 dest="dataset_type",
                                 type=dataset_type_arg,
                                 choices=[DatasetType.ARG25],  # required=True,
                                 default=DatasetType.ARG25,
                                 metavar=" ".join([dt.name for dt in [DatasetType.ARG25]]))

        self.parser.add_argument("--band", help="The band(s) to process", action="store",
                                 default=Ls57Arg25Bands,  # required=True,
                                 dest="bands", type=ls57_arg_band_arg, nargs="+",
                                 metavar=" ".join([b.name for b in Ls57Arg25Bands]))

        self.parser.add_argument("--mask-pqa-apply", help="Apply PQA mask", action="store_true", dest="mask_pqa_apply",
                                 default=True)

        self.parser.add_argument("--mask-pqa-mask", help="The PQA mask to apply", action="store", dest="mask_pqa_mask",
                                 type=pqa_mask_arg, nargs="+", choices=PqaMask,
                                 default=[PqaMask.PQ_MASK_SATURATION, PqaMask.PQ_MASK_CONTIGUITY, PqaMask.PQ_MASK_CLOUD],
                                 metavar=" ".join([ts.name for ts in PqaMask]))

        self.parser.add_argument("--cell", help="The cell(s) to validate", action="store",
                                 required=True, dest="cell", type=cell_arg, nargs="+",
                                 metavar="(x,y) [(x,y) (x,y) ...]")

        group = self.parser.add_mutually_exclusive_group()

        group.add_argument("--quiet", help="Less output", action="store_const", dest="log_level", const=logging.WARN)
        group.add_argument("--verbose", help="More output", action="store_const", dest="log_level", const=logging.DEBUG)

        self.parser.set_defaults(log_level=logging.INFO)

    def process_arguments(self, args):

        self.input_directory = args.input_directory
        self.output_directory = args.output_directory

        self.acq_min = parse_date_min(args.acq_min)
        self.acq_max = parse_date_max(args.acq_max)

        self.satellites = args.satellites

        if args.epoch:
            self.epoch = EpochParameter(int(args.epoch[0]), int(args.epoch[1]))

        self.seasons = args.season

        self.statistics = args.statistic

        self.samples = args.samples

        self.dataset_type = args.dataset_type
        self.bands = args.bands

        self.mask_pqa_apply = args.mask_pqa_apply
        self.mask_pqa_mask = args.mask_pqa_mask

        _log.setLevel(args.log_level)

        self.cells = args.cell

        self.interpolation = args.interpolation

    def log_arguments(self):

        _log.info("""
        cells = {cells}
        acq = {acq_min} to {acq_max}
        epoch = {epoch}
        satellites = {satellites}
        output directory = {output_directory}
        seasons = {seasons}
        statistics = {statistics}
        epoch = {epoch}
        dataset to retrieve = {dataset_type}
        bands = {bands}
        PQA mask = {pqa_mask}
        interpolation = {interpolation}
        """.format(cells=self.cells, acq_min=self.acq_min, acq_max=self.acq_max,
                   epoch="{increment:d} / {duration:d}".format(increment=self.epoch.increment, duration=self.epoch.duration),
                   satellites=" ".join([ts.name for ts in self.satellites]),
                   output_directory=self.output_directory,
                   seasons=" ".join([s.name for s in self.seasons]),
                   statistics=" ".join([s.name for s in self.statistics]),
                   samples=self.samples and self.samples or "",
                   dataset_type=self.dataset_type.name, bands=" ".join([b.name for b in self.bands]),
                   pqa_mask=self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
                   interpolation=self.interpolation.name))

    def run(self):

        self.setup_arguments()
        self.process_arguments(self.parser.parse_args())
        self.log_arguments()

        self.go()

    def get_epochs(self):

        from dateutil.rrule import rrule, YEARLY
        from dateutil.relativedelta import relativedelta

        for dt in rrule(YEARLY, interval=self.epoch.increment, dtstart=self.acq_min, until=self.acq_max):
            acq_min = dt.date()
            acq_max = acq_min + relativedelta(years=self.epoch.duration, days=-1)

            acq_min = max(self.acq_min, acq_min)
            acq_max = min(self.acq_max, acq_max)

            yield acq_min, acq_max

    def get_random_locations(self):

        x_list = numpy.random.randint(0, 4000, self.samples)
        y_list = numpy.random.randint(0, 4000, self.samples)

        from itertools import izip

        for x, y in izip(x_list, y_list):
            yield x, y

    def go(self):

        from itertools import product

        ndv = get_dataset_type_ndv(self.dataset_type)

        for cell in self.cells:
            for season in self.seasons:
                for acq_min, acq_max in self.get_epochs():

                    if acq_min >= date(2015, 1, 1):
                        _log.debug("Skipping extra epoch {acq_min} to {acq_max}".format(acq_min=acq_min, acq_max=acq_max))
                        continue

                    _log.info("Processing cell ({x:03d},{y:04d}) - {season} - {acq_min} to {acq_max}".format(
                        x=cell.x, y=cell.y, season=season.name, acq_min=acq_min, acq_max=acq_max))

                    statistics_filename = self.get_statistics_filename(cell=cell, acq_min=acq_min, acq_max=acq_max, season=season)

                    _log.debug("Statistics file is %s", statistics_filename)

                    for x, y in self.get_random_locations():
                        _log.debug("\tChecking ({x:03d},{y:04d})".format(x=x, y=y))

                        calculated_statistics = read_pixel_statistics(statistics_filename, x=x, y=y)
                        # _log.info("calculated statistics = [%s]", calculated_statistics)

                        calculated_statistics_reshaped = dict()

                        for index, (band, statistic) in enumerate(product(self.bands, self.statistics), start=0):
                            _log.debug("%s - %s = %d", band.name, statistic.name, calculated_statistics[index])

                            if statistic not in calculated_statistics_reshaped:
                                calculated_statistics_reshaped[statistic] = dict()

                            calculated_statistics_reshaped[statistic][band] = calculated_statistics[index][0][0]

                        pixel_values = dict()

                        acq_dates = None

                        for band in self.bands:
                            acq_dates, pixel_values[band] = read_pixel_time_series(x=cell.x, y=cell.y,
                                                                                   satellites=self.satellites,
                                                                                   acq_min=acq_min, acq_max=acq_max,
                                                                                   season=season,
                                                                                   dataset_type=self.dataset_type,
                                                                                   mask_pqa_apply=self.mask_pqa_apply,
                                                                                   mask_pqa_mask=self.mask_pqa_mask,
                                                                                   band=band,
                                                                                   x_offset=x, y_offset=y)

                            _log.debug("band %s is %s", band.name, pixel_values[band])

                        _log.debug("acq dates are %s", acq_dates)

                        csv_filename = self.get_csv_filename(cell, acq_min, acq_max, season, x, y)
                        _log.debug("csv filename is %s", csv_filename)

                        with open(csv_filename, "wb") as csv_file:

                            csv_writer = csv.DictWriter(csv_file, delimiter=",", fieldnames=[""] + [b.name.replace("_", " ") for b in self.bands])

                            csv_writer.writeheader()

                            for statistic in self.statistics:
                                row = {"": statistic.name}

                                for band in self.bands:
                                    row[band.name.replace("_", " ")] = calculated_statistics_reshaped[statistic][band]

                                csv_writer.writerow(row)

                                row = {"": statistic.name + " CALCULATED"}

                                for band in self.bands:
                                    xxx = numpy.array(pixel_values[band])
                                    row[band.name.replace("_", " ")] = numpy.percentile(xxx[xxx != ndv], PERCENTILE[statistic], interpolation=self.interpolation.value)

                                csv_writer.writerow(row)

                            for index, d in enumerate(acq_dates, start=0):

                                row = {"": d}

                                for band in self.bands:
                                    row[band.name.replace("_", " ")] = pixel_values[band][index]

                                csv_writer.writerow(row)

    def get_statistics_filename(self, cell, acq_min, acq_max, season):

        season = SEASONS[season]

        season_start = "{month}{day:02d}".format(month=season[0][0].name[:3], day=season[0][1])
        season_end = "{month}{day:02d}".format(month=season[1][0].name[:3], day=season[1][1])

        return os.path.join(self.input_directory,
                            "ARG25_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{season_start}_{season_end}_STATS.tif".format(
                                x=cell.x, y=cell.y, acq_min=acq_min.strftime("%Y%m%d"),
                                acq_max=acq_max.strftime("%Y%m%d"),
                                season_start=season_start, season_end=season_end))

    def get_csv_filename(self, cell, acq_min, acq_max, season, x, y):

        season = SEASONS[season]

        season_start = "{month}{day:02d}".format(month=season[0][0].name[:3], day=season[0][1])
        season_end = "{month}{day:02d}".format(month=season[1][0].name[:3], day=season[1][1])

        return os.path.join(self.output_directory,
                            "ARG25_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{season_start}_{season_end}_{x_offset:04d}_{y_offset:04d}.csv".format(
                                x=cell.x, y=cell.y, acq_min=acq_min.strftime("%Y%m%d"),
                                acq_max=acq_max.strftime("%Y%m%d"),
                                season_start=season_start, season_end=season_end,
                                x_offset=x, y_offset=y))


def read_pixel_statistics(path, x, y):

    out = list()

    raster = gdal.Open(path, GA_ReadOnly)
    assert raster

    for b in range(1, raster.RasterCount+1):

        band = raster.GetRasterBand(b)
        assert band

        data = band.ReadAsArray(x, y, 1, 1)
        out.append(data)

        band.FlushCache()
        del band

    raster.FlushCache()
    del raster

    return out


def read_pixel_time_series(x, y, satellites, acq_min, acq_max, season, dataset_type,
                           mask_pqa_apply, mask_pqa_mask, band, x_offset, y_offset):

    ndv = get_dataset_type_ndv(dataset_type)

    tiles = get_tiles(x, y, satellites, acq_min, acq_max, season, dataset_type, mask_pqa_apply)

    stack = get_dataset_data_stack(tiles, dataset_type, band.name, ndv=ndv,
                                   x=x_offset, y=y_offset,
                                   x_size=1, y_size=1,
                                   mask_pqa_apply=mask_pqa_apply, mask_pqa_mask=mask_pqa_mask)

    return [tile.end_datetime for tile in tiles], [s[0][0] for s in stack]


def get_tiles(x, y, satellites, acq_min, acq_max, season, dataset_type, mask_pqa_apply):

    acq_min, acq_max, criteria = build_season_date_criteria(acq_min, acq_max, season,
                                                            seasons=SEASONS, extend=True)

    dataset_types = [dataset_type]

    if mask_pqa_apply:
        dataset_types.append(DatasetType.PQ25)

    tiles = list_tiles_as_list(x=[x], y=[y], satellites=satellites,
                                    acq_min=acq_min, acq_max=acq_max,
                                    dataset_types=dataset_types, include=criteria)

    return tiles


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    Arg25BandStatisticsValidator().run()
