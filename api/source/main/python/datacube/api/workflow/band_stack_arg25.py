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
import os
import sys
from datacube.api import parse_date_min, parse_date_max, Month, PqaMask, writeable_dir
from datacube.api import satellite_arg, pqa_mask_arg, dataset_type_arg, season_arg, output_format_arg, OutputFormat
from datacube.api.query import Season, list_cells_as_generator, build_season_date_criteria, list_tiles_as_list
from datacube.api.model import Satellite, DatasetType, Ls57Arg25Bands
from datacube.api.utils import get_dataset_type_ndv, get_dataset_metadata
from datacube.api.utils import get_dataset_type_datatype, get_mask_pqa, get_dataset_data_masked, format_date
from datacube.api.workflow import Task


_log = logging.getLogger()


SEASONS = {
    Season.SUMMER: ((Month.NOVEMBER, 17), (Month.APRIL, 25)),
    Season.AUTUMN: ((Month.FEBRUARY, 16), (Month.JULY, 25)),
    Season.WINTER: ((Month.MAY, 17), (Month.OCTOBER, 25)),
    Season.SPRING: ((Month.AUGUST, 17), (Month.JANUARY, 25))
}


def ls57_arg_band_arg(s):
    if s in [t.name for t in Ls57Arg25Bands]:
        return Ls57Arg25Bands[s]
    raise argparse.ArgumentTypeError("{0} is not a supported LS57 ARG25 band".format(s))


class Arg25BandStackWorkflow(object):

    def __init__(self, name="Arg25 Band Stack Workflow"):

        # Workflow.__init__(self, name="Arg25 Band Stack Workflow")

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

        self.output_format = None

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

        self.parser.add_argument("--epoch", help="Size of year chunks (e.g. 5 means do in 5 chunks of 5 years)",
                                 action="store", dest="epoch", type=int, default=5)

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

        self.parser.add_argument("--season", help="The seasons for which to produce statistics", action="store",
                                 default=Season,  # required=True,
                                 dest="season", type=season_arg, nargs="+",
                                 metavar=" ".join([s.name for s in Season]))

        self.parser.add_argument("--output-format", help="The format of the output dataset",
                                 action="store",
                                 dest="output_format",
                                 type=output_format_arg,
                                 choices=OutputFormat, default=OutputFormat.GEOTIFF,
                                 metavar=" ".join([f.name for f in OutputFormat]))

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

        self.epoch = args.epoch

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

        self.output_format = args.output_format

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
                   epoch=self.epoch and self.epoch or "",
                   satellites=" ".join([ts.name for ts in self.satellites]),
                   output_directory=self.output_directory,
                   pqa_mask=self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
                   local_scheduler=self.local_scheduler, workers=self.workers))

        _log.info("""
        dataset to retrieve = {dataset_type}
        bands = {bands}
        seasons = {seasons}
        """.format(dataset_type=self.dataset_type.name, bands=" ".join([b.name for b in self.bands]),
                   seasons=" ".join([s.name for s in self.seasons])))

        _log.info("""
        output format = {output_format}
        """.format(output_format=self.output_format.name))

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
                yield self.create_task(x=cell.x, y=cell.y, acq_min=acq_min, acq_max=acq_max, season=season)

    def create_task(self, x, y, acq_min, acq_max, season):
        _log.info("Creating task for %s %s %s %s %s", x, y, acq_min, acq_max, season)
        return Arg25BandStackTask(x=x, y=y,
                                  acq_min=acq_min, acq_max=acq_max, season=season,
                                  satellites=self.satellites,
                                  dataset_type=self.dataset_type, bands=self.bands,
                                  mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask, output_format=self.output_format)

    def run(self):

        self.setup_arguments()
        self.process_arguments(self.parser.parse_args())
        self.log_arguments()

        luigi.build(self.create_tasks(), local_scheduler=self.local_scheduler, workers=self.workers)


class Arg25BandStackTask(Task):

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

    output_format = luigi.Parameter()

    def requires(self):

        _log.info("bands = %s", self.bands)

        for band in self.bands:
            yield self.create_band_task(band)

    def create_band_task(self, band):

        yield Arg25BandStackBandTask(x=self.x, y=self.y,
                                     acq_min=self.acq_min, acq_max=self.acq_max, season=self.season,
                                     satellites=self.satellites, dataset_type=self.dataset_type, band=band,
                                     mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask, output_format=self.output_format)


class Arg25BandStackBandTask(Task):

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

    output_format = luigi.Parameter()

    def output(self):
        acq_min = self.acq_min.strftime("%Y%m%d")
        acq_max = self.acq_max.strftime("%Y%m%d")

        season = SEASONS[self.season]
        season_start = "{month}{day:02d}".format(month=season[0][0].name[:3], day=season[0][1])
        season_end = "{month}{day:02d}".format(month=season[1][0].name[:3], day=season[1][1])

        return luigi.LocalTarget(
            "ARG25_{x:03d}_{y:04d}_{acq_min}_{acq_max}_{season_start}_{season_end}_{band}_STACK.tif".format(x=self.x,
                                                                                                            y=self.y,
                                                                                                            acq_min=acq_min,
                                                                                                            acq_max=acq_max,
                                                                                                            season_start=season_start,
                                                                                                            season_end=season_end,
                                                                                                            band=self.band.name
                                                                                                            ))

    def run(self):

        _log.info("Creating stack for band [%s]", self.band.name)

        data_type = get_dataset_type_datatype(self.dataset_type)
        ndv = get_dataset_type_ndv(self.dataset_type)
        metadata = None
        driver = None
        raster = None

        acq_min, acq_max, criteria = build_season_date_criteria(self.acq_min, self.acq_max, self.season,
                                                                seasons=SEASONS, extend=True)

        _log.info("\tacq %s to %s criteria is %s", acq_min, acq_max, criteria)

        dataset_types = [self.dataset_type]

        if self.mask_pqa_apply:
            dataset_types.append(DatasetType.PQ25)

        tiles = list_tiles_as_list(x=[self.x], y=[self.y], satellites=self.satellites,
                                   acq_min=acq_min, acq_max=acq_max,
                                   dataset_types=dataset_types, include=criteria)

        for index, tile in enumerate(tiles, start=1):

            dataset = tile.datasets[self.dataset_type]
            assert dataset

            # band = dataset.bands[self.band]
            # assert band
            band = self.band

            pqa = (self.mask_pqa_apply and DatasetType.PQ25 in tile.datasets) and tile.datasets[DatasetType.PQ25] or None

            if self.dataset_type not in tile.datasets:
                _log.debug("No [%s] dataset present for [%s] - skipping", self.dataset_type.name, tile.end_datetime)
                continue

            filename = self.output().path

            if not metadata:
                metadata = get_dataset_metadata(dataset)
                assert metadata

            if not driver:

                if self.output_format == OutputFormat.GEOTIFF:
                    driver = gdal.GetDriverByName("GTiff")

                elif self.output_format == OutputFormat.ENVI:
                    driver = gdal.GetDriverByName("ENVI")

                assert driver

            if not raster:

                if self.output_format == OutputFormat.GEOTIFF:
                    raster = driver.Create(filename, metadata.shape[0], metadata.shape[1], len(tiles), data_type, options=["BIGTIFF=YES", "INTERLEAVE=BAND"])

                elif self.output_format == OutputFormat.ENVI:
                    raster = driver.Create(filename, metadata.shape[0], metadata.shape[1], len(tiles), data_type, options=["INTERLEAVE=BSQ"])

                assert raster

                # NOTE: could do this without the metadata!!
                raster.SetGeoTransform(metadata.transform)
                raster.SetProjection(metadata.projection)

            raster.SetMetadata(self.generate_raster_metadata())

            mask = None

            if pqa:
                mask = get_mask_pqa(pqa, self.mask_pqa_mask, mask=mask)

            _log.info("Stacking [%s] band data from [%s] with PQA [%s] and PQA mask [%s] to [%s]",
                      band.name, dataset.path,
                      pqa and pqa.path or "", pqa and self.mask_pqa_mask or "",
                      filename)

            data = get_dataset_data_masked(dataset, mask=mask, ndv=ndv)

            _log.debug("data is [%s]", data)

            stack_band = raster.GetRasterBand(index)

            stack_band.SetDescription(os.path.basename(dataset.path))
            stack_band.SetNoDataValue(ndv)
            stack_band.WriteArray(data[band])
            stack_band.ComputeStatistics(True)
            stack_band.SetMetadata({"ACQ_DATE": format_date(tile.end_datetime), "SATELLITE": dataset.satellite.name})

            stack_band.FlushCache()
            del stack_band

        if raster:
            raster.FlushCache()
            del raster
            raster = None

    def generate_raster_metadata(self):
        return {
            "X_INDEX": "{x:03d}".format(x=self.x),
            "Y_INDEX": "{y:04d}".format(y=self.y),
            "DATASET_TYPE": "{dataset_type} STACK".format(dataset_type=self.dataset_type.name),
            "ACQUISITION_DATE": "{acq_min} to {acq_max}".format(acq_min=format_date(self.acq_min), acq_max=format_date(self.acq_max)),
            "SEASON": self.season.name,
            "SATELLITES": " ".join([s.name for s in self.satellites]),
            "PIXEL_QUALITY_FILTER": self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or ""
        }


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    Arg25BandStackWorkflow().run()
