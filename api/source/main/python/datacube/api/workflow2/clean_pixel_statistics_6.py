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

# This program was originally written by Simon Oldfield and modified by Biswajit Bala. 
# It supports six bands and three percentile values.
import argparse
import csv
import gdal
import logging
import luigi
import os
import osr
import sys
from datacube.api.model import Ls8Arg25Bands, Tile
from datacube.api import writeable_dir, satellite_arg, Satellite, dataset_type_arg, DatasetType, Season, season_arg
from datacube.api import statistic_arg
from datacube.api import output_format_arg, Statistic, pqa_mask_arg, PqaMask, wofs_mask_arg, WofsMask
from datacube.api import OutputFormat, PERCENTILE
from datacube.api import parse_date_min, parse_date_max
from datacube.api.tool.band_statistics_arg25_validator import cell_arg
from datacube.api.utils import get_dataset_type_ndv, get_dataset_data_stack, log_mem
from datacube.api.utils import calculate_stack_statistic_count_observed
from datacube.api.utils import PercentileInterpolation, format_date, calculate_stack_statistic_percentile
from datacube.api.workflow2 import Task, FileDependencyTask


__author__ = "Simon Oldfield"

_log = logging.getLogger()

# Needs to run prepare.csv.py program before running this program
#This program calculates interpolated "linear" pixel tif files of 1001 for each percentile and each bands
class CleanPixelStatistics(object):

    def __init__(self, name="Clean Pixel Statistics"):

        self.name = name

        self.parser = argparse.ArgumentParser(prog=sys.argv[0], description=self.name)

        self.cells = None

        self.acq_min = None
        self.acq_max = None

        self.seasons = None

        self.satellites = None

        self.output_directory = None

        self.mask_pqa_apply = None
        self.mask_pqa_mask = None

        self.mask_wofs_apply = None
        self.mask_wofs_mask = None

        self.include_ls7_slc_off = None
        self.include_ls8_pre_wrs2 = None

        self.dataset_type = None

        self.output_format = None

        self.statistics = None

        self.local_scheduler = None
        self.workers = None

    def setup_arguments(self):

        self.parser.add_argument("--cell", help="The cell(s) to validate", action="store",
                                 required=True, dest="cell", type=cell_arg, nargs="+",
                                 metavar="(x,y) [(x,y) (x,y) ...]")

        self.parser.add_argument("--output-directory", help="output directory", action="store", dest="output_directory",
                                 type=writeable_dir, required=True)

        self.parser.add_argument("--acq-min", help="Acquisition Date", action="store", dest="acq_min", type=str,
                                 default="1985")

        self.parser.add_argument("--acq-max", help="Acquisition Date", action="store", dest="acq_max", type=str,
                                 default="2014")

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

        self.parser.add_argument("--mask-wofs-apply", help="Apply WOFS mask", action="store_true",
                                 dest="mask_wofs_apply",
                                 default=False)

        self.parser.add_argument("--mask-wofs-mask", help="The WOFS mask to apply", action="store", dest="mask_wofs_mask",
                                 type=wofs_mask_arg, nargs="+", choices=WofsMask,
                                 default=[WofsMask.WET],
                                 metavar=" ".join([ts.name for ts in WofsMask]))

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

        self.parser.add_argument("--season", help="The seasons for which to produce statistics", action="store",
                                 default=Season,  # required=True,
                                 dest="season", type=season_arg, nargs="+",
                                 metavar=" ".join([s.name for s in Season]))

        self.parser.add_argument("--no-ls7-slc-off",
                                 help="Exclude LS7 SLC OFF datasets",
                                 action="store_false", dest="include_ls7_slc_off", default=True)

        self.parser.add_argument("--no-ls8-pre-wrs2",
                                 help="Exclude LS8 PRE-WRS2 datasets",
                                 action="store_false", dest="include_ls8_pre_wrs2", default=True)

        self.parser.add_argument("--output-format", help="The format of the output dataset",
                                 action="store",
                                 dest="output_format",
                                 type=output_format_arg,
                                 choices=OutputFormat, default=OutputFormat.GEOTIFF,
                                 metavar=" ".join([f.name for f in OutputFormat]))

        self.parser.add_argument("--statistic", help="The statistic to produce", action="store",
                                 default=Statistic,  # required=True,
                                 dest="statistic", type=statistic_arg,  nargs="+",
                                 metavar=" ".join([s.name for s in Statistic]))
        #self.parser.add_argument("--statistic", help="The statistic to produce", action="store",
        #                         default=[Statistic.PERCENTILE_50,],  # required=True,
        #                         dest="statistic", type=statistic_arg,  nargs="+",
        #                         choices=[Statistic.PERCENTILE_50, Statistic.COUNT_OBSERVED],
        #                         metavar=" ".join([s.name for s in Statistic]))

        self.parser.add_argument("--local-scheduler", help="Use local luigi scheduler rather than MPI",
                                 action="store_true",
                                 dest="local_scheduler", default=True)

        self.parser.add_argument("--workers", help="Number of worker tasks", action="store", dest="workers", type=int,
                                 default=16)

    def process_arguments(self, args):

        self.cells = args.cell

        self.output_directory = args.output_directory

        self.acq_min = parse_date_min(args.acq_min)
        self.acq_max = parse_date_max(args.acq_max)

        self.satellites = args.satellites

        self.seasons = args.season

        self.mask_pqa_apply = args.mask_pqa_apply
        self.mask_pqa_mask = args.mask_pqa_mask

        self.mask_wofs_apply = args.mask_wofs_apply
        self.mask_wofs_mask = args.mask_wofs_mask

        self.include_ls7_slc_off = args.include_ls7_slc_off
        self.include_ls8_pre_wrs2 = args.include_ls8_pre_wrs2

        self.local_scheduler = args.local_scheduler
        self.workers = args.workers

        _log.setLevel(args.log_level)

        self.dataset_type = args.dataset_type

        self.output_format = args.output_format

        self.statistics = args.statistic

    def log_arguments(self):

        _log.info("""
        cells = {cells}
        """.format(cells=self.cells))

        _log.info("""
        acq = {acq_min} to {acq_max}
        satellites = {satellites}
        output directory = {output_directory}
        output format = {output_format}
        PQA mask = {pqa_mask}
        WOFS mask = {wofs_mask}
        statistics = {statistics}
        local scheduler = {local_scheduler}
        workers = {workers}
        """.format(acq_min=self.acq_min, acq_max=self.acq_max,
                   satellites=" ".join([ts.name for ts in self.satellites]),
                   output_directory=self.output_directory,
                   output_format=self.output_format.name,
                   pqa_mask=self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
                   wofs_mask=self.mask_wofs_apply and " ".join([mask.name for mask in self.mask_wofs_mask]) or "",
                   statistics=" ".join([s.name for s in self.statistics]),
                   local_scheduler=self.local_scheduler, workers=self.workers))

        _log.info("""
        datasets to retrieve = {dataset_type}
        seasons = {seasons}
        """.format(dataset_type=self.dataset_type.name,
                   seasons=" ".join([s.name for s in self.seasons])))

    def create_tasks(self):

        # Get default six bands as input for creating interpolated images.
        bands = [Ls8Arg25Bands.BLUE, Ls8Arg25Bands.GREEN, Ls8Arg25Bands.RED, Ls8Arg25Bands.NEAR_INFRARED, Ls8Arg25Bands.SHORT_WAVE_INFRARED_1, Ls8Arg25Bands.SHORT_WAVE_INFRARED_2]
        #bands = [Ls8Arg25Bands.BLUE, Ls8Arg25Bands.GREEN, Ls8Arg25Bands.RED]
        # bands = [Ls8Arg25Bands.RED]

        from itertools import product

        for (x,y), season,statistic, band in product(self.cells, self.seasons,self.statistics, bands):

            _log.info("x={x:03d} y={y:04d} season={season} statistic={statistic} band={band}".format(x=x, y=y, season=season, statistic=statistic, band=band))
            yield self.create_task(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max, dataset_type=self.dataset_type, band=band, season=season, statistic=statistic)

    def create_task(self, x, y, acq_min, acq_max, dataset_type, band, season, statistic):
        _log.info("Creating task for x={x:03d} y={y:04d} acq={acq_min} to {acq_max} dataset={dataset_type} band={band} season={season} statistic={statistic}".format(x=x, y=y, acq_min=acq_min, acq_max=acq_max, dataset_type=dataset_type, band=band, season=season, statistic=statistic))
        return CleanPixelStatisticTask(x=x, y=y, satellites=self.satellites, acq_min=acq_min, acq_max=acq_max,
                                       dataset_type=dataset_type, band=band, season=season,
                                       mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                       mask_wofs_apply=self.mask_wofs_apply, mask_wofs_mask=self.mask_wofs_mask,
                                       statistic=statistic, output_directory=self.output_directory, output_format=self.output_format)

    def run(self):

        self.setup_arguments()
        self.process_arguments(self.parser.parse_args())
        self.log_arguments()

        luigi.build(self.create_tasks(), local_scheduler=self.local_scheduler, workers=self.workers)


class CleanPixelStatisticTask(Task):

    x = luigi.IntParameter()
    y = luigi.IntParameter()

    satellites = luigi.Parameter(is_list=True)

    acq_min = luigi.DateParameter()
    acq_max = luigi.DateParameter()

    season = luigi.Parameter()

    dataset_type = luigi.Parameter()
    band = luigi.Parameter()

    mask_pqa_apply = luigi.BooleanParameter()
    mask_pqa_mask = luigi.Parameter(is_list=True)
    
    mask_wofs_apply = luigi.BooleanParameter()
    mask_wofs_mask = luigi.Parameter(is_list=True)

    statistic = luigi.Parameter()

    output_directory = luigi.Parameter()
    output_format = luigi.Parameter()

    def output(self):
        EXTENSION = {
            OutputFormat.GEOTIFF: "tif",
            OutputFormat.ENVI: "dat"
        }
	
        return luigi.LocalTarget(os.path.join(self.output_directory, "LS8_{x:03}_{y:04d}_ARG25_{statistic}_{band}_{season}.{ext}".format(x=self.x, y=self.y,statistic=self.statistic.name,
        #return luigi.LocalTarget(os.path.join(self.output_directory, "LS8_{x:03}_{y:04d}_ARG25_{bd}_{band}_{season}.{ext}".format(x=self.x, y=self.y,  bd=self.band.name,
	band=self.band.name, 
	season=self.season.name, ext=EXTENSION[self.output_format])))

    def requires(self):
        return FileDependencyTask(path=os.path.join(self.output_directory, "input/datasets_{x:03}_{y:04d}_{season}.csv".format(x=self.x, y=self.y, season=self.season.name)))

    def run(self):
        # Get the tiles from the CSV

        tiles = []

        with open(self.input().path, "rb") as f:
            reader = csv.DictReader(f)
            for record in reader:
                _log.info("Found CSV record [%s]", record)
                tiles.append(Tile.from_csv_record(record))

        _log.info("Found %d tiles", len(tiles))

        # Calculate the statistic and store to output file

        ndv = get_dataset_type_ndv(self.dataset_type)

        DRIVERS = {
            OutputFormat.GEOTIFF: "GTiff",
            OutputFormat.ENVI: "ENVI"
        }

        driver = gdal.GetDriverByName(DRIVERS[self.output_format])
        assert driver

        transform = (self.x, 0.00025, 0.0, self.y+1, 0.0, -0.00025)

        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)

        projection = srs.ExportToWkt()

        from itertools import product

        bands = [self.band]
        statistics = [self.statistic]
        interpolation = PercentileInterpolation.LINEAR

        for index, (b, statistic) in enumerate(product(bands, statistics), start=1):

            _log.info("Doing band [%s] statistic  which is band number [%s]", b,  index)

            stack = get_dataset_data_stack(tiles, self.dataset_type, b.name, ndv=ndv,
                                           mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask)

            _log.info("Got stack of length [%d]", len(stack))

            #if len(stack) == 0:
            #    return

            # TODO
	    stack_stat=0

            log_mem("Before {p}".format(p=statistic))
	    # support only four statstical measures to get the product 
	    if statistic == Statistic.PERCENTILE_10 or statistic == Statistic.PERCENTILE_50 or statistic == Statistic.PERCENTILE_90:
            	stack_stat = calculate_stack_statistic_percentile(stack=stack, percentile=PERCENTILE[statistic],
                                                                  ndv=ndv, interpolation=interpolation)
	    elif statistic == Statistic.COUNT_OBSERVED:
                stack_stat = calculate_stack_statistic_count_observed(stack=stack, ndv=ndv)
	    	
       	    _log.info("Doing statistics for  [%s] statistic ", statistic)	


            del stack

	    log_mem("After {statistic}".format(statistic=statistic.name))
	
            filename = self.output().path

            OPTIONS = {
                OutputFormat.GEOTIFF: ["TILED=YES", "BIGTIFF=YES", "COMPRESS=LZW", "INTERLEAVE=BAND"],
                OutputFormat.ENVI: ["INTERLEAVE=BSQ"]
            }

            raster = driver.Create(filename, 4000, 4000, 1, gdal.GDT_Int16, options=OPTIONS[self.output_format])
            assert raster

            # NOTE: could do this without the metadata!!
            raster.SetGeoTransform(transform)
            raster.SetProjection(projection)

            raster.SetMetadata(self.generate_raster_metadata())

            band = raster.GetRasterBand(index)
            assert band

            # TODO
            band.SetNoDataValue(ndv)
            band.SetDescription("{band} - {stat}".format(band=b, stat=statistic.name))

            band.WriteArray(stack_stat)

            band.ComputeStatistics(True)
            band.FlushCache()

            del band

            raster.FlushCache()
            del raster

            del stack_stat

            _log.info("Done band [%s] x [%03d] y=[%04d]", b, self.x, self.y)

    def generate_raster_metadata(self):
        return {
            "X_INDEX": "{x:03d}".format(x=self.x),
            "Y_INDEX": "{y:04d}".format(y=self.y),
            "DATASET_TYPE": self.dataset_type.name,
            "ACQUISITION_DATE": "{acq_min} to {acq_max}".format(acq_min=format_date(self.acq_min), acq_max=format_date(self.acq_max)),
            "SATELLITES": " ".join([s.name for s in self.satellites]),
            "PIXEL_QUALITY_FILTER": self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
            "WATER_FILTER": self.mask_wofs_apply and " ".join([mask.name for mask in self.mask_wofs_mask]) or ""
        }

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    CleanPixelStatistics().run()
