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


import os
import argparse
import gdal
import gdalconst
import numpy
import osr
import sys
from datacube.api import PERCENTILE, Statistic, OutputFormat, writeable_dir, output_format_arg, Season
from datacube.api.model import Pq25Bands, Ls8Arg25Bands
from datacube.api.query import connect_to_db
from datacube.api.tool.band_statistics_arg25_validator import cell_arg
from datacube.api.utils import consolidate_masks, PqaMask, apply_mask, calculate_stack_statistic_percentile
from datacube.api.utils import PercentileInterpolation, NDV

__author__ = "Simon Oldfield"

import logging

_log = logging.getLogger()


def read_data(path, bands, x=0, y=0, x_size=None, y_size=None):
    """
    Return one or more bands from a dataset

    :param dataset: The dataset from which to read the band
    :param bands: A list of bands to read from the dataset
    :param x:
    :param y:
    :param x_size:
    :param y_size:
    :return: dictionary of band/data as numpy array
    """

    out = dict()

    raster = gdal.Open(path, gdalconst.GA_ReadOnly)
    assert raster

    if not x_size:
        x_size = raster.RasterXSize

    if not y_size:
        y_size = raster.RasterYSize

    for b in bands:
        band = raster.GetRasterBand(b.value)
        assert band

        data = band.ReadAsArray(x, y, x_size, y_size)
        out[b] = data

        band.FlushCache()
        del band

    raster.FlushCache()
    del raster

    return out


class CleanPixelStatisticsCellFixer(object):

    def __init__(self):

        self.parser = argparse.ArgumentParser(prog=sys.argv[0], description="Clean Pixel Statistics Cell Fixer")

        self.cell = None
        self.band = None
        self.output_directory = None
        self.output_format = None
        self.chunk_size_x = None
        self.chunk_size_y = None

    def setup_arguments(self):

        self.parser.add_argument("--cell", help="The cell(s) to validate", action="store",
                                 required=True, dest="cell", type=cell_arg, # nargs="+",
                                 metavar="x,y")

        self.parser.add_argument("--band", help="The band to retrieve", action="store", dest="band", type=str)

        self.parser.add_argument("--output-directory", help="Output directory", action="store", dest="output_directory",
                                 type=writeable_dir, required=True)

        self.parser.add_argument("--output-format", help="The format of the output dataset",
                                 action="store",
                                 dest="output_format",
                                 type=output_format_arg,
                                 choices=OutputFormat, default=OutputFormat.GEOTIFF,
                                 metavar=" ".join([f.name for f in OutputFormat]))

        self.parser.add_argument("--chunk-size-x", help="X chunk size", action="store", dest="chunk_size_x", type=int,
                                 choices=range(1, 4000 + 1), required=True)
        self.parser.add_argument("--chunk-size-y", help="Y chunk size", action="store", dest="chunk_size_y", type=int,
                                 choices=range(1, 4000 + 1), required=True)

    def process_arguments(self, args):

        self.cell = args.cell
        self.band = Ls8Arg25Bands[args.band]
        self.output_directory = args.output_directory
        self.output_format = args.output_format
        self.chunk_size_x = args.chunk_size_x
        self.chunk_size_y = args.chunk_size_y

    def log_arguments(self):

        _log.info("""
        cell = {cell}
        band to retrieve = {band}
        output directory = {output}
        output format = {output_format}
        chunk size = {chunk_size_x} x {chunk_size_y}
        """.format(cell=self.cell,
                   band=self.band.name,
                   output=self.output_directory,
                   output_format=self.output_format.name,
                   chunk_size_x=self.chunk_size_x, chunk_size_y=self.chunk_size_y))

    def run(self):

        self.setup_arguments()
        self.process_arguments(self.parser.parse_args())
        self.log_arguments()

        self.go()

    def get_chunks(self):

        from itertools import product

        for xmin, ymin in product(range(0, 4000, self.chunk_size_x), range(0, 4000, self.chunk_size_y)):
            xmax = (xmin + self.chunk_size_x - 1) <= 3999 and (xmin + self.chunk_size_x - 1) or 3999
            xsize = xmax - xmin + 1

            ymax = (ymin + self.chunk_size_y - 1) <= 3999 and (ymin + self.chunk_size_y - 1) or 3999
            ysize = ymax - ymin + 1

            yield xmin, xmax, xsize, ymin, ymax, ysize

    def go(self):

        arg_datasets = []
        pqa_datasets = []

        for nbar in self.list_nbar_datasets():

            pqa = nbar.replace("_NBAR_", "_PQA_")

            _log.info("NBAR=[%s] PQA=[%s]", os.path.basename(nbar), os.path.basename(pqa))

            if os.path.isfile(nbar) and os.path.isfile(pqa):
                arg_datasets.append(nbar)
                pqa_datasets.append(pqa)

            else:
                _log.info("\t *** SKIPPING NBAR=[%s] PQA=[%s]", os.path.basename(nbar), os.path.basename(pqa))

        pqa_mask = consolidate_masks([PqaMask.PQ_MASK_SATURATION, PqaMask.PQ_MASK_CONTIGUITY, PqaMask.PQ_MASK_CLOUD])

        stack = numpy.empty((len(arg_datasets), 4000, 4000), dtype=numpy.int16)

        for index, (nbar, pqa) in enumerate(zip(arg_datasets, pqa_datasets), start=0):

            _log.info("Stacking NBAR [%s] with PQA [%s]", os.path.basename(nbar), os.path.basename(pqa))

            # Create PQA mask

            data = read_data(pqa, [Pq25Bands.PQ])[Pq25Bands.PQ]

            mask = numpy.ma.make_mask_none(numpy.shape(data))
            mask = numpy.ma.mask_or(mask, numpy.ma.masked_where(data & pqa_mask != pqa_mask, data).mask)

            del data

            # Read the NBAR data

            data = read_data(nbar, [Ls8Arg25Bands.RED])[Ls8Arg25Bands.RED]

            # Apply PQA mask

            data = apply_mask(data, mask, ndv=-999)

            # Stack...

            stack[index] = data

            del data

        _log.info("Got stack of shape [%s]", numpy.shape(stack))

        median = calculate_stack_statistic_percentile(stack=stack, percentile=PERCENTILE[Statistic.PERCENTILE_50],
                                                      ndv=NDV, interpolation=PercentileInterpolation.LINEAR)

        del stack

        DRIVERS = {
            OutputFormat.GEOTIFF: "GTiff",
            OutputFormat.ENVI: "ENVI"
        }

        driver = gdal.GetDriverByName(DRIVERS[self.output_format])
        assert driver

        filename = self.output()

        OPTIONS = {
            OutputFormat.GEOTIFF: ["TILED=YES", "BIGTIFF=YES", "COMPRESS=LZW", "INTERLEAVE=BAND"],
            OutputFormat.ENVI: ["INTERLEAVE=BSQ"]
        }

        raster = driver.Create(filename, 4000, 4000, 1, gdal.GDT_Int16, options=OPTIONS[self.output_format])
        assert raster

        transform = (self.cell.x, 0.00025, 0.0, self.cell.y+1, 0.0, -0.00025)

        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)

        projection = srs.ExportToWkt()

        raster = driver.Create(filename, 4000, 4000, 1, gdal.GDT_Int16, options=["INTERLEAVE=BSQ"])
        assert raster

        # NOTE: could do this without the metadata!!
        raster.SetGeoTransform(transform)
        raster.SetProjection(projection)

        # raster.SetMetadata(self.generate_raster_metadata())

        band = raster.GetRasterBand(1)
        assert band

        # TODO
        band.SetNoDataValue(-999)
        band.SetDescription("{band} - {percentile} ({interpolation})".format(band=self.band.name,
                                                                             percentile=Statistic.PERCENTILE_50.name,
                                                                             interpolation=PercentileInterpolation.LINEAR))

        band.WriteArray(median)

        del median

        band.ComputeStatistics(True)
        band.FlushCache()

        del band

        raster.FlushCache()
        del raster

    def output(self):
        EXTENSION = {
            OutputFormat.GEOTIFF: "tif",
            OutputFormat.ENVI: "dat"
        }

        return os.path.join(self.dir, "LS8_{x:03}_{y:04d}_ARG25_{band}_{season}.{ext}".format(x=self.cell.x, y=self.cell.y, band=self.band.name, season=Season.APR_TO_SEP.name, ext=EXTENSION[self.output_format]))

    def list_nbar_datasets(self):

        conn, cursor = None, None

        datasets = []

        try:
            # connect to database

            conn, cursor = connect_to_db()

            # # WINTER
            #
            # sql = """
            #     select tile_pathname from tile join dataset using (dataset_id) join acquisition using (acquisition_id)
            #     where
            #       x_index=%(x)s and y_index=%(y)s and satellite_id=3 and level_id=2 and
            #       (
            #         end_datetime::date between '2013-06-01'::date and '2013-08-31'::date or
            #         end_datetime::date between '2014-06-01'::date and '2014-08-31'::date or
            #         end_datetime::date between '2015-06-01'::date and '2015-08-31'::date) and
            #       tile_type_id=1 and tile_class_id in (1,3)
            # """

            # APR to SEP

            sql = """
                select tile_pathname from tile join dataset using (dataset_id) join acquisition using (acquisition_id)
                where
                  x_index=%(x)s and y_index=%(y)s and satellite_id=3 and level_id=2 and
                  (
                    end_datetime::date between '2013-04-01'::date and '2013-09-30'::date or
                    end_datetime::date between '2014-04-01'::date and '2014-09-30'::date or
                    end_datetime::date between '2015-04-01'::date and '2015-09-30'::date) and
                  tile_type_id=1 and tile_class_id in (1,3)
            """

            params = {"x": self.cell.x, "y": self.cell.y}

            _log.info(cursor.mogrify(sql, params))

            cursor.execute(sql, params)

            for record in cursor:
                _log.debug(record)
                datasets.append(record["tile_pathname"])

            return datasets

        except Exception as e:

            _log.error("Caught exception %s", e)
            conn.rollback()
            raise

        finally:

            conn = cursor = None


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    # main()

    CleanPixelStatisticsCellFixer().run()